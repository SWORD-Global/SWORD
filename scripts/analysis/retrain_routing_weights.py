"""
Retrain routing weights (LogReg) with effective_slope.

Reads junction labels from backwater QC, pulls reach attributes from
v17c DuckDB, constructs pairwise log1p-difference features, and fits
a LogisticRegression.  Compares old weights (MERIT slope) vs new
weights (SWOT-preferred slope).

Usage:
    uv run python scripts/analysis/retrain_routing_weights.py
"""

import math
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import GroupKFold

DB_PATH = Path("data/duckdb/sword_v17c.duckdb")
LABELS_PATH = Path("data/backwater_junction_labels_v003.parquet")

REGION_MAP = {
    "af": "AF",
    "eu": "EU",
    "as": "AS",
    "oc": "OC",
    "sa": "SA",
    "na": "NA",
}

OLD_WEIGHTS = {
    "effective_width": 1.972,
    "facc": 0.227,
    "slope": -0.228,
    "pathlen": 0.234,
    "stream_order": 0.288,
}


def load_reach_attrs(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Load reach attributes needed for routing features."""
    return con.execute("""
        SELECT
            reach_id,
            region,
            width,
            width_obs_p50,
            n_obs,
            facc,
            slope,
            slope_obs_p50,
            slope_obs_reliable,
            slope_obs_n,
            stream_order,
            pathlen_hw,
            reach_length
        FROM reaches
    """).fetchdf()


def effective_width(row: pd.Series, min_obs: int = 5) -> float:
    n = row.get("n_obs", 0)
    if pd.isna(n):
        n = 0
    if n >= min_obs:
        w = row.get("width_obs_p50")
        if w is not None and not pd.isna(w) and w > 0:
            return w
    w = row.get("width", 0)
    if pd.isna(w):
        return 0.0
    return float(w) if w else 0.0


def effective_slope(row: pd.Series, min_obs: int = 5) -> float:
    reliable = row.get("slope_obs_reliable", 0)
    if pd.isna(reliable):
        reliable = 0
    slope_n = row.get("slope_obs_n", 0)
    if pd.isna(slope_n):
        slope_n = 0
    if reliable == 1 and slope_n >= min_obs:
        s = row.get("slope_obs_p50")
        if s is not None and not pd.isna(s):
            return float(s)
    s = row.get("slope", 0)
    if pd.isna(s):
        return 0.0
    return float(s) if s else 0.0


def build_pairs(junctions: pd.DataFrame, reach_df: pd.DataFrame, con) -> pd.DataFrame:
    """Build pairwise training data from junction labels.

    At each junction, the human_branch is the correct choice. We pair it
    against every other candidate at that junction (successors of junction_reach_id
    from topology).
    """
    # Index reach attrs by reach_id
    reach_idx = reach_df.set_index("reach_id")

    # Precompute effective width/slope for all reaches
    ew_map = {}
    es_map = {}
    merit_slope_map = {}
    for rid, row in reach_idx.iterrows():
        ew_map[rid] = effective_width(row)
        es_map[rid] = effective_slope(row)
        s = row.get("slope", 0)
        merit_slope_map[rid] = float(s) if not pd.isna(s) and s else 0.0

    # Get topology: for each junction_reach_id, find upstream neighbors
    junction_ids = junctions["junction_reach_id"].unique().tolist()

    # Query topology for upstream neighbors of junction reaches
    topo = con.execute("""
        SELECT reach_id, neighbor_reach_id
        FROM reach_topology
        WHERE direction = 'up'
        AND reach_id = ANY($1::BIGINT[])
    """, [junction_ids]).fetchdf()

    # Build upstream neighbor map
    upstream_map: dict[int, list[int]] = {}
    for _, row in topo.iterrows():
        rid = int(row["reach_id"])
        nid = int(row["neighbor_reach_id"])
        upstream_map.setdefault(rid, []).append(nid)

    pairs = []
    for _, jrow in junctions.iterrows():
        jid = int(jrow["junction_reach_id"])
        human = int(jrow["human_branch"])
        continent = jrow["continent"]

        candidates = upstream_map.get(jid, [])
        if human not in candidates:
            continue

        for alt in candidates:
            if alt == human:
                continue
            if alt not in reach_idx.index or human not in reach_idx.index:
                continue

            # Features for human (A) and alternative (B)
            def _feats(rid):
                ew = math.log1p(max(ew_map.get(rid, 0), 0))
                facc_val = reach_idx.loc[rid].get("facc", 0)
                facc_val = float(facc_val) if not pd.isna(facc_val) else 0.0
                lf = math.log1p(max(facc_val, 0))
                # effective slope (SWOT-preferred)
                es_val = max(es_map.get(rid, 0), 0)
                ls_eff = math.log1p(es_val)
                # MERIT slope (original)
                ms_val = max(merit_slope_map.get(rid, 0), 0)
                ls_merit = math.log1p(ms_val)
                pathlen = reach_idx.loc[rid].get("pathlen_hw", 0)
                pathlen = float(pathlen) if not pd.isna(pathlen) else 0.0
                lp = math.log1p(max(pathlen, 0))
                so = reach_idx.loc[rid].get("stream_order", 0)
                so = float(so) if not pd.isna(so) else 0.0
                return ew, lf, ls_eff, ls_merit, lp, so

            h_ew, h_lf, h_ls_eff, h_ls_merit, h_lp, h_so = _feats(human)
            a_ew, a_lf, a_ls_eff, a_ls_merit, a_lp, a_so = _feats(alt)

            pairs.append({
                "junction_id": jid,
                "continent": continent,
                # Pairwise diffs (human - alt): label=1 means A wins
                "d_ew": h_ew - a_ew,
                "d_facc": h_lf - a_lf,
                "d_slope_eff": h_ls_eff - a_ls_eff,
                "d_slope_merit": h_ls_merit - a_ls_merit,
                "d_pathlen": h_lp - a_lp,
                "d_so": h_so - a_so,
            })

    return pd.DataFrame(pairs)


def train_and_compare(pairs_df: pd.DataFrame):
    """Train LogReg with MERIT slope vs effective slope, compare."""
    feat_cols_merit = ["d_ew", "d_facc", "d_slope_merit", "d_pathlen", "d_so"]
    feat_cols_eff = ["d_ew", "d_facc", "d_slope_eff", "d_pathlen", "d_so"]
    feat_names = ["effective_width", "facc", "slope", "pathlen", "stream_order"]

    # Augment with reversed pairs: (human-alt, label=1) + (alt-human, label=0)
    # This is the standard Bradley-Terry pairwise formulation.
    fwd = pairs_df.copy()
    rev = pairs_df.copy()
    for col in ["d_ew", "d_facc", "d_slope_eff", "d_slope_merit", "d_pathlen", "d_so"]:
        rev[col] = -rev[col]
    augmented = pd.concat([fwd, rev], ignore_index=True)
    y = np.array([1] * len(fwd) + [0] * len(rev))
    # Groups: same junction for both fwd and rev (for CV)
    groups = augmented["junction_id"].values

    gkf = GroupKFold(n_splits=5)

    for label, feat_cols in [("MERIT slope", feat_cols_merit), ("effective slope", feat_cols_eff)]:
        X = augmented[feat_cols].values

        # Full fit
        clf = LogisticRegression(max_iter=1000, fit_intercept=False)
        clf.fit(X, y)
        coefs = clf.coef_[0]

        # Leave-junction-out CV
        cv_accs = []
        for train_idx, test_idx in gkf.split(X, y, groups):
            clf_cv = LogisticRegression(max_iter=1000, fit_intercept=False)
            clf_cv.fit(X[train_idx], y[train_idx])
            cv_accs.append(clf_cv.score(X[test_idx], y[test_idx]))

        cv_mean = np.mean(cv_accs)
        cv_std = np.std(cv_accs)
        train_acc = clf.score(X, y)

        print(f"\n{'=' * 60}")
        print(f"Model: {label}")
        print(f"{'=' * 60}")
        print(f"Training accuracy: {train_acc:.1%}")
        print(f"Leave-junction-out CV: {cv_mean:.1%} ± {cv_std:.1%}")
        print(f"\nWeights:")
        total = sum(abs(c) for c in coefs)
        for name, c in zip(feat_names, coefs):
            print(f"  {name:20s}: {c:+.3f}  ({abs(c) / total:5.1%})")

        print(f"\nAs ROUTING_WEIGHTS dict:")
        print("ROUTING_WEIGHTS = {")
        for name, c in zip(feat_names, coefs):
            print(f'    "{name}": {c:.3f},')
        print("}")

    # Direct comparison
    print(f"\n{'=' * 60}")
    print("COMPARISON: old (hardcoded) vs new (effective slope)")
    print(f"{'=' * 60}")
    X_eff = augmented[feat_cols_eff].values
    clf_new = LogisticRegression(max_iter=1000, fit_intercept=False)
    clf_new.fit(X_eff, y)
    new_coefs = clf_new.coef_[0]

    print(f"\n{'Feature':20s} {'Old':>8s} {'New':>8s} {'Delta':>8s}")
    print("-" * 50)
    old_list = [OLD_WEIGHTS[n] for n in feat_names]
    for name, old_c, new_c in zip(feat_names, old_list, new_coefs):
        print(f"{name:20s} {old_c:+8.3f} {new_c:+8.3f} {new_c - old_c:+8.3f}")


def main():
    print("Loading junction labels...")
    junctions = pd.read_parquet(LABELS_PATH)
    print(f"  {len(junctions)} junctions")

    print("Loading reach attributes from DuckDB...")
    con = duckdb.connect(str(DB_PATH), read_only=True)
    reach_df = load_reach_attrs(con)
    print(f"  {len(reach_df):,} reaches")

    # Check SWOT slope coverage at labeled junctions
    junction_rids = set(junctions["junction_reach_id"].unique())
    human_rids = set(junctions["human_branch"].unique())
    all_rids = junction_rids | human_rids
    relevant = reach_df[reach_df["reach_id"].isin(all_rids)]
    n_swot = ((relevant["slope_obs_reliable"] == True) & (relevant["slope_obs_n"] >= 5)).sum()
    print(f"  SWOT slope available for {n_swot}/{len(relevant)} labeled reaches ({n_swot / len(relevant):.0%})")

    print("Building pairwise training data...")
    pairs_df = build_pairs(junctions, reach_df, con)
    con.close()
    print(f"  {len(pairs_df)} pairs from {pairs_df['junction_id'].nunique()} junctions")

    train_and_compare(pairs_df)


if __name__ == "__main__":
    main()
