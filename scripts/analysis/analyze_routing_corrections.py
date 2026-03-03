#!/usr/bin/env python3
"""
Analyze Backwater QC Routing Corrections
-----------------------------------------
Uses reroute + confirm fixes as labeled training data to diagnose and improve
the (effective_width, log_facc) ranking function used by compute_main_neighbors()
and compute_best_headwater_outlet().

Reroute = algorithm was wrong (label=0).
Confirm = algorithm was right (label=1).

Outputs:
1. Signal reliability table (facc vs width accuracy)
2. SWOT effect (error rate with/without SWOT observations)
3. Counterfactual rankings with alternative tuples
4. Context patterns (error rate by lakeflag, dist_out quartile, region, n_rch_up)
5. Feature table CSV for downstream ML analysis

Usage:
    python scripts/analysis/analyze_routing_corrections.py \
        --db data/duckdb/sword_v17c.duckdb \
        --output outputs/routing_analysis/
"""

import argparse
import math
import os
import sys

import duckdb
import pandas as pd


def load_corrections(conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Load all corrections from backwater_routing_fixes."""
    return conn.execute(
        """
        SELECT * FROM backwater_routing_fixes
        WHERE fix_type IN ('reroute', 'confirm')
    """
    ).fetchdf()


def extract_junction_features(
    conn: duckdb.DuckDBPyConnection, corrections: pd.DataFrame
) -> pd.DataFrame:
    """Build per-junction feature table with reach attributes for both branches."""
    records = []

    for _, fix in corrections.iterrows():
        jid = fix["junction_reach_id"]
        old_up = fix["old_rch_id_up_main"]
        new_up = fix["new_rch_id_up_main"]
        ft = fix["fix_type"]

        if pd.isna(jid):
            continue

        jid = int(jid)

        # Label: 0 = algorithm wrong (reroute), 1 = algorithm correct (confirm)
        label = 0 if ft == "reroute" else 1

        # For confirms, old and new are the same (algorithm picked correctly)
        # We still want the junction context
        reach_ids = {jid}
        if pd.notna(old_up):
            reach_ids.add(int(old_up))
        if pd.notna(new_up):
            reach_ids.add(int(new_up))

        # Query reach attributes for all relevant reaches
        placeholders = ",".join(["?"] * len(reach_ids))
        reach_data = conn.execute(
            f"""
            SELECT reach_id, facc, width, wse, n_rch_up, n_rch_down, lakeflag,
                   type, dist_out, region, network, main_side,
                   width_obs_median, n_obs
            FROM reaches
            WHERE reach_id IN ({placeholders})
        """,
            list(reach_ids),
        ).fetchdf()

        if reach_data.empty:
            continue

        reach_map = {int(r["reach_id"]): r for _, r in reach_data.iterrows()}

        junction = reach_map.get(jid, {})

        # Features for old branch (algorithm's pick)
        old_data = reach_map.get(int(old_up), {}) if pd.notna(old_up) else {}
        new_data = reach_map.get(int(new_up), {}) if pd.notna(new_up) else {}

        def safe_get(d, key, default=None):
            if isinstance(d, dict):
                return d.get(key, default)
            if isinstance(d, pd.Series):
                val = d.get(key)
                return val if pd.notna(val) else default
            return default

        def effective_width(d):
            """Compute effective_width: SWOT if available, else GRWL."""
            swot_w = safe_get(d, "width_obs_median")
            grwl_w = safe_get(d, "width")
            if swot_w is not None and swot_w > 0:
                return swot_w
            return grwl_w if grwl_w is not None else 0

        def log_facc(d):
            f = safe_get(d, "facc")
            if f is not None and f > 0:
                return math.log10(f)
            return 0

        record = {
            "junction_reach_id": jid,
            "outlet_id": fix["outlet_id"],
            "fix_type": ft,
            "label": label,
            "region": safe_get(fix, "region") or safe_get(junction, "region"),
            # Junction context
            "junction_lakeflag": safe_get(junction, "lakeflag"),
            "junction_type": safe_get(junction, "type"),
            "junction_dist_out": safe_get(junction, "dist_out"),
            "junction_n_rch_up": safe_get(junction, "n_rch_up"),
            "junction_n_rch_down": safe_get(junction, "n_rch_down"),
            "junction_main_side": safe_get(junction, "main_side"),
            "junction_network": safe_get(junction, "network"),
            # Old branch (algorithm's pick)
            "old_branch_id": int(old_up) if pd.notna(old_up) else None,
            "old_facc": safe_get(old_data, "facc"),
            "old_width": safe_get(old_data, "width"),
            "old_width_obs_median": safe_get(old_data, "width_obs_median"),
            "old_n_obs": safe_get(old_data, "n_obs"),
            "old_effective_width": effective_width(old_data),
            "old_log_facc": log_facc(old_data),
            # New branch (correct answer for reroutes)
            "new_branch_id": int(new_up) if pd.notna(new_up) else None,
            "new_facc": safe_get(new_data, "facc"),
            "new_width": safe_get(new_data, "width"),
            "new_width_obs_median": safe_get(new_data, "width_obs_median"),
            "new_n_obs": safe_get(new_data, "n_obs"),
            "new_effective_width": effective_width(new_data),
            "new_log_facc": log_facc(new_data),
            # Metadata from fix
            "fix_old_branch_facc": fix.get("old_branch_facc"),
            "fix_new_branch_facc": fix.get("new_branch_facc"),
            "fix_old_branch_width": fix.get("old_branch_width"),
            "fix_new_branch_width": fix.get("new_branch_width"),
        }
        records.append(record)

    return pd.DataFrame(records)


def analyze_signal_reliability(features: pd.DataFrame) -> pd.DataFrame:
    """For reroutes, check which signal (facc vs width) favored the correct branch."""
    reroutes = features[features["label"] == 0].copy()
    if reroutes.empty:
        return pd.DataFrame()

    # For reroutes: old_branch was algorithm's pick (wrong), new_branch is correct
    # facc favored correct = new_facc > old_facc
    reroutes["facc_favored_correct"] = reroutes["new_facc"] > reroutes["old_facc"]
    reroutes["width_favored_correct"] = (
        reroutes["new_effective_width"] > reroutes["old_effective_width"]
    )
    reroutes["both_wrong"] = (
        ~reroutes["facc_favored_correct"] & ~reroutes["width_favored_correct"]
    )

    n = len(reroutes)
    summary = {
        "total_reroutes": n,
        "facc_favored_correct": reroutes["facc_favored_correct"].sum(),
        "facc_pct": 100 * reroutes["facc_favored_correct"].mean(),
        "width_favored_correct": reroutes["width_favored_correct"].sum(),
        "width_pct": 100 * reroutes["width_favored_correct"].mean(),
        "both_wrong": reroutes["both_wrong"].sum(),
        "both_wrong_pct": 100 * reroutes["both_wrong"].mean(),
    }
    return pd.DataFrame([summary])


def analyze_swot_effect(features: pd.DataFrame) -> pd.DataFrame:
    """Compare error rate when SWOT obs exist on both branches vs GRWL-only."""
    reroutes = features[features["label"] == 0].copy()
    if reroutes.empty:
        return pd.DataFrame()

    reroutes["old_has_swot"] = reroutes["old_n_obs"].fillna(0) > 0
    reroutes["new_has_swot"] = reroutes["new_n_obs"].fillna(0) > 0
    reroutes["both_have_swot"] = reroutes["old_has_swot"] & reroutes["new_has_swot"]
    reroutes["neither_has_swot"] = ~reroutes["old_has_swot"] & ~reroutes["new_has_swot"]

    rows = []
    for label, mask in [
        ("both_have_swot", reroutes["both_have_swot"]),
        ("one_has_swot", ~reroutes["both_have_swot"] & ~reroutes["neither_has_swot"]),
        ("neither_has_swot", reroutes["neither_has_swot"]),
    ]:
        subset = reroutes[mask]
        rows.append(
            {
                "swot_category": label,
                "n_reroutes": len(subset),
                "pct_of_total": 100 * len(subset) / len(reroutes)
                if len(reroutes) > 0
                else 0,
            }
        )

    return pd.DataFrame(rows)


def counterfactual_rankings(features: pd.DataFrame) -> pd.DataFrame:
    """Re-rank each reroute junction with alternative ranking tuples.

    Reports how many reroutes each alternative would have avoided.
    """
    reroutes = features[features["label"] == 0].copy()
    if reroutes.empty:
        return pd.DataFrame()

    strategies = {
        "current_(eff_width,log_facc)": lambda r: (
            r["old_effective_width"] >= r["new_effective_width"]
        ),
        "facc_only": lambda r: r["old_log_facc"] >= r["new_log_facc"],
        "width_only": lambda r: r["old_effective_width"] >= r["new_effective_width"],
        "(log_facc,eff_width)": lambda r: (
            (r["old_log_facc"], r["old_effective_width"])
            >= (r["new_log_facc"], r["new_effective_width"])
        ),
    }

    rows = []
    for name, still_wrong_fn in strategies.items():
        still_wrong = reroutes.apply(still_wrong_fn, axis=1).sum()
        would_fix = len(reroutes) - still_wrong
        rows.append(
            {
                "strategy": name,
                "still_wrong": int(still_wrong),
                "would_fix": int(would_fix),
                "fix_pct": 100 * would_fix / len(reroutes) if len(reroutes) > 0 else 0,
            }
        )

    return pd.DataFrame(rows)


def context_patterns(features: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Error rate by lakeflag, dist_out quartile, region, n_rch_up."""
    results = {}

    for col, label in [
        ("junction_lakeflag", "by_lakeflag"),
        ("region", "by_region"),
        ("junction_n_rch_up", "by_n_rch_up"),
    ]:
        if col not in features.columns:
            continue
        grouped = (
            features.groupby(col)
            .agg(
                n_total=("label", "count"),
                n_correct=("label", "sum"),
                n_wrong=("label", lambda x: (x == 0).sum()),
            )
            .reset_index()
        )
        grouped["error_rate"] = 100 * grouped["n_wrong"] / grouped["n_total"]
        results[label] = grouped

    # dist_out quartiles
    if "junction_dist_out" in features.columns:
        features = features.copy()
        features["dist_out_quartile"] = pd.qcut(
            features["junction_dist_out"].fillna(0),
            4,
            labels=["Q1", "Q2", "Q3", "Q4"],
            duplicates="drop",
        )
        grouped = (
            features.groupby("dist_out_quartile", observed=True)
            .agg(
                n_total=("label", "count"),
                n_correct=("label", "sum"),
                n_wrong=("label", lambda x: (x == 0).sum()),
            )
            .reset_index()
        )
        grouped["error_rate"] = 100 * grouped["n_wrong"] / grouped["n_total"]
        results["by_dist_out_quartile"] = grouped

    return results


def main():
    parser = argparse.ArgumentParser(
        description="Analyze backwater QC routing corrections"
    )
    parser.add_argument("--db", required=True, help="Path to sword_v17c.duckdb")
    parser.add_argument(
        "--output",
        default="outputs/routing_analysis",
        help="Output directory for analysis results",
    )
    args = parser.parse_args()

    if not os.path.exists(args.db):
        print(f"ERROR: Database not found: {args.db}", file=sys.stderr)
        sys.exit(1)

    os.makedirs(args.output, exist_ok=True)

    conn = duckdb.connect(args.db, read_only=True)

    # Load corrections
    corrections = load_corrections(conn)
    if corrections.empty:
        print("No corrections found in backwater_routing_fixes table")
        print("Run import_backwater_fixes.py first")
        conn.close()
        return

    n_reroutes = (corrections["fix_type"] == "reroute").sum()
    n_confirms = (corrections["fix_type"] == "confirm").sum()
    total = n_reroutes + n_confirms
    print(f"Loaded {total} corrections: {n_reroutes} reroutes, {n_confirms} confirms")
    print(f"Baseline accuracy: {n_confirms}/{total} = {100 * n_confirms / total:.1f}%")

    # Build feature table
    print("\nExtracting junction features...")
    features = extract_junction_features(conn, corrections)
    conn.close()

    if features.empty:
        print("No features extracted")
        return

    # Save feature table
    features_path = os.path.join(args.output, "junction_features.csv")
    features.to_csv(features_path, index=False)
    print(f"Feature table: {features_path} ({len(features)} rows)")

    # Signal reliability
    print("\n--- Signal Reliability ---")
    reliability = analyze_signal_reliability(features)
    if not reliability.empty:
        print(reliability.to_string(index=False))
        reliability.to_csv(
            os.path.join(args.output, "signal_reliability.csv"), index=False
        )

    # SWOT effect
    print("\n--- SWOT Effect ---")
    swot = analyze_swot_effect(features)
    if not swot.empty:
        print(swot.to_string(index=False))
        swot.to_csv(os.path.join(args.output, "swot_effect.csv"), index=False)

    # Counterfactual rankings
    print("\n--- Counterfactual Rankings ---")
    counterfactual = counterfactual_rankings(features)
    if not counterfactual.empty:
        print(counterfactual.to_string(index=False))
        counterfactual.to_csv(
            os.path.join(args.output, "counterfactual_rankings.csv"), index=False
        )

    # Context patterns
    print("\n--- Context Patterns ---")
    patterns = context_patterns(features)
    for name, df in patterns.items():
        print(f"\n{name}:")
        print(df.to_string(index=False))
        df.to_csv(os.path.join(args.output, f"context_{name}.csv"), index=False)

    print(f"\nAll outputs saved to {args.output}/")


if __name__ == "__main__":
    main()
