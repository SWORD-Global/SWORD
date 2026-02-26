#!/usr/bin/env python3
"""Spot-check SWOT aggregated stats in v17c.duckdb against raw parquet.

Picks N reaches with high n_obs for a given region, recomputes p50 and n_obs
from raw parquet using identical filters, and reports discrepancies.

Usage:
    python scripts/analysis/spot_check_swot_stats.py --region NA --n 20
    python scripts/analysis/spot_check_swot_stats.py --region AF --n 10
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from sword_duckdb.swot_filters import build_reach_filter_sql

DB_PATH = "data/duckdb/sword_v17c.duckdb"
SWOT_PATH = Path("/Volumes/SWORD_DATA/data/swot/parquet_lake_D")

REACH_RANGES = {
    "AF": (11000000000, 19999999999),
    "EU": (21000000000, 29999999999),
    "AS": (31000000000, 49999999999),
    "OC": (51000000000, 59999999999),
    "SA": (61000000000, 69999999999),
    "NA": (71000000000, 99999999999),
}

# p50 tolerance: APPROX_QUANTILE (t-digest) has ~1% relative error
P50_TOL_FRAC = 0.05  # 5% — generous for spot check
N_OBS_TOL_FRAC = 0.10  # 10% — n_obs may differ slightly if edge files missing


<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> investigate-issue-190
def detect_columns(con: duckdb.DuckDBPyConnection, swot_path: Path) -> set[str]:
    sample = next(
        (
            f
            for f in (swot_path / "reaches").iterdir()
<<<<<<< HEAD
=======
def detect_columns(con: duckdb.DuckDBPyConnection) -> set[str]:
    sample = next(
        (
            f
            for f in (SWOT_PATH / "reaches").iterdir()
>>>>>>> ad53e4b (feat: add DL-GROD ingestion and obstruction lint checks (#127))
=======
>>>>>>> investigate-issue-190
            if f.suffix == ".parquet" and not f.name.startswith("._")
        ),
        None,
    )
    if not sample:
<<<<<<< HEAD
<<<<<<< HEAD
        raise RuntimeError(f"No parquet files in {swot_path / 'reaches'}")
=======
        raise RuntimeError(f"No parquet files in {SWOT_PATH / 'reaches'}")
>>>>>>> ad53e4b (feat: add DL-GROD ingestion and obstruction lint checks (#127))
=======
        raise RuntimeError(f"No parquet files in {swot_path / 'reaches'}")
>>>>>>> investigate-issue-190
    return set(
        c.lower()
        for c in con.execute(f"SELECT * FROM read_parquet('{sample}') LIMIT 1")
        .df()
        .columns.tolist()
    )


def get_sample_reaches(
    con: duckdb.DuckDBPyConnection, region: str, n: int
) -> list[int]:
    rows = con.execute(
        """
        SELECT reach_id
        FROM reaches
        WHERE region = ?
          AND n_obs IS NOT NULL
          AND n_obs > 10
          AND wse_obs_p50 IS NOT NULL
          AND slope_obs_p50 IS NOT NULL
        ORDER BY n_obs DESC
        LIMIT ?
        """,
        [region, n],
    ).fetchall()
    return [r[0] for r in rows]


def fetch_stored(con: duckdb.DuckDBPyConnection, reach_ids: list[int]) -> pd.DataFrame:
    ids = ", ".join(str(r) for r in reach_ids)
    return con.execute(
        f"""
        SELECT reach_id, n_obs,
               wse_obs_p50, width_obs_p50, slope_obs_p50
        FROM reaches
        WHERE reach_id IN ({ids})
        """
    ).df()


def recompute_from_parquet(
    con: duckdb.DuckDBPyConnection,
    reach_ids: list[int],
    region: str,
    where_clause: str,
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> investigate-issue-190
    swot_path: Path = SWOT_PATH,
) -> pd.DataFrame:
    id_min, id_max = REACH_RANGES[region]
    parquet_glob = str(swot_path / "reaches" / "*.parquet")
<<<<<<< HEAD
=======
) -> pd.DataFrame:
    id_min, id_max = REACH_RANGES[region]
    parquet_glob = str(SWOT_PATH / "reaches" / "*.parquet")
>>>>>>> ad53e4b (feat: add DL-GROD ingestion and obstruction lint checks (#127))
=======
>>>>>>> investigate-issue-190
    ids = ", ".join(str(r) for r in reach_ids)

    return con.execute(
        f"""
        SELECT
            TRY_CAST(reach_id AS BIGINT) AS reach_id,
            COUNT(*) AS n_obs_raw,
            APPROX_QUANTILE(wse,   0.5) AS wse_p50_raw,
            APPROX_QUANTILE(width, 0.5) AS width_p50_raw,
            APPROX_QUANTILE(slope, 0.5) AS slope_p50_raw
        FROM read_parquet('{parquet_glob}')
        WHERE TRY_CAST(reach_id AS BIGINT) BETWEEN {id_min} AND {id_max}
          AND TRY_CAST(reach_id AS BIGINT) IN ({ids})
          AND {where_clause}
        GROUP BY reach_id
        """
    ).df()


def compare(stored: pd.DataFrame, recomputed: pd.DataFrame) -> pd.DataFrame:
    df = stored.merge(recomputed, on="reach_id", how="inner")

    def _pct_diff(a, b):
        denom = b.abs().clip(lower=1e-9)
        return ((a - b) / denom).abs()

    df["n_obs_pct_diff"] = _pct_diff(df["n_obs"], df["n_obs_raw"])
    df["wse_pct_diff"] = _pct_diff(df["wse_obs_p50"], df["wse_p50_raw"])
    df["width_pct_diff"] = _pct_diff(df["width_obs_p50"], df["width_p50_raw"])
    df["slope_pct_diff"] = _pct_diff(df["slope_obs_p50"], df["slope_p50_raw"])

    return df


def main() -> None:
    parser = argparse.ArgumentParser(description="Spot-check SWOT aggregated stats")
    parser.add_argument("--region", default="NA", choices=list(REACH_RANGES))
    parser.add_argument("--n", type=int, default=20, help="Number of reaches to check")
    parser.add_argument("--db", default=DB_PATH)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> investigate-issue-190
    parser.add_argument(
        "--swot-path",
        default=str(SWOT_PATH),
        help="Path to SWOT parquet directory (default: %(default)s)",
    )
    args = parser.parse_args()

    swot_path = Path(args.swot_path)
    if not swot_path.exists():
        print(f"ERROR: SWOT data not found at {swot_path}", file=sys.stderr)
<<<<<<< HEAD
=======
    args = parser.parse_args()

    if not SWOT_PATH.exists():
        print(f"ERROR: SWOT data not found at {SWOT_PATH}", file=sys.stderr)
>>>>>>> ad53e4b (feat: add DL-GROD ingestion and obstruction lint checks (#127))
=======
>>>>>>> investigate-issue-190
        sys.exit(1)

    print(f"Connecting to {args.db} ...")
    con = duckdb.connect(args.db, read_only=True)

    print("Detecting parquet columns ...")
<<<<<<< HEAD
<<<<<<< HEAD
    colnames = detect_columns(con, swot_path)
=======
    colnames = detect_columns(con)
>>>>>>> ad53e4b (feat: add DL-GROD ingestion and obstruction lint checks (#127))
=======
    colnames = detect_columns(con, swot_path)
>>>>>>> investigate-issue-190
    where_clause = build_reach_filter_sql(colnames)

    print(f"Sampling {args.n} high-n_obs reaches from {args.region} ...")
    reach_ids = get_sample_reaches(con, args.region, args.n)
    if not reach_ids:
        print("ERROR: No qualifying reaches found — SWOT aggregation may not have run.")
        sys.exit(1)
    print(f"  Got {len(reach_ids)} reaches: {reach_ids[:5]} ...")

    print("Fetching stored stats from v17c ...")
    stored = fetch_stored(con, reach_ids)

    print("Recomputing from raw parquet (this may take 30-60s) ...")
<<<<<<< HEAD
<<<<<<< HEAD
    recomputed = recompute_from_parquet(
        con, reach_ids, args.region, where_clause, swot_path
    )
=======
    recomputed = recompute_from_parquet(con, reach_ids, args.region, where_clause)
>>>>>>> ad53e4b (feat: add DL-GROD ingestion and obstruction lint checks (#127))
=======
    recomputed = recompute_from_parquet(
        con, reach_ids, args.region, where_clause, swot_path
    )
>>>>>>> investigate-issue-190

    if recomputed.empty:
        print("ERROR: No raw parquet rows matched — check region ID ranges or filters.")
        sys.exit(1)

    print(f"  Matched {len(recomputed)}/{len(reach_ids)} reaches in raw parquet.\n")

    df = compare(stored, recomputed)

    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 200)
    pd.set_option("display.float_format", "{:.4f}".format)

    print("=== Per-reach comparison ===")
    cols = [
        "reach_id",
        "n_obs",
        "n_obs_raw",
        "n_obs_pct_diff",
        "wse_obs_p50",
        "wse_p50_raw",
        "wse_pct_diff",
        "slope_obs_p50",
        "slope_p50_raw",
        "slope_pct_diff",
    ]
    print(df[cols].to_string(index=False))

    print("\n=== Summary (mean pct diff) ===")
    for col in ["n_obs_pct_diff", "wse_pct_diff", "width_pct_diff", "slope_pct_diff"]:
        mean = df[col].mean()
        flag = " *** WARN" if mean > 0.10 else ""
        print(f"  {col:25s}: {mean:.3f}{flag}")

    n_obs_fail = (df["n_obs_pct_diff"] > N_OBS_TOL_FRAC).sum()
    wse_fail = (df["wse_pct_diff"] > P50_TOL_FRAC).sum()
    width_fail = (df["width_pct_diff"] > P50_TOL_FRAC).sum()
    slope_fail = (df["slope_pct_diff"] > P50_TOL_FRAC).sum()

    total_fail = n_obs_fail + wse_fail + width_fail + slope_fail
    print(f"\n  Reaches exceeding tolerance ({P50_TOL_FRAC * 100:.0f}%):")
    print(
        f"    n_obs: {n_obs_fail}  wse: {wse_fail}  width: {width_fail}  slope: {slope_fail}"
    )

    if total_fail == 0:
        print("\nPASS — all spot-checked values within tolerance.")
    else:
        print(f"\nFAIL — {total_fail} check(s) exceeded tolerance. Review above.")
        sys.exit(1)


if __name__ == "__main__":
    main()
