#!/usr/bin/env python3
"""
Investigate A030 WSE Monotonicity Violations
============================================

Categorizes reach pairs where WSE increases downstream by magnitude,
obstruction proximity, and topological context.
"""

import duckdb

DB_PATH = "tests/sword_duckdb/fixtures/sword_test_minimal.duckdb"


def investigate_a030(db_path: str):
    conn = duckdb.connect(db_path)

    # 1. Identify all inversions with context
    query = """
    SELECT
        r1.reach_id as up_id,
        r2.reach_id as dn_id,
        r1.region,
        r1.wse as wse_up,
        r2.wse as wse_down,
        (r2.wse - r1.wse) as wse_increase,
        r1.obstr_type as up_obstr,
        r2.obstr_type as dn_obstr,
        r1.lakeflag as up_lake,
        r2.lakeflag as dn_lake,
        r1.river_name as up_name,
        r2.river_name as dn_name
    FROM reaches r1
    JOIN reach_topology rt ON r1.reach_id = rt.reach_id AND r1.region = rt.region
    JOIN reaches r2 ON rt.neighbor_reach_id = r2.reach_id AND rt.region = r2.region
    WHERE rt.direction = 'down'
        AND r1.wse IS NOT NULL AND r1.wse != -9999
        AND r2.wse IS NOT NULL AND r2.wse != -9999
        AND r2.wse > r1.wse
    """

    df = conn.execute(query).fetchdf()

    if df.empty:
        print("No A030 violations found in this database.")
        return

    # 2. Categorize
    def categorize(row):
        if row["wse_increase"] < 0.01:
            return "Precision Jitter (<1cm)"
        if row["wse_increase"] < 0.1:
            return "Minor Noise (<10cm)"
        if row["up_obstr"] > 0 or row["dn_obstr"] > 0:
            return "Potential Backwater (Near Dam)"
        if row["up_lake"] == 1 or row["dn_lake"] == 1:
            return "Lake Level Flatness/Noise"
        if row["wse_increase"] > 5.0:
            return "Severe Inversion (>5m)"
        return "Significant Inversion"

    df["category"] = df.apply(categorize, axis=1)

    summary = df.groupby("category").size().reset_index(name="count")
    summary["pct"] = (summary["count"] / len(df) * 100).round(1)

    print("\n=== A030 Violation Summary ===")
    print(summary.to_string(index=False))

    print("\n=== Top 5 Severe Inversions ===")
    cols = ["up_id", "dn_id", "wse_up", "wse_down", "wse_increase", "category"]
    print(df.sort_values("wse_increase", ascending=False).head(5)[cols])

    conn.close()


if __name__ == "__main__":
    investigate_a030(DB_PATH)
