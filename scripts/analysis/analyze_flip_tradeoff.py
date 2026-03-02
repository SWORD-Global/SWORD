#!/usr/bin/env python3
"""
Trade-off Analysis: Elevation vs. Hydrology
===========================================

Analyzes the 1,374 'flipped' reaches to see:
1. Are the new uphill flows significant (>2m) or just jitter?
2. Are they in tidal zones (lakeflag=3) or near dams (obstr_type > 0)?
3. Does the FACC snap confirm the flip despite the elevation rise?
"""

import duckdb
import pandas as pd

DB_PATH = "/Users/jakegearon/projects/SWORD/data/duckdb/sword_v17c.duckdb"


def analyze_tradeoff(db_path: str):
    conn = duckdb.connect(db_path)

    query = """
    SELECT 
        rt.reach_id as up_id, rt.neighbor_reach_id as dn_id,
        r1.wse as wse_up, r2.wse as wse_dn,
        r1.facc as facc_up, r2.facc as facc_dn,
        r1.lakeflag, r1.obstr_type
    FROM reach_topology rt
    JOIN reaches r1 ON rt.reach_id = r1.reach_id AND rt.region = r1.region
    JOIN reaches r2 ON rt.neighbor_reach_id = r2.reach_id AND rt.region = r2.region
    WHERE rt.direction = 'down' AND rt.region = 'AS'
      AND (
        (r1.facc > r2.facc * 2) 
        OR 
        ((r2.wse - r1.wse) > 5 AND r1.facc > r2.facc * 1.1)
      )
    """
    df = conn.execute(query).fetchdf()

    df["new_wse_delta"] = df["wse_up"] - df["wse_dn"]
    df["is_newly_uphill"] = df["new_wse_delta"] > 0.1

    newly_uphill = df[df["is_newly_uphill"]].copy()

    print(f"Total flipped edges: {len(df)}")
    print(
        f"Edges that become 'Uphill' after flipping: {len(newly_uphill)} ({100 * len(newly_uphill) / len(df):.1f}%)"
    )

    if not newly_uphill.empty:
        print("\n=== Profile of New Uphill Flows ===")
        bins = [0.1, 0.5, 2.0, 10.0, 1000]
        labels = [
            "Micro Jitter (<0.5m)",
            "Minor (<2m)",
            "Significant (2-10m)",
            "Extreme (>10m)",
        ]
        newly_uphill["rise_cat"] = pd.cut(
            newly_uphill["new_wse_delta"], bins=bins, labels=labels
        )
        print(newly_uphill.groupby("rise_cat").size())

        tidal = len(newly_uphill[newly_uphill["lakeflag"] == 3])
        dams = len(newly_uphill[newly_uphill["obstr_type"] > 0])
        print("\nContext of newly uphill reaches:")
        print(f"  Tidal Reaches: {tidal}")
        print(f"  Near Dams:     {dams}")

    conn.close()


if __name__ == "__main__":
    analyze_tradeoff(DB_PATH)
