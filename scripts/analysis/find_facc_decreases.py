#!/usr/bin/env python3
"""
Find ALL FACC Decreases (Topological Reversals)
==============================================

Finds every downstream edge where fACC decreases by more than 10%.
This is a pure topological signal that does not depend on noisy WSE data.
"""

import duckdb
import pandas as pd

DB_PATH = "/Users/jakegearon/projects/SWORD/data/duckdb/sword_v17c.duckdb"


def find_facc_decreases(db_path: str):
    conn = duckdb.connect(db_path)

    query = """
    SELECT
        rt.reach_id as up_id,
        rt.neighbor_reach_id as dn_id,
        r1.region,
        r1.facc as facc_up,
        r2.facc as facc_dn,
        (r1.facc - r2.facc) as facc_drop,
        ((r1.facc - r2.facc) / r1.facc) * 100 as drop_pct,
        r1.wse as wse_up,
        r2.wse as wse_dn,
        r1.river_name
    FROM reach_topology rt
    JOIN reaches r1 ON rt.reach_id = r1.reach_id AND rt.region = r1.region
    JOIN reaches r2 ON rt.neighbor_reach_id = r2.reach_id AND rt.region = r2.region
    WHERE rt.direction = 'down'
      AND r1.facc > 10
      AND r2.facc < r1.facc * 0.9
    """

    print("Searching for topological fACC decreases...")
    df = conn.execute(query).fetchdf()

    print("\n=== FACC Decrease Summary ===")
    print(f"Total violations found: {len(df)}")

    if not df.empty:
        print("\n=== Regional Breakdown ===")
        print(df.groupby("region").size().sort_values(ascending=False))

        print("\n=== Severity Breakdown ===")
        df["severity"] = pd.cut(
            df["drop_pct"],
            bins=[0, 25, 50, 90, 100],
            labels=[
                "Minor (10-25%)",
                "Moderate (25-50%)",
                "Major (50-90%)",
                "Extreme (>90%)",
            ],
        )
        print(df.groupby("severity").size())

        both = df[df["wse_dn"] > df["wse_up"] + 0.1]
        print(
            f"\nViolations that also have WSE inversions: {len(both)} ({100 * len(both) / len(df):.1f}%)"
        )

        df.to_csv("ALL_FACC_DECREASES.csv", index=False)
        print("\nSaved all violations to ALL_FACC_DECREASES.csv")

    conn.close()


if __name__ == "__main__":
    find_facc_decreases(DB_PATH)
