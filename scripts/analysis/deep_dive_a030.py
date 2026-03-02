#!/usr/bin/env python3
"""
Deep Dive: A030 WSE Inversions
==============================

Performs advanced spatial and topological analysis on reaches where
WSE increases downstream.
"""

import argparse
import duckdb

DB_PATH = "/Users/jakegearon/projects/SWORD/data/duckdb/sword_v17c.duckdb"


def deep_dive(db_path: str):
    print(f"Connecting to {db_path}...")
    conn = duckdb.connect(db_path)

    query = """
    SELECT
        rt.reach_id as up_id,
        rt.neighbor_reach_id as dn_id,
        r1.region,
        r1.wse as wse_up,
        r2.wse as wse_dn,
        (r2.wse - r1.wse) as wse_delta,
        r1.width as width_up,
        r1.facc as facc_up,
        r1.lakeflag as lake_up,
        r2.lakeflag as lake_dn,
        r1.obstr_type as obstr_up,
        r2.obstr_type as obstr_dn,
        r1.river_name
    FROM reach_topology rt
    JOIN reaches r1 ON rt.reach_id = r1.reach_id AND rt.region = r1.region
    JOIN reaches r2 ON rt.neighbor_reach_id = r2.reach_id AND rt.region = r2.region
    WHERE rt.direction = 'down'
      AND r1.wse IS NOT NULL AND r1.wse != -9999
      AND r2.wse IS NOT NULL AND r2.wse != -9999
    """

    print("Loading data...")
    df = conn.execute(query).fetchdf()

    inv = df[df["wse_delta"] > 0.1].copy()
    print(f"Analyzing {len(inv)} significant inversions (>10cm)...")

    # Regional Distribution
    print("\n=== Regional Distribution ===")
    print(inv.groupby("region").size().sort_values(ascending=False))

    # Width Correlation
    def width_class(w):
        if w < 50:
            return "1. Small (<50m)"
        if w < 100:
            return "2. Medium (50-100m)"
        if w < 300:
            return "3. Large (100-300m)"
        return "4. Very Large (>300m)"

    inv["width_class"] = inv["width_up"].apply(width_class)
    print("\n=== Width Class Distribution ===")
    print(inv.groupby("width_class").size())

    # Chain Analysis
    print("\n=== Chain Analysis ===")
    chains = []
    visited = set()
    for _, row in inv.iterrows():
        if row["up_id"] in visited:
            continue

        curr_chain = [row["up_id"], row["dn_id"]]
        visited.add(row["up_id"])

        next_dn = row["dn_id"]
        while True:
            step = inv[inv["up_id"] == next_dn]
            if step.empty:
                break
            next_dn = step.iloc[0]["dn_id"]
            curr_chain.append(next_dn)
            visited.add(step.iloc[0]["up_id"])

        if len(curr_chain) > 2:
            chains.append(curr_chain)

    print(f"Total multi-reach 'uphill' chains found: {len(chains)}")

    chains.sort(key=len, reverse=True)
    if chains:
        print("\n=== Top 5 Longest Chains (Regional Hotspots) ===")
        for c in chains[:5]:
            # Get total elevation rise over the chain
            rise = inv[inv["up_id"].isin(c)]["wse_delta"].sum()
            print(f"Length {len(c)}: {c[0]} -> {c[-1]} | Total Rise: {rise:.2f}m")

    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default=DB_PATH)
    args = parser.parse_args()
    deep_dive(args.db)
