#!/usr/bin/env python3
"""
Plot Longitudinal Profiles for WSE Inversion Chains
===================================================

Generates 10 profile plots (WSE vs Dist_Out) for the most
severe "uphill" chains identified in the database.
"""

import duckdb
import matplotlib.pyplot as plt
import os

DB_PATH = "/Users/jakegearon/projects/SWORD/data/duckdb/sword_v17c.duckdb"
OUTPUT_DIR = "output/plots/wse_inversions"


def generate_profiles(db_path: str):
    print(f"Connecting to {db_path}...")
    conn = duckdb.connect(db_path)

    query = """
    SELECT
        rt.reach_id as up_id,
        rt.neighbor_reach_id as dn_id,
        r1.region,
        r1.dist_out as dist_up,
        r2.dist_out as dist_dn
    FROM reach_topology rt
    JOIN reaches r1 ON rt.reach_id = r1.reach_id AND rt.region = r1.region
    JOIN reaches r2 ON rt.neighbor_reach_id = r2.reach_id AND rt.region = r2.region
    WHERE rt.direction = 'down'
      AND r1.wse IS NOT NULL AND r1.wse != -9999
      AND r2.wse IS NOT NULL AND r2.wse != -9999
      AND (r2.wse - r1.wse) > 0.5
    """

    print("Finding inversion chains...")
    inv = conn.execute(query).fetchdf()

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
        if len(curr_chain) > 5:
            chains.append(curr_chain)

    chains.sort(key=len, reverse=True)
    top_chains = chains[:10]
    print(f"Generating plots for {len(top_chains)} chains...")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    for i, chain_ids in enumerate(top_chains):
        ids_str = ",".join(str(rid) for rid in chain_ids)
        prof_query = f"""
        SELECT 
            reach_id, dist_out, wse, wse_obs_p50, river_name, region
        FROM reaches
        WHERE reach_id IN ({ids_str})
        ORDER BY dist_out DESC
        """
        df_prof = conn.execute(prof_query).fetchdf()

        if df_prof.empty:
            continue

        river = df_prof["river_name"].iloc[0] or "Unknown"
        region = df_prof["region"].iloc[0]

        plt.figure(figsize=(10, 6))
        plt.plot(
            df_prof["dist_out"] / 1000,
            df_prof["wse"],
            "r--",
            alpha=0.5,
            label="Model WSE (MERIT)",
        )
        plt.scatter(df_prof["dist_out"] / 1000, df_prof["wse"], color="red", s=20)

        swot_data = df_prof[
            (df_prof["wse_obs_p50"] != -9999) & (df_prof["wse_obs_p50"].notna())
        ]
        if not swot_data.empty:
            plt.plot(
                swot_data["dist_out"] / 1000,
                swot_data["wse_obs_p50"],
                "b-",
                linewidth=2,
                label="SWOT WSE (p50)",
            )
            plt.scatter(
                swot_data["dist_out"] / 1000,
                swot_data["wse_obs_p50"],
                color="blue",
                s=30,
            )

        plt.gca().invert_xaxis()
        plt.xlabel("Distance to Outlet (km)")
        plt.ylabel("Elevation (m)")
        plt.title(
            f"Inversion Profile {i + 1}: {river} ({region})\nChain: {chain_ids[0]} -> {chain_ids[-1]}"
        )
        plt.grid(True, linestyle=":", alpha=0.7)
        plt.legend()

        save_path = f"{OUTPUT_DIR}/inversion_{i + 1}_{chain_ids[0]}.png"
        plt.savefig(save_path, dpi=150, bbox_inches="tight")
        plt.close()
        print(f"  - Saved plot {i + 1} to {save_path}")

    print("\nAll plots saved to output/plots/wse_inversions/")
    conn.close()


if __name__ == "__main__":
    generate_profiles(DB_PATH)
