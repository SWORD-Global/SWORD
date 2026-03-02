#!/usr/bin/env python3
"""
Methodical Validation of Reversed Reaches
=========================================

Calculates a Confidence Score (0-100) for each "uphill" chain using:
1. WSE Rise Magnitude
2. fACC Trend (decreases downstream?)
3. SWOT Satellite Trend
4. Chain Length
"""

import duckdb
import pandas as pd

DB_PATH = "/Users/jakegearon/projects/SWORD/data/duckdb/sword_v17c.duckdb"


def validate_chains(db_path: str):
    print(f"Connecting to {db_path}...")
    conn = duckdb.connect(db_path)

    query = """
    SELECT
        rt.reach_id as up_id,
        rt.neighbor_reach_id as dn_id,
        r1.wse as wse_up,
        r2.wse as wse_dn,
        (r2.wse - r1.wse) as wse_rise,
        r1.facc as facc_up,
        r2.facc as facc_dn,
        r1.wse_obs_p50 as obs_up,
        r2.wse_obs_p50 as obs_dn,
        r1.region,
        r1.river_name
    FROM reach_topology rt
    JOIN reaches r1 ON rt.reach_id = r1.reach_id AND rt.region = r1.region
    JOIN reaches r2 ON rt.neighbor_reach_id = r2.reach_id AND rt.region = r2.region
    WHERE rt.direction = 'down'
      AND r1.wse IS NOT NULL AND r1.wse != -9999
      AND r2.wse IS NOT NULL AND r2.wse != -9999
      AND (r2.wse - r1.wse) > 0.1
    """
    df = conn.execute(query).fetchdf()

    visited = set()
    results = []

    for _, row in df.iterrows():
        if row["up_id"] in visited:
            continue

        chain = [row]
        visited.add(row["up_id"])
        next_dn = row["dn_id"]

        while True:
            step = df[df["up_id"] == next_dn]
            if step.empty:
                break
            chain.append(step.iloc[0])
            visited.add(step.iloc[0]["up_id"])
            next_dn = step.iloc[0]["dn_id"]

        if len(chain) < 2:
            continue

        total_rise = sum(s["wse_rise"] for s in chain)
        chain_len = len(chain)
        mag_score = min(25, total_rise * 2)
        facc_decreases = sum(1 for s in chain if s["facc_dn"] < s["facc_up"])
        facc_score = (facc_decreases / chain_len) * 30
        swot_chain = [
            s
            for s in chain
            if s["obs_up"] != -9999 and s["obs_dn"] != -9999 and pd.notna(s["obs_up"])
        ]
        if swot_chain:
            swot_rise = sum(1 for s in swot_chain if s["obs_dn"] > s["obs_up"])
            swot_score = (swot_rise / len(swot_chain)) * 25
        else:
            swot_score = 0
        len_score = min(20, chain_len * 2)
        total_confidence = mag_score + facc_score + swot_score + len_score

        results.append(
            {
                "chain_start": chain[0]["up_id"],
                "chain_end": chain[-1]["dn_id"],
                "region": chain[0]["region"],
                "river": chain[0]["river_name"],
                "length": chain_len,
                "total_rise_m": total_rise,
                "facc_contradiction_pct": (facc_decreases / chain_len) * 100,
                "confidence_score": round(total_confidence, 1),
            }
        )

    report_df = pd.DataFrame(results).sort_values("confidence_score", ascending=False)
    report_df.to_csv("REVERSED_VALIDATION_REPORT.csv", index=False)

    print("\n=== Validation Report Summary ===")
    print(f"Total chains analyzed: {len(report_df)}")
    print(
        f"High Confidence (>80 pts): {len(report_df[report_df['confidence_score'] > 80])}"
    )
    print(
        f"Medium Confidence (50-80): {len(report_df[(report_df['confidence_score'] >= 50) & (report_df['confidence_score'] <= 80)])}"
    )
    print(
        f"Low Confidence (<50 pts):  {len(report_df[report_df['confidence_score'] < 50])}"
    )

    print("\n=== Top 5 Definitively Reversed Chains ===")
    cols = [
        "chain_start",
        "river",
        "confidence_score",
        "total_rise_m",
        "facc_contradiction_pct",
    ]
    print(report_df.head(5)[cols])

    conn.close()


if __name__ == "__main__":
    validate_chains(DB_PATH)
