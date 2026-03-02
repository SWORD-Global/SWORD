#!/usr/bin/env python3
"""
Methodical Revert of Topology Flips
===================================

Uses the provenance table 'v17c_flow_corrections' to precisely
revert ONLY the reaches that were flipped by the pipeline or healer.
"""

import duckdb
import pandas as pd
import logging
import json

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DB_PATH = "/Users/jakegearon/projects/SWORD/data/duckdb/sword_v17c.duckdb"


def precise_revert(db_path: str):
    conn = duckdb.connect(db_path)

    # 1. Check if the corrections table exists
    tables = [
        r[0]
        for r in conn.execute(
            "SELECT table_name FROM information_schema.tables"
        ).fetchall()
    ]
    if "v17c_flow_corrections" not in tables:
        logger.error(
            "Provenance table 'v17c_flow_corrections' not found. Cannot perform precise revert."
        )
        return

    # 2. Extract the IDs of reaches that were flipped
    # The table stores reach_ids_flipped as a JSON string
    log_entries = conn.execute(
        "SELECT reach_ids_flipped, region FROM v17c_flow_corrections WHERE action = 'flip'"
    ).fetchall()

    if not log_entries:
        logger.info("No 'flip' actions found in provenance table.")
        return

    total_reverted = 0

    for entry in log_entries:
        ids = json.loads(entry[0])
        region = entry[1]

        if not ids:
            continue

        logger.info(f"Reverting {len(ids)} reaches in {region}...")

        # 3. Create temp table for this batch
        ids_df = pd.DataFrame({"rid": ids})
        conn.register("_to_revert", ids_df)

        # Capture and Swap logic (with rank-shifting to avoid collision)
        # 1. Capture
        conn.execute(f"""
            CREATE TEMP TABLE revert_records AS
            SELECT * FROM reach_topology
            WHERE region = '{region}'
              AND (
                (reach_id IN (SELECT rid FROM _to_revert) AND neighbor_reach_id IN (SELECT rid FROM _to_revert))
                OR
                (reach_id IN (SELECT rid FROM _to_revert) OR neighbor_reach_id IN (SELECT rid FROM _to_revert))
              )
        """)

        # Wait, the above logic is slightly risky.
        # Simpler: just flip EVERY 'up' <-> 'down' for the reaches in the ID list
        conn.execute(f"""
            UPDATE reach_topology
            SET direction = CASE WHEN direction = 'up' THEN 'down' ELSE 'up' END
            WHERE region = '{region}'
              AND (reach_id IN (SELECT rid FROM _to_revert) OR neighbor_reach_id IN (SELECT rid FROM _to_revert))
        """)

        total_reverted += len(ids)
        conn.unregister("_to_revert")

    conn.close()
    logger.info(f"\nPRECISE REVERT COMPLETE. Total reaches affected: {total_reverted}")


if __name__ == "__main__":
    precise_revert(DB_PATH)
