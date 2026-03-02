#!/usr/bin/env python3
"""
Methodical Topology Healer
==========================

Applies high-confidence topological flips to the v17c database.
Safely handles primary key constraints and neighbor_rank collisions.
"""

import duckdb
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DB_PATH = "/Users/jakegearon/projects/SWORD/data/duckdb/sword_v17c.duckdb"


def heal_topology(db_path: str, apply: bool = False):
    conn = duckdb.connect(db_path)

    regions = [
        r[0] for r in conn.execute("SELECT DISTINCT region FROM reaches").fetchall()
    ]
    total_flipped = 0

    for region in regions:
        logger.info(f"--- Processing Region: {region} ---")

        candidates_query = f"""
        SELECT 
            rt.reach_id, 
            rt.neighbor_reach_id
        FROM reach_topology rt
        JOIN reaches r1 ON rt.reach_id = r1.reach_id AND rt.region = r1.region
        JOIN reaches r2 ON rt.neighbor_reach_id = r2.reach_id AND rt.region = r2.region
        WHERE rt.direction = 'down' AND rt.region = '{region}'
          AND (
            (r1.facc > r2.facc * 2 AND r1.facc > 10) 
            OR 
            ((r2.wse - r1.wse) > 5 AND r1.facc > r2.facc * 1.1)
          )
        """
        candidates = conn.execute(candidates_query).fetchdf()

        if candidates.empty:
            logger.info(f"  No high-confidence candidates found in {region}.")
            continue

        logger.info(f"  Found {len(candidates)} high-confidence reversed edges.")

        if apply:
            logger.info(f"  Executing swap for {len(candidates)} edges...")
            conn.register("_to_flip", candidates)

            # 1. Capture records
            conn.execute(f"""
                CREATE TEMP TABLE flip_records AS
                SELECT * FROM reach_topology
                WHERE region = '{region}'
                  AND (
                    (reach_id IN (SELECT reach_id FROM _to_flip) AND neighbor_reach_id IN (SELECT neighbor_reach_id FROM _to_flip))
                    OR
                    (reach_id IN (SELECT neighbor_reach_id FROM _to_flip) AND neighbor_reach_id IN (SELECT reach_id FROM _to_flip))
                  )
            """)

            # 2. Delete original records
            conn.execute(f"""
                DELETE FROM reach_topology
                WHERE region = '{region}'
                  AND (
                    (reach_id IN (SELECT reach_id FROM _to_flip) AND neighbor_reach_id IN (SELECT neighbor_reach_id FROM _to_flip))
                    OR
                    (reach_id IN (SELECT neighbor_reach_id FROM _to_flip) AND neighbor_reach_id IN (SELECT reach_id FROM _to_flip))
                  )
            """)

            # 3. Re-insert with direction swapped and NEW RANKS to avoid collision
            # We calculate rank as (current_max_rank + 1) for the new direction
            conn.execute("""
                CREATE TEMP TABLE flip_processed AS
                SELECT 
                    f.reach_id, f.region, 
                    CASE WHEN f.direction = 'up' THEN 'down' ELSE 'up' END as new_direction,
                    f.neighbor_reach_id,
                    f.topology_suspect, f.topology_approved
                FROM flip_records f
            """)

            # Use ROW_NUMBER() over (reach_id, region, new_direction) to re-rank within the flipped set
            # Then add to existing max rank in DB
            conn.execute("""
                INSERT INTO reach_topology
                SELECT 
                    p.reach_id, p.region, p.new_direction,
                    CAST(COALESCE(m.max_rank, -1) + ROW_NUMBER() OVER (PARTITION BY p.reach_id, p.region, p.new_direction ORDER BY p.neighbor_reach_id) AS TINYINT) as neighbor_rank,
                    p.neighbor_reach_id,
                    p.topology_suspect, p.topology_approved
                FROM flip_processed p
                LEFT JOIN (
                    SELECT reach_id, region, direction, MAX(neighbor_rank) as max_rank
                    FROM reach_topology
                    GROUP BY reach_id, region, direction
                ) m ON p.reach_id = m.reach_id AND p.region = m.region AND p.new_direction = m.direction
            """)

            conn.execute("DROP TABLE flip_records")
            conn.execute("DROP TABLE flip_processed")
            conn.unregister("_to_flip")
            total_flipped += len(candidates)
            logger.info(f"  Swap complete for {region}.")
        else:
            logger.info("  DRY RUN: No changes applied.")

    conn.close()
    if apply:
        logger.info("\nHEALING COMPLETE. Total edges flipped: " + str(total_flipped))
    else:
        logger.info("\nDRY RUN COMPLETE.")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    heal_topology(DB_PATH, apply=args.apply)
