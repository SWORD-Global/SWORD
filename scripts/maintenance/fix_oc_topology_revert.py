#!/usr/bin/env python3
"""
Fix OC Topology Revert — Restore OC topology edges to v17b baseline
====================================================================

Issue #191 attempted to flip 1,112 reversed reaches. The revert missed
112 reaches in OC whose topology rows still diverge from v17b (swapped
directions, shifted ranks, missing/extra rows). This causes 85 extra
G018 violations (271 vs 186 baseline).

Fix strategy:
  1. Find all OC reach_ids with any topology difference between v17b and v17c
  2. Delete those reaches' v17c topology rows
  3. Re-insert the v17b rows (with default FALSE for topology_suspect/approved)

Usage:
    uv run python scripts/maintenance/fix_oc_topology_revert.py [--dry-run]
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import duckdb

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
V17B_PATH = PROJECT_ROOT / "data" / "duckdb" / "sword_v17b.duckdb"
V17C_PATH = PROJECT_ROOT / "data" / "duckdb" / "sword_v17c.duckdb"


def find_affected_reach_ids(conn: duckdb.DuckDBPyConnection) -> list[int]:
    """Find OC reach_ids with any topology row difference between v17b and v17c."""
    rows = conn.execute("""
        SELECT DISTINCT reach_id FROM (
            -- rows in v17b not in v17c
            SELECT b.reach_id
            FROM v17b.reach_topology b
            WHERE b.region = 'OC'
              AND NOT EXISTS (
                SELECT 1 FROM reach_topology c
                WHERE c.reach_id = b.reach_id AND c.region = b.region
                  AND c.direction = b.direction AND c.neighbor_rank = b.neighbor_rank
                  AND c.neighbor_reach_id = b.neighbor_reach_id
              )
            UNION
            -- rows in v17c not in v17b
            SELECT c.reach_id
            FROM reach_topology c
            WHERE c.region = 'OC'
              AND NOT EXISTS (
                SELECT 1 FROM v17b.reach_topology b
                WHERE b.reach_id = c.reach_id AND b.region = c.region
                  AND b.direction = c.direction AND b.neighbor_rank = c.neighbor_rank
                  AND b.neighbor_reach_id = c.neighbor_reach_id
              )
        )
        ORDER BY reach_id
    """).fetchall()
    return [r[0] for r in rows]


def report_diff_stats(conn: duckdb.DuckDBPyConnection) -> tuple[int, int, int]:
    """Count rows in v17b-only, v17c-only, and total OC rows in each DB."""
    v17b_only = conn.execute("""
        SELECT COUNT(*)
        FROM v17b.reach_topology b
        WHERE b.region = 'OC'
          AND NOT EXISTS (
            SELECT 1 FROM reach_topology c
            WHERE c.reach_id = b.reach_id AND c.region = b.region
              AND c.direction = b.direction AND c.neighbor_rank = b.neighbor_rank
              AND c.neighbor_reach_id = b.neighbor_reach_id
          )
    """).fetchone()[0]
    v17c_only = conn.execute("""
        SELECT COUNT(*)
        FROM reach_topology c
        WHERE c.region = 'OC'
          AND NOT EXISTS (
            SELECT 1 FROM v17b.reach_topology b
            WHERE b.reach_id = c.reach_id AND b.region = c.region
              AND b.direction = c.direction AND b.neighbor_rank = c.neighbor_rank
              AND b.neighbor_reach_id = c.neighbor_reach_id
          )
    """).fetchone()[0]
    v17b_total = conn.execute(
        "SELECT COUNT(*) FROM v17b.reach_topology WHERE region = 'OC'"
    ).fetchone()[0]
    v17c_total = conn.execute(
        "SELECT COUNT(*) FROM reach_topology WHERE region = 'OC'"
    ).fetchone()[0]
    return v17b_only, v17c_only, v17b_total, v17c_total


def apply_fix(conn: duckdb.DuckDBPyConnection, reach_ids: list[int]) -> tuple[int, int]:
    """Delete affected v17c rows and re-insert from v17b.

    Returns (deleted_count, inserted_count).
    """
    # Register reach_ids as temp table for efficient bulk operations
    conn.execute("CREATE TEMP TABLE _affected_ids (reach_id BIGINT)")
    conn.executemany(
        "INSERT INTO _affected_ids VALUES ($1)", [(rid,) for rid in reach_ids]
    )

    # Count rows before delete
    before_count = conn.execute("""
        SELECT COUNT(*) FROM reach_topology
        WHERE region = 'OC' AND reach_id IN (SELECT reach_id FROM _affected_ids)
    """).fetchone()[0]

    # Delete v17c rows for affected reaches
    conn.execute("""
        DELETE FROM reach_topology
        WHERE region = 'OC' AND reach_id IN (SELECT reach_id FROM _affected_ids)
    """)
    logger.info(
        "Deleted %d v17c rows for %d affected reaches", before_count, len(reach_ids)
    )

    # Insert v17b rows for affected reaches
    conn.execute("""
        INSERT INTO reach_topology
            (reach_id, region, direction, neighbor_rank,
             neighbor_reach_id, topology_suspect, topology_approved)
        SELECT
            b.reach_id, b.region, b.direction, b.neighbor_rank,
            b.neighbor_reach_id, FALSE, FALSE
        FROM v17b.reach_topology b
        WHERE b.region = 'OC'
          AND b.reach_id IN (SELECT reach_id FROM _affected_ids)
    """)

    inserted_count = conn.execute("""
        SELECT COUNT(*) FROM reach_topology
        WHERE region = 'OC' AND reach_id IN (SELECT reach_id FROM _affected_ids)
    """).fetchone()[0]
    logger.info("Inserted %d v17b rows", inserted_count)

    conn.execute("DROP TABLE _affected_ids")
    return before_count, inserted_count


def verify_fix(conn: duckdb.DuckDBPyConnection) -> dict:
    """Verify zero topology differences remain and check G018 counts."""
    # Remaining differences per region
    region_diffs = {}
    for region in ["NA", "SA", "EU", "AF", "AS", "OC"]:
        # Count rows that differ in any way (not just direction)
        v17b_only = conn.execute(f"""
            SELECT COUNT(*)
            FROM v17b.reach_topology b
            WHERE b.region = '{region}'
              AND NOT EXISTS (
                SELECT 1 FROM reach_topology c
                WHERE c.reach_id = b.reach_id AND c.region = b.region
                  AND c.direction = b.direction AND c.neighbor_rank = b.neighbor_rank
                  AND c.neighbor_reach_id = b.neighbor_reach_id
              )
        """).fetchone()[0]
        v17c_only = conn.execute(f"""
            SELECT COUNT(*)
            FROM reach_topology c
            WHERE c.region = '{region}'
              AND NOT EXISTS (
                SELECT 1 FROM v17b.reach_topology b
                WHERE b.reach_id = c.reach_id AND b.region = c.region
                  AND b.direction = c.direction AND b.neighbor_rank = c.neighbor_rank
                  AND b.neighbor_reach_id = c.neighbor_reach_id
              )
        """).fetchone()[0]
        region_diffs[region] = v17b_only + v17c_only

    # G018 counts
    g018_v17b = conn.execute("""
        SELECT COUNT(*)
        FROM v17b.reach_topology t
        JOIN v17b.reaches r1 ON t.reach_id = r1.reach_id AND t.region = r1.region
        JOIN v17b.reaches r2 ON t.neighbor_reach_id = r2.reach_id AND t.region = r2.region
        WHERE t.direction = 'down'
            AND r1.reach_length > 0 AND r1.reach_length != -9999
            AND r1.dist_out IS NOT NULL AND r1.dist_out > 0 AND r1.dist_out != -9999
            AND r2.dist_out IS NOT NULL AND r2.dist_out > 0 AND r2.dist_out != -9999
            AND ABS(r1.dist_out - r2.dist_out - r1.reach_length)
                / r1.reach_length > 0.2
            AND t.region = 'OC'
    """).fetchone()[0]

    g018_v17c = conn.execute("""
        SELECT COUNT(*)
        FROM reach_topology t
        JOIN reaches r1 ON t.reach_id = r1.reach_id AND t.region = r1.region
        JOIN reaches r2 ON t.neighbor_reach_id = r2.reach_id AND t.region = r2.region
        WHERE t.direction = 'down'
            AND r1.reach_length > 0 AND r1.reach_length != -9999
            AND r1.dist_out IS NOT NULL AND r1.dist_out > 0 AND r1.dist_out != -9999
            AND r2.dist_out IS NOT NULL AND r2.dist_out > 0 AND r2.dist_out != -9999
            AND ABS(r1.dist_out - r2.dist_out - r1.reach_length)
                / r1.reach_length > 0.2
            AND t.region = 'OC'
    """).fetchone()[0]

    return {
        "region_diffs": region_diffs,
        "g018_v17b_oc": g018_v17b,
        "g018_v17c_oc": g018_v17c,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Restore OC topology edges to v17b baseline"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report differences without modifying v17c",
    )
    args = parser.parse_args()

    if not V17B_PATH.exists():
        logger.error("v17b database not found: %s", V17B_PATH)
        return 1
    if not V17C_PATH.exists():
        logger.error("v17c database not found: %s", V17C_PATH)
        return 1

    # Open v17c read-write, attach v17b read-only
    v17c_conn = duckdb.connect(str(V17C_PATH))
    v17c_conn.execute(f"ATTACH '{V17B_PATH}' AS v17b (READ_ONLY)")

    # Step 1: Report current state
    v17b_only, v17c_only, v17b_total, v17c_total = report_diff_stats(v17c_conn)
    logger.info("OC topology row counts — v17b: %d, v17c: %d", v17b_total, v17c_total)
    logger.info("Rows in v17b but not v17c: %d", v17b_only)
    logger.info("Rows in v17c but not v17b: %d", v17c_only)

    # Step 2: Find affected reaches
    reach_ids = find_affected_reach_ids(v17c_conn)
    logger.info("Affected reach_ids: %d", len(reach_ids))

    if not reach_ids:
        logger.info("No topology differences found — nothing to fix.")
        v17c_conn.close()
        return 0

    for rid in reach_ids:
        logger.info("  %d", rid)

    if args.dry_run:
        logger.info("DRY RUN — no changes made.")
        v17c_conn.close()
        return 0

    # Step 3: Apply fix
    logger.info(
        "Applying fix: replacing topology rows for %d reaches...", len(reach_ids)
    )
    deleted, inserted = apply_fix(v17c_conn, reach_ids)
    logger.info("Deleted %d v17c rows, inserted %d v17b rows.", deleted, inserted)

    # Step 4: Verify
    logger.info("Verifying fix...")
    result = verify_fix(v17c_conn)

    all_zero = all(v == 0 for v in result["region_diffs"].values())
    for region, diff_count in result["region_diffs"].items():
        status = "OK" if diff_count == 0 else "DIFF"
        logger.info("  %s topology differences: %d  [%s]", region, diff_count, status)

    logger.info(
        "  G018 OC — v17b: %d, v17c: %d",
        result["g018_v17b_oc"],
        result["g018_v17c_oc"],
    )

    if result["g018_v17b_oc"] == result["g018_v17c_oc"]:
        logger.info("  G018 counts match. Fix verified.")
    else:
        logger.warning(
            "  G018 counts differ: v17b=%d vs v17c=%d (delta=%d)",
            result["g018_v17b_oc"],
            result["g018_v17c_oc"],
            result["g018_v17c_oc"] - result["g018_v17b_oc"],
        )

    v17c_conn.close()

    if not all_zero:
        logger.error("Verification FAILED — topology differences remain.")
        return 1

    logger.info("Fix complete. All OC topology rows now match v17b baseline.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
