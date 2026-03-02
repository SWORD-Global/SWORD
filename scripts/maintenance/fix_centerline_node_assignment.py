#!/usr/bin/env python3
"""Fix centerline-node spatial misallocations (N013).

Two-step fix per region:

1. **Spatial nearest-node reassignment** — For the ~959 reaches where
   centerline points are >500m from their assigned node, reassign each
   centerline to the nearest node on the same reach.

2. **Recompute nodes.cl_id_min/cl_id_max** — Globally recalculate from
   actual centerline assignments, fixing the 5.3M bookkeeping mismatches
   where cl_id NOT BETWEEN cl_id_min AND cl_id_max.

Root cause: UNC's sequential cl_id-based grouping doesn't match spatial
order on inverted/sinuous reaches.  Inherited from v17b.

Usage
-----
    # All regions
    python scripts/maintenance/fix_centerline_node_assignment.py \
        --db data/duckdb/sword_v17c.duckdb --all

    # Single region
    python scripts/maintenance/fix_centerline_node_assignment.py \
        --db data/duckdb/sword_v17c.duckdb --region NA

    # Dry run (report only, no writes)
    python scripts/maintenance/fix_centerline_node_assignment.py \
        --db data/duckdb/sword_v17c.duckdb --region NA --dry-run
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

import duckdb

REGIONS = ["NA", "SA", "EU", "AF", "AS", "OC"]
DIST_THRESHOLD_M = 500.0

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def count_spatial_violations(
    con: duckdb.DuckDBPyConnection,
    region: str,
    threshold: float = DIST_THRESHOLD_M,
) -> int:
    """Count centerlines >threshold meters from their assigned node."""
    row = con.execute(
        """
        SELECT COUNT(*)
        FROM centerlines c
        JOIN nodes n ON c.node_id = n.node_id AND c.region = n.region
        WHERE c.region = ?
          AND 111000.0 * SQRT(
              POWER(LEAST(ABS(c.x - n.x), 360.0 - ABS(c.x - n.x))
                    * COS(RADIANS((c.y + n.y) / 2.0)), 2)
              + POWER(c.y - n.y, 2)
          ) > ?
        """,
        [region, threshold],
    ).fetchone()
    return row[0]


def count_clid_range_violations(
    con: duckdb.DuckDBPyConnection,
    region: str,
) -> int:
    """Count centerlines whose cl_id is NOT BETWEEN their node's cl_id_min/max."""
    row = con.execute(
        """
        SELECT COUNT(*)
        FROM centerlines c
        JOIN nodes n ON c.node_id = n.node_id AND c.region = n.region
        WHERE c.region = ?
          AND NOT (c.cl_id BETWEEN n.cl_id_min AND n.cl_id_max)
        """,
        [region],
    ).fetchone()
    return row[0]


def count_affected_reaches(
    con: duckdb.DuckDBPyConnection,
    region: str,
    threshold: float = DIST_THRESHOLD_M,
) -> int:
    """Count distinct reaches with spatial violations."""
    row = con.execute(
        """
        SELECT COUNT(DISTINCT c.reach_id)
        FROM centerlines c
        JOIN nodes n ON c.node_id = n.node_id AND c.region = n.region
        WHERE c.region = ?
          AND 111000.0 * SQRT(
              POWER(LEAST(ABS(c.x - n.x), 360.0 - ABS(c.x - n.x))
                    * COS(RADIANS((c.y + n.y) / 2.0)), 2)
              + POWER(c.y - n.y, 2)
          ) > ?
        """,
        [region, threshold],
    ).fetchone()
    return row[0]


def step1_spatial_reassignment(
    con: duckdb.DuckDBPyConnection,
    region: str,
    threshold: float = DIST_THRESHOLD_M,
    dry_run: bool = False,
) -> int:
    """Reassign centerlines on affected reaches to their nearest node.

    Only touches reaches that have at least one centerline >threshold
    from its assigned node.
    """
    n_reaches = count_affected_reaches(con, region, threshold)
    if n_reaches == 0:
        logger.info(f"  [{region}] Step 1: No spatial violations — skipping")
        return 0

    logger.info(f"  [{region}] Step 1: {n_reaches} reaches with spatial violations")

    if dry_run:
        return 0

    # Use a CTE to find affected reaches, then update centerlines on those
    # reaches to the nearest node on the same reach.
    con.execute(
        """
        WITH bad_reaches AS (
            SELECT DISTINCT c.reach_id
            FROM centerlines c
            JOIN nodes n ON c.node_id = n.node_id AND c.region = n.region
            WHERE c.region = ?
              AND 111000.0 * SQRT(
                  POWER(LEAST(ABS(c.x - n.x), 360.0 - ABS(c.x - n.x))
                        * COS(RADIANS((c.y + n.y) / 2.0)), 2)
                  + POWER(c.y - n.y, 2)
              ) > ?
        ),
        nearest AS (
            SELECT
                c.cl_id,
                c.region,
                (SELECT n2.node_id
                 FROM nodes n2
                 WHERE n2.reach_id = c.reach_id
                   AND n2.region = c.region
                 ORDER BY POWER((c.x - n2.x) * COS(RADIANS((c.y + n2.y) / 2.0)), 2)
                        + POWER(c.y - n2.y, 2)
                 LIMIT 1
                ) AS new_node_id
            FROM centerlines c
            WHERE c.reach_id IN (SELECT reach_id FROM bad_reaches)
              AND c.region = ?
        )
        UPDATE centerlines
        SET node_id = nearest.new_node_id
        FROM nearest
        WHERE centerlines.cl_id = nearest.cl_id
          AND centerlines.region = nearest.region
          AND centerlines.node_id != nearest.new_node_id
        """,
        [region, threshold, region],
    )
    n_updated = con.execute("SELECT changes()").fetchone()[0]
    logger.info(f"  [{region}] Step 1: Reassigned {n_updated:,} centerlines")
    return n_updated


def step2_recompute_clid_ranges(
    con: duckdb.DuckDBPyConnection,
    region: str,
    dry_run: bool = False,
) -> int:
    """Recompute nodes.cl_id_min/cl_id_max from actual centerline assignments."""
    before = count_clid_range_violations(con, region)
    logger.info(f"  [{region}] Step 2: {before:,} cl_id range violations before fix")

    if dry_run:
        return 0

    con.execute(
        """
        UPDATE nodes
        SET cl_id_min = sub.min_cl, cl_id_max = sub.max_cl
        FROM (
            SELECT node_id, region, MIN(cl_id) AS min_cl, MAX(cl_id) AS max_cl
            FROM centerlines
            WHERE region = ?
            GROUP BY node_id, region
        ) sub
        WHERE nodes.node_id = sub.node_id
          AND nodes.region = sub.region
          AND (nodes.cl_id_min != sub.min_cl OR nodes.cl_id_max != sub.max_cl)
        """,
        [region],
    )
    n_updated = con.execute("SELECT changes()").fetchone()[0]
    logger.info(f"  [{region}] Step 2: Updated cl_id_min/max on {n_updated:,} nodes")
    return n_updated


def process_region(
    con: duckdb.DuckDBPyConnection,
    region: str,
    threshold: float,
    dry_run: bool = False,
) -> dict:
    """Run both fix steps for one region."""
    t0 = time.time()
    logger.info(f"=== Region {region} ===")

    # Before counts
    spatial_before = count_spatial_violations(con, region, threshold)
    range_before = count_clid_range_violations(con, region)
    logger.info(
        f"  [{region}] Before: {spatial_before:,} spatial violations, "
        f"{range_before:,} cl_id range violations"
    )

    # Step 1: spatial nearest-node reassignment
    cl_updated = step1_spatial_reassignment(con, region, threshold, dry_run)

    # Step 2: recompute cl_id_min/cl_id_max
    node_updated = step2_recompute_clid_ranges(con, region, dry_run)

    # After counts
    spatial_after = count_spatial_violations(con, region, threshold)
    range_after = count_clid_range_violations(con, region)
    logger.info(
        f"  [{region}] After:  {spatial_after:,} spatial violations, "
        f"{range_after:,} cl_id range violations"
    )

    elapsed = time.time() - t0
    logger.info(f"  [{region}] Done in {elapsed:.1f}s")

    return {
        "region": region,
        "spatial_before": spatial_before,
        "spatial_after": spatial_after,
        "range_before": range_before,
        "range_after": range_after,
        "cl_updated": cl_updated,
        "node_updated": node_updated,
        "elapsed": elapsed,
    }


def drop_rtree_indexes(
    con: duckdb.DuckDBPyConnection,
    tables: list[str],
) -> list[tuple[str, str, str]]:
    """Drop RTREE indexes on specified tables, return info for recreation."""
    table_list = ", ".join(f"'{t}'" for t in tables)
    indexes = con.execute(
        f"SELECT index_name, table_name, sql FROM duckdb_indexes() "
        f"WHERE sql LIKE '%RTREE%' AND table_name IN ({table_list})"
    ).fetchall()

    for idx_name, tbl, _sql in indexes:
        logger.info(f"Dropping RTREE index: {idx_name} (on {tbl})")
        con.execute(f'DROP INDEX "{idx_name}"')

    return indexes


def recreate_rtree_indexes(
    con: duckdb.DuckDBPyConnection,
    indexes: list[tuple[str, str, str]],
) -> None:
    """Recreate previously dropped RTREE indexes."""
    for idx_name, tbl, create_sql in indexes:
        logger.info(f"Recreating RTREE index: {idx_name} (on {tbl})")
        con.execute(create_sql)


def main():
    parser = argparse.ArgumentParser(
        description="Fix centerline-node spatial misallocations (N013)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--db",
        required=True,
        help="Path to DuckDB database (e.g., data/duckdb/sword_v17c.duckdb)",
    )
    grp = parser.add_mutually_exclusive_group(required=True)
    grp.add_argument("--region", choices=REGIONS, help="Single region to process")
    grp.add_argument("--all", action="store_true", help="Process all regions")
    parser.add_argument(
        "--threshold",
        type=float,
        default=DIST_THRESHOLD_M,
        help=f"Distance threshold in meters (default: {DIST_THRESHOLD_M})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report violations but don't write to database",
    )
    parser.add_argument("--verbose", "-v", action="store_true")

    args = parser.parse_args()
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    db_path = Path(args.db)
    if not db_path.exists():
        logger.error(f"DuckDB file not found: {db_path}")
        sys.exit(1)

    regions = REGIONS if args.all else [args.region]

    con = duckdb.connect(str(db_path), read_only=args.dry_run)
    con.execute("INSTALL spatial; LOAD spatial;")

    # Drop RTREE indexes before updates (centerlines and nodes both may have them)
    rtree_indexes = []
    if not args.dry_run:
        rtree_indexes = drop_rtree_indexes(con, ["centerlines", "nodes"])

    results = []
    for region in regions:
        results.append(process_region(con, region, args.threshold, args.dry_run))

    # Recreate RTREE indexes
    if not args.dry_run and rtree_indexes:
        recreate_rtree_indexes(con, rtree_indexes)

    con.close()

    # Summary table
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(
        f"{'Region':<8} {'Spatial Before':>15} {'Spatial After':>14} "
        f"{'Range Before':>13} {'Range After':>12} {'CL Updated':>11} "
        f"{'Nodes Updated':>14} {'Time':>7}"
    )
    print("-" * 80)
    for r in results:
        print(
            f"{r['region']:<8} {r['spatial_before']:>15,} {r['spatial_after']:>14,} "
            f"{r['range_before']:>13,} {r['range_after']:>12,} {r['cl_updated']:>11,} "
            f"{r['node_updated']:>14,} {r['elapsed']:>6.1f}s"
        )

    total_spatial_before = sum(r["spatial_before"] for r in results)
    total_spatial_after = sum(r["spatial_after"] for r in results)
    total_range_before = sum(r["range_before"] for r in results)
    total_range_after = sum(r["range_after"] for r in results)
    total_cl = sum(r["cl_updated"] for r in results)
    total_nodes = sum(r["node_updated"] for r in results)
    total_time = sum(r["elapsed"] for r in results)
    print("-" * 80)
    print(
        f"{'TOTAL':<8} {total_spatial_before:>15,} {total_spatial_after:>14,} "
        f"{total_range_before:>13,} {total_range_after:>12,} {total_cl:>11,} "
        f"{total_nodes:>14,} {total_time:>6.1f}s"
    )

    if args.dry_run:
        print("\n(Dry run — no changes written)")
    else:
        print("\nDone. Verify with:")
        print(f"  python -m src.sword_duckdb.lint.cli --db {args.db} --checks N013")


if __name__ == "__main__":
    main()
