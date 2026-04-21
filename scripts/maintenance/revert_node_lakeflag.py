#!/usr/bin/env python3
"""
Revert node-level lakeflag values in v17c DuckDB to their original v17b NetCDF values.

This script restores ALL node-level lakeflag values from the v17b NetCDF source files,
undoing any reach-level propagations that may have overwritten them:
- HarP corrections (200,201 nodes)
- GCS sync propagation (~810 nodes)
- Classifier reconciliation (~4,028 reaches worth of nodes)

Node lakeflag was originally derived from GRWL satellite data at 30m resolution,
aggregated to ~200m node segments. This is independent spatial information that
should not be overwritten by reach-level decisions.

Usage:
    # Dry run (default) — report only
    uv run python scripts/maintenance/revert_node_lakeflag.py

    # Apply corrections
    uv run python scripts/maintenance/revert_node_lakeflag.py --apply

    # Custom paths
    uv run python scripts/maintenance/revert_node_lakeflag.py \
        --db data/duckdb/sword_v17c.duckdb \
        --netcdf-dir data/netcdf \
        --apply
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Any

import duckdb
import netCDF4 as nc
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

REGIONS = ["NA", "SA", "EU", "AF", "AS", "OC"]
VERSION = "v17b"


def read_netcdf_node_lakeflags(nc_path: Path) -> tuple[np.ndarray, np.ndarray]:
    """
    Read node IDs and lakeflag values from NetCDF file.

    Returns:
        Tuple of (node_ids, lakeflags) as numpy arrays
    """
    with nc.Dataset(str(nc_path), "r") as ds:
        nodes_group = ds.groups["nodes"]
        node_ids = nodes_group.variables["node_id"][:].filled().astype(np.int64)
        lakeflags = nodes_group.variables["lakeflag"][:].filled().astype(np.int8)
    return node_ids, lakeflags


def get_rtree_indexes(conn: duckdb.DuckDBPyConnection) -> list[tuple[str, str, str]]:
    """Query and return all RTREE indexes."""
    return conn.execute(
        "SELECT index_name, table_name, sql FROM duckdb_indexes() WHERE sql LIKE '%RTREE%'"
    ).fetchall()


def drop_rtree_indexes(conn: duckdb.DuckDBPyConnection, indexes: list[tuple[str, str, str]]) -> None:
    """Drop all RTREE indexes."""
    for idx_name, _tbl, _sql in indexes:
        conn.execute(f'DROP INDEX "{idx_name}"')


def recreate_rtree_indexes(conn: duckdb.DuckDBPyConnection, indexes: list[tuple[str, str, str]]) -> None:
    """Recreate all RTREE indexes."""
    for _idx_name, _tbl, sql in indexes:
        conn.execute(sql)


def process_region(
    conn: duckdb.DuckDBPyConnection,
    region: str,
    nc_path: Path,
    apply: bool,
) -> dict[str, Any]:
    """
    Process a single region: read NetCDF, compare with DuckDB, optionally update.

    Returns stats dict with keys:
        - nodes_checked: total nodes checked
        - nodes_differ: nodes where lakeflag differs
        - nodes_updated: nodes actually updated (0 if not apply)
    """
    print(f"\n--- Processing {region} ---")
    print(f"  NetCDF: {nc_path}")

    # Read NetCDF data
    node_ids, nc_lakeflags = read_netcdf_node_lakeflags(nc_path)
    total_nodes = len(node_ids)
    print(f"  Nodes in NetCDF: {total_nodes:,}")

    # Create temp table using pandas (much faster than executemany)
    df = pd.DataFrame({
        "node_id": node_ids,
        "nc_lakeflag": nc_lakeflags,
    })
    
    conn.execute("DROP TABLE IF EXISTS _netcdf_nodes")
    conn.execute("CREATE TEMP TABLE _netcdf_nodes (node_id BIGINT, nc_lakeflag TINYINT)")
    
    # Use DuckDB's native pandas integration for fast insert
    conn.register("_netcdf_df", df)
    conn.execute("INSERT INTO _netcdf_nodes SELECT * FROM _netcdf_df")
    conn.unregister("_netcdf_df")

    # Find differences
    diff_result = conn.execute(
        f"""
        SELECT 
            n.node_id,
            n.lakeflag as db_lakeflag,
            nc.nc_lakeflag
        FROM nodes n
        JOIN _netcdf_nodes nc ON n.node_id = nc.node_id
        WHERE n.region = '{region}'
          AND n.lakeflag != nc.nc_lakeflag
        """
    ).fetchall()

    nodes_differ = len(diff_result)
    print(f"  Nodes differing: {nodes_differ:,}")

    stats = {
        "region": region,
        "nodes_checked": total_nodes,
        "nodes_differ": nodes_differ,
        "nodes_updated": 0,
    }

    if not apply:
        conn.execute("DROP TABLE IF EXISTS _netcdf_nodes")
        return stats

    if nodes_differ == 0:
        print(f"  No updates needed for {region}")
        conn.execute("DROP TABLE IF EXISTS _netcdf_nodes")
        return stats

    # Apply mode: perform updates with RTREE safety
    print(f"  Applying {nodes_differ:,} updates...")

    # Install and load spatial extension
    conn.execute("INSTALL spatial; LOAD spatial;")

    # Get RTREE indexes
    indexes = get_rtree_indexes(conn)

    # Drop RTREE indexes
    drop_rtree_indexes(conn, indexes)

    try:
        # Create temp table for logging (include reach_id for lint_fix_log)
        conn.execute("DROP TABLE IF EXISTS _lakeflag_changes")
        conn.execute(
            f"""
            CREATE TEMP TABLE _lakeflag_changes AS
            SELECT 
                n.node_id,
                n.reach_id,
                n.region,
                n.lakeflag AS old_lakeflag,
                nc.nc_lakeflag AS new_lakeflag
            FROM nodes n
            JOIN _netcdf_nodes nc ON n.node_id = nc.node_id
            WHERE n.region = '{region}'
              AND n.lakeflag != nc.nc_lakeflag
            """
        )

        # Bulk update nodes
        conn.execute(
            f"""
            UPDATE nodes
            SET lakeflag = nc.nc_lakeflag
            FROM _netcdf_nodes nc
            WHERE nodes.node_id = nc.node_id
              AND nodes.region = '{region}'
              AND nodes.lakeflag != nc.nc_lakeflag
            """
        )

        # Log to lint_fix_log (one entry per node, with node_id in notes)
        conn.execute(
            """
            INSERT INTO lint_fix_log (
                check_id,
                reach_id,
                region,
                action,
                column_changed,
                old_value,
                new_value,
                notes
            )
            SELECT 
                'NODE_REVERT',
                reach_id,
                region,
                'fix',
                'nodes.lakeflag',
                CAST(old_lakeflag AS VARCHAR),
                CAST(new_lakeflag AS VARCHAR),
                '[node_revert] node_id=' || CAST(node_id AS VARCHAR) || ' restored from v17b NetCDF'
            FROM _lakeflag_changes
            """
        )

        nodes_updated = conn.execute(
            "SELECT COUNT(*) FROM _lakeflag_changes"
        ).fetchone()[0]

        stats["nodes_updated"] = nodes_updated
        print(f"  Updated: {nodes_updated:,} nodes")

        # Cleanup temp tables
        conn.execute("DROP TABLE IF EXISTS _lakeflag_changes")

    finally:
        # Always recreate RTREE indexes
        recreate_rtree_indexes(conn, indexes)

    conn.execute("DROP TABLE IF EXISTS _netcdf_nodes")
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Revert node lakeflag values to v17b NetCDF originals"
    )
    parser.add_argument(
        "--db",
        default="data/duckdb/sword_v17c.duckdb",
        help="Path to v17c DuckDB",
    )
    parser.add_argument(
        "--netcdf-dir",
        default="data/netcdf",
        help="Directory containing v17b NetCDF files",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually write corrections to DB (default: dry run)",
    )
    parser.add_argument(
        "--regions",
        nargs="+",
        choices=REGIONS,
        default=REGIONS,
        help="Specific regions to process (default: all)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    db_path = Path(args.db)
    nc_dir = Path(args.netcdf_dir)

    if not db_path.exists():
        print(f"Database not found: {db_path}", file=sys.stderr)
        sys.exit(1)

    if not nc_dir.exists():
        print(f"NetCDF directory not found: {nc_dir}", file=sys.stderr)
        sys.exit(1)

    # Check source files exist
    missing = []
    for region in args.regions:
        nc_path = nc_dir / f"{region.lower()}_sword_{VERSION}.nc"
        if not nc_path.exists():
            missing.append(str(nc_path))

    if missing:
        print("ERROR: Missing source NetCDF files:")
        for f in missing:
            print(f"  - {f}")
        sys.exit(1)

    mode = "APPLY" if args.apply else "DRY RUN"
    print("=" * 60)
    print(f"Node Lakeflag Revert ({mode})")
    print("=" * 60)
    print(f"Database: {db_path}")
    print(f"NetCDF dir: {nc_dir}")
    print(f"Regions: {args.regions}")
    print()

    # Connect to database
    conn = duckdb.connect(str(db_path), read_only=not args.apply)

    # Process each region
    all_stats = []
    total_differ = 0
    total_updated = 0

    for region in args.regions:
        nc_path = nc_dir / f"{region.lower()}_sword_{VERSION}.nc"
        stats = process_region(conn, region, nc_path, args.apply)
        all_stats.append(stats)
        total_differ += stats["nodes_differ"]
        total_updated += stats["nodes_updated"]

    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"{'Region':<8} {'Checked':>12} {'Differ':>12} {'Updated':>12}")
    print("-" * 48)
    for stats in all_stats:
        print(
            f"{stats['region']:<8} "
            f"{stats['nodes_checked']:>12,} "
            f"{stats['nodes_differ']:>12,} "
            f"{stats['nodes_updated']:>12,}"
        )
    print("-" * 48)
    print(
        f"{'TOTAL':<8} "
        f"{sum(s['nodes_checked'] for s in all_stats):>12,} "
        f"{total_differ:>12,} "
        f"{total_updated:>12,}"
    )

    if not args.apply:
        print(f"\nDRY RUN complete. Use --apply to write {total_differ} corrections.")
    else:
        print(f"\nApplied {total_updated} node lakeflag corrections.")

    conn.close()


if __name__ == "__main__":
    main()
