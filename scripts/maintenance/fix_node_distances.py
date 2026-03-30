#!/usr/bin/env python3
"""Fix node-level interpolated distances for flow-corrected and single-node reaches.

Bug 1 (HIGH): 639 flow-corrected reaches have inverted node distances because
    offset = reach.dist_out - node.dist_out uses stale v17b node dist_out.
    Fix: use reach_length - offset for flipped reaches.

Bug 2 (LOW): 25,732 single-node reaches place node at upstream edge (offset=0).
    Fix: use reach_length/2 (centroid).

Usage:
    python scripts/maintenance/fix_node_distances.py
    python scripts/maintenance/fix_node_distances.py --db data/duckdb/sword_v17c.duckdb
    python scripts/maintenance/fix_node_distances.py --region NA
    python scripts/maintenance/fix_node_distances.py --dry-run
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

import duckdb

REGIONS = ["NA", "SA", "EU", "AF", "AS", "OC"]


def get_flipped_reach_ids(conn: duckdb.DuckDBPyConnection, region: str) -> set[int]:
    """Get flow-corrected reach IDs from v17c_flow_corrections table."""
    # Check table exists
    tables = conn.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_name = 'v17c_flow_corrections'"
    ).fetchall()
    if not tables:
        print("  WARNING: v17c_flow_corrections table not found, assuming 0 flips")
        return set()

    rows = conn.execute(
        "SELECT DISTINCT reach_id FROM ("
        "  SELECT UNNEST("
        "    string_split(trim(reach_ids_flipped, '[] '), ',')::BIGINT[]"
        "  ) AS reach_id "
        "  FROM v17c_flow_corrections "
        "  WHERE region = ? AND action = 'flip'"
        ")",
        [region.upper()],
    ).fetchall()
    return {r[0] for r in rows}


def fix_region(
    conn: duckdb.DuckDBPyConnection, region: str, dry_run: bool = False
) -> dict:
    """Fix node distances for one region. Returns stats dict."""
    reg = region.upper()
    t0 = time.time()
    print(f"\n{'=' * 50}")
    print(f"Region: {reg}")

    # Get flipped reach IDs
    flipped = get_flipped_reach_ids(conn, reg)
    print(f"  Flow-corrected reaches: {len(flipped)}")

    # Count single-node reaches
    n_single = conn.execute(
        "SELECT COUNT(*) FROM reaches WHERE region = ? AND n_nodes = 1", [reg]
    ).fetchone()[0]
    print(f"  Single-node reaches: {n_single}")

    # Count total nodes
    n_nodes = conn.execute(
        "SELECT COUNT(*) FROM nodes WHERE region = ?", [reg]
    ).fetchone()[0]
    print(f"  Total nodes: {n_nodes:,}")

    if dry_run:
        # Sample a few flipped reaches to show current vs corrected
        if flipped:
            sample_id = next(iter(flipped))
            rows = conn.execute(
                """
                SELECT n.node_id, n.node_order,
                       n.hydro_dist_out AS current_hdo,
                       r.hydro_dist_out AS reach_hdo,
                       r.dist_out AS r_dist_out,
                       n.dist_out AS n_dist_out,
                       r.reach_length
                FROM nodes n
                JOIN reaches r ON n.reach_id = r.reach_id AND n.region = r.region
                WHERE n.reach_id = ? AND n.region = ?
                ORDER BY n.node_order
                """,
                [sample_id, reg],
            ).fetchall()
            print(f"\n  Sample flipped reach {sample_id} ({len(rows)} nodes):")
            print(
                f"  {'node_order':>10} {'current_hdo':>12} {'reach_hdo':>10} {'offset_old':>10} {'offset_new':>10}"
            )
            for row in rows[:5]:
                _nid, node_order, cur_hdo, reach_hdo, r_do, n_do, rlen = row
                old_ofs = r_do - n_do
                new_ofs = rlen - (r_do - n_do)
                print(
                    f"  {node_order:>10} {cur_hdo:>12.1f} {reach_hdo:>10.1f} {old_ofs:>10.1f} {new_ofs:>10.1f}"
                )
            if len(rows) > 5:
                print(f"  ... ({len(rows) - 5} more nodes)")

        print("\n  DRY RUN — no changes made")
        return {
            "region": reg,
            "nodes": n_nodes,
            "flipped": len(flipped),
            "single": n_single,
        }

    # Use the shared interpolation function (same logic as pipeline)
    from sword_v17c_pipeline.stages.output import propagate_reach_to_nodes

    # Load spatial extension + drop RTREE indexes
    conn.execute("INSTALL spatial; LOAD spatial;")
    rtree_indexes = conn.execute(
        "SELECT index_name, table_name, sql FROM duckdb_indexes() "
        "WHERE sql LIKE '%RTREE%'"
    ).fetchall()
    for idx_name, _tbl, _sql in rtree_indexes:
        conn.execute(f'DROP INDEX "{idx_name}"')

    try:
        count = propagate_reach_to_nodes(conn, reg, flipped or None)
        elapsed = time.time() - t0
        print(f"  Updated {count:,} nodes in {elapsed:.1f}s")
    finally:
        # Always recreate RTREE indexes
        for _idx_name, _tbl, sql in rtree_indexes:
            conn.execute(sql)

    return {"region": reg, "nodes": count, "flipped": len(flipped), "single": n_single}


def main():
    parser = argparse.ArgumentParser(
        description="Fix node-level interpolated distances"
    )
    parser.add_argument(
        "--db",
        default="data/duckdb/sword_v17c.duckdb",
        help="Path to v17c DuckDB (default: data/duckdb/sword_v17c.duckdb)",
    )
    parser.add_argument("--region", help="Single region (default: all 6)")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would change without modifying DB",
    )
    args = parser.parse_args()

    regions = [args.region.upper()] if args.region else REGIONS

    print(f"Database: {args.db}")
    print(f"Regions: {', '.join(regions)}")
    if args.dry_run:
        print("MODE: dry-run")

    conn = duckdb.connect(args.db, read_only=args.dry_run)

    # Ensure v17c node columns exist
    from sword_duckdb.schema import add_v17c_columns

    if not args.dry_run:
        add_v17c_columns(conn)

    results = []
    for region in regions:
        stats = fix_region(conn, region, dry_run=args.dry_run)
        results.append(stats)

    if not args.dry_run:
        conn.execute("CHECKPOINT")
    conn.close()

    # Summary
    print(f"\n{'=' * 50}")
    print("Summary:")
    total_nodes = sum(r["nodes"] for r in results)
    total_flipped = sum(r["flipped"] for r in results)
    total_single = sum(r["single"] for r in results)
    print(f"  Total nodes updated: {total_nodes:,}")
    print(f"  Flow-corrected reaches: {total_flipped}")
    print(f"  Single-node reaches: {total_single:,}")


if __name__ == "__main__":
    main()
