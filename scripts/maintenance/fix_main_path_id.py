"""Fix main_path_id collisions across regions in sword_v17c.duckdb.

main_path_id was assigned as a sequential counter (1, 2, 3, ...) per region,
causing collisions when all regions share the same table. This script adds
a region-based offset so IDs are globally unique.

See: https://github.com/ealtenau/SWORD/issues/178

Usage:
    python scripts/maintenance/fix_main_path_id.py --db data/duckdb/sword_v17c.duckdb
    python scripts/maintenance/fix_main_path_id.py --db data/duckdb/sword_v17c.duckdb --dry-run
"""

import argparse
import sys

import duckdb

REGION_OFFSETS = {
    "AF": 1_000_000,
    "AS": 2_000_000,
    "EU": 3_000_000,
    "NA": 4_000_000,
    "OC": 5_000_000,
    "SA": 6_000_000,
}


def fix_main_path_id(db_path: str, *, dry_run: bool = False) -> None:
    con = duckdb.connect(db_path, read_only=dry_run)

    # Check current state
    print("Current main_path_id distribution:")
    stats = con.execute("""
        SELECT
            region,
            COUNT(DISTINCT main_path_id) as n_ids,
            MIN(main_path_id) as min_id,
            MAX(main_path_id) as max_id
        FROM reaches
        WHERE main_path_id IS NOT NULL
        GROUP BY region
        ORDER BY region
    """).fetchdf()
    print(stats.to_string(index=False))
    print()

    # Check for collisions before fix
    collisions_before = con.execute("""
        SELECT COUNT(*) FROM (
            SELECT main_path_id
            FROM reaches
            WHERE main_path_id IS NOT NULL
            GROUP BY main_path_id
            HAVING COUNT(DISTINCT (best_headwater, best_outlet)) > 1
        )
    """).fetchone()[0]
    print(f"Collisions before fix: {collisions_before}")

    # Check if already fixed (IDs already in offset range)
    already_offset = con.execute("""
        SELECT COUNT(*) FROM reaches
        WHERE main_path_id IS NOT NULL AND main_path_id >= 1000000
    """).fetchone()[0]
    if already_offset > 0:
        print(
            f"WARNING: {already_offset} reaches already have main_path_id >= 1,000,000."
        )
        print("IDs may have already been offset. Aborting to avoid double-offset.")
        con.close()
        sys.exit(1)

    if dry_run:
        print("\n[DRY RUN] Would apply these offsets:")
        for region, offset in sorted(REGION_OFFSETS.items()):
            count = con.execute(
                "SELECT COUNT(*) FROM reaches WHERE region = ? AND main_path_id IS NOT NULL",
                [region],
            ).fetchone()[0]
            print(f"  {region}: +{offset:,} ({count:,} reaches)")
        con.close()
        return

    # RTREE index pattern: drop indexes, update, recreate
    con.execute("INSTALL spatial; LOAD spatial;")
    indexes = con.execute(
        "SELECT index_name, table_name, sql FROM duckdb_indexes() WHERE sql LIKE '%RTREE%'"
    ).fetchall()

    for idx_name, _tbl, _sql in indexes:
        con.execute(f'DROP INDEX "{idx_name}"')
        print(f"Dropped RTREE index: {idx_name}")

    # Apply offsets
    for region, offset in sorted(REGION_OFFSETS.items()):
        result = con.execute(
            "UPDATE reaches SET main_path_id = main_path_id + ? WHERE region = ? AND main_path_id IS NOT NULL",
            [offset, region],
        )
        count = result.fetchone()[0]
        print(f"  {region}: offset +{offset:,} applied to {count:,} reaches")

    # Recreate RTREE indexes
    for idx_name, _tbl, sql in indexes:
        con.execute(sql)
        print(f"Recreated RTREE index: {idx_name}")

    # Verify
    print("\nAfter fix:")
    stats_after = con.execute("""
        SELECT
            region,
            COUNT(DISTINCT main_path_id) as n_ids,
            MIN(main_path_id) as min_id,
            MAX(main_path_id) as max_id
        FROM reaches
        WHERE main_path_id IS NOT NULL
        GROUP BY region
        ORDER BY region
    """).fetchdf()
    print(stats_after.to_string(index=False))

    collisions_after = con.execute("""
        SELECT COUNT(*) FROM (
            SELECT main_path_id
            FROM reaches
            WHERE main_path_id IS NOT NULL
            GROUP BY main_path_id
            HAVING COUNT(DISTINCT (best_headwater, best_outlet)) > 1
        )
    """).fetchone()[0]
    print(f"\nCollisions after fix: {collisions_after}")

    if collisions_after > 0:
        print("WARNING: Collisions remain. Investigate manually.")
    else:
        print("All main_path_id values are now globally unique.")

    con.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Fix main_path_id collisions across regions"
    )
    parser.add_argument("--db", required=True, help="Path to sword_v17c.duckdb")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would change without modifying",
    )
    args = parser.parse_args()
    fix_main_path_id(args.db, dry_run=args.dry_run)
