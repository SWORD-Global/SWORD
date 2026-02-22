#!/usr/bin/env python3
"""
Sync v17b reach geometry from PostgreSQL into DuckDB v17c.

DuckDB geometries (rebuilt from NetCDF) lack endpoint overlap vertices that
make adjacent reaches visually connect.  The authoritative full-fidelity
LineStrings live in PostgreSQL table ``sword_reaches_v17b``.

This script copies those geometries into DuckDB v17c once, making DuckDB
the single source of truth for all subsequent exports.

Usage:
    python scripts/maintenance/sync_v17b_geometry.py \
        --duckdb data/duckdb/sword_v17c.duckdb \
        --pg "postgresql://localhost/postgres" \
        --v17b-table sword_reaches_v17b
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import duckdb

try:
    import psycopg2
except ImportError:
    print("Error: psycopg2 not installed. Run: uv pip install psycopg2-binary")
    sys.exit(1)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

BATCH_SIZE = 10_000


def sync_geometry(
    duckdb_path: str,
    pg_dsn: str,
    v17b_table: str = "sword_reaches_v17b",
    v17b_geom_col: str = "geometry",
    dry_run: bool = False,
) -> int:
    """Copy v17b reach geometry from PG into DuckDB v17c.

    Follows the RTREE Update Pattern: drop RTREE indexes, UPDATE, recreate.

    Returns number of reaches updated.
    """
    # --- Connect DuckDB (read-write) ---
    logger.info(f"Opening DuckDB: {duckdb_path}")
    duck = duckdb.connect(duckdb_path, read_only=dry_run)
    duck.execute("INSTALL spatial; LOAD spatial;")

    # --- Connect PostgreSQL ---
    logger.info(f"Connecting to PostgreSQL: {pg_dsn}")
    pg = psycopg2.connect(pg_dsn)

    # Verify source table
    with pg.cursor() as cur:
        cur.execute(
            f"SELECT COUNT(*) FROM {v17b_table}"  # noqa: S608
        )
        pg_count = cur.fetchone()[0]
    logger.info(f"v17b source: {pg_count:,} rows in {v17b_table}")

    if dry_run:
        logger.info("Dry run — no changes written.")
        duck.close()
        pg.close()
        return 0

    # --- Step 1: Drop RTREE indexes on reaches ---
    rtree_indexes = duck.execute(
        "SELECT index_name, table_name, sql FROM duckdb_indexes() "
        "WHERE sql LIKE '%RTREE%' AND table_name = 'reaches'"
    ).fetchall()
    for idx_name, _tbl, _sql in rtree_indexes:
        logger.info(f"Dropping RTREE index: {idx_name}")
        duck.execute(f'DROP INDEX "{idx_name}"')

    # --- Step 2: Read v17b geometry from PG in batches, update DuckDB ---
    updated = 0
    with pg.cursor("v17b_geom_cursor") as cur:
        cur.itersize = BATCH_SIZE
        cur.execute(
            f"SELECT reach_id, ST_AsBinary({v17b_geom_col}) AS geom "  # noqa: S608
            f"FROM {v17b_table} "
            f"WHERE {v17b_geom_col} IS NOT NULL"
        )

        batch = []
        for row in cur:
            reach_id, wkb = row
            batch.append((bytes(wkb), reach_id))

            if len(batch) >= BATCH_SIZE:
                duck.executemany(
                    "UPDATE reaches SET geom = ST_GeomFromWKB($1) WHERE reach_id = $2",
                    batch,
                )
                updated += len(batch)
                logger.info(f"  Updated {updated:,} reaches...")
                batch = []

        if batch:
            duck.executemany(
                "UPDATE reaches SET geom = ST_GeomFromWKB($1) WHERE reach_id = $2",
                batch,
            )
            updated += len(batch)

    logger.info(f"Updated geometry for {updated:,} reaches")

    # --- Step 3: Recompute bbox/centroid ---
    logger.info("Recomputing bbox/centroid columns...")
    duck.execute("""
        UPDATE reaches SET
            x     = ST_X(ST_Centroid(geom)),
            y     = ST_Y(ST_Centroid(geom)),
            x_min = ST_XMin(geom),
            x_max = ST_XMax(geom),
            y_min = ST_YMin(geom),
            y_max = ST_YMax(geom)
        WHERE geom IS NOT NULL
    """)

    # --- Step 4: Recreate RTREE indexes ---
    for idx_name, _tbl, create_sql in rtree_indexes:
        logger.info(f"Recreating RTREE index: {idx_name}")
        duck.execute(create_sql)

    # --- Verify ---
    non_null = duck.execute(
        "SELECT COUNT(*) FROM reaches WHERE geom IS NOT NULL"
    ).fetchone()[0]
    total = duck.execute("SELECT COUNT(*) FROM reaches").fetchone()[0]
    logger.info(f"Verification: {non_null:,} / {total:,} reaches have geometry")

    duck.close()
    pg.close()
    return updated


def main():
    parser = argparse.ArgumentParser(
        description="Sync v17b reach geometry from PostgreSQL into DuckDB v17c",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--duckdb",
        required=True,
        help="Path to DuckDB v17c database",
    )
    parser.add_argument(
        "--pg",
        required=True,
        help='PostgreSQL connection string (e.g., "postgresql://localhost/postgres")',
    )
    parser.add_argument(
        "--v17b-table",
        default="sword_reaches_v17b",
        help="v17b reaches table name in PostgreSQL (default: sword_reaches_v17b)",
    )
    parser.add_argument(
        "--v17b-geom-col",
        default="geometry",
        help="Geometry column name in v17b table (default: geometry)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show counts without writing",
    )
    parser.add_argument("--verbose", "-v", action="store_true")

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    db_path = Path(args.duckdb)
    if not db_path.exists():
        logger.error(f"DuckDB file not found: {db_path}")
        sys.exit(1)

    updated = sync_geometry(
        duckdb_path=str(db_path),
        pg_dsn=args.pg,
        v17b_table=args.v17b_table,
        v17b_geom_col=args.v17b_geom_col,
        dry_run=args.dry_run,
    )
    logger.info(f"Done. {updated:,} reaches updated.")


if __name__ == "__main__":
    main()
