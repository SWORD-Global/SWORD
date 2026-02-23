#!/usr/bin/env python3
"""
SWORD DuckDB to PostgreSQL/PostGIS Loader
------------------------------------------

This script exports data from a SWORD DuckDB database and loads it into
PostgreSQL with PostGIS geometry support.

Usage:
    python load_from_duckdb.py --duckdb data/duckdb/sword_v17c.duckdb \
                               --pg "postgresql://user:pass@host:5432/sword_db" \
                               --region NA

    # Load all regions
    python load_from_duckdb.py --duckdb data/duckdb/sword_v17c.duckdb \
                               --pg "postgresql://user:pass@host:5432/sword_db" \
                               --all

    # Dry run (show what would be loaded)
    python load_from_duckdb.py --duckdb data/duckdb/sword_v17c.duckdb \
                               --pg "..." --region NA --dry-run

Prerequisites:
    1. PostgreSQL with PostGIS extension installed
    2. Schema created via: psql -d sword_db -f create_postgres_schema.sql
    3. Python packages: duckdb, psycopg2-binary, tqdm

Data volumes (approximate):
    - centerlines: 66.9M rows (~50 GB in PostgreSQL with geometry)
    - nodes: 11.1M rows (~10 GB)
    - reaches: 248.7K rows (~500 MB)
    - reach_topology: ~1M rows (~100 MB)
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

import duckdb

try:
    import psycopg2
    from psycopg2 import sql
    from psycopg2.extras import execute_values
except ImportError:
    print("Error: psycopg2 not installed. Run: pip install psycopg2-binary")
    sys.exit(1)

try:
    from tqdm import tqdm
except ImportError:
    # Fallback if tqdm not available
    def tqdm(iterable, **kwargs):
        return iterable


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Valid regions
VALID_REGIONS = ["NA", "SA", "EU", "AF", "AS", "OC"]

# Batch sizes for different tables (tuned for memory/performance)
BATCH_SIZES = {
    "centerlines": 50_000,  # 66.9M rows - use smaller batches
    "nodes": 25_000,  # 11.1M rows
    "reaches": 10_000,  # 248.7K rows
    "reach_topology": 50_000,  # ~1M rows
    "reach_swot_orbits": 100_000,
    "reach_ice_flags": 100_000,
    "default": 10_000,
}


def get_table_columns(duck_conn, table_name: str) -> list[tuple[str, str]]:
    """Get column names and types from DuckDB table."""
    result = duck_conn.execute(f"DESCRIBE {table_name}").fetchall()
    return [(row[0], row[1]) for row in result]


def duckdb_to_pg_type(duckdb_type: str) -> str:
    """Map DuckDB types to PostgreSQL types."""
    type_map = {
        "BIGINT": "BIGINT",
        "INTEGER": "INTEGER",
        "SMALLINT": "SMALLINT",
        "TINYINT": "SMALLINT",
        "DOUBLE": "DOUBLE PRECISION",
        "FLOAT": "REAL",
        "VARCHAR": "VARCHAR",
        "BOOLEAN": "BOOLEAN",
        "TIMESTAMP": "TIMESTAMP",
        "DATE": "DATE",
        "GEOMETRY": "GEOMETRY",
        "JSON": "JSONB",
        "BIGINT[]": "BIGINT[]",
        "VARCHAR[]": "VARCHAR[]",
    }
    # Handle VARCHAR(n)
    if duckdb_type.startswith("VARCHAR"):
        return duckdb_type
    return type_map.get(duckdb_type.upper(), "VARCHAR")


def count_rows(duck_conn, table_name: str, region: Optional[str] = None) -> int:
    """Count rows in a table, optionally filtered by region."""
    if region:
        # Check if table has region column
        cols = [c[0] for c in get_table_columns(duck_conn, table_name)]
        if "region" in cols:
            return duck_conn.execute(
                f"SELECT COUNT(*) FROM {table_name} WHERE region = ?", [region]
            ).fetchone()[0]
    return duck_conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]


def export_table_to_postgres(
    duck_conn,
    pg_conn,
    table_name: str,
    region: Optional[str] = None,
    batch_size: Optional[int] = None,
    dry_run: bool = False,
    convert_geometry: bool = True,
) -> int:
    """
    Export a DuckDB table to PostgreSQL.

    Parameters
    ----------
    duck_conn : duckdb.DuckDBPyConnection
        DuckDB connection
    pg_conn : psycopg2.connection
        PostgreSQL connection
    table_name : str
        Table name to export
    region : str, optional
        Filter by region (if table has region column)
    batch_size : int, optional
        Rows per batch (default from BATCH_SIZES)
    dry_run : bool
        If True, only count rows without loading
    convert_geometry : bool
        If True, convert DuckDB GEOMETRY to PostGIS

    Returns
    -------
    int
        Number of rows loaded
    """
    if batch_size is None:
        batch_size = BATCH_SIZES.get(table_name, BATCH_SIZES["default"])

    # Get columns
    columns = get_table_columns(duck_conn, table_name)
    col_names = [c[0] for c in columns]
    has_region = "region" in col_names
    has_geom = "geom" in col_names

    # Count rows
    total_rows = count_rows(duck_conn, table_name, region if has_region else None)
    logger.info(f"Table {table_name}: {total_rows:,} rows to load")

    if dry_run:
        return total_rows

    if total_rows == 0:
        return 0

    # Build query
    if has_region and region:
        query = f"SELECT * FROM {table_name} WHERE region = ?"
        params = [region]
    else:
        query = f"SELECT * FROM {table_name}"
        params = []

    # For geometry columns, we need to convert to WKB
    if has_geom and convert_geometry:
        # Replace geom with ST_AsWKB(geom) for proper PostGIS transfer
        col_list = []
        for c in col_names:
            if c == "geom":
                col_list.append("ST_AsWKB(geom) as geom")
            else:
                col_list.append(c)
        if has_region and region:
            query = f"SELECT {', '.join(col_list)} FROM {table_name} WHERE region = ?"
        else:
            query = f"SELECT {', '.join(col_list)} FROM {table_name}"

    # Prepare PostgreSQL insert
    pg_cursor = pg_conn.cursor()

    # Build INSERT statement
    placeholders = ", ".join(["%s"] * len(col_names))
    insert_sql = (
        f"INSERT INTO {table_name} ({', '.join(col_names)}) VALUES ({placeholders})"
    )

    # For geometry, use ST_SetSRID(ST_GeomFromWKB(...), 4326)
    if has_geom and convert_geometry:
        # Find geometry column index
        geom_idx = col_names.index("geom")
        # Use execute_values with a template that handles geometry
        col_templates = []
        for i, c in enumerate(col_names):
            if c == "geom":
                col_templates.append("ST_SetSRID(ST_GeomFromWKB(%s), 4326)")
            else:
                col_templates.append("%s")
        insert_template = f"({', '.join(col_templates)})"
        insert_sql = f"INSERT INTO {table_name} ({', '.join(col_names)}) VALUES %s"

    # Stream data in batches
    loaded = 0
    offset = 0

    with tqdm(total=total_rows, desc=f"Loading {table_name}", unit="rows") as pbar:
        while True:
            # Fetch batch from DuckDB
            batch_query = f"{query} LIMIT {batch_size} OFFSET {offset}"
            if params:
                rows = duck_conn.execute(batch_query, params).fetchall()
            else:
                rows = duck_conn.execute(batch_query).fetchall()

            if not rows:
                break

            # Convert rows to list of tuples
            batch_data = [tuple(row) for row in rows]

            # Insert into PostgreSQL
            try:
                if has_geom and convert_geometry:
                    execute_values(
                        pg_cursor, insert_sql, batch_data, template=insert_template
                    )
                else:
                    pg_cursor.executemany(insert_sql, batch_data)

                pg_conn.commit()
                loaded += len(batch_data)
                pbar.update(len(batch_data))
            except Exception as e:
                pg_conn.rollback()
                logger.error(f"Error inserting batch at offset {offset}: {e}")
                # Try to continue with next batch
                if "duplicate key" in str(e).lower():
                    logger.warning("Duplicate key error - rows may already exist")
                else:
                    raise

            offset += batch_size

    pg_cursor.close()
    logger.info(f"Loaded {loaded:,} rows into {table_name}")
    return loaded


def load_geometry_from_coordinates(
    pg_conn, table_name: str, region: Optional[str] = None
) -> int:
    """
    Generate PostGIS geometry from x,y coordinates for rows with NULL geometry.

    This handles cases where DuckDB geometry is NULL but x,y coordinates exist.

    Parameters
    ----------
    pg_conn : psycopg2.connection
        PostgreSQL connection
    table_name : str
        Table to update (centerlines, nodes, or reaches)
    region : str, optional
        Filter by region

    Returns
    -------
    int
        Number of rows updated
    """
    pg_cursor = pg_conn.cursor()

    if table_name == "reaches":
        # Reaches need LINESTRING from centerlines
        logger.info(
            f"Building reach geometries from centerlines for {region or 'all regions'}..."
        )

        if region:
            update_sql = """
                UPDATE reaches r
                SET geom = subq.line_geom
                FROM (
                    SELECT
                        reach_id,
                        region,
                        ST_MakeLine(
                            ST_SetSRID(ST_MakePoint(x, y), 4326)
                            ORDER BY cl_id
                        ) as line_geom
                    FROM centerlines
                    WHERE region = %s
                    GROUP BY reach_id, region
                ) subq
                WHERE r.reach_id = subq.reach_id
                  AND r.region = subq.region
                  AND r.geom IS NULL
            """
            pg_cursor.execute(update_sql, [region])
        else:
            update_sql = """
                UPDATE reaches r
                SET geom = subq.line_geom
                FROM (
                    SELECT
                        reach_id,
                        region,
                        ST_MakeLine(
                            ST_SetSRID(ST_MakePoint(x, y), 4326)
                            ORDER BY cl_id
                        ) as line_geom
                    FROM centerlines
                    GROUP BY reach_id, region
                ) subq
                WHERE r.reach_id = subq.reach_id
                  AND r.region = subq.region
                  AND r.geom IS NULL
            """
            pg_cursor.execute(update_sql)

    else:
        # Centerlines and nodes use POINT geometry
        if region:
            update_sql = f"""
                UPDATE {table_name}
                SET geom = ST_SetSRID(ST_MakePoint(x, y), 4326)
                WHERE geom IS NULL AND region = %s
            """
            pg_cursor.execute(update_sql, [region])
        else:
            update_sql = f"""
                UPDATE {table_name}
                SET geom = ST_SetSRID(ST_MakePoint(x, y), 4326)
                WHERE geom IS NULL
            """
            pg_cursor.execute(update_sql)

    updated = pg_cursor.rowcount
    pg_conn.commit()
    pg_cursor.close()

    logger.info(f"Updated {updated:,} geometries in {table_name}")
    return updated


def overwrite_reach_geom_from_v17b(
    pg_conn,
    v17b_dsn: str = "dbname=postgres host=localhost",
    v17b_table: str = "sword_reaches_v17b",
    v17b_geom_col: str = "geometry",
) -> int:
    """
    Replace reach geometries with the v17b originals (which include endpoint
    overlap vertices so adjacent reaches visually connect in GIS tools).

    The DuckDB→NetCDF rebuild strips those overlap points.  This step copies
    the full-fidelity geometries from the authoritative v17b PostgreSQL table
    via dblink, then recomputes bbox/centroid columns to match.

    Parameters
    ----------
    pg_conn : psycopg2.connection
        Connection to the **target** (v17c) database.
    v17b_dsn : str
        libpq connection string for the database hosting the v17b table.
    v17b_table : str
        Fully-qualified table name for v17b reaches.
    v17b_geom_col : str
        Geometry column name in the v17b table.

    Returns
    -------
    int
        Number of rows updated.
    """
    cur = pg_conn.cursor()

    # Ensure dblink extension
    cur.execute("CREATE EXTENSION IF NOT EXISTS dblink")
    pg_conn.commit()

    # Quick sanity check: does the remote table exist?
    try:
        cur.execute(
            "SELECT COUNT(*) FROM dblink(%s, %s) AS t(cnt bigint)",
            [v17b_dsn, f"SELECT COUNT(*) FROM {v17b_table} LIMIT 1"],
        )
        remote_cnt = cur.fetchone()[0]
        logger.info(
            f"v17b geometry source: {v17b_table} in '{v17b_dsn}' ({remote_cnt:,} rows)"
        )
    except Exception as e:
        logger.warning(
            f"Cannot reach v17b geometry source ({e}) — skipping geometry overwrite"
        )
        pg_conn.rollback()
        cur.close()
        return 0

    # Copy geometries
    logger.info(
        "Overwriting reach geometries with v17b originals (endpoint overlap)..."
    )
    cur.execute(
        f"""
        UPDATE reaches r
        SET geom = sub.{v17b_geom_col}
        FROM dblink(
            %s,
            $q$SELECT reach_id, {v17b_geom_col} FROM {v17b_table}$q$
        ) AS sub(reach_id bigint, {v17b_geom_col} geometry)
        WHERE r.reach_id = sub.reach_id
        """,
        [v17b_dsn],
    )
    updated = cur.rowcount
    logger.info(f"Updated {updated:,} reach geometries from v17b")

    # Recompute bbox / centroid
    logger.info("Recomputing bbox/centroid columns...")
    cur.execute("""
        UPDATE reaches SET
            x     = ST_X(ST_Centroid(geom)),
            y     = ST_Y(ST_Centroid(geom)),
            x_min = ST_XMin(geom),
            x_max = ST_XMax(geom),
            y_min = ST_YMin(geom),
            y_max = ST_YMax(geom)
    """)

    pg_conn.commit()
    cur.close()
    return updated


def truncate_table(pg_conn, table_name: str, region: Optional[str] = None):
    """Truncate a table or delete rows for a specific region."""
    pg_cursor = pg_conn.cursor()

    # Check if table has region column by querying information_schema
    pg_cursor.execute(
        """
        SELECT column_name FROM information_schema.columns
        WHERE table_name = %s AND column_name = 'region'
    """,
        [table_name],
    )

    has_region = pg_cursor.fetchone() is not None

    if region and has_region:
        logger.info(f"Deleting existing {region} data from {table_name}...")
        pg_cursor.execute(
            sql.SQL("DELETE FROM {} WHERE region = %s").format(
                sql.Identifier(table_name)
            ),
            [region],
        )
    else:
        logger.info(f"Truncating {table_name}...")
        pg_cursor.execute(
            sql.SQL("TRUNCATE {} CASCADE").format(sql.Identifier(table_name))
        )

    pg_conn.commit()
    pg_cursor.close()


def main():
    parser = argparse.ArgumentParser(
        description="Load SWORD data from DuckDB to PostgreSQL/PostGIS",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Load North America region
    python load_from_duckdb.py --duckdb sword_v17c.duckdb --pg "postgresql://..." --region NA

    # Load all regions
    python load_from_duckdb.py --duckdb sword_v17c.duckdb --pg "postgresql://..." --all

    # Dry run to see row counts
    python load_from_duckdb.py --duckdb sword_v17c.duckdb --pg "..." --all --dry-run

    # Load specific tables only
    python load_from_duckdb.py --duckdb sword_v17c.duckdb --pg "..." --region NA --tables reaches nodes
        """,
    )

    parser.add_argument("--duckdb", required=True, help="Path to DuckDB database file")
    parser.add_argument(
        "--pg",
        required=True,
        help="PostgreSQL connection string (e.g., postgresql://user:pass@host:5432/db)",
    )
    parser.add_argument(
        "--region",
        choices=VALID_REGIONS,
        help="Region to load (NA, SA, EU, AF, AS, OC)",
    )
    parser.add_argument("--all", action="store_true", help="Load all regions")
    parser.add_argument(
        "--tables", nargs="+", help="Specific tables to load (default: all)"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Show row counts without loading"
    )
    parser.add_argument(
        "--no-truncate",
        action="store_true",
        help="Do not truncate tables before loading",
    )
    parser.add_argument(
        "--skip-geometry",
        action="store_true",
        help="Skip geometry generation (faster, no spatial queries)",
    )
    parser.add_argument(
        "--skip-v17b-geom",
        action="store_true",
        help="Skip overwriting reach geometry from v17b source",
    )
    parser.add_argument(
        "--v17b-dsn",
        default="dbname=postgres host=localhost",
        help="libpq DSN for database hosting v17b reaches (default: dbname=postgres host=localhost)",
    )
    parser.add_argument(
        "--v17b-table",
        default="sword_reaches_v17b",
        help="v17b reaches table name (default: sword_reaches_v17b)",
    )
    parser.add_argument("--batch-size", type=int, help="Override default batch size")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Validate arguments
    if not args.region and not args.all:
        parser.error("Must specify --region or --all")

    regions = VALID_REGIONS if args.all else [args.region]

    # Define tables to load in order (respects foreign key dependencies)
    # Tables with region column first, then supporting tables
    default_tables = [
        "centerlines",
        "centerline_neighbors",
        "nodes",
        "reaches",
        "reach_topology",
        "reach_swot_orbits",
        "reach_ice_flags",
        "sword_versions",
        "sword_operations",
        "sword_value_snapshots",
        "sword_source_lineage",
        "sword_reconstruction_recipes",
        "sword_snapshots",
        "v17c_sections",
        "v17c_section_slope_validation",
        "facc_corrections",
        "v17c_flow_corrections",
        "facc_fix_log",
        "lint_fix_log",
        "imagery_acquisitions",
        "reach_imagery",
        "reach_geometries",
    ]

    tables_to_load = args.tables if args.tables else default_tables

    # Check DuckDB file exists
    duckdb_path = Path(args.duckdb)
    if not duckdb_path.exists():
        logger.error(f"DuckDB file not found: {duckdb_path}")
        sys.exit(1)

    logger.info(f"Connecting to DuckDB: {duckdb_path}")
    duck_conn = duckdb.connect(str(duckdb_path), read_only=True)

    # Load spatial extension
    try:
        duck_conn.execute("INSTALL spatial; LOAD spatial;")
    except Exception:
        logger.warning(
            "DuckDB spatial extension not available - geometry will use x,y coordinates"
        )

    logger.info("Connecting to PostgreSQL...")
    try:
        pg_conn = psycopg2.connect(args.pg)
    except Exception as e:
        logger.error(f"Failed to connect to PostgreSQL: {e}")
        sys.exit(1)

    # Check PostGIS is installed
    pg_cursor = pg_conn.cursor()
    pg_cursor.execute("SELECT PostGIS_Version()")
    postgis_version = pg_cursor.fetchone()[0]
    logger.info(f"PostGIS version: {postgis_version}")
    pg_cursor.close()

    start_time = datetime.now()
    total_loaded = 0

    try:
        # Get list of tables that actually exist in DuckDB
        existing_tables = set(
            row[0]
            for row in duck_conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
            ).fetchall()
        )

        # Filter to tables that exist
        tables_to_load = [t for t in tables_to_load if t in existing_tables]

        if args.dry_run:
            logger.info("=== DRY RUN - showing row counts ===")

        for region in regions:
            logger.info(f"\n{'=' * 60}")
            logger.info(f"Processing region: {region}")
            logger.info(f"{'=' * 60}")

            for table_name in tables_to_load:
                # Check if table has region column
                cols = [c[0] for c in get_table_columns(duck_conn, table_name)]
                has_region = "region" in cols

                # For tables without region column, only process once (first region)
                if not has_region and region != regions[0]:
                    continue

                try:
                    # Truncate/delete before loading
                    if not args.dry_run and not args.no_truncate:
                        truncate_table(
                            pg_conn, table_name, region if has_region else None
                        )

                    # Export data
                    batch_size = args.batch_size if args.batch_size else None
                    rows = export_table_to_postgres(
                        duck_conn,
                        pg_conn,
                        table_name,
                        region=region if has_region else None,
                        batch_size=batch_size,
                        dry_run=args.dry_run,
                        convert_geometry=not args.skip_geometry,
                    )
                    total_loaded += rows

                except Exception as e:
                    logger.error(f"Error loading {table_name}: {e}")
                    if args.verbose:
                        import traceback

                        traceback.print_exc()
                    # Continue with next table
                    continue

            # Generate geometry from coordinates if geometry was NULL
            if not args.dry_run and not args.skip_geometry:
                logger.info(f"\nGenerating missing geometries for {region}...")
                for table_name in ["centerlines", "nodes", "reaches"]:
                    if table_name in tables_to_load:
                        try:
                            load_geometry_from_coordinates(pg_conn, table_name, region)
                        except Exception as e:
                            logger.error(
                                f"Error generating geometry for {table_name}: {e}"
                            )

        # After all regions loaded: overwrite reach geometries with v17b originals
        # (run once, not per-region, since it's a global UPDATE by reach_id)
        if not args.dry_run and not args.skip_v17b_geom and "reaches" in tables_to_load:
            try:
                overwrite_reach_geom_from_v17b(
                    pg_conn,
                    v17b_dsn=args.v17b_dsn,
                    v17b_table=args.v17b_table,
                )
            except Exception as e:
                logger.error(f"Error overwriting reach geometry from v17b: {e}")
                if args.verbose:
                    import traceback

                    traceback.print_exc()

    finally:
        duck_conn.close()
        pg_conn.close()

    elapsed = datetime.now() - start_time
    logger.info(f"\n{'=' * 60}")
    logger.info("Load complete!")
    logger.info(f"Total rows: {total_loaded:,}")
    logger.info(f"Elapsed time: {elapsed}")
    logger.info(f"{'=' * 60}")


if __name__ == "__main__":
    main()
