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

# Tables that have a `region` column but must be loaded wholesale, not per-region.
# These contain audit-style rows whose `region` may be NULL, may hold multi-region
# values such as 'AF,AS,EU,NA,OC,SA' / 'AF,SA' / 'ALL', or may be mis-cased — so a
# per-region WHERE filter silently loses rows. Routing them through the loader's
# "no region column" path makes truncate-and-reload wholesale and deterministic.
AUDIT_TABLES = frozenset(
    {
        "sword_operations",
        "facc_fix_log",
        "lint_fix_log",
    }
)

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
        # Use execute_values with a template that handles geometry
        col_templates = []
        for i, c in enumerate(col_names):
            if c == "geom":
                col_templates.append("ST_SetSRID(ST_GeomFromWKB(%s), 4326)")
            else:
                col_templates.append("%s")
        insert_template = f"({', '.join(col_templates)})"
        insert_sql = f"INSERT INTO {table_name} ({', '.join(col_names)}) VALUES %s"

    # Determine ORDER BY for deterministic batching (LIMIT/OFFSET requires stable order)
    pk_map = {
        "centerlines": "cl_id",
        "centerline_neighbors": "cl_id, region, neighbor_rank",
        "nodes": "node_id",
        "reaches": "reach_id",
        "reach_topology": "reach_id, direction, neighbor_rank",
        # Must sort by the full primary key: (reach_id, region, orbit_rank).
        # Sorting by reach_id alone is non-deterministic whenever a reach has
        # multiple orbit_rank rows, which makes LIMIT/OFFSET batches overlap
        # and triggers duplicate-key errors against the PK.
        "reach_swot_orbits": "reach_id, region, orbit_rank",
        "reach_ice_flags": "reach_id",
        "sword_operations": "operation_id",
        "sword_value_snapshots": "snapshot_id",
        "v17c_sections": "section_id",
        "v17c_section_slope_validation": "section_id",
    }
    order_col = pk_map.get(table_name, col_names[0])
    order_clause = f" ORDER BY {order_col}"

    # Stream data in batches
    loaded = 0
    offset = 0

    with tqdm(total=total_rows, desc=f"Loading {table_name}", unit="rows") as pbar:
        while True:
            # Fetch batch from DuckDB
            batch_query = f"{query}{order_clause} LIMIT {batch_size} OFFSET {offset}"
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
    v17b_gpkg_dir: str = "",
    regions: Optional[list[str]] = None,
    # Legacy dblink params (unused, kept for CLI compat)
    v17b_dsn: str = "",
    v17b_table: str = "",
    v17b_geom_col: str = "",
) -> int:
    """
    Replace reach geometries with the v17b GPKG originals (which include
    endpoint overlap vertices so adjacent reaches visually connect in GIS).

    Reads directly from v17b GeoPackage files on disk — no dblink or
    external PostgreSQL table required.

    Parameters
    ----------
    pg_conn : psycopg2.connection
        Connection to the **target** (v17c) database.
    v17b_gpkg_dir : str
        Directory containing ``{region}_sword_reaches_v17b.gpkg`` files.
    regions : list[str] or None
        Regions to process. None = all 6.

    Returns
    -------
    int
        Number of rows updated.
    """
    import geopandas as gpd

    if regions is None:
        regions = ["af", "as", "eu", "na", "oc", "sa"]
    else:
        regions = [r.lower() for r in regions]

    gpkg_dir = Path(v17b_gpkg_dir)
    if not gpkg_dir.is_dir():
        logger.error(
            f"v17b GPKG directory not found: {gpkg_dir} — "
            "pass --v17b-gpkg-dir to specify the directory containing "
            "{region}_sword_reaches_v17b.gpkg files"
        )
        return 0

    cur = pg_conn.cursor()
    total_updated = 0

    for region in regions:
        gpkg_path = gpkg_dir / f"{region}_sword_reaches_v17b.gpkg"
        if not gpkg_path.exists():
            logger.warning(f"v17b GPKG not found for {region.upper()}: {gpkg_path}")
            continue

        logger.info(f"Loading v17b geometries for {region.upper()} from {gpkg_path}")
        gdf = gpd.read_file(str(gpkg_path), columns=["reach_id"])
        logger.info(f"  {len(gdf)} reaches loaded")

        batch_size = 1000
        updated = 0
        items = [
            (int(r["reach_id"]), r.geometry.wkb_hex)
            for _, r in gdf.iterrows()
            if r.geometry is not None
        ]

        for i in range(0, len(items), batch_size):
            batch = items[i : i + batch_size]
            for rid, wkb_hex in batch:
                cur.execute(
                    "UPDATE reaches SET geom = ST_SetSRID("
                    "ST_GeomFromWKB(decode(%s, 'hex')), 4326) "
                    "WHERE reach_id = %s",
                    (wkb_hex, rid),
                )
            pg_conn.commit()
            updated += len(batch)

        logger.info(f"  Updated {updated:,} geometries for {region.upper()}")
        total_updated += updated

    # Recompute bbox / centroid for all updated regions
    logger.info("Recomputing bbox/centroid columns...")
    cur.execute("""
        UPDATE reaches SET
            x     = ST_X(ST_Centroid(geom)),
            y     = ST_Y(ST_Centroid(geom)),
            x_min = ST_XMin(geom),
            x_max = ST_XMax(geom),
            y_min = ST_YMin(geom),
            y_max = ST_YMax(geom)
        WHERE geom IS NOT NULL
    """)
    pg_conn.commit()

    # Verify no NULL geometries remain
    cur.execute(
        "SELECT region, COUNT(*) FROM reaches WHERE geom IS NULL GROUP BY region"
    )
    nulls = cur.fetchall()
    if nulls:
        for rgn, cnt in nulls:
            logger.error(f"  {rgn}: {cnt} reaches still have NULL geometry!")
        raise RuntimeError(
            f"Geometry overwrite incomplete — "
            f"{sum(c for _, c in nulls)} reaches have NULL geometry"
        )
    else:
        logger.info("Verified: no NULL geometries in reaches table")

    # Verify endpoint connectivity: adjacent reaches must share
    # at least one endpoint (within ~10m). The v17b GPKG
    # geometries include overlap vertices for this purpose.
    logger.info("Verifying endpoint connectivity...")
    cur.execute("""
        WITH pairs AS (
            SELECT r.region,
                   r.reach_id,
                   r.geom AS r_geom,
                   dn.geom AS dn_geom
            FROM reaches r
            JOIN reaches dn ON r.rch_id_dn_1 = dn.reach_id
            WHERE r.rch_id_dn_1 > 0
              AND r.geom IS NOT NULL
              AND dn.geom IS NOT NULL
        )
        SELECT region,
               COUNT(*) AS total,
               SUM(CASE WHEN LEAST(
                   ST_Distance(
                       ST_StartPoint(r_geom),
                       ST_StartPoint(dn_geom)),
                   ST_Distance(
                       ST_StartPoint(r_geom),
                       ST_EndPoint(dn_geom)),
                   ST_Distance(
                       ST_EndPoint(r_geom),
                       ST_StartPoint(dn_geom)),
                   ST_Distance(
                       ST_EndPoint(r_geom),
                       ST_EndPoint(dn_geom))
               ) = 0 THEN 1 ELSE 0 END) AS exact
        FROM pairs
        GROUP BY region ORDER BY region
    """)
    for rgn, total, exact in cur.fetchall():
        pct = 100.0 * exact / total if total else 0
        logger.info(f"  {rgn}: {exact}/{total} pairs share an endpoint ({pct:.1f}%)")
        if pct < 98:
            logger.warning(
                f"  {rgn}: only {pct:.1f}% endpoint connectivity "
                "— v17b GPKG geometries may be missing or wrong"
            )

    cur.close()
    return total_updated


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
        help="Skip overwriting reach geometry from v17b GeoPackages",
    )
    parser.add_argument(
        "--v17b-gpkg-dir",
        default=None,
        help="Directory containing {region}_sword_reaches_v17b.gpkg files (required unless --skip-v17b-geom)",
    )
    parser.add_argument("--batch-size", type=int, help="Override default batch size")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Validate arguments
    if not args.region and not args.all:
        parser.error("Must specify --region or --all")

    if not args.skip_v17b_geom and not args.dry_run and args.v17b_gpkg_dir is None:
        parser.error(
            "--v17b-gpkg-dir is required unless --skip-v17b-geom or --dry-run is set"
        )

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

        # Pre-compute which tables have a region column.
        # Audit tables (see AUDIT_TABLES) are treated as region-less here so they
        # truncate and reload wholesale, avoiding silent row loss on NULL / multi-
        # region / mis-cased values that no per-region WHERE clause would match.
        table_has_region = {}
        for table_name in tables_to_load:
            cols = [c[0] for c in get_table_columns(duck_conn, table_name)]
            has_region_col = "region" in cols
            table_has_region[table_name] = (
                has_region_col and table_name not in AUDIT_TABLES
            )

        for region in regions:
            logger.info(f"\n{'=' * 60}")
            logger.info(f"Processing region: {region}")
            logger.info(f"{'=' * 60}")

            # Truncate/delete in REVERSE order to respect FK dependencies
            if not args.dry_run and not args.no_truncate:
                for table_name in reversed(tables_to_load):
                    has_region = table_has_region[table_name]
                    if not has_region and region != regions[0]:
                        continue
                    try:
                        truncate_table(
                            pg_conn, table_name, region if has_region else None
                        )
                    except Exception as e:
                        logger.error(f"Error truncating {table_name}: {e}")
                        if args.verbose:
                            import traceback

                            traceback.print_exc()

            # Load in forward order (parent tables before children)
            for table_name in tables_to_load:
                has_region = table_has_region[table_name]

                # For tables without region column, only process once (first region)
                if not has_region and region != regions[0]:
                    continue

                try:
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
        # (reads from GPKG files on disk — no dblink dependency)
        if not args.dry_run and not args.skip_v17b_geom and "reaches" in tables_to_load:
            try:
                overwrite_reach_geom_from_v17b(
                    pg_conn,
                    v17b_gpkg_dir=args.v17b_gpkg_dir,
                    regions=regions,
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
