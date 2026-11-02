#!/usr/bin/env python3
"""
Unified SWORD v17c Export Script
---------------------------------

Export v17c data from DuckDB to GeoPackage, GeoParquet, NetCDF, or PostgreSQL.

Examples:
    # GeoPackage — all regions, reaches only
    python scripts/maintenance/export_sword.py \
        --source data/duckdb/sword_v17c.duckdb \
        --format gpkg --all --tables reaches \
        --output-dir outputs/gpkg/

    # GeoPackage — one region, reaches + nodes
    python scripts/maintenance/export_sword.py \
        --source data/duckdb/sword_v17c.duckdb \
        --format gpkg --region NA --output-dir outputs/gpkg/

    # GeoParquet — all regions
    python scripts/maintenance/export_sword.py \
        --source data/duckdb/sword_v17c.duckdb \
        --format parquet --all --output-dir outputs/parquet/

    # NetCDF — single region
    python scripts/maintenance/export_sword.py \
        --source data/duckdb/sword_v17c.duckdb \
        --format netcdf --region NA --output-dir outputs/netcdf/

    # PostgreSQL (delegates to load_from_duckdb.py)
    python scripts/maintenance/export_sword.py \
        --source data/duckdb/sword_v17c.duckdb \
        --format postgres --all --pg-target "postgresql://localhost/sword_v17c"
"""

from __future__ import annotations

import argparse
import logging
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

VALID_REGIONS = ("NA", "SA", "EU", "AF", "AS", "OC")
VALID_FORMATS = ("gpkg", "parquet", "netcdf", "postgres")
VALID_TABLES = ("reaches", "nodes", "centerlines", "topology")

# Add project root to path so we can import column_order
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_PROJECT_ROOT / "src"))

# Default v17b NetCDF directory for passthrough of area_fits / discharge_models
V17B_NC_DIR = _PROJECT_ROOT / "data" / "netcdf"

from sword_duckdb.column_order import reorder_columns  # noqa: E402


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _downcast_nullable_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """Convert pandas nullable extension types to numpy-compatible types.

    DuckDB's fetchdf() returns nullable Int64/Float64/boolean dtypes that
    numpy/netCDF4 can't interpret directly.
    """
    for col in df.columns:
        dtype = df[col].dtype
        if isinstance(dtype, pd.Int8Dtype):
            df[col] = df[col].astype("float64").fillna(-9999).astype("int8")
        elif isinstance(dtype, pd.Int16Dtype):
            df[col] = df[col].astype("float64").fillna(-9999).astype("int16")
        elif isinstance(dtype, pd.Int32Dtype):
            df[col] = df[col].astype("float64").fillna(-9999).astype("int32")
        elif isinstance(dtype, pd.Int64Dtype):
            df[col] = df[col].astype("float64").fillna(-9999).astype("int64")
        elif isinstance(dtype, pd.UInt8Dtype):
            df[col] = df[col].astype("float64").fillna(-9999).astype("int16")
        elif isinstance(dtype, pd.UInt16Dtype):
            df[col] = df[col].astype("float64").fillna(-9999).astype("int32")
        elif isinstance(dtype, pd.UInt32Dtype):
            df[col] = df[col].astype("float64").fillna(-9999).astype("int64")
        elif isinstance(dtype, pd.UInt64Dtype):
            df[col] = df[col].astype("float64").fillna(-9999).astype("int64")
        elif isinstance(dtype, pd.Float32Dtype):
            df[col] = df[col].astype("float32")
        elif isinstance(dtype, pd.Float64Dtype):
            df[col] = df[col].astype("float64")
        elif isinstance(dtype, pd.BooleanDtype):
            df[col] = df[col].fillna(False).astype("bool")
        elif isinstance(dtype, pd.StringDtype):
            df[col] = df[col].fillna("").astype(str)
    return df


def _connect_duckdb(path: str) -> duckdb.DuckDBPyConnection:
    """Open DuckDB read-only with spatial extension."""
    con = duckdb.connect(path, read_only=True)
    con.execute("INSTALL spatial; LOAD spatial;")
    return con


def _query_region(
    con: duckdb.DuckDBPyConnection,
    table: str,
    region: str,
    geom_as_wkb: bool = False,
) -> pd.DataFrame:
    """Query a full table for one region, returning a DataFrame.

    If *geom_as_wkb* is True the geometry column is returned as raw WKB bytes
    (suitable for ``shapely.from_wkb``).
    """
    cols = [row[0] for row in con.execute(f"DESCRIBE {table}").fetchall()]
    has_region = "region" in cols

    select_parts = []
    for c in cols:
        if c == "geom" and geom_as_wkb:
            select_parts.append("ST_AsWKB(geom) AS geom")
        else:
            select_parts.append(c)

    where = f"WHERE region = '{region}'" if has_region else ""
    order_by = _canonical_order_clause(table, cols)
    sql = f"SELECT {', '.join(select_parts)} FROM {table} {where}{order_by}"
    return con.execute(sql).fetchdf()


def _canonical_order_clause(table: str, cols: list[str]) -> str:
    """Return a stable ORDER BY clause for exported tables.

    Export order must be deterministic and logically grouped. NetCDF consumers
    in particular expect node arrays to be contiguous by reach and to follow
    within-reach node order rather than DuckDB physical storage order.
    """
    order_cols: list[str]

    if table == "reaches":
        order_cols = ["reach_id"]
    elif table == "nodes":
        order_cols = ["reach_id"]
        if "node_order" in cols:
            order_cols.append("node_order")
        order_cols.append("node_id")
    elif table == "centerlines":
        order_cols = ["reach_id", "cl_id"]
    elif table == "reach_topology":
        order_cols = ["reach_id", "direction", "neighbor_rank", "neighbor_reach_id"]
    elif table == "reach_swot_orbits":
        order_cols = ["reach_id"]
        if "orbit_rank" in cols:
            order_cols.append("orbit_rank")
        elif "orbit_index" in cols:
            order_cols.append("orbit_index")
        order_cols.append("orbit_id" if "orbit_id" in cols else "pass_tile")
    elif table == "reach_ice_flags":
        order_cols = ["reach_id"]
        if "day_of_year" in cols:
            order_cols.append("day_of_year")
        elif "julian_day" in cols:
            order_cols.append("julian_day")
    else:
        order_cols = []

    if not order_cols:
        return ""
    # NULLS LAST ensures fill-value / NULL node_order doesn't break contiguity
    parts = [f"{c} NULLS LAST" for c in order_cols]
    return f" ORDER BY {', '.join(parts)}"


# ---------------------------------------------------------------------------
# GeoPackage export
# ---------------------------------------------------------------------------


def _df_to_geodataframe(
    df: pd.DataFrame,
    table_name: str,
    geom_type: str = "linestring",
):
    """Convert a pandas DataFrame to a GeoDataFrame.

    *geom_type*:
      - ``"linestring"`` — interpret ``geom`` column as WKB
      - ``"point"`` — build Point geometry from ``x``, ``y`` columns
      - ``"none"`` — no geometry (returns plain DataFrame for non-spatial layers)
    """
    import geopandas as gpd
    import shapely

    if geom_type == "none":
        return df

    if geom_type == "linestring":
        if "geom" not in df.columns:
            raise ValueError(
                f"Table {table_name} has no 'geom' column. "
                "Run sync_v17b_geometry.py first to populate geometry."
            )
        null_geom = df["geom"].isna()
        if null_geom.all():
            raise ValueError(
                f"All geometries in {table_name} are NULL. "
                "Run sync_v17b_geometry.py first."
            )
        if null_geom.any():
            logger.warning(
                f"{null_geom.sum():,} / {len(df):,} rows in {table_name} "
                "have NULL geometry — they will be excluded."
            )
            df = df[~null_geom].copy()

        # DuckDB returns bytearray; shapely needs bytes
        geom_bytes = df["geom"].apply(
            lambda g: bytes(g) if isinstance(g, bytearray) else g
        )
        geometry = shapely.from_wkb(geom_bytes)
        df = df.drop(columns=["geom"])
        gdf = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")

    elif geom_type == "point":
        geometry = gpd.points_from_xy(df["x"], df["y"])
        # Keep x/y as attribute columns too — they're useful metadata
        gdf = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")

    else:
        raise ValueError(f"Unknown geom_type: {geom_type!r}")

    return gdf


def export_gpkg(
    con: duckdb.DuckDBPyConnection,
    region: str,
    tables: list[str],
    output_dir: Path,
) -> Path:
    """Export one region to a GeoPackage file. Returns the output path."""
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / f"sword_{region}_v17c.gpkg"

    for table in tables:
        logger.info(f"  [{region}] Exporting {table} to GeoPackage...")

        if table == "reaches":
            df = _query_region(con, "reaches", region, geom_as_wkb=True)
            gdf = _df_to_geodataframe(df, "reaches", geom_type="linestring")
            gdf = reorder_columns(gdf, "reaches")
            gdf.to_file(str(out_path), layer="reaches", driver="GPKG")
            logger.info(f"    reaches: {len(gdf):,} rows")

        elif table == "nodes":
            df = _query_region(con, "nodes", region, geom_as_wkb=False)
            # Drop DuckDB geom (Point stored as DuckDB GEOMETRY) — rebuild from x,y
            if "geom" in df.columns:
                df = df.drop(columns=["geom"])
            gdf = _df_to_geodataframe(df, "nodes", geom_type="point")
            gdf = reorder_columns(gdf, "nodes")
            gdf.to_file(str(out_path), layer="nodes", driver="GPKG")
            logger.info(f"    nodes: {len(gdf):,} rows")

        elif table == "centerlines":
            df = _query_region(con, "centerlines", region, geom_as_wkb=False)
            if "geom" in df.columns:
                df = df.drop(columns=["geom"])
            gdf = _df_to_geodataframe(df, "centerlines", geom_type="point")
            gdf = reorder_columns(gdf, "centerlines")
            gdf.to_file(str(out_path), layer="centerlines", driver="GPKG")
            logger.info(f"    centerlines: {len(gdf):,} rows")

        elif table == "topology":
            df = _query_region(con, "reach_topology", region, geom_as_wkb=False)
            # Non-spatial layer — write as regular table in GPKG
            if "geom" in df.columns:
                df = df.drop(columns=["geom"])
            # fiona needs at least a GeoDataFrame for GPKG, so create with None geometry
            import geopandas as gpd

            gdf = gpd.GeoDataFrame(df)
            gdf.to_file(str(out_path), layer="reach_topology", driver="GPKG")
            logger.info(f"    topology: {len(gdf):,} rows")

    logger.info(f"  [{region}] Wrote {out_path}")
    return out_path


# ---------------------------------------------------------------------------
# GeoParquet export
# ---------------------------------------------------------------------------


def export_parquet(
    con: duckdb.DuckDBPyConnection,
    region: str,
    tables: list[str],
    output_dir: Path,
) -> list[Path]:
    """Export one region to GeoParquet files. Returns list of output paths."""
    output_dir.mkdir(parents=True, exist_ok=True)
    paths = []

    for table in tables:
        logger.info(f"  [{region}] Exporting {table} to GeoParquet...")

        if table == "reaches":
            df = _query_region(con, "reaches", region, geom_as_wkb=True)
            gdf = _df_to_geodataframe(df, "reaches", geom_type="linestring")
            gdf = reorder_columns(gdf, "reaches")
            out = output_dir / f"sword_{region}_v17c_reaches.parquet"
            gdf.to_parquet(str(out), compression="snappy")

        elif table == "nodes":
            df = _query_region(con, "nodes", region, geom_as_wkb=False)
            if "geom" in df.columns:
                df = df.drop(columns=["geom"])
            gdf = _df_to_geodataframe(df, "nodes", geom_type="point")
            gdf = reorder_columns(gdf, "nodes")
            out = output_dir / f"sword_{region}_v17c_nodes.parquet"
            gdf.to_parquet(str(out), compression="snappy")

        elif table == "centerlines":
            df = _query_region(con, "centerlines", region, geom_as_wkb=False)
            if "geom" in df.columns:
                df = df.drop(columns=["geom"])
            gdf = _df_to_geodataframe(df, "centerlines", geom_type="point")
            gdf = reorder_columns(gdf, "centerlines")
            out = output_dir / f"sword_{region}_v17c_centerlines.parquet"
            gdf.to_parquet(str(out), compression="snappy")

        elif table == "topology":
            df = _query_region(con, "reach_topology", region, geom_as_wkb=False)
            out = output_dir / f"sword_{region}_v17c_topology.parquet"
            df.to_parquet(str(out), compression="snappy")

        else:
            continue

        logger.info(f"    {table}: {len(df):,} rows → {out.name}")
        paths.append(out)

    return paths


# ---------------------------------------------------------------------------
# NetCDF export
# ---------------------------------------------------------------------------


def export_netcdf(
    con: duckdb.DuckDBPyConnection,
    region: str,
    output_dir: Path,
    v17b_nc_dir: Path = V17B_NC_DIR,
    *,
    overwrite: bool = False,
) -> Path:
    """Use the single validated published-layout NetCDF writer."""
    from sword_duckdb.netcdf import export_region

    return export_region(con, region, v17b_nc_dir, output_dir, overwrite=overwrite)


def export_postgres(
    source: str,
    regions: list[str],
    pg_target: str,
) -> None:
    """Delegate PostgreSQL export to the existing load_from_duckdb.py script."""
    loader = _PROJECT_ROOT / "scripts" / "maintenance" / "load_from_duckdb.py"
    if not loader.exists():
        logger.error(f"load_from_duckdb.py not found at {loader}")
        sys.exit(1)

    if len(regions) == 6:
        cmd = [
            sys.executable,
            str(loader),
            "--duckdb",
            source,
            "--pg",
            pg_target,
            "--all",
        ]
    else:
        for region in regions:
            cmd = [
                sys.executable,
                str(loader),
                "--duckdb",
                source,
                "--pg",
                pg_target,
                "--region",
                region,
            ]
            logger.info(f"Running: {' '.join(cmd)}")
            result = subprocess.run(cmd, check=False)
            if result.returncode != 0:
                logger.error(f"load_from_duckdb.py failed for region {region}")
        return

    logger.info(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, check=False)
    if result.returncode != 0:
        logger.error("load_from_duckdb.py failed")
        sys.exit(1)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(
        description="Unified SWORD v17c export to GeoPackage, GeoParquet, NetCDF, or PostgreSQL",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # All regions, reaches only, to GeoPackage
    python scripts/maintenance/export_sword.py \\
        --source data/duckdb/sword_v17c.duckdb \\
        --format gpkg --all --tables reaches

    # NA region, all default tables, to GeoParquet
    python scripts/maintenance/export_sword.py \\
        --source data/duckdb/sword_v17c.duckdb \\
        --format parquet --region NA

    # NetCDF export
    python scripts/maintenance/export_sword.py \\
        --source data/duckdb/sword_v17c.duckdb \\
        --format netcdf --all
        """,
    )

    parser.add_argument("--source", required=True, help="Path to DuckDB database file")
    parser.add_argument(
        "--format",
        required=True,
        choices=VALID_FORMATS,
        help="Export format",
    )
    parser.add_argument(
        "--region",
        choices=VALID_REGIONS,
        help="Single region to export",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Export all 6 regions",
    )
    parser.add_argument(
        "--tables",
        nargs="+",
        choices=VALID_TABLES,
        default=["reaches", "nodes"],
        help="Tables to export (default: reaches nodes)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        help="Output directory (default: outputs/{format}/)",
    )
    parser.add_argument(
        "--pg-target",
        default="postgresql://localhost/sword_v17c",
        help="PostgreSQL DSN for --format postgres",
    )
    parser.add_argument(
        "--v17b-nc-dir",
        type=Path,
        default=V17B_NC_DIR,
        help="Directory containing the required complete v17b NetCDF templates",
    )
    parser.add_argument(
        "--overwrite", action="store_true", help="Replace validated NetCDF files"
    )
    parser.add_argument("--verbose", "-v", action="store_true")

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Validate args
    if not args.region and not args.all:
        parser.error("Must specify --region or --all")

    regions = list(VALID_REGIONS) if args.all else [args.region]
    fmt = args.format

    source_path = Path(args.source)
    if not source_path.exists():
        logger.error(f"DuckDB file not found: {source_path}")
        sys.exit(1)

    output_dir = args.output_dir or Path("outputs") / fmt

    # --- PostgreSQL: delegate ---
    if fmt == "postgres":
        export_postgres(str(source_path), regions, args.pg_target)
        return

    # --- File-based formats: connect DuckDB and export ---
    start = datetime.now()
    if fmt == "netcdf":
        from sword_duckdb.netcdf_schema import validate_templates

        validate_templates(args.v17b_nc_dir, regions)
        con = duckdb.connect(str(source_path), read_only=True)
    else:
        con = _connect_duckdb(str(source_path))

    try:
        for region in regions:
            logger.info(f"{'=' * 50}")
            logger.info(f"Exporting {region} as {fmt}")
            logger.info(f"{'=' * 50}")

            if fmt == "gpkg":
                export_gpkg(con, region, args.tables, output_dir)
            elif fmt == "parquet":
                export_parquet(con, region, args.tables, output_dir)
            elif fmt == "netcdf":
                export_netcdf(
                    con,
                    region,
                    output_dir,
                    v17b_nc_dir=args.v17b_nc_dir,
                    overwrite=args.overwrite,
                )
    finally:
        con.close()

    elapsed = datetime.now() - start
    logger.info(f"\nExport complete. Elapsed: {elapsed}")


if __name__ == "__main__":
    main()
