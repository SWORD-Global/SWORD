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
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import numpy as np
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
    sql = f"SELECT {', '.join(select_parts)} FROM {table} {where}"
    return con.execute(sql).fetchdf()


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


def _pad_array_2d(values: list[list], max_dim: int, fill: int = 0) -> np.ndarray:
    """Pad ragged lists into a fixed [max_dim, N] array."""
    n = len(values)
    arr = np.full((max_dim, n), fill, dtype=np.int64)
    for i, row in enumerate(values):
        for j, val in enumerate(row[:max_dim]):
            arr[j, i] = val
    return arr


def export_netcdf(
    con: duckdb.DuckDBPyConnection,
    region: str,
    output_dir: Path,
) -> Path:
    """Export one region to NetCDF4 format."""
    import netCDF4

    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / f"{region.lower()}_sword_v17c.nc"

    logger.info(f"  [{region}] Querying data for NetCDF...")

    # --- Query tables and downcast nullable dtypes for numpy/netCDF4 ---
    reaches_df = _query_region(con, "reaches", region, geom_as_wkb=False)
    if "geom" in reaches_df.columns:
        reaches_df = reaches_df.drop(columns=["geom"])
    reaches_df = _downcast_nullable_dtypes(reaches_df)

    nodes_df = _query_region(con, "nodes", region, geom_as_wkb=False)
    if "geom" in nodes_df.columns:
        nodes_df = nodes_df.drop(columns=["geom"])
    nodes_df = _downcast_nullable_dtypes(nodes_df)

    centerlines_df = _query_region(con, "centerlines", region, geom_as_wkb=False)
    if "geom" in centerlines_df.columns:
        centerlines_df = centerlines_df.drop(columns=["geom"])
    centerlines_df = _downcast_nullable_dtypes(centerlines_df)

    # Topology
    topo_df = _query_region(con, "reach_topology", region, geom_as_wkb=False)

    # SWOT orbits (if table exists)
    try:
        orbits_df = _query_region(con, "reach_swot_orbits", region, geom_as_wkb=False)
    except duckdb.CatalogException:
        orbits_df = None

    # Ice flags (if table exists)
    try:
        ice_df = _query_region(con, "reach_ice_flags", region, geom_as_wkb=False)
    except duckdb.CatalogException:
        ice_df = None

    num_reaches = len(reaches_df)
    num_nodes = len(nodes_df)
    num_points = len(centerlines_df)

    logger.info(
        f"  [{region}] reaches={num_reaches:,}  nodes={num_nodes:,}  "
        f"centerlines={num_points:,}"
    )

    # --- Create NetCDF file ---
    ds = netCDF4.Dataset(str(out_path), "w", format="NETCDF4")

    # Global attributes
    ds.Name = region
    ds.production_date = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    ds.source = "SWORD v17c (DuckDB export)"
    ds.Conventions = "CF-1.6"

    if num_reaches > 0:
        ds.x_min = (
            float(reaches_df["x_min"].min()) if "x_min" in reaches_df.columns else 0.0
        )
        ds.x_max = (
            float(reaches_df["x_max"].max()) if "x_max" in reaches_df.columns else 0.0
        )
        ds.y_min = (
            float(reaches_df["y_min"].min()) if "y_min" in reaches_df.columns else 0.0
        )
        ds.y_max = (
            float(reaches_df["y_max"].max()) if "y_max" in reaches_df.columns else 0.0
        )

    # --- Reaches group ---
    rch_grp = ds.createGroup("reaches")
    rch_grp.createDimension("num_reaches", num_reaches)
    rch_grp.createDimension("num_rch_neighbors", 4)

    # Build reach_id → index mapping for topology reconstruction
    reach_ids = reaches_df["reach_id"].values
    reach_id_to_idx = {int(rid): i for i, rid in enumerate(reach_ids)}

    # Write scalar reach variables
    _SKIP_COLS = {"region"}
    _STRING_COLS = {
        "river_name",
        "river_name_en",
        "river_name_local",
        "edit_flag",
        "version",
    }
    for col in reaches_df.columns:
        if col in _SKIP_COLS:
            continue
        vals = reaches_df[col].values
        if col in _STRING_COLS or reaches_df[col].dtype == object:
            var = rch_grp.createVariable(col, str, ("num_reaches",))
            # Convert to string, replacing None/NaN with ""
            str_vals = pd.Series(vals).fillna("").astype(str).values
            for i, s in enumerate(str_vals):
                var[i] = s
        elif np.issubdtype(vals.dtype, np.integer):
            var = rch_grp.createVariable(col, "i8", ("num_reaches",), fill_value=-9999)
            var[:] = np.where(pd.isna(reaches_df[col]), -9999, vals)
        elif np.issubdtype(vals.dtype, np.floating):
            var = rch_grp.createVariable(
                col, "f8", ("num_reaches",), fill_value=-9999.0
            )
            var[:] = np.where(pd.isna(reaches_df[col]), -9999.0, vals)
        elif np.issubdtype(vals.dtype, np.bool_):
            var = rch_grp.createVariable(col, "i1", ("num_reaches",), fill_value=0)
            var[:] = vals.astype(np.int8)
        else:
            # Fallback: write as string
            var = rch_grp.createVariable(col, str, ("num_reaches",))
            str_vals = pd.Series(vals).fillna("").astype(str).values
            for i, s in enumerate(str_vals):
                var[i] = s

    # Reconstruct rch_id_up[4, N] and rch_id_dn[4, N] from topology table
    if not topo_df.empty:
        up_neighbors: list[list[int]] = [[] for _ in range(num_reaches)]
        dn_neighbors: list[list[int]] = [[] for _ in range(num_reaches)]

        for _, row in topo_df.iterrows():
            rid = int(row["reach_id"])
            idx = reach_id_to_idx.get(rid)
            if idx is None:
                continue
            nbr = int(row["neighbor_reach_id"])
            if row["direction"] == "up":
                up_neighbors[idx].append(nbr)
            else:
                dn_neighbors[idx].append(nbr)

        rch_id_up = _pad_array_2d(up_neighbors, 4)
        rch_id_dn = _pad_array_2d(dn_neighbors, 4)

        var_up = rch_grp.createVariable(
            "rch_id_up", "i8", ("num_rch_neighbors", "num_reaches"), fill_value=0
        )
        var_up[:] = rch_id_up

        var_dn = rch_grp.createVariable(
            "rch_id_dn", "i8", ("num_rch_neighbors", "num_reaches"), fill_value=0
        )
        var_dn[:] = rch_id_dn

    # SWOT orbits [75, num_reaches]
    if orbits_df is not None and not orbits_df.empty:
        max_orbits = 75
        rch_grp.createDimension("num_orbits", max_orbits)
        orbit_arr = np.zeros((max_orbits, num_reaches), dtype="U20")

        for _, row in orbits_df.iterrows():
            rid = int(row["reach_id"])
            idx = reach_id_to_idx.get(rid)
            if idx is None:
                continue
            orbit_idx = int(row.get("orbit_index", row.get("orbit_rank", 0)))
            if 0 <= orbit_idx < max_orbits:
                orbit_arr[orbit_idx, idx] = str(
                    row.get("pass_tile", row.get("orbit_id", ""))
                )

        var_orb = rch_grp.createVariable(
            "swot_orbits", str, ("num_orbits", "num_reaches")
        )
        for i in range(max_orbits):
            for j in range(num_reaches):
                var_orb[i, j] = orbit_arr[i, j]

    # Ice flags [366, num_reaches]
    if ice_df is not None and not ice_df.empty:
        rch_grp.createDimension("julian_day", 366)
        ice_arr = np.full((366, num_reaches), -9999, dtype=np.int16)

        for _, row in ice_df.iterrows():
            rid = int(row["reach_id"])
            idx = reach_id_to_idx.get(rid)
            if idx is None:
                continue
            day = int(row.get("day_of_year", row.get("julian_day", 0)))
            if 1 <= day <= 366:
                ice_arr[day - 1, idx] = int(
                    row.get("ice_flag", row.get("iceflag", -9999))
                )

        var_ice = rch_grp.createVariable(
            "iceflag_daily", "i2", ("julian_day", "num_reaches"), fill_value=-9999
        )
        var_ice[:] = ice_arr

    # --- Nodes group ---
    node_grp = ds.createGroup("nodes")
    node_grp.createDimension("num_nodes", num_nodes)

    for col in nodes_df.columns:
        if col in _SKIP_COLS:
            continue
        vals = nodes_df[col].values
        if col in _STRING_COLS or nodes_df[col].dtype == object:
            var = node_grp.createVariable(col, str, ("num_nodes",))
            str_vals = pd.Series(vals).fillna("").astype(str).values
            for i, s in enumerate(str_vals):
                var[i] = s
        elif np.issubdtype(vals.dtype, np.integer):
            var = node_grp.createVariable(col, "i8", ("num_nodes",), fill_value=-9999)
            var[:] = np.where(pd.isna(nodes_df[col]), -9999, vals)
        elif np.issubdtype(vals.dtype, np.floating):
            var = node_grp.createVariable(col, "f8", ("num_nodes",), fill_value=-9999.0)
            var[:] = np.where(pd.isna(nodes_df[col]), -9999.0, vals)
        elif np.issubdtype(vals.dtype, np.bool_):
            var = node_grp.createVariable(col, "i1", ("num_nodes",), fill_value=0)
            var[:] = vals.astype(np.int8)
        else:
            var = node_grp.createVariable(col, str, ("num_nodes",))
            str_vals = pd.Series(vals).fillna("").astype(str).values
            for i, s in enumerate(str_vals):
                var[i] = s

    # --- Centerlines group ---
    cl_grp = ds.createGroup("centerlines")
    cl_grp.createDimension("num_points", num_points)

    for col in centerlines_df.columns:
        if col in _SKIP_COLS:
            continue
        vals = centerlines_df[col].values
        if col in _STRING_COLS or centerlines_df[col].dtype == object:
            var = cl_grp.createVariable(col, str, ("num_points",))
            str_vals = pd.Series(vals).fillna("").astype(str).values
            for i, s in enumerate(str_vals):
                var[i] = s
        elif np.issubdtype(vals.dtype, np.integer):
            var = cl_grp.createVariable(col, "i8", ("num_points",), fill_value=-9999)
            var[:] = np.where(pd.isna(centerlines_df[col]), -9999, vals)
        elif np.issubdtype(vals.dtype, np.floating):
            var = cl_grp.createVariable(col, "f8", ("num_points",), fill_value=-9999.0)
            var[:] = np.where(pd.isna(centerlines_df[col]), -9999.0, vals)
        else:
            var = cl_grp.createVariable(col, str, ("num_points",))
            str_vals = pd.Series(vals).fillna("").astype(str).values
            for i, s in enumerate(str_vals):
                var[i] = s

    ds.close()
    logger.info(f"  [{region}] Wrote {out_path}")
    return out_path


# ---------------------------------------------------------------------------
# PostgreSQL export (delegate to load_from_duckdb.py)
# ---------------------------------------------------------------------------


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
                export_netcdf(con, region, output_dir)
    finally:
        con.close()

    elapsed = datetime.now() - start
    logger.info(f"\nExport complete. Elapsed: {elapsed}")


if __name__ == "__main__":
    main()
