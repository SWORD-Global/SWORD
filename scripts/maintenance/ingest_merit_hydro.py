#!/usr/bin/env python3
"""Ingest MERIT Hydro raster data into SWORD node attributes.

Reads MERIT Hydro v1.0.1 TIFFs (elv, upa, wth) from disk and assigns
per-node wse, facc, and width values via nearest-neighbor lookup, then
writes results back to nodes in v17c.duckdb.

Data layout on disk:
    {MERIT_ROOT}/{REGION}/elv/{zone_dir}/{tile}_elv.tif
    {MERIT_ROOT}/{REGION}/upa/{zone_dir}/{tile}_upa.tif
    {MERIT_ROOT}/{REGION}/wth/{zone_dir}/{tile}_wth.tif

Algorithm (per tile):
    1. Bbox-filter SWORD nodes to those within tile extent
    2. Read elv/upa/wth arrays via rasterio
    3. Keep only pixels with upa >= FACC_MIN_KM2 and no NoData
    4. cKDTree k=1 nearest-neighbor from MERIT pixels to SWORD nodes
    5. Assign elv -> wse, upa -> facc, wth -> width where distance < MAX_DIST_DEG
    6. Bulk UPDATE DuckDB

Usage:
    python scripts/maintenance/ingest_merit_hydro.py --region NA
    python scripts/maintenance/ingest_merit_hydro.py --all
    python scripts/maintenance/ingest_merit_hydro.py --region NA --dry-run
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import rasterio
from scipy.spatial import cKDTree

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

MERIT_ROOT = Path("/Volumes/SWORD_DATA/data/MERIT_Hydro")
DB_PATH = Path("data/duckdb/sword_v17c.duckdb")

FACC_MIN_KM2 = 10.0  # only pixels with meaningful flow accumulation
MAX_DIST_DEG = 0.01  # ~1 km at equator — reject truly isolated nodes
NODATA_SENTINEL = -9999.0

REGIONS = ("NA", "SA", "EU", "AF", "AS", "OC")

# SWORD region -> DuckDB region string
REGION_MAP = {r: r for r in REGIONS}


# ---------------------------------------------------------------------------
# Tile discovery
# ---------------------------------------------------------------------------


def find_tiles(
    region: str, merit_root: Path = MERIT_ROOT
) -> list[tuple[Path, Path, Path]]:
    """Return (elv_tif, upa_tif, wth_tif) triples for all tiles in a region."""
    elv_root = merit_root / region / "elv"
    upa_root = merit_root / region / "upa"
    wth_root = merit_root / region / "wth"

    if not elv_root.exists():
        raise FileNotFoundError(f"MERIT Hydro elv directory not found: {elv_root}")

    triples: list[tuple[Path, Path, Path]] = []
    for zone_dir in sorted(elv_root.iterdir()):
        if not zone_dir.is_dir():
            continue
        for elv_tif in sorted(zone_dir.glob("*_elv.tif")):
            stem = elv_tif.stem.replace("_elv", "")
            zone_name = zone_dir.name.replace("elv_", "")
            upa_tif = upa_root / f"upa_{zone_name}" / f"{stem}_upa.tif"
            wth_tif = wth_root / f"wth_{zone_name}" / f"{stem}_wth.tif"
            if upa_tif.exists() and wth_tif.exists():
                triples.append((elv_tif, upa_tif, wth_tif))
            else:
                log.warning("Missing upa/wth for %s — skipping", elv_tif.name)

    log.info("Found %d complete tiles for region %s", len(triples), region)
    return triples


# ---------------------------------------------------------------------------
# Node loading
# ---------------------------------------------------------------------------


def load_nodes(con: duckdb.DuckDBPyConnection, region: str) -> pd.DataFrame:
    """Load node_id, x, y for all nodes in a region."""
    return con.execute(
        "SELECT node_id, x, y FROM nodes WHERE region = ? ORDER BY node_id",
        [region],
    ).df()


# ---------------------------------------------------------------------------
# Per-tile processing
# ---------------------------------------------------------------------------


def _read_band(src: rasterio.DatasetReader, band: int) -> tuple[np.ndarray, float]:
    """Read a rasterio band, returning (array, nodata_value)."""
    data = src.read(band).astype(np.float64)
    nodata = src.nodata if src.nodata is not None else NODATA_SENTINEL
    return data, float(nodata)


def process_tile(
    elv_tif: Path,
    upa_tif: Path,
    wth_tif: Path,
    nodes: pd.DataFrame,
) -> pd.DataFrame | None:
    """Assign MERIT values to nodes overlapping this tile.

    Returns a DataFrame with columns (node_id, wse, facc, width) for nodes
    that got an assignment, or None if no nodes overlap.
    """
    with rasterio.open(elv_tif) as src:
        bounds = src.bounds  # (left, bottom, right, top)
        transform = src.transform
        nrows, ncols = src.height, src.width

    # Bbox filter nodes
    mask = (
        (nodes["x"] >= bounds.left)
        & (nodes["x"] <= bounds.right)
        & (nodes["y"] >= bounds.bottom)
        & (nodes["y"] <= bounds.top)
    )
    local_nodes = nodes[mask]
    if local_nodes.empty:
        return None

    # Read all three bands
    with rasterio.open(elv_tif) as src:
        elv_arr, elv_nd = _read_band(src, 1)
    with rasterio.open(upa_tif) as src:
        upa_arr, upa_nd = _read_band(src, 1)
    with rasterio.open(wth_tif) as src:
        wth_arr, wth_nd = _read_band(src, 1)

    # Build pixel coordinate arrays
    rows, cols = np.meshgrid(np.arange(nrows), np.arange(ncols), indexing="ij")
    # rasterio transform: x = left + col*xres, y = top + row*yres (yres negative)
    xres = transform.a
    yres = transform.e  # negative
    px_lon = bounds.left + (cols + 0.5) * xres
    px_lat = bounds.top + (rows + 0.5) * yres

    px_lon = px_lon.flatten()
    px_lat = px_lat.flatten()
    elv_flat = elv_arr.flatten()
    upa_flat = upa_arr.flatten()
    wth_flat = wth_arr.flatten()

    # Keep only valid pixels with sufficient flow accumulation
    valid = (
        (upa_flat >= FACC_MIN_KM2)
        & (elv_flat != elv_nd)
        & np.isfinite(elv_flat)
        & (upa_flat != upa_nd)
        & np.isfinite(upa_flat)
        & (wth_flat != wth_nd)
        & np.isfinite(wth_flat)
        & (wth_flat > 0)
    )
    if not valid.any():
        return None

    mh_pts = np.column_stack((px_lon[valid], px_lat[valid]))
    mh_elv = elv_flat[valid]
    mh_upa = upa_flat[valid]
    mh_wth = wth_flat[valid]

    # KDTree nearest-neighbor
    kdt = cKDTree(mh_pts)
    node_pts = local_nodes[["x", "y"]].values
    dists, idxs = kdt.query(node_pts, k=1)

    # Reject nodes too far from any valid MERIT pixel
    close = dists < MAX_DIST_DEG

    result = pd.DataFrame(
        {
            "node_id": local_nodes["node_id"].values[close],
            "wse": mh_elv[idxs[close]],
            "facc": mh_upa[idxs[close]],
            "width": mh_wth[idxs[close]],
        }
    )
    return result if not result.empty else None


# ---------------------------------------------------------------------------
# DuckDB update
# ---------------------------------------------------------------------------


def update_nodes(
    con: duckdb.DuckDBPyConnection,
    results: pd.DataFrame,
    region: str,
    dry_run: bool,
) -> int:
    """Bulk-update nodes from a results DataFrame. Returns row count updated."""
    if results.empty:
        return 0

    if dry_run:
        log.info("[dry-run] Would update %d nodes in %s", len(results), region)
        return len(results)

    # Register as a temporary view for the UPDATE
    con.register("_merit_updates", results)
    con.execute(
        """
        UPDATE nodes
        SET wse   = u.wse,
            facc  = u.facc,
            width = u.width
        FROM _merit_updates u
        WHERE nodes.node_id = u.node_id
          AND nodes.region  = ?
    """,
        [region],
    )
    con.unregister("_merit_updates")
    return len(results)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def run_region(
    con: duckdb.DuckDBPyConnection,
    region: str,
    dry_run: bool,
    merit_root: Path = MERIT_ROOT,
) -> None:
    log.info("=== Region %s ===", region)

    tiles = find_tiles(region, merit_root)
    nodes = load_nodes(con, region)
    log.info("Loaded %d nodes for %s", len(nodes), region)

    all_results: list[pd.DataFrame] = []
    n_tiles_hit = 0

    for i, (elv_tif, upa_tif, wth_tif) in enumerate(tiles, 1):
        if i % 50 == 0 or i == len(tiles):
            log.info("  Tile %d/%d ...", i, len(tiles))
        result = process_tile(elv_tif, upa_tif, wth_tif, nodes)
        if result is not None:
            all_results.append(result)
            n_tiles_hit += 1

    if not all_results:
        log.warning("No nodes matched any MERIT tiles for region %s", region)
        return

    # Merge: later tiles overwrite earlier ones for the same node_id
    combined = (
        pd.concat(all_results, ignore_index=True)
        .sort_values("node_id")
        .drop_duplicates(subset="node_id", keep="last")
    )

    log.info(
        "  %d tiles matched, %d unique nodes to update", n_tiles_hit, len(combined)
    )

    n_updated = update_nodes(con, combined, region, dry_run)
    log.info("  Updated %d nodes in %s", n_updated, region)


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest MERIT Hydro into SWORD nodes")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--region", choices=REGIONS, help="Single region to process")
    group.add_argument("--all", action="store_true", help="Process all regions")
    parser.add_argument("--db", default=str(DB_PATH), help="Path to v17c DuckDB")
    parser.add_argument(
        "--merit-root",
        default=str(MERIT_ROOT),
        help="MERIT Hydro root directory",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report what would be updated without writing",
    )
    args = parser.parse_args()

    MERIT_ROOT = Path(args.merit_root)

    if not MERIT_ROOT.exists():
        log.error("MERIT Hydro root not found: %s", MERIT_ROOT)
        sys.exit(1)

    db_path = Path(args.db)
    if not db_path.exists():
        log.error("DuckDB not found: %s", db_path)
        sys.exit(1)

    regions = REGIONS if args.all else [args.region]

    read_only = args.dry_run
    con = duckdb.connect(str(db_path), read_only=read_only)

    for region in regions:
        run_region(con, region, args.dry_run, merit_root=MERIT_ROOT)

    if not args.dry_run:
        log.info("All done. Committing.")

    con.close()


if __name__ == "__main__":
    main()
