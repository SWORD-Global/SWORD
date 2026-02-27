#!/usr/bin/env python3
"""Rebuild reach LineString geometries with endpoint overlap vertices.

Ports the legacy ``define_geometry()`` algorithm (src/_legacy/updates/
sword_utils.py:696) to work entirely from DuckDB tables.  No PostgreSQL
dependency — reads ``centerlines`` + ``centerline_neighbors`` + ``reaches``
and writes back reach geometries that share vertices at junctions, matching
the original SWORD shapefiles distributed by UNC.

Usage
-----
    # All regions
    python scripts/maintenance/rebuild_reach_geometry.py \
        --db data/duckdb/sword_v17c.duckdb --all

    # Single region
    python scripts/maintenance/rebuild_reach_geometry.py \
        --db data/duckdb/sword_v17c.duckdb --region NA

    # Dry run (report only, no writes)
    python scripts/maintenance/rebuild_reach_geometry.py \
        --db data/duckdb/sword_v17c.duckdb --region NA --dry-run

Reference
---------
Legacy implementation: src/_legacy/updates/sword_utils.py
    - find_common_points() (line 531)
    - define_geometry()    (line 696)
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from collections import defaultdict
from pathlib import Path

import duckdb
import numpy as np
from geopy.distance import geodesic
from shapely.geometry import LineString

REGIONS = ["NA", "SA", "EU", "AF", "AS", "OC"]
MAX_DIST_M = 500  # meters — matches production threshold (sword.py:148)
BATCH_SIZE = 5_000

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------


def load_region_data(
    con: duckdb.DuckDBPyConnection, region: str
) -> tuple[dict, dict, dict]:
    """Load centerline, neighbor, and reach data for one region."""
    primary = con.execute(
        """
        SELECT cl_id, x, y, reach_id
        FROM centerlines
        WHERE region = ?
        ORDER BY cl_id
        """,
        [region],
    ).fetchnumpy()

    neighbors = con.execute(
        """
        SELECT cl_id, neighbor_rank, reach_id
        FROM centerline_neighbors
        WHERE region = ? AND reach_id IS NOT NULL AND reach_id > 0
        ORDER BY cl_id, neighbor_rank
        """,
        [region],
    ).fetchnumpy()

    reach_attrs = con.execute(
        """
        SELECT reach_id, facc, wse, width
        FROM reaches
        WHERE region = ?
        """,
        [region],
    ).fetchnumpy()

    return primary, neighbors, reach_attrs


# ---------------------------------------------------------------------------
# Index construction
# ---------------------------------------------------------------------------


def build_indexes(
    primary: dict, neighbors: dict
) -> tuple[np.ndarray, dict, dict, dict]:
    """Build the [4,N] reach_id array and fast lookup dictionaries.

    Returns
    -------
    reach_id : ndarray, shape (4, N)
    cl_id_to_idx : dict  cl_id -> array index
    reach_primary : dict  reach_id -> sorted list of array indices
    reach_neighbor : dict  reach_id -> list of (array_idx, rank)
    """
    n = len(primary["cl_id"])
    cl_ids = primary["cl_id"]

    cl_id_to_idx: dict[int, int] = {}
    for i in range(n):
        cl_id_to_idx[int(cl_ids[i])] = i

    # Reconstruct [4, N]
    reach_id = np.zeros((4, n), dtype=np.int64)
    reach_id[0, :] = primary["reach_id"]

    for i in range(len(neighbors["cl_id"])):
        cid = int(neighbors["cl_id"][i])
        rank = int(neighbors["neighbor_rank"][i])
        rid = int(neighbors["reach_id"][i])
        if cid in cl_id_to_idx and 1 <= rank <= 3:
            reach_id[rank, cl_id_to_idx[cid]] = rid

    # reach_id -> sorted primary indices
    reach_primary: dict[int, list[int]] = defaultdict(list)
    for i in range(n):
        rid = int(reach_id[0, i])
        if rid > 0:
            reach_primary[rid].append(i)

    # reach_id -> neighbor indices  (centerlines that reference this reach
    # in rows 1-3, i.e. this reach sits at the boundary of these CLs)
    reach_neighbor: dict[int, list[tuple[int, int]]] = defaultdict(list)
    for i in range(n):
        for rank in range(1, 4):
            rid = int(reach_id[rank, i])
            if rid > 0:
                reach_neighbor[rid].append((i, rank))

    return reach_id, cl_id_to_idx, dict(reach_primary), dict(reach_neighbor)


# ---------------------------------------------------------------------------
# Common-point detection  (legacy: find_common_points)
# ---------------------------------------------------------------------------


def find_common_points(
    reach_id: np.ndarray,
    cl_x: np.ndarray,
    cl_y: np.ndarray,
    cl_id: np.ndarray,
    reach_attrs: dict,
    reach_primary: dict[int, list[int]],
) -> np.ndarray:
    """Identify junction points shared by 3+ reaches.

    A point is "common" if it sits at a ≥3-way junction.  The
    ``define_geometry`` step skips adding overlap vertices at common points
    because they are already shared naturally.

    Faithfully ports ``find_common_points()`` from sword_utils.py:531.
    """
    # Reach attribute lookups
    reach_facc: dict[int, float] = {}
    reach_wse: dict[int, float] = {}
    reach_wth: dict[int, float] = {}
    for i in range(len(reach_attrs["reach_id"])):
        rid = int(reach_attrs["reach_id"][i])
        reach_facc[rid] = float(reach_attrs["facc"][i])
        reach_wse[rid] = float(reach_attrs["wse"][i])
        reach_wth[rid] = float(reach_attrs["width"][i])

    n = reach_id.shape[1]
    valid_primary = set(int(x) for x in np.unique(reach_id[0, :]) if x > 0)

    # row_sums: how many of the 4 rows have non-zero reach_id
    binary = (reach_id > 0).astype(np.int8)
    row_sums = binary.sum(axis=0)

    multi_pts = np.where(row_sums > 2)[0]
    common = np.zeros(n, dtype=np.int8)

    for pt_idx in multi_pts:
        if common[pt_idx] == 1:
            continue

        # All reaches referencing this point (across rows 0-3)
        ngh_rows = np.where(reach_id[:, pt_idx] > 0)[0]
        nghs = reach_id[ngh_rows, pt_idx]
        nghs = np.array([r for r in nghs if r in valid_primary])

        if len(nghs) == 0:
            continue

        # Check if any neighbor endpoint is already common
        flag = [int(common[pt_idx])]
        for k in range(1, len(nghs)):
            ngh_rid = int(nghs[k])
            r_indices = reach_primary.get(ngh_rid, [])
            if len(r_indices) == 0:
                flag.append(0)
                continue
            r_cl_ids = cl_id[r_indices]
            mn = r_indices[np.argmin(r_cl_ids)]
            mx = r_indices[np.argmax(r_cl_ids)]

            d1 = geodesic((cl_y[pt_idx], cl_x[pt_idx]), (cl_y[mn], cl_x[mn])).m
            d2 = geodesic((cl_y[pt_idx], cl_x[pt_idx]), (cl_y[mx], cl_x[mx])).m

            flag.append(int(common[mn]) if d1 < d2 else int(common[mx]))

        if max(flag) == 1:
            continue

        # Determine dominant reach by facc > wse > width priority
        facc = np.array([reach_facc.get(int(r), -9999.0) for r in nghs])
        wse = np.array([reach_wse.get(int(r), -9999.0) for r in nghs])
        wth = np.array([reach_wth.get(int(r), -9999.0) for r in nghs])

        f = np.where(facc == np.max(facc))[0]
        h = np.where(wse == np.min(wse))[0]
        w = np.where(wth == np.max(wth))[0]

        if len(f) == 1:
            if f[0] == 0:
                common[pt_idx] = 1
        elif len(h) == 1:
            if h[0] == 0:
                common[pt_idx] = 1
        elif len(w) == 1:
            if w[0] == 0:
                common[pt_idx] = 1
        else:
            common[pt_idx] = 1

    return common


# ---------------------------------------------------------------------------
# Geometry builder  (legacy: define_geometry)
# ---------------------------------------------------------------------------


def build_geometries(
    reach_id: np.ndarray,
    cl_x: np.ndarray,
    cl_y: np.ndarray,
    cl_id: np.ndarray,
    common: np.ndarray,
    reach_primary: dict[int, list[int]],
    reach_neighbor: dict[int, list[tuple[int, int]]],
    max_dist: float,
    region: str,
) -> dict[int, LineString]:
    """Build reach LineStrings with overlap vertices at endpoints.

    Faithfully ports ``define_geometry()`` from sword_utils.py:696.
    Uses pre-built indexes for O(k)-per-reach instead of O(N).

    Parameters
    ----------
    reach_id : (4, N) array
    cl_x, cl_y, cl_id : (N,) arrays
    common : (N,) int8 array  (1 = common junction point)
    reach_primary : reach_id -> list of primary CL indices
    reach_neighbor : reach_id -> list of (CL index, rank) neighbor entries
    max_dist : threshold in meters for endpoint connection
    region : two-letter region code

    Returns
    -------
    dict  reach_id -> LineString
    """
    geom: dict[int, LineString] = {}
    used: set[tuple[int, int]] = set()  # (rank, cl_idx) pairs already used

    unq_rch = sorted(set(int(x) for x in reach_id[0, :] if x > 0))

    for rid in unq_rch:
        indices = reach_primary.get(rid, [])
        if len(indices) == 0:
            continue

        indices_arr = np.array(indices)
        sort_order = np.argsort(cl_id[indices_arr])
        sort_ind = indices_arr[sort_order]

        x_coords = cl_x[sort_ind].copy()
        y_coords = cl_y[sort_ind].copy()

        # Gather unused neighbor junction points for this reach
        in_rch_up_dn = []
        for cl_idx, rank in reach_neighbor.get(rid, []):
            if (rank, cl_idx) not in used:
                in_rch_up_dn.append(cl_idx)
        in_rch_up_dn = (
            np.unique(in_rch_up_dn) if in_rch_up_dn else np.array([], dtype=int)
        )

        if len(in_rch_up_dn) == 0:
            if len(x_coords) >= 2:
                geom[rid] = LineString(zip(x_coords, y_coords))
            continue

        # Classify each neighbor point as closer to start (end1) or end (end2)
        first_pt = (cl_y[sort_ind[0]], cl_x[sort_ind[0]])
        last_pt = (cl_y[sort_ind[-1]], cl_x[sort_ind[-1]])

        end1_pts, end2_pts = [], []

        for cl_idx in in_rch_up_dn:
            xp, yp = cl_x[cl_idx], cl_y[cl_idx]

            # Asia dateline edge case: Only skip if it's NOT an antimeridian wrap.
            # If distance is small (< 500m) but signs differ, it's a wrap.
            if region == "AS":
                dist_wrap = geodesic((yp, xp), first_pt).m
                # If they are on opposite sides of the dateline but far apart in
                # planar space, only connect if geodesic distance is small.
                if xp * first_pt[1] < 0 and abs(xp) > 170 and abs(first_pt[1]) > 170:
                    # This is likely a wrap - distance is already calculated by geodesic
                    pass
                elif xp < 0 and first_pt[1] > 0 and dist_wrap > max_dist:
                    continue
                elif xp > 0 and first_pt[1] < 0 and dist_wrap > max_dist:
                    continue

            d1 = geodesic((yp, xp), first_pt).m
            d2 = geodesic((yp, xp), last_pt).m

            entry = (cl_idx, d1 if d1 < d2 else d2, xp, yp, int(common[cl_idx]))
            if d1 < d2:
                end1_pts.append(entry)
            elif d1 > d2:
                end2_pts.append(entry)

        # --- Prepend overlap vertex to start (end1) ---
        if end1_pts and common[sort_ind[0]] != 1:
            end1_pts.sort(key=lambda e: e[1])  # sort by distance
            # Prefer common neighbor points
            common_idx = [i for i, e in enumerate(end1_pts) if e[4] == 1]
            idx1 = common_idx[0] if common_idx else 0
            chosen = end1_pts[idx1]

            if chosen[1] <= max_dist:
                x_coords = np.insert(x_coords, 0, chosen[2])
                y_coords = np.insert(y_coords, 0, chosen[3])
            else:
                # Fallback: closest point in the neighbor reach's own CLs
                ngh_reach = int(reach_id[0, chosen[0]])
                ngh_indices = reach_primary.get(ngh_reach, [])
                if ngh_indices:
                    ngh_x = cl_x[ngh_indices]
                    ngh_y = cl_y[ngh_indices]
                    d = [
                        geodesic(first_pt, (ngh_y[c], ngh_x[c])).m
                        for c in range(len(ngh_x))
                    ]
                    if min(d) <= max_dist:
                        best = int(np.argmin(d))
                        x_coords = np.insert(x_coords, 0, ngh_x[best])
                        y_coords = np.insert(y_coords, 0, ngh_y[best])

            # Mark connections as used: find CLs of the CURRENT reach that
            # reference the neighbor in rows 1-3, preventing the neighbor
            # from re-using them as overlap vertices when it is processed.
            # Legacy: connections[rank, col] where col has primary=current,
            # rank refs neighbor (sword_utils.py:834-846).
            ngh1 = int(reach_id[0, chosen[0]])
            for cl_idx_r in reach_primary.get(rid, []):
                for rank in range(1, 4):
                    if int(reach_id[rank, cl_idx_r]) == ngh1:
                        used.add((rank, cl_idx_r))

        # --- Append overlap vertex to end (end2) ---
        if end2_pts and common[sort_ind[-1]] != 1:
            end2_pts.sort(key=lambda e: e[1])
            common_idx = [i for i, e in enumerate(end2_pts) if e[4] == 1]
            idx2 = common_idx[0] if common_idx else 0
            chosen = end2_pts[idx2]

            if chosen[1] <= max_dist:
                x_coords = np.append(x_coords, chosen[2])
                y_coords = np.append(y_coords, chosen[3])
            else:
                ngh_reach = int(reach_id[0, chosen[0]])
                ngh_indices = reach_primary.get(ngh_reach, [])
                if ngh_indices:
                    ngh_x = cl_x[ngh_indices]
                    ngh_y = cl_y[ngh_indices]
                    d = [
                        geodesic(last_pt, (ngh_y[c], ngh_x[c])).m
                        for c in range(len(ngh_x))
                    ]
                    if min(d) <= max_dist:
                        best = int(np.argmin(d))
                        x_coords = np.append(x_coords, ngh_x[best])
                        y_coords = np.append(y_coords, ngh_y[best])

            ngh2 = int(reach_id[0, chosen[0]])
            for cl_idx_r in reach_primary.get(rid, []):
                for rank in range(1, 4):
                    if int(reach_id[rank, cl_idx_r]) == ngh2:
                        used.add((rank, cl_idx_r))

        # Build LineString
        if len(x_coords) >= 2:
            geom[rid] = LineString(zip(x_coords, y_coords))

    return geom


# ---------------------------------------------------------------------------
# Snap-endpoint post-processing
# ---------------------------------------------------------------------------


def snap_endpoints(
    con: duckdb.DuckDBPyConnection,
    geom: dict[int, LineString],
    cl_x: np.ndarray,
    cl_y: np.ndarray,
    reach_primary: dict[int, list[int]],
    region: str,
    snap_threshold: float = 500.0,
) -> int:
    """Post-processing: snap reach endpoints to neighbor CL points.

    For topology edges where ``build_geometries`` didn't produce an
    overlap vertex (typically because ``centerline_neighbors`` had no
    entry for that reach), extend the linestring by prepending/appending
    the closest primary centerline point from the neighbor reach.

    Only snaps when the current endpoint gap is > 1m AND the closest
    neighbor CL point is within ``snap_threshold`` meters.

    Returns the number of geometries modified.
    """
    edges = con.execute(
        """
        SELECT reach_id, neighbor_reach_id
        FROM reach_topology
        WHERE region = ? AND direction = 'down'
        """,
        [region],
    ).fetchall()

    modified = 0

    for rid, nid in edges:
        if rid not in geom or nid not in geom:
            continue

        g1 = geom[rid]
        g2 = geom[nid]
        coords1 = list(g1.coords)

        # Find which endpoints are closest between the two reaches
        s1, e1 = np.array(coords1[0]), np.array(coords1[-1])
        s2 = np.array(g2.coords[0])
        e2 = np.array(g2.coords[-1])

        # Use geodesic distance for accuracy at all latitudes
        d_e1s2 = geodesic((e1[1], e1[0]), (s2[1], s2[0])).m
        d_e1e2 = geodesic((e1[1], e1[0]), (e2[1], e2[0])).m
        d_s1s2 = geodesic((s1[1], s1[0]), (s2[1], s2[0])).m
        d_s1e2 = geodesic((s1[1], s1[0]), (e2[1], e2[0])).m

        gaps = [
            (d_e1s2, "e1-s2"),
            (d_e1e2, "e1-e2"),
            (d_s1s2, "s1-s2"),
            (d_s1e2, "s1-e2"),
        ]
        min_gap, closest = min(gaps, key=lambda x: x[0])

        if min_gap < 1.0:
            continue  # already connected

        # Determine which reach endpoint needs snapping and find the
        # closest primary CL point in the OTHER reach to snap to.
        snapped = False
        if closest in ("e1-s2", "e1-e2"):
            # End of reach 1 needs to connect to reach 2
            nid_indices = reach_primary.get(nid, [])
            if nid_indices:
                ngh_x, ngh_y = cl_x[nid_indices], cl_y[nid_indices]
                dists = np.array(
                    [
                        geodesic((e1[1], e1[0]), (ngh_y[c], ngh_x[c])).m
                        for c in range(len(ngh_x))
                    ]
                )
                if np.min(dists) < snap_threshold:
                    best = int(np.argmin(dists))
                    coords1.append((float(ngh_x[best]), float(ngh_y[best])))
                    geom[rid] = LineString(coords1)
                    snapped = True
        else:
            # Start of reach 1 needs to connect to reach 2
            nid_indices = reach_primary.get(nid, [])
            if nid_indices:
                ngh_x, ngh_y = cl_x[nid_indices], cl_y[nid_indices]
                dists = np.array(
                    [
                        geodesic((s1[1], s1[0]), (ngh_y[c], ngh_x[c])).m
                        for c in range(len(ngh_x))
                    ]
                )
                if np.min(dists) < snap_threshold:
                    best = int(np.argmin(dists))
                    coords1.insert(0, (float(ngh_x[best]), float(ngh_y[best])))
                    geom[rid] = LineString(coords1)
                    snapped = True

        if snapped:
            modified += 1

    return modified


# ---------------------------------------------------------------------------
# Write results back to DuckDB
# ---------------------------------------------------------------------------


def write_geometries(
    con: duckdb.DuckDBPyConnection,
    geom: dict[int, LineString],
) -> int:
    """UPDATE reaches SET geom = ... for all built geometries.

    Also recomputes bbox/centroid columns (x, y, x_min, x_max, y_min, y_max).
    """
    reach_ids = list(geom.keys())
    updated = 0

    for i in range(0, len(reach_ids), BATCH_SIZE):
        batch_ids = reach_ids[i : i + BATCH_SIZE]
        params = [(geom[rid].wkb, rid) for rid in batch_ids]
        con.executemany(
            "UPDATE reaches SET geom = ST_GeomFromWKB($1) WHERE reach_id = $2",
            params,
        )
        updated += len(batch_ids)
        if updated % 10_000 == 0 or updated == len(reach_ids):
            logger.info(f"  Written {updated:,} / {len(reach_ids):,} reaches")

    # Recompute bbox/centroid
    logger.info("Recomputing bbox/centroid columns...")
    con.execute(
        """
        UPDATE reaches SET
            x     = ST_X(ST_Centroid(geom)),
            y     = ST_Y(ST_Centroid(geom)),
            x_min = ST_XMin(geom),
            x_max = ST_XMax(geom),
            y_min = ST_YMin(geom),
            y_max = ST_YMax(geom)
        WHERE geom IS NOT NULL
        """
    )

    return updated


# ---------------------------------------------------------------------------
# Region processing
# ---------------------------------------------------------------------------


def process_region(
    con: duckdb.DuckDBPyConnection,
    region: str,
    max_dist: float,
    dry_run: bool = False,
) -> int:
    """Run the full geometry rebuild for one region."""
    t0 = time.time()
    logger.info(f"=== Region {region} ===")

    # Load data
    logger.info(f"[{region}] Loading centerline data...")
    primary, neighbors, reach_attrs = load_region_data(con, region)
    n_cl = len(primary["cl_id"])
    n_ngh = len(neighbors["cl_id"])
    n_rch = len(reach_attrs["reach_id"])
    logger.info(
        f"[{region}] {n_cl:,} centerlines, {n_ngh:,} neighbor entries, "
        f"{n_rch:,} reaches"
    )

    if n_cl == 0:
        logger.warning(f"[{region}] No centerline data — skipping")
        return 0

    # Build indexes
    logger.info(f"[{region}] Building indexes...")
    reach_id, _cl_id_to_idx, reach_primary, reach_neighbor = build_indexes(
        primary, neighbors
    )
    cl_x = primary["x"]
    cl_y = primary["y"]
    cl_id = primary["cl_id"]

    # Find common points
    logger.info(f"[{region}] Computing common junction points...")
    common = find_common_points(reach_id, cl_x, cl_y, cl_id, reach_attrs, reach_primary)
    n_common = int(common.sum())
    logger.info(f"[{region}] {n_common:,} common junction points identified")

    # Build geometries
    logger.info(f"[{region}] Building reach geometries with overlap vertices...")
    geom = build_geometries(
        reach_id,
        cl_x,
        cl_y,
        cl_id,
        common,
        reach_primary,
        reach_neighbor,
        max_dist,
        region,
    )
    logger.info(f"[{region}] Built {len(geom):,} reach geometries")

    # Snap endpoints for reaches that didn't get overlap vertices
    logger.info(f"[{region}] Snapping remaining endpoint gaps...")
    n_snapped = snap_endpoints(con, geom, cl_x, cl_y, reach_primary, region)
    logger.info(f"[{region}] Snapped {n_snapped:,} additional endpoints")

    if dry_run:
        logger.info(f"[{region}] Dry run — skipping write")
        return 0

    # Write
    logger.info(f"[{region}] Writing geometries to DuckDB...")
    updated = write_geometries(con, geom)

    elapsed = time.time() - t0
    logger.info(f"[{region}] Done in {elapsed:.1f}s — {updated:,} reaches updated")
    return updated


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(
        description="Rebuild reach geometries with endpoint overlap vertices",
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
        "--max-dist",
        type=float,
        default=MAX_DIST_M,
        help=f"Max distance (m) for endpoint connection (default: {MAX_DIST_M})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Build geometries but don't write to database",
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

    # Drop RTREE indexes before updates
    rtree_indexes = []
    if not args.dry_run:
        rtree_indexes = con.execute(
            "SELECT index_name, table_name, sql FROM duckdb_indexes() "
            "WHERE sql LIKE '%RTREE%' AND table_name = 'reaches'"
        ).fetchall()
        for idx_name, _tbl, _sql in rtree_indexes:
            logger.info(f"Dropping RTREE index: {idx_name}")
            con.execute(f'DROP INDEX "{idx_name}"')

    total = 0
    for region in regions:
        total += process_region(con, region, args.max_dist, args.dry_run)

    # Recreate RTREE indexes
    if not args.dry_run:
        for idx_name, _tbl, create_sql in rtree_indexes:
            logger.info(f"Recreating RTREE index: {idx_name}")
            con.execute(create_sql)

    # Verify
    if not args.dry_run:
        non_null = con.execute(
            "SELECT COUNT(*) FROM reaches WHERE geom IS NOT NULL"
        ).fetchone()[0]
        total_rows = con.execute("SELECT COUNT(*) FROM reaches").fetchone()[0]
        logger.info(
            f"Verification: {non_null:,} / {total_rows:,} reaches have geometry"
        )

    con.close()
    logger.info(f"Done. {total:,} reaches updated across {len(regions)} region(s).")


if __name__ == "__main__":
    main()
