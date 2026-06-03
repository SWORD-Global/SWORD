#!/usr/bin/env python3
"""Revert v17c node geometry fields to the v17b NetCDF baseline.

This is the guarded 0.0.12 repair path for the D0-D2 continuity issue.
Default mode is audit/dry-run only. Use --execute only after the audit output
has been reviewed.

Restored fields:
  - nodes.x, nodes.y, nodes.geom
  - nodes.node_length
  - nodes.cl_id_min, nodes.cl_id_max
  - centerlines.node_id for reaches with node geometry diffs
  - reach scalar geometry metadata when it differs from v17b

Preserved fields:
  - node_id, reach_id, region
  - nodes.dist_out and node_order
  - v17c analytical/statistical columns
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import netCDF4 as nc
import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DB = PROJECT_ROOT / "data" / "duckdb" / "sword_v17c.duckdb"
DEFAULT_V17B_NETCDF = PROJECT_ROOT / "data" / "netcdf"
REGIONS = ("NA", "SA", "EU", "AF", "AS", "OC")
KNOWN_EXTRA_DIFF_REACHES = {("OC", 51111300061)}

OP_REACH_SQL = """
SELECT DISTINCT
    CAST(json_extract_string(operation_details, '$.reach_id') AS BIGINT) AS reach_id,
    region,
    reason
FROM sword_operations
WHERE (reason LIKE '%scrambled%' OR reason LIKE '%POM node geolocation%')
  AND status = 'COMPLETED'
  AND json_extract_string(operation_details, '$.reach_id') IS NOT NULL
ORDER BY region, reach_id
"""


@dataclass
class RegionAudit:
    region: str
    node_rows: int
    target_node_rows: int
    target_reaches: int
    target_rows_in_ops_reaches: int
    target_rows_extra_known: int
    coordinate_changed_target_rows: int
    global_coordinate_diff_rows: int
    global_geometry_field_diff_rows: int
    unexpected_coordinate_diff_rows: int
    moved_gt_100m: int
    max_move_m: float
    max_move_node_id: int | None
    centerline_rows_checked: int
    centerline_node_id_diffs: int
    reach_scalar_diff_rows: int
    orphan_v17b_nodes: int
    orphan_v17c_nodes: int
    orphan_v17b_centerlines: int
    orphan_v17c_centerlines: int
    orphan_v17b_reaches: int
    orphan_v17c_reaches: int


def _as_array(var, dtype) -> np.ndarray:
    arr = var[:]
    if hasattr(arr, "filled"):
        arr = arr.filled()
    return np.asarray(arr, dtype=dtype)


def _haversine_m(
    lon1: np.ndarray,
    lat1: np.ndarray,
    lon2: np.ndarray,
    lat2: np.ndarray,
) -> np.ndarray:
    radius_m = 6_371_000.0
    p1 = np.radians(lat1)
    p2 = np.radians(lat2)
    dp = np.radians(lat2 - lat1)
    dl = np.radians(lon2 - lon1)
    a = np.sin(dp / 2.0) ** 2 + np.cos(p1) * np.cos(p2) * np.sin(dl / 2.0) ** 2
    return radius_m * 2.0 * np.arcsin(np.sqrt(a))


def read_v17b_nodes(nc_path: Path) -> pd.DataFrame:
    with nc.Dataset(str(nc_path), "r") as ds:
        group = ds.groups["nodes"]
        cl_ids = _as_array(group.variables["cl_ids"], np.int64)
        return pd.DataFrame(
            {
                "node_id": _as_array(group.variables["node_id"], np.int64),
                "reach_id_b": _as_array(group.variables["reach_id"], np.int64),
                "x_b": _as_array(group.variables["x"], np.float64),
                "y_b": _as_array(group.variables["y"], np.float64),
                "node_length_b": _as_array(group.variables["node_length"], np.float64),
                "cl_id_min_b": cl_ids[0, :],
                "cl_id_max_b": cl_ids[1, :],
            }
        )


def read_v17b_centerlines_for_reaches(
    nc_path: Path, reach_ids: set[int]
) -> pd.DataFrame:
    if not reach_ids:
        return pd.DataFrame(columns=["cl_id", "reach_id_b", "node_id_b"])

    with nc.Dataset(str(nc_path), "r") as ds:
        group = ds.groups["centerlines"]
        reach_id = _as_array(group.variables["reach_id"], np.int64)[0, :]
        mask = np.isin(reach_id, list(reach_ids))
        node_id = _as_array(group.variables["node_id"], np.int64)[0, :]
        return pd.DataFrame(
            {
                "cl_id": _as_array(group.variables["cl_id"], np.int64)[mask],
                "reach_id_b": reach_id[mask],
                "node_id_b": node_id[mask],
            }
        )


def read_v17b_reaches(nc_path: Path) -> pd.DataFrame:
    with nc.Dataset(str(nc_path), "r") as ds:
        group = ds.groups["reaches"]
        cl_ids = _as_array(group.variables["cl_ids"], np.int64)
        return pd.DataFrame(
            {
                "reach_id": _as_array(group.variables["reach_id"], np.int64),
                "x_b": _as_array(group.variables["x"], np.float64),
                "y_b": _as_array(group.variables["y"], np.float64),
                "x_min_b": _as_array(group.variables["x_min"], np.float64),
                "x_max_b": _as_array(group.variables["x_max"], np.float64),
                "y_min_b": _as_array(group.variables["y_min"], np.float64),
                "y_max_b": _as_array(group.variables["y_max"], np.float64),
                "reach_length_b": _as_array(
                    group.variables["reach_length"], np.float64
                ),
                "cl_id_min_b": cl_ids[0, :],
                "cl_id_max_b": cl_ids[1, :],
                "n_nodes_b": _as_array(group.variables["n_nodes"], np.int64),
            }
        )


def get_operation_reaches(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    return con.execute(OP_REACH_SQL).fetchdf()


def node_diffs_for_region(
    con: duckdb.DuckDBPyConnection,
    region: str,
    nc_path: Path,
    op_reaches: set[tuple[str, int]],
    xy_tol: float,
    length_tol: float,
) -> tuple[pd.DataFrame, int, int, dict[str, int]]:
    v17b = read_v17b_nodes(nc_path)
    v17c = con.execute(
        """
        SELECT node_id, reach_id AS reach_id_c, region, x AS x_c, y AS y_c,
               node_length AS node_length_c,
               cl_id_min AS cl_id_min_c,
               cl_id_max AS cl_id_max_c
        FROM nodes
        WHERE region = ?
        """,
        [region],
    ).fetchdf()

    merged = v17b.merge(v17c, on="node_id", how="outer", indicator=True)
    orphan_v17b = int((merged["_merge"] == "left_only").sum())
    orphan_v17c = int((merged["_merge"] == "right_only").sum())
    both = merged[merged["_merge"] == "both"].copy()

    both["region"] = region
    both["reach_id"] = both["reach_id_c"].astype(np.int64)
    both["x_diff"] = (np.abs(both["x_b"] - both["x_c"]) > xy_tol).to_numpy()
    both["y_diff"] = (np.abs(both["y_b"] - both["y_c"]) > xy_tol).to_numpy()
    both["xy_diff"] = both["x_diff"] | both["y_diff"]
    both["node_length_diff"] = (
        np.abs(both["node_length_b"] - both["node_length_c"]) > length_tol
    ).to_numpy()
    both["cl_ids_diff"] = (
        (both["cl_id_min_b"] != both["cl_id_min_c"])
        | (both["cl_id_max_b"] != both["cl_id_max_c"])
    ).to_numpy()
    both["geometry_field_diff"] = (
        both["xy_diff"] | both["node_length_diff"] | both["cl_ids_diff"]
    )
    both["reach_mismatch"] = both["reach_id_b"] != both["reach_id_c"]
    both["in_operation_reach"] = [
        (region, int(reach_id)) in op_reaches for reach_id in both["reach_id"]
    ]
    both["known_extra_diff"] = [
        (region, int(reach_id)) in KNOWN_EXTRA_DIFF_REACHES
        for reach_id in both["reach_id"]
    ]

    # Revert target:
    #   - every node in the 344 operation-log rederived reaches, because
    #     rederive_nodes rewrote whole reaches
    #   - only coordinate-different nodes in the known OC split-revert residue
    target_mask = both["in_operation_reach"] | (
        both["known_extra_diff"] & both["xy_diff"]
    )
    diff = both[target_mask].copy()

    stats = {
        "global_coordinate_diff_rows": int(both["xy_diff"].sum()),
        "global_geometry_field_diff_rows": int(both["geometry_field_diff"].sum()),
        "unexpected_coordinate_diff_rows": int((both["xy_diff"] & ~target_mask).sum()),
    }

    if not diff.empty:
        diff["distance_moved_m"] = _haversine_m(
            diff["x_b"].to_numpy(),
            diff["y_b"].to_numpy(),
            diff["x_c"].to_numpy(),
            diff["y_c"].to_numpy(),
        )

    columns = [
        "node_id",
        "region",
        "reach_id",
        "x_b",
        "y_b",
        "node_length_b",
        "cl_id_min_b",
        "cl_id_max_b",
        "x_c",
        "y_c",
        "node_length_c",
        "cl_id_min_c",
        "cl_id_max_c",
        "distance_moved_m",
        "in_operation_reach",
        "known_extra_diff",
        "reach_mismatch",
        "xy_diff",
        "node_length_diff",
        "cl_ids_diff",
        "geometry_field_diff",
    ]
    return diff[columns], orphan_v17b, orphan_v17c, stats


def centerline_diffs_for_region(
    con: duckdb.DuckDBPyConnection,
    region: str,
    nc_path: Path,
    reach_ids: set[int],
) -> tuple[pd.DataFrame, int, int, int]:
    v17b = read_v17b_centerlines_for_reaches(nc_path, reach_ids)
    if v17b.empty:
        return pd.DataFrame(), 0, 0, 0

    reach_df = pd.DataFrame({"reach_id": sorted(reach_ids)})
    con.register("target_reaches", reach_df)
    try:
        v17c = con.execute(
            """
            SELECT cl_id, reach_id AS reach_id_c, region, node_id AS node_id_c
            FROM centerlines
            WHERE region = ?
              AND reach_id IN (SELECT reach_id FROM target_reaches)
            """,
            [region],
        ).fetchdf()
    finally:
        con.unregister("target_reaches")

    merged = v17b.merge(v17c, on="cl_id", how="outer", indicator=True)
    orphan_v17b = int((merged["_merge"] == "left_only").sum())
    orphan_v17c = int((merged["_merge"] == "right_only").sum())
    both = merged[merged["_merge"] == "both"].copy()
    diff = both[both["node_id_b"] != both["node_id_c"]].copy()
    if not diff.empty:
        diff["region"] = region
    columns = ["cl_id", "region", "reach_id_b", "node_id_b", "reach_id_c", "node_id_c"]
    return diff[columns], len(both), orphan_v17b, orphan_v17c


def reach_scalar_diffs_for_region(
    con: duckdb.DuckDBPyConnection,
    region: str,
    nc_path: Path,
    xy_tol: float,
    length_tol: float,
) -> tuple[pd.DataFrame, int, int]:
    v17b = read_v17b_reaches(nc_path)
    v17c = con.execute(
        """
        SELECT reach_id, region,
               x AS x_c, y AS y_c,
               x_min AS x_min_c, x_max AS x_max_c,
               y_min AS y_min_c, y_max AS y_max_c,
               reach_length AS reach_length_c,
               cl_id_min AS cl_id_min_c,
               cl_id_max AS cl_id_max_c,
               n_nodes AS n_nodes_c
        FROM reaches
        WHERE region = ?
        """,
        [region],
    ).fetchdf()
    merged = v17b.merge(v17c, on="reach_id", how="outer", indicator=True)
    orphan_v17b = int((merged["_merge"] == "left_only").sum())
    orphan_v17c = int((merged["_merge"] == "right_only").sum())
    both = merged[merged["_merge"] == "both"].copy()

    diff = np.zeros(len(both), dtype=bool)
    for col in ("x", "y", "x_min", "x_max", "y_min", "y_max"):
        diff |= np.abs(both[f"{col}_b"] - both[f"{col}_c"]) > xy_tol
    diff |= np.abs(both["reach_length_b"] - both["reach_length_c"]) > length_tol
    diff |= both["cl_id_min_b"] != both["cl_id_min_c"]
    diff |= both["cl_id_max_b"] != both["cl_id_max_c"]
    diff |= both["n_nodes_b"] != both["n_nodes_c"]

    out = both[diff].copy()
    if not out.empty:
        out["region"] = region
    columns = [
        "reach_id",
        "region",
        "x_b",
        "y_b",
        "x_min_b",
        "x_max_b",
        "y_min_b",
        "y_max_b",
        "reach_length_b",
        "cl_id_min_b",
        "cl_id_max_b",
        "n_nodes_b",
    ]
    return out[columns], orphan_v17b, orphan_v17c


def apply_region_updates(
    con: duckdb.DuckDBPyConnection,
    node_diffs: pd.DataFrame,
    centerline_diffs: pd.DataFrame,
    reach_diffs: pd.DataFrame,
) -> tuple[int, int, int]:
    node_updates = node_diffs[
        [
            "node_id",
            "region",
            "x_b",
            "y_b",
            "node_length_b",
            "cl_id_min_b",
            "cl_id_max_b",
        ]
    ].rename(
        columns={
            "x_b": "x",
            "y_b": "y",
            "node_length_b": "node_length",
            "cl_id_min_b": "cl_id_min",
            "cl_id_max_b": "cl_id_max",
        }
    )
    con.register("node_updates", node_updates)
    try:
        node_count = con.execute(
            """
            UPDATE nodes
            SET x = u.x,
                y = u.y,
                geom = ST_Point(u.x, u.y),
                node_length = u.node_length,
                cl_id_min = u.cl_id_min,
                cl_id_max = u.cl_id_max
            FROM node_updates u
            WHERE nodes.node_id = u.node_id
              AND nodes.region = u.region
            """
        ).fetchone()[0]
    finally:
        con.unregister("node_updates")

    cl_count = 0
    if not centerline_diffs.empty:
        cl_updates = centerline_diffs[["cl_id", "region", "node_id_b"]].rename(
            columns={"node_id_b": "node_id"}
        )
        con.register("centerline_updates", cl_updates)
        try:
            cl_count = con.execute(
                """
                UPDATE centerlines
                SET node_id = u.node_id
                FROM centerline_updates u
                WHERE centerlines.cl_id = u.cl_id
                  AND centerlines.region = u.region
                """
            ).fetchone()[0]
        finally:
            con.unregister("centerline_updates")

    reach_count = 0
    if not reach_diffs.empty:
        reach_updates = reach_diffs.rename(
            columns={
                "x_b": "x",
                "y_b": "y",
                "x_min_b": "x_min",
                "x_max_b": "x_max",
                "y_min_b": "y_min",
                "y_max_b": "y_max",
                "reach_length_b": "reach_length",
                "cl_id_min_b": "cl_id_min",
                "cl_id_max_b": "cl_id_max",
                "n_nodes_b": "n_nodes",
            }
        )
        con.register("reach_updates", reach_updates)
        try:
            reach_count = con.execute(
                """
                UPDATE reaches
                SET x = u.x,
                    y = u.y,
                    x_min = u.x_min,
                    x_max = u.x_max,
                    y_min = u.y_min,
                    y_max = u.y_max,
                    reach_length = u.reach_length,
                    cl_id_min = u.cl_id_min,
                    cl_id_max = u.cl_id_max,
                    n_nodes = u.n_nodes
                FROM reach_updates u
                WHERE reaches.reach_id = u.reach_id
                  AND reaches.region = u.region
                """
            ).fetchone()[0]
        finally:
            con.unregister("reach_updates")

    return int(node_count), int(cl_count), int(reach_count)


def assert_apply_scope_is_known(
    node_diffs: pd.DataFrame, audits: list[RegionAudit]
) -> None:
    if node_diffs.empty:
        raise RuntimeError("No target node rows found; refusing to execute.")

    unexpected_coordinate_rows = sum(
        audit.unexpected_coordinate_diff_rows for audit in audits
    )
    if unexpected_coordinate_rows:
        raise RuntimeError(
            "Unexpected coordinate diffs outside rederive ops and known OC "
            f"split-revert residue: {unexpected_coordinate_rows:,}; refusing to execute."
        )

    reach_mismatches = node_diffs[node_diffs["reach_mismatch"]]
    if not reach_mismatches.empty:
        sample = reach_mismatches[["region", "reach_id", "node_id"]].head(20)
        raise RuntimeError(
            "Node reach_id differs between v17b and v17c; refusing to execute. "
            f"Sample:\n{sample}"
        )


def print_summary(
    audits: list[RegionAudit],
    all_node_diffs: pd.DataFrame,
    all_centerline_diffs: pd.DataFrame,
    all_reach_diffs: pd.DataFrame,
) -> None:
    print("\nRegion audit")
    print("-" * 100)
    for audit in audits:
        print(
            f"{audit.region}: target_nodes={audit.target_node_rows:,} "
            f"target_reaches={audit.target_reaches:,} "
            f"coord_diffs={audit.global_coordinate_diff_rows:,} "
            f">100m={audit.moved_gt_100m:,} "
            f"cl_node_id_diffs={audit.centerline_node_id_diffs:,} "
            f"reach_scalar_diffs={audit.reach_scalar_diff_rows:,} "
            f"unexpected_coord_rows={audit.unexpected_coordinate_diff_rows:,}"
        )

    total_nodes = len(all_node_diffs)
    total_reaches = (
        all_node_diffs[["region", "reach_id"]].drop_duplicates().shape[0]
        if total_nodes
        else 0
    )
    coordinate_changed_target_rows = (
        int(all_node_diffs["xy_diff"].sum()) if total_nodes else 0
    )
    gt_100 = int((all_node_diffs["distance_moved_m"] > 100.0).sum())
    max_row = None
    if total_nodes:
        max_row = all_node_diffs.loc[all_node_diffs["distance_moved_m"].idxmax()]

    print("\nRevert target")
    print("-" * 100)
    print(f"Target node rows to restore: {total_nodes:,}")
    print(f"Target reaches: {total_reaches:,}")
    print(f"Target rows with coordinate diffs: {coordinate_changed_target_rows:,}")
    print(f"Target rows moved >100 m by spherical distance: {gt_100:,}")
    if max_row is not None:
        print(
            "Max move: "
            f"{float(max_row['distance_moved_m']):,.2f} m "
            f"node_id={int(max_row['node_id'])} "
            f"reach_id={int(max_row['reach_id'])} "
            f"region={max_row['region']}"
        )

    print("\nKnown-scope split")
    print("-" * 100)
    print(
        "Rows in rederive operation reaches: "
        f"{int(all_node_diffs['in_operation_reach'].sum()):,}"
    )
    print(
        "Rows in known extra OC split-revert residue: "
        f"{int(all_node_diffs['known_extra_diff'].sum()):,}"
    )
    unexpected_coordinate_rows = sum(
        audit.unexpected_coordinate_diff_rows for audit in audits
    )
    print(f"Unexpected coordinate diff rows: {unexpected_coordinate_rows:,}")

    print("\nGlobal audit counters (not all are revert targets)")
    print("-" * 100)
    print(
        "Global coordinate-different rows: "
        f"{sum(audit.global_coordinate_diff_rows for audit in audits):,}"
    )
    print(
        "Global geometry-field-different rows "
        "(x/y OR node_length OR cl_id_min/max): "
        f"{sum(audit.global_geometry_field_diff_rows for audit in audits):,}"
    )

    print("\nDependent assignment diffs")
    print("-" * 100)
    print(
        f"centerlines.node_id diffs in node-diff reaches: {len(all_centerline_diffs):,}"
    )
    print(f"Reach scalar geometry metadata diffs: {len(all_reach_diffs):,}")
    if not all_reach_diffs.empty:
        print(all_reach_diffs[["region", "reach_id"]].to_string(index=False))


def write_report(
    output_path: Path,
    audits: list[RegionAudit],
    all_node_diffs: pd.DataFrame,
    all_centerline_diffs: pd.DataFrame,
    all_reach_diffs: pd.DataFrame,
    executed: bool,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "executed": executed,
        "region_audits": [asdict(audit) for audit in audits],
        "global": {
            "target_node_rows": int(len(all_node_diffs)),
            "target_reaches": int(
                all_node_diffs[["region", "reach_id"]].drop_duplicates().shape[0]
                if not all_node_diffs.empty
                else 0
            ),
            "moved_gt_100m": int(
                (all_node_diffs["distance_moved_m"] > 100.0).sum()
                if not all_node_diffs.empty
                else 0
            ),
            "centerline_node_id_diffs": int(len(all_centerline_diffs)),
            "reach_scalar_diff_rows": int(len(all_reach_diffs)),
            "unexpected_coordinate_diff_rows": int(
                sum(audit.unexpected_coordinate_diff_rows for audit in audits)
            ),
        },
    }
    output_path.write_text(json.dumps(payload, indent=2) + "\n")
    print(f"\nWrote audit report: {output_path}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Audit or revert v17c node geometry fields to v17b."
    )
    parser.add_argument("--db", default=str(DEFAULT_DB), help="v17c DuckDB path")
    parser.add_argument(
        "--v17b-netcdf-dir",
        default=str(DEFAULT_V17B_NETCDF),
        help="Directory containing *_sword_v17b.nc files",
    )
    parser.add_argument(
        "--regions",
        nargs="+",
        choices=REGIONS,
        default=list(REGIONS),
        help="Regions to process",
    )
    parser.add_argument("--xy-tol", type=float, default=1e-12)
    parser.add_argument("--length-tol", type=float, default=1e-6)
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Apply updates. Omit for audit/dry-run.",
    )
    parser.add_argument(
        "--yes-revert-v17c-node-geometry",
        action="store_true",
        help="Required with --execute.",
    )
    parser.add_argument(
        "--report",
        default=str(
            PROJECT_ROOT / "outputs" / "v17c_0_0_12_geometry_revert_audit.json"
        ),
        help="JSON audit report path",
    )
    args = parser.parse_args()

    db_path = Path(args.db)
    nc_dir = Path(args.v17b_netcdf_dir)
    if not db_path.exists():
        print(f"Database not found: {db_path}", file=sys.stderr)
        return 2
    if not nc_dir.exists():
        print(f"v17b NetCDF directory not found: {nc_dir}", file=sys.stderr)
        return 2
    for region in args.regions:
        nc_path = nc_dir / f"{region.lower()}_sword_v17b.nc"
        if not nc_path.exists():
            print(f"Missing v17b NetCDF for {region}: {nc_path}", file=sys.stderr)
            return 2
    if args.execute and not args.yes_revert_v17c_node_geometry:
        print(
            "--execute requires --yes-revert-v17c-node-geometry",
            file=sys.stderr,
        )
        return 2

    con = duckdb.connect(str(db_path), read_only=not args.execute)
    con.execute("INSTALL spatial; LOAD spatial;")

    op_df = get_operation_reaches(con)
    op_reaches = {
        (str(row.region), int(row.reach_id)) for row in op_df.itertuples(index=False)
    }

    print("=" * 100)
    print("SWORD v17c-0.0.12 node geometry revert audit")
    print("=" * 100)
    print(f"Mode: {'EXECUTE' if args.execute else 'DRY RUN'}")
    print(f"DB: {db_path}")
    print(f"v17b NetCDF dir: {nc_dir}")
    print(f"Operation-log rederive reaches: {len(op_reaches):,}")
    print(f"Known extra diff reaches: {sorted(KNOWN_EXTRA_DIFF_REACHES)}")

    audits: list[RegionAudit] = []
    node_diff_frames: list[pd.DataFrame] = []
    centerline_diff_frames: list[pd.DataFrame] = []
    reach_diff_frames: list[pd.DataFrame] = []

    for region in args.regions:
        print(f"\nProcessing {region}...")
        nc_path = nc_dir / f"{region.lower()}_sword_v17b.nc"
        node_diffs, orphan_node_b, orphan_node_c, node_stats = node_diffs_for_region(
            con,
            region,
            nc_path,
            op_reaches,
            args.xy_tol,
            args.length_tol,
        )
        reach_ids = (
            set(node_diffs["reach_id"].astype(int).tolist())
            if not node_diffs.empty
            else set()
        )
        cl_diffs, cl_checked, orphan_cl_b, orphan_cl_c = centerline_diffs_for_region(
            con, region, nc_path, reach_ids
        )
        reach_diffs, orphan_reach_b, orphan_reach_c = reach_scalar_diffs_for_region(
            con, region, nc_path, args.xy_tol, args.length_tol
        )

        if not node_diffs.empty:
            node_diff_frames.append(node_diffs)
        if not cl_diffs.empty:
            centerline_diff_frames.append(cl_diffs)
        if not reach_diffs.empty:
            reach_diff_frames.append(reach_diffs)

        max_move_m = 0.0
        max_move_node_id = None
        if not node_diffs.empty:
            idx = node_diffs["distance_moved_m"].idxmax()
            max_move_m = float(node_diffs.loc[idx, "distance_moved_m"])
            max_move_node_id = int(node_diffs.loc[idx, "node_id"])

        audits.append(
            RegionAudit(
                region=region,
                node_rows=int(
                    con.execute(
                        "SELECT COUNT(*) FROM nodes WHERE region = ?", [region]
                    ).fetchone()[0]
                ),
                target_node_rows=len(node_diffs),
                target_reaches=(
                    node_diffs[["region", "reach_id"]].drop_duplicates().shape[0]
                    if not node_diffs.empty
                    else 0
                ),
                target_rows_in_ops_reaches=(
                    int(node_diffs["in_operation_reach"].sum())
                    if not node_diffs.empty
                    else 0
                ),
                target_rows_extra_known=(
                    int(node_diffs["known_extra_diff"].sum())
                    if not node_diffs.empty
                    else 0
                ),
                coordinate_changed_target_rows=(
                    int(node_diffs["xy_diff"].sum()) if not node_diffs.empty else 0
                ),
                global_coordinate_diff_rows=node_stats["global_coordinate_diff_rows"],
                global_geometry_field_diff_rows=node_stats[
                    "global_geometry_field_diff_rows"
                ],
                unexpected_coordinate_diff_rows=node_stats[
                    "unexpected_coordinate_diff_rows"
                ],
                moved_gt_100m=(
                    int((node_diffs["distance_moved_m"] > 100.0).sum())
                    if not node_diffs.empty
                    else 0
                ),
                max_move_m=max_move_m,
                max_move_node_id=max_move_node_id,
                centerline_rows_checked=cl_checked,
                centerline_node_id_diffs=len(cl_diffs),
                reach_scalar_diff_rows=len(reach_diffs),
                orphan_v17b_nodes=orphan_node_b,
                orphan_v17c_nodes=orphan_node_c,
                orphan_v17b_centerlines=orphan_cl_b,
                orphan_v17c_centerlines=orphan_cl_c,
                orphan_v17b_reaches=orphan_reach_b,
                orphan_v17c_reaches=orphan_reach_c,
            )
        )

    all_node_diffs = (
        pd.concat(node_diff_frames, ignore_index=True)
        if node_diff_frames
        else pd.DataFrame()
    )
    all_centerline_diffs = (
        pd.concat(centerline_diff_frames, ignore_index=True)
        if centerline_diff_frames
        else pd.DataFrame()
    )
    all_reach_diffs = (
        pd.concat(reach_diff_frames, ignore_index=True)
        if reach_diff_frames
        else pd.DataFrame()
    )

    print_summary(audits, all_node_diffs, all_centerline_diffs, all_reach_diffs)
    write_report(
        Path(args.report),
        audits,
        all_node_diffs,
        all_centerline_diffs,
        all_reach_diffs,
        executed=args.execute,
    )

    if not args.execute:
        print("\nDry run only. No database writes performed.")
        return 0

    assert_apply_scope_is_known(all_node_diffs, audits)

    con.execute("BEGIN")
    indexes = con.execute(
        """
        SELECT index_name, table_name, sql
        FROM duckdb_indexes()
        WHERE sql LIKE '%RTREE%'
          AND table_name IN ('nodes', 'centerlines', 'reaches')
        """
    ).fetchall()
    try:
        for idx_name, _table_name, _sql in indexes:
            con.execute(f'DROP INDEX "{idx_name}"')

        total_node_updates = 0
        total_cl_updates = 0
        total_reach_updates = 0
        for region in args.regions:
            region_node_diffs = all_node_diffs[all_node_diffs["region"] == region]
            region_cl_diffs = all_centerline_diffs[
                all_centerline_diffs["region"] == region
            ]
            region_reach_diffs = all_reach_diffs[all_reach_diffs["region"] == region]
            if region_node_diffs.empty and region_cl_diffs.empty:
                continue
            node_count, cl_count, reach_count = apply_region_updates(
                con, region_node_diffs, region_cl_diffs, region_reach_diffs
            )
            total_node_updates += node_count
            total_cl_updates += cl_count
            total_reach_updates += reach_count
            print(
                f"{region}: updated nodes={node_count:,}, "
                f"centerlines={cl_count:,}, reaches={reach_count:,}"
            )

        # Minimal provenance record; the JSON report has full counts.
        op_id = con.execute(
            "SELECT COALESCE(MAX(operation_id), 0) + 1 FROM sword_operations"
        ).fetchone()[0]
        con.execute(
            """
            INSERT INTO sword_operations (
                operation_id, operation_type, table_name, entity_ids, region,
                user_id, session_id, started_at, completed_at, operation_details,
                affected_columns, reason, status
            )
            VALUES (?, 'UPDATE', 'nodes', ?, 'ALL', 'codex', 'v17c_0_0_12',
                    CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, ?, ?, ?, 'COMPLETED')
            """,
            [
                op_id,
                all_node_diffs["node_id"].astype(int).tolist(),
                json.dumps(
                    {
                        "release": "v17c-0.0.12",
                        "operation_kind": "REVERT_NODE_GEOMETRY_TO_V17B",
                        "node_updates": total_node_updates,
                        "centerline_updates": total_cl_updates,
                        "reach_scalar_updates": total_reach_updates,
                        "report": str(Path(args.report).resolve()),
                    }
                ),
                [
                    "x",
                    "y",
                    "geom",
                    "node_length",
                    "cl_id_min",
                    "cl_id_max",
                ],
                "v17c-0.0.12 D0-D2 continuity: restore v17b node geometry",
            ],
        )

        for _idx_name, _table_name, sql in indexes:
            con.execute(sql)
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise

    print("\nEXECUTE complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
