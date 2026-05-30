#!/usr/bin/env python3
"""Repair node_order after the v17c-0.0.12 coordinate continuity revert.

Default mode is dry-run. It builds a projection-based candidate over the 344
reaches whose nodes were rederived in v17c 0.0.8 / 0.0.10 and then restored to
v17b coordinates in 0.0.12.

The repair is semantic only:
  - nodes.node_order
  - reaches.dn_node_id
  - reaches.up_node_id
  - node distance fields interpolated from node_length midpoint offsets

It never updates node coordinates, node_length, cl_id_min/cl_id_max, or
centerlines.node_id.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast

import duckdb
import numpy as np
import pandas as pd
from pyproj import CRS, Transformer
from shapely.geometry import LineString, Point

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DB = PROJECT_ROOT / "data" / "duckdb" / "sword_v17c.duckdb"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "outputs"
REGIONS = ("NA", "SA", "EU", "AF", "AS", "OC")

AFFECTED_REACHES_SQL = """
SELECT DISTINCT
    CAST(json_extract_string(operation_details, '$.reach_id') AS BIGINT) AS reach_id,
    region
FROM sword_operations
WHERE (reason LIKE '%scrambled%' OR reason LIKE '%POM node geolocation%')
  AND status = 'COMPLETED'
  AND json_extract_string(operation_details, '$.reach_id') IS NOT NULL
ORDER BY region, reach_id
"""

DISTANCE_COLUMNS = (
    "dist_out",
    "hydro_dist_out",
    "dist_out_dijkstra",
    "hydro_dist_hw",
    "pathlen_hw",
    "pathlen_out",
)

EXPECTED_POM_REACH_ID = 35301100891
EXPECTED_POM_REGION = "AS"
EXPECTED_POM_SEQUENCE = (
    list(range(2, 24)) + [25, 26, 24] + list(range(27, 74)) + [75, 74, 1]
)


@dataclass
class ProjectionIssue:
    region: str
    reach_id: int
    reason: str


@dataclass
class RepairSummary:
    generated_at: str
    db: str
    affected_reaches: int
    affected_nodes: int
    projection_failures: int
    near_tie_reaches: int
    reaches_with_projected_node_order_change: int
    nodes_with_projected_node_order_change: int
    reaches_with_boundary_change: int
    reaches_with_distance_formula_mismatch: int
    nodes_with_distance_formula_mismatch: int
    max_distance_formula_mismatch_m: float
    per_region_reaches_changed: dict[str, int]
    per_region_nodes_changed: dict[str, int]
    projection_orientation_counts: dict[str, int]
    pom_reach_sequence_matches: bool
    pom_reach_sequence: list[int]
    dry_run_files: dict[str, str]
    executed: bool = False
    operation_id: int | None = None
    nodes_updated: int = 0
    reaches_updated: int = 0
    distance_nodes_recomputed: int = 0
    before_snapshot_file: str | None = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Projection-based node_order repair for v17c-0.0.12."
    )
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument(
        "--prefix",
        default="node_order_projection_repair_20260528",
        help="Output filename prefix.",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Apply the validated candidate to the DuckDB database.",
    )
    parser.add_argument(
        "--tie-tolerance-m",
        type=float,
        default=0.001,
        help="Projection spacing below this threshold is reported as a near tie.",
    )
    return parser.parse_args()


def circular_mean_lon(lons: pd.Series | np.ndarray) -> float:
    radians = np.radians(np.asarray(lons, dtype=float))
    return float(np.degrees(np.arctan2(np.sin(radians).sum(), np.cos(radians).sum())))


def local_transformer(lons: pd.Series, lats: pd.Series) -> Transformer:
    lon0 = circular_mean_lon(lons)
    lat0 = float(np.mean(np.asarray(lats, dtype=float)))
    crs = CRS.from_proj4(
        f"+proj=aeqd +lat_0={lat0} +lon_0={lon0} +datum=WGS84 +units=m +no_defs"
    )
    return Transformer.from_crs("EPSG:4326", crs, always_xy=True)


def node_index_within_reach(node_id: int, reach_id: int) -> int:
    del reach_id  # The node index is encoded independently of the reach suffix.
    return int((int(node_id) // 10) % 1000)


def load_affected_reaches(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    reaches = con.execute(AFFECTED_REACHES_SQL).fetchdf()
    reaches["reach_id"] = reaches["reach_id"].astype("int64")
    reaches["region"] = reaches["region"].astype(str)
    return reaches


def fetch_region_frames(
    con: duckdb.DuckDBPyConnection, region: str, reach_ids: list[int]
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    reach_ids_sql = ",".join(str(int(rid)) for rid in reach_ids)
    nodes = con.execute(
        f"""
        SELECT node_id, region, reach_id, x, y, node_order, dist_out,
               node_length
        FROM nodes
        WHERE region = ?
          AND reach_id IN ({reach_ids_sql})
        ORDER BY reach_id, node_id
        """,
        [region],
    ).fetchdf()
    centerlines = con.execute(
        f"""
        SELECT cl_id, region, reach_id, x, y
        FROM centerlines
        WHERE region = ?
          AND reach_id IN ({reach_ids_sql})
        ORDER BY reach_id, cl_id
        """,
        [region],
    ).fetchdf()
    reaches = con.execute(
        f"""
        SELECT reach_id, region, dn_node_id, up_node_id, reach_length,
               dist_out, hydro_dist_out, hydro_dist_hw, dist_out_dijkstra,
               pathlen_hw, pathlen_out
        FROM reaches
        WHERE region = ?
          AND reach_id IN ({reach_ids_sql})
        ORDER BY reach_id
        """,
        [region],
    ).fetchdf()
    topology = con.execute(
        f"""
        SELECT reach_id, region, direction, neighbor_reach_id
        FROM reach_topology
        WHERE region = ?
          AND reach_id IN ({reach_ids_sql})
          AND neighbor_reach_id != 0
        """,
        [region],
    ).fetchdf()
    return nodes, centerlines, reaches, topology


def fetch_endpoint_map(
    con: duckdb.DuckDBPyConnection, region: str, reach_ids: set[int]
) -> dict[int, tuple[float, float, float, float]]:
    if not reach_ids:
        return {}
    reach_ids_sql = ",".join(str(int(rid)) for rid in sorted(reach_ids))
    endpoint_df = con.execute(
        f"""
        WITH ranked AS (
            SELECT reach_id, x, y,
                   ROW_NUMBER() OVER (
                       PARTITION BY reach_id, region ORDER BY cl_id ASC
                   ) AS rn_asc,
                   ROW_NUMBER() OVER (
                       PARTITION BY reach_id, region ORDER BY cl_id DESC
                   ) AS rn_desc
            FROM centerlines
            WHERE region = ?
              AND reach_id IN ({reach_ids_sql})
        )
        SELECT reach_id,
               MAX(CASE WHEN rn_asc = 1 THEN x END) AS start_x,
               MAX(CASE WHEN rn_asc = 1 THEN y END) AS start_y,
               MAX(CASE WHEN rn_desc = 1 THEN x END) AS end_x,
               MAX(CASE WHEN rn_desc = 1 THEN y END) AS end_y
        FROM ranked
        GROUP BY reach_id
        """,
        [region],
    ).fetchdf()
    return {
        int(row["reach_id"]): (
            float(row["start_x"]),
            float(row["start_y"]),
            float(row["end_x"]),
            float(row["end_y"]),
        )
        for row in endpoint_df.to_dict("records")
    }


def choose_projection_orientation(
    reach_id: int,
    reach_nodes: pd.DataFrame,
    reach_topology: pd.DataFrame,
    endpoint_map: dict[int, tuple[float, float, float, float]],
) -> tuple[str, str]:
    """Choose whether ascending cl_id is downstream-to-upstream.

    Topology endpoints are authoritative where available. If a pathological
    isolated reach lacks useful neighbors, fall back to the current node_order
    correlation so dry-runs remain complete and auditable.
    """
    endpoints = endpoint_map.get(int(reach_id))
    if endpoints is not None and not reach_topology.empty:
        neighbor_ids = set(reach_topology["neighbor_reach_id"].astype(int).tolist())
        relevant = [endpoints]
        relevant.extend(
            endpoint_map[nid] for nid in neighbor_ids if nid in endpoint_map
        )
        lons = []
        lats = []
        for sx, sy, ex, ey in relevant:
            lons.extend([sx, ex])
            lats.extend([sy, ey])
        transformer = local_transformer(pd.Series(lons), pd.Series(lats))
        start = np.asarray(transformer.transform(endpoints[0], endpoints[1]))
        end = np.asarray(transformer.transform(endpoints[2], endpoints[3]))

        votes: list[str] = []
        for direction, group in reach_topology.groupby("direction"):
            start_distances: list[float] = []
            end_distances: list[float] = []
            for neighbor_id in group["neighbor_reach_id"].astype(int):
                neighbor_endpoints = endpoint_map.get(int(neighbor_id))
                if neighbor_endpoints is None:
                    continue
                n_start = np.asarray(
                    transformer.transform(neighbor_endpoints[0], neighbor_endpoints[1])
                )
                n_end = np.asarray(
                    transformer.transform(neighbor_endpoints[2], neighbor_endpoints[3])
                )
                start_distances.append(
                    float(
                        min(
                            np.linalg.norm(start - n_start),
                            np.linalg.norm(start - n_end),
                        )
                    )
                )
                end_distances.append(
                    float(
                        min(
                            np.linalg.norm(end - n_start),
                            np.linalg.norm(end - n_end),
                        )
                    )
                )
            if not start_distances:
                continue
            if direction == "down":
                votes.append(
                    "cl_id_forward"
                    if min(start_distances) <= min(end_distances)
                    else "cl_id_reverse"
                )
            elif direction == "up":
                votes.append(
                    "cl_id_forward"
                    if min(end_distances) <= min(start_distances)
                    else "cl_id_reverse"
                )

        if votes and len(set(votes)) == 1:
            return votes[0], "topology_endpoint"

    if len(reach_nodes) <= 1:
        return "cl_id_forward", "single_node_fallback"

    corr = float(
        np.corrcoef(reach_nodes["node_order"], reach_nodes["raw_projection"])[0, 1]
    )
    if np.isnan(corr) or corr >= 0:
        return "cl_id_forward", "node_order_correlation"
    return "cl_id_reverse", "node_order_correlation"


def build_candidate(
    con: duckdb.DuckDBPyConnection,
    affected_reaches: pd.DataFrame,
    tie_tolerance_m: float,
) -> tuple[pd.DataFrame, pd.DataFrame, list[ProjectionIssue]]:
    node_rows: list[pd.DataFrame] = []
    reach_rows: list[dict[str, object]] = []
    issues: list[ProjectionIssue] = []

    for region in REGIONS:
        region_reaches = affected_reaches[affected_reaches["region"] == region]
        if region_reaches.empty:
            continue
        reach_ids = region_reaches["reach_id"].astype(int).tolist()
        nodes, centerlines, reaches, topology = fetch_region_frames(
            con, region, reach_ids
        )
        neighbor_ids = set(topology["neighbor_reach_id"].astype(int).tolist())
        endpoint_map = fetch_endpoint_map(con, region, set(reach_ids) | neighbor_ids)

        reach_lookup = {int(row["reach_id"]): row for row in reaches.to_dict("records")}
        topology_groups = {
            int(cast(Any, rid)): group.copy()
            for rid, group in topology.groupby("reach_id")
        }
        empty_topology = topology.iloc[0:0].copy()

        for reach_id in reach_ids:
            reach_nodes = nodes[nodes["reach_id"] == reach_id].copy()
            reach_centerlines = centerlines[centerlines["reach_id"] == reach_id].copy()
            reach = reach_lookup.get(int(reach_id))
            if reach is None:
                issues.append(
                    ProjectionIssue(region, int(reach_id), "missing reach row")
                )
                continue
            if len(reach_nodes) == 0:
                issues.append(ProjectionIssue(region, int(reach_id), "missing nodes"))
                continue
            if len(reach_centerlines) < 2:
                issues.append(
                    ProjectionIssue(
                        region, int(reach_id), "fewer than 2 centerline points"
                    )
                )
                continue

            transformer = local_transformer(
                reach_centerlines["x"], reach_centerlines["y"]
            )
            line_coords = [
                transformer.transform(float(row.x), float(row.y))
                for row in reach_centerlines.itertuples(index=False)
            ]
            line = LineString(line_coords)
            if line.length == 0:
                issues.append(
                    ProjectionIssue(region, int(reach_id), "zero-length centerline")
                )
                continue

            reach_nodes["raw_projection"] = [
                line.project(Point(*transformer.transform(float(row.x), float(row.y))))
                for row in reach_nodes.itertuples(index=False)
            ]
            reach_topology = topology_groups.get(
                int(reach_id),
                empty_topology,
            )
            orientation, orientation_source = choose_projection_orientation(
                int(reach_id), reach_nodes, reach_topology, endpoint_map
            )
            if orientation == "cl_id_forward":
                reach_nodes["oriented_projection_m"] = reach_nodes["raw_projection"]
            else:
                reach_nodes["oriented_projection_m"] = (
                    line.length - reach_nodes["raw_projection"]
                )

            ordered = reach_nodes.sort_values(
                ["oriented_projection_m", "node_id"], kind="mergesort"
            ).copy()
            ordered["candidate_node_order"] = np.arange(1, len(ordered) + 1)
            ordered["order_changed"] = ordered["node_order"].astype(int) != ordered[
                "candidate_node_order"
            ].astype(int)
            ordered["projection_orientation"] = orientation
            ordered["orientation_source"] = orientation_source
            ordered["projection_tie_tolerance_m"] = tie_tolerance_m
            projection_diffs = np.diff(ordered["oriented_projection_m"].to_numpy())
            ordered["min_projection_gap_m"] = (
                float(np.min(projection_diffs)) if len(projection_diffs) else None
            )
            ordered["near_projection_tie"] = (
                False
                if len(projection_diffs) == 0
                else bool(np.min(projection_diffs) < tie_tolerance_m)
            )

            candidate_dn = int(ordered.iloc[0]["node_id"])
            candidate_up = int(ordered.iloc[-1]["node_id"])
            current_dn = int(reach["dn_node_id"])
            current_up = int(reach["up_node_id"])
            changed_nodes = int(ordered["order_changed"].sum())

            reach_rows.append(
                {
                    "region": region,
                    "reach_id": int(reach_id),
                    "n_nodes": int(len(ordered)),
                    "changed_nodes": changed_nodes,
                    "current_dn": current_dn,
                    "current_up": current_up,
                    "candidate_dn": candidate_dn,
                    "candidate_up": candidate_up,
                    "orientation": orientation,
                    "orientation_source": orientation_source,
                    "min_projection_gap_m": ordered["min_projection_gap_m"].iloc[0],
                    "near_projection_tie": bool(ordered["near_projection_tie"].iloc[0]),
                    "reach_changes": changed_nodes > 0,
                    "boundary_changes": (
                        current_dn != candidate_dn or current_up != candidate_up
                    ),
                }
            )

            node_rows.append(
                ordered[
                    [
                        "region",
                        "reach_id",
                        "node_id",
                        "node_order",
                        "dist_out",
                        "node_length",
                        "candidate_node_order",
                        "oriented_projection_m",
                        "projection_orientation",
                        "orientation_source",
                        "order_changed",
                        "near_projection_tie",
                    ]
                ].rename(columns={"node_order": "current_node_order"})
            )

    if not node_rows:
        return pd.DataFrame(), pd.DataFrame(), issues

    candidate_nodes = pd.concat(node_rows, ignore_index=True)
    candidate_reaches = pd.DataFrame(reach_rows).sort_values(["region", "reach_id"])
    candidate_nodes = candidate_nodes.sort_values(["region", "reach_id", "node_id"])
    return candidate_nodes, candidate_reaches, issues


def validate_candidate(
    candidate_nodes: pd.DataFrame,
    candidate_reaches: pd.DataFrame,
    issues: list[ProjectionIssue],
) -> list[str]:
    errors: list[str] = []
    if issues:
        errors.append(f"{len(issues)} projection failures")
    if candidate_reaches["reach_id"].nunique() != 344:
        errors.append(
            f"expected 344 affected reaches, found {candidate_reaches['reach_id'].nunique()}"
        )
    if len(candidate_nodes) != 23_000:
        errors.append(f"expected 23,000 affected nodes, found {len(candidate_nodes)}")

    for keys, group in candidate_nodes.groupby(["region", "reach_id"]):
        region, reach_id = cast(tuple[Any, Any], keys)
        orders = group["candidate_node_order"].astype(int).sort_values().tolist()
        expected = list(range(1, len(group) + 1))
        if orders != expected:
            errors.append(f"{region} {reach_id}: candidate node_order is not 1..n")
        if group["node_id"].duplicated().any():
            errors.append(f"{region} {reach_id}: duplicate node_id in candidate")

    pom_rows = candidate_nodes[
        (candidate_nodes["region"] == EXPECTED_POM_REGION)
        & (candidate_nodes["reach_id"] == EXPECTED_POM_REACH_ID)
    ].sort_values("candidate_node_order")
    pom_sequence = [
        node_index_within_reach(node_id, EXPECTED_POM_REACH_ID)
        for node_id in pom_rows["node_id"].tolist()
    ]
    if pom_sequence != EXPECTED_POM_SEQUENCE:
        errors.append(
            f"AS 35301100891 does not match POM sequence (got {pom_sequence})"
        )

    boundary_mismatch = candidate_reaches[
        candidate_reaches["boundary_changes"] & ~candidate_reaches["reach_changes"]
    ]
    if not boundary_mismatch.empty:
        errors.append("boundary changes found without node_order changes")

    return errors


def write_outputs(
    output_dir: Path,
    prefix: str,
    candidate_nodes: pd.DataFrame,
    candidate_reaches: pd.DataFrame,
    issues: list[ProjectionIssue],
    summary: RepairSummary,
) -> dict[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    all_nodes_path = output_dir / f"{prefix}_candidate_nodes.csv"
    changed_nodes_path = output_dir / f"{prefix}_candidate_nodes_changed.csv"
    reaches_path = output_dir / f"{prefix}_candidate_reaches.csv"
    issues_path = output_dir / f"{prefix}_projection_issues.json"
    summary_path = output_dir / f"{prefix}_summary.json"

    candidate_nodes.to_csv(all_nodes_path, index=False)
    candidate_nodes[candidate_nodes["order_changed"]].to_csv(
        changed_nodes_path, index=False
    )
    candidate_reaches.to_csv(reaches_path, index=False)
    issues_path.write_text(
        json.dumps([asdict(issue) for issue in issues], indent=2) + "\n",
        encoding="utf-8",
    )
    files = {
        "candidate_nodes": str(all_nodes_path),
        "candidate_nodes_changed": str(changed_nodes_path),
        "candidate_reaches": str(reaches_path),
        "projection_issues": str(issues_path),
        "summary": str(summary_path),
    }
    summary.dry_run_files = files
    summary_path.write_text(
        json.dumps(asdict(summary), indent=2) + "\n", encoding="utf-8"
    )
    return files


def summarize_distance_formula_mismatches(
    con: duckdb.DuckDBPyConnection,
    candidate_reaches: pd.DataFrame,
) -> tuple[int, int, float]:
    con.register(
        "candidate_distance_check_reaches",
        candidate_reaches[["region", "reach_id"]],
    )
    try:
        row = con.execute(
            """
            WITH ofs AS (
                SELECT
                    n.node_id,
                    n.reach_id,
                    n.region,
                    n.dist_out,
                    n.hydro_dist_out,
                    n.dist_out_dijkstra,
                    n.hydro_dist_hw,
                    n.pathlen_hw,
                    n.pathlen_out,
                    r.dist_out AS reach_dist_out,
                    r.hydro_dist_out AS reach_hydro_dist_out,
                    r.dist_out_dijkstra AS reach_dist_out_dijkstra,
                    r.hydro_dist_hw AS reach_hydro_dist_hw,
                    r.pathlen_hw AS reach_pathlen_hw,
                    r.pathlen_out AS reach_pathlen_out,
                    r.reach_length,
                    GREATEST(0, LEAST(r.reach_length,
                        SUM(n.node_length) OVER (
                            PARTITION BY n.reach_id, n.region
                            ORDER BY n.node_order, n.node_id
                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                        ) - 0.5 * n.node_length
                    )) AS o
                FROM nodes n
                JOIN reaches r
                  ON n.reach_id = r.reach_id
                 AND n.region = r.region
                JOIN candidate_distance_check_reaches cr
                  ON n.reach_id = cr.reach_id
                 AND n.region = cr.region
            ),
            expected AS (
                SELECT
                    *,
                    CASE WHEN reach_dist_out IS NULL THEN NULL
                        ELSE GREATEST(0, reach_dist_out - reach_length + o) END
                        AS expected_dist_out,
                    CASE WHEN reach_hydro_dist_out IS NULL THEN NULL
                        ELSE GREATEST(0, reach_hydro_dist_out - reach_length + o) END
                        AS expected_hydro_dist_out,
                    CASE WHEN reach_dist_out_dijkstra IS NULL THEN NULL
                        ELSE GREATEST(0, reach_dist_out_dijkstra - reach_length + o) END
                        AS expected_dist_out_dijkstra,
                    CASE WHEN reach_hydro_dist_hw IS NULL THEN NULL
                        ELSE GREATEST(0, reach_hydro_dist_hw + reach_length - o) END
                        AS expected_hydro_dist_hw,
                    CASE WHEN reach_pathlen_hw IS NULL THEN NULL
                        ELSE GREATEST(0, reach_pathlen_hw - reach_length + o) END
                        AS expected_pathlen_hw,
                    CASE WHEN reach_pathlen_out IS NULL THEN NULL
                        ELSE GREATEST(0, reach_pathlen_out + reach_length - o) END
                        AS expected_pathlen_out
                FROM ofs
            ),
            diffs AS (
                SELECT
                    region,
                    reach_id,
                    GREATEST(
                        CASE WHEN dist_out IS NULL AND expected_dist_out IS NULL THEN 0
                             WHEN dist_out IS NULL OR expected_dist_out IS NULL THEN 1e30
                             ELSE ABS(dist_out - expected_dist_out) END,
                        CASE WHEN hydro_dist_out IS NULL AND expected_hydro_dist_out IS NULL THEN 0
                             WHEN hydro_dist_out IS NULL OR expected_hydro_dist_out IS NULL THEN 1e30
                             ELSE ABS(hydro_dist_out - expected_hydro_dist_out) END,
                        CASE WHEN dist_out_dijkstra IS NULL AND expected_dist_out_dijkstra IS NULL THEN 0
                             WHEN dist_out_dijkstra IS NULL OR expected_dist_out_dijkstra IS NULL THEN 1e30
                             ELSE ABS(dist_out_dijkstra - expected_dist_out_dijkstra) END,
                        CASE WHEN hydro_dist_hw IS NULL AND expected_hydro_dist_hw IS NULL THEN 0
                             WHEN hydro_dist_hw IS NULL OR expected_hydro_dist_hw IS NULL THEN 1e30
                             ELSE ABS(hydro_dist_hw - expected_hydro_dist_hw) END,
                        CASE WHEN pathlen_hw IS NULL AND expected_pathlen_hw IS NULL THEN 0
                             WHEN pathlen_hw IS NULL OR expected_pathlen_hw IS NULL THEN 1e30
                             ELSE ABS(pathlen_hw - expected_pathlen_hw) END,
                        CASE WHEN pathlen_out IS NULL AND expected_pathlen_out IS NULL THEN 0
                             WHEN pathlen_out IS NULL OR expected_pathlen_out IS NULL THEN 1e30
                             ELSE ABS(pathlen_out - expected_pathlen_out) END
                    ) AS max_abs_diff
                FROM expected
            )
            SELECT
                COUNT(DISTINCT CASE WHEN max_abs_diff > 1e-6 THEN reach_id END)
                    AS reach_mismatches,
                SUM(CASE WHEN max_abs_diff > 1e-6 THEN 1 ELSE 0 END)
                    AS node_mismatches,
                COALESCE(MAX(max_abs_diff), 0) AS max_abs_diff
            FROM diffs
            """
        ).fetchone()
    finally:
        con.unregister("candidate_distance_check_reaches")

    if row is None:
        return 0, 0, 0.0
    return int(row[0]), int(row[1]), float(row[2])


def build_summary(
    con: duckdb.DuckDBPyConnection,
    db: Path,
    candidate_nodes: pd.DataFrame,
    candidate_reaches: pd.DataFrame,
    issues: list[ProjectionIssue],
) -> RepairSummary:
    changed_nodes = candidate_nodes[candidate_nodes["order_changed"]]
    changed_reaches = candidate_reaches[candidate_reaches["reach_changes"]]
    pom_rows = candidate_nodes[
        (candidate_nodes["region"] == EXPECTED_POM_REGION)
        & (candidate_nodes["reach_id"] == EXPECTED_POM_REACH_ID)
    ].sort_values("candidate_node_order")
    pom_sequence = [
        node_index_within_reach(node_id, EXPECTED_POM_REACH_ID)
        for node_id in pom_rows["node_id"].tolist()
    ]
    distance_reaches, distance_nodes, max_distance_diff = (
        summarize_distance_formula_mismatches(con, candidate_reaches)
    )
    return RepairSummary(
        generated_at=datetime.now(timezone.utc).isoformat(),
        db=str(db),
        affected_reaches=int(candidate_reaches["reach_id"].nunique()),
        affected_nodes=int(len(candidate_nodes)),
        projection_failures=len(issues),
        near_tie_reaches=int(candidate_reaches["near_projection_tie"].sum()),
        reaches_with_projected_node_order_change=int(len(changed_reaches)),
        nodes_with_projected_node_order_change=int(len(changed_nodes)),
        reaches_with_boundary_change=int(candidate_reaches["boundary_changes"].sum()),
        reaches_with_distance_formula_mismatch=distance_reaches,
        nodes_with_distance_formula_mismatch=distance_nodes,
        max_distance_formula_mismatch_m=max_distance_diff,
        per_region_reaches_changed={
            str(k): int(v)
            for k, v in changed_reaches.groupby("region").size().to_dict().items()
        },
        per_region_nodes_changed={
            str(k): int(v)
            for k, v in changed_nodes.groupby("region").size().to_dict().items()
        },
        projection_orientation_counts={
            str(k): int(v)
            for k, v in candidate_reaches.groupby("orientation")
            .size()
            .to_dict()
            .items()
        },
        pom_reach_sequence_matches=pom_sequence == EXPECTED_POM_SEQUENCE,
        pom_reach_sequence=pom_sequence,
        dry_run_files={},
    )


def write_before_snapshot(
    con: duckdb.DuckDBPyConnection,
    output_dir: Path,
    prefix: str,
    snapshot_reaches: pd.DataFrame,
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    snapshot_path = output_dir / f"{prefix}_before_apply_snapshot.csv"
    con.register(
        "changed_reaches_for_snapshot", snapshot_reaches[["region", "reach_id"]]
    )
    try:
        snapshot = con.execute(
            """
            SELECT n.region, n.reach_id, n.node_id, n.node_order,
                   n.dist_out, n.hydro_dist_out, n.dist_out_dijkstra,
                   n.hydro_dist_hw, n.pathlen_hw, n.pathlen_out,
                   r.dn_node_id, r.up_node_id
            FROM nodes n
            JOIN reaches r
              ON n.reach_id = r.reach_id
             AND n.region = r.region
            JOIN changed_reaches_for_snapshot cr
              ON n.reach_id = cr.reach_id
             AND n.region = cr.region
            ORDER BY n.region, n.reach_id, n.node_id
            """
        ).fetchdf()
    finally:
        con.unregister("changed_reaches_for_snapshot")
    snapshot.to_csv(snapshot_path, index=False)
    return snapshot_path


def fetchone_int(cursor: duckdb.DuckDBPyConnection) -> int:
    row = cursor.fetchone()
    if row is None:
        raise RuntimeError("Expected DuckDB query to return one row")
    return int(row[0])


def apply_candidate(
    con: duckdb.DuckDBPyConnection,
    output_dir: Path,
    prefix: str,
    candidate_nodes: pd.DataFrame,
    candidate_reaches: pd.DataFrame,
    summary: RepairSummary,
) -> RepairSummary:
    changed_nodes = candidate_nodes[candidate_nodes["order_changed"]].copy()
    changed_reaches = candidate_reaches[candidate_reaches["reach_changes"]].copy()
    boundary_reaches = candidate_reaches[candidate_reaches["boundary_changes"]].copy()
    distance_reaches = candidate_reaches.copy()

    distance_mismatch_reaches, distance_mismatch_nodes, max_distance_diff = (
        summarize_distance_formula_mismatches(con, distance_reaches)
    )
    if changed_nodes.empty and boundary_reaches.empty and distance_mismatch_nodes == 0:
        return summary

    snapshot_path = write_before_snapshot(con, output_dir, prefix, distance_reaches)

    con.execute("INSTALL spatial; LOAD spatial;")
    rtree_indexes = con.execute(
        """
        SELECT index_name, table_name, sql
        FROM duckdb_indexes()
        WHERE sql LIKE '%RTREE%'
          AND table_name IN ('nodes', 'reaches')
        """
    ).fetchall()
    for index_name, _table_name, _sql in rtree_indexes:
        con.execute(f'DROP INDEX IF EXISTS "{index_name}"')

    candidate_update = changed_nodes[
        ["region", "reach_id", "node_id", "candidate_node_order"]
    ].copy()
    boundary_update = boundary_reaches[
        ["region", "reach_id", "candidate_dn", "candidate_up"]
    ].copy()
    distance_reach_update = distance_reaches[["region", "reach_id"]].copy()

    operation_id = fetchone_int(
        con.execute("SELECT COALESCE(MAX(operation_id), 0) + 1 FROM sword_operations")
    )
    details = {
        "operation_kind": "PROJECTED_NODE_ORDER_REPAIR",
        "release": "v17c-0.0.12",
        "affected_reaches": int(candidate_reaches["reach_id"].nunique()),
        "changed_reaches": int(len(changed_reaches)),
        "changed_nodes": int(len(changed_nodes)),
        "boundary_changes": int(len(boundary_reaches)),
        "distance_reaches_recomputed": int(distance_reaches["reach_id"].nunique()),
        "distance_nodes_mismatched_before": distance_mismatch_nodes,
        "distance_reaches_mismatched_before": distance_mismatch_reaches,
        "max_distance_mismatch_before_m": max_distance_diff,
        "distance_columns_recomputed": list(DISTANCE_COLUMNS),
        "candidate_summary": summary.dry_run_files.get("summary"),
        "before_snapshot": str(snapshot_path),
    }
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    entity_ids = (
        candidate_nodes["node_id"].astype(int).tolist()
        if changed_nodes.empty
        else changed_nodes["node_id"].astype(int).tolist()
    )
    reason = (
        "v17c-0.0.12 node distance repair: midpoint distances over "
        "v17b-restored coordinates"
        if changed_nodes.empty
        else "v17c-0.0.12 node_order repair: projection order over "
        "v17b-restored coordinates"
    )
    affected_columns = [f"nodes.{column}" for column in DISTANCE_COLUMNS]
    if not changed_nodes.empty:
        affected_columns.insert(0, "nodes.node_order")
    if not boundary_reaches.empty:
        affected_columns.extend(["reaches.dn_node_id", "reaches.up_node_id"])

    con.register("candidate_node_order_update", candidate_update)
    con.register("candidate_boundary_update", boundary_update)
    con.register("candidate_distance_reaches", distance_reach_update)
    try:
        con.execute("BEGIN TRANSACTION")
        con.execute(
            """
            INSERT INTO sword_operations (
                operation_id, operation_type, table_name, entity_ids, region,
                user_id, session_id, started_at, operation_details,
                affected_columns, reason, source_operation_id, status
            )
            VALUES (?, 'UPDATE', 'nodes', ?, 'ALL', 'jake',
                    'v17c_0_0_12_node_order_projection_repair', ?, ?,
                    ?, ?,
                    998, 'PENDING')
            """,
            [
                operation_id,
                entity_ids,
                now,
                json.dumps(details),
                affected_columns,
                reason,
            ],
        )

        nodes_result = con.execute(
            """
            UPDATE nodes
            SET node_order = c.candidate_node_order
            FROM candidate_node_order_update c
            WHERE nodes.node_id = c.node_id
              AND nodes.region = c.region
            """
        )
        nodes_updated = fetchone_int(nodes_result)

        reaches_updated = 0
        if not boundary_update.empty:
            reaches_result = con.execute(
                """
                UPDATE reaches
                SET dn_node_id = c.candidate_dn,
                    up_node_id = c.candidate_up
                FROM candidate_boundary_update c
                WHERE reaches.reach_id = c.reach_id
                  AND reaches.region = c.region
                """
            )
            reaches_updated = fetchone_int(reaches_result)

        distance_result = con.execute(
            """
            WITH ofs AS (
                SELECT n.node_id, n.region,
                       r.dist_out, r.hydro_dist_out, r.dist_out_dijkstra,
                       r.hydro_dist_hw, r.pathlen_hw, r.pathlen_out,
                       r.reach_length,
                       GREATEST(0, LEAST(r.reach_length,
                           SUM(n.node_length) OVER (
                               PARTITION BY n.reach_id, n.region
                               ORDER BY n.node_order, n.node_id
                               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                           ) - 0.5 * n.node_length
                       )) AS o
                FROM nodes n
                JOIN reaches r
                  ON n.reach_id = r.reach_id
                 AND n.region = r.region
                JOIN candidate_distance_reaches cr
                  ON n.reach_id = cr.reach_id
                 AND n.region = cr.region
            )
            UPDATE nodes
            SET dist_out = CASE WHEN ofs.dist_out IS NULL THEN NULL
                    ELSE GREATEST(0, ofs.dist_out - ofs.reach_length + ofs.o) END,
                hydro_dist_out = CASE WHEN ofs.hydro_dist_out IS NULL THEN NULL
                    ELSE GREATEST(0, ofs.hydro_dist_out - ofs.reach_length + ofs.o) END,
                dist_out_dijkstra = CASE WHEN ofs.dist_out_dijkstra IS NULL THEN NULL
                    ELSE GREATEST(0, ofs.dist_out_dijkstra - ofs.reach_length + ofs.o) END,
                hydro_dist_hw = CASE WHEN ofs.hydro_dist_hw IS NULL THEN NULL
                    ELSE GREATEST(0, ofs.hydro_dist_hw + ofs.reach_length - ofs.o) END,
                pathlen_hw = CASE WHEN ofs.pathlen_hw IS NULL THEN NULL
                    ELSE GREATEST(0, ofs.pathlen_hw - ofs.reach_length + ofs.o) END,
                pathlen_out = CASE WHEN ofs.pathlen_out IS NULL THEN NULL
                    ELSE GREATEST(0, ofs.pathlen_out + ofs.reach_length - ofs.o) END
            FROM ofs
            WHERE nodes.node_id = ofs.node_id
              AND nodes.region = ofs.region
            """
        )
        distance_nodes_recomputed = fetchone_int(distance_result)

        completed = datetime.now(timezone.utc).replace(tzinfo=None)
        con.execute(
            """
            UPDATE sword_operations
            SET completed_at = ?, status = 'COMPLETED'
            WHERE operation_id = ?
            """,
            [completed, operation_id],
        )
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    finally:
        for relation in (
            "candidate_node_order_update",
            "candidate_boundary_update",
            "candidate_distance_reaches",
        ):
            try:
                con.unregister(relation)
            except duckdb.InvalidInputException:
                pass
        for _index_name, _table_name, sql in rtree_indexes:
            con.execute(sql)

    con.execute("CHECKPOINT")
    summary.executed = True
    summary.operation_id = operation_id
    summary.nodes_updated = nodes_updated
    summary.reaches_updated = reaches_updated
    summary.distance_nodes_recomputed = distance_nodes_recomputed
    summary.before_snapshot_file = str(snapshot_path)
    (
        summary.reaches_with_distance_formula_mismatch,
        summary.nodes_with_distance_formula_mismatch,
        summary.max_distance_formula_mismatch_m,
    ) = summarize_distance_formula_mismatches(con, distance_reaches)
    return summary


def post_apply_check(
    con: duckdb.DuckDBPyConnection,
    candidate_nodes: pd.DataFrame,
    candidate_reaches: pd.DataFrame,
) -> list[str]:
    errors: list[str] = []
    con.register(
        "candidate_nodes_postcheck",
        candidate_nodes[["region", "reach_id", "node_id", "candidate_node_order"]],
    )
    con.register(
        "candidate_reaches_postcheck",
        candidate_reaches[["region", "reach_id", "candidate_dn", "candidate_up"]],
    )
    try:
        node_mismatch = fetchone_int(
            con.execute(
                """
            SELECT COUNT(*)
            FROM nodes n
            JOIN candidate_nodes_postcheck c
              ON n.node_id = c.node_id
             AND n.region = c.region
            WHERE n.node_order != c.candidate_node_order
            """
            )
        )
        boundary_mismatch = fetchone_int(
            con.execute(
                """
            SELECT COUNT(*)
            FROM reaches r
            JOIN candidate_reaches_postcheck c
              ON r.reach_id = c.reach_id
             AND r.region = c.region
            WHERE r.dn_node_id != c.candidate_dn
               OR r.up_node_id != c.candidate_up
            """
            )
        )
    finally:
        con.unregister("candidate_nodes_postcheck")
        con.unregister("candidate_reaches_postcheck")
    if node_mismatch:
        errors.append(f"{node_mismatch} node_order rows do not match candidate")
    if boundary_mismatch:
        errors.append(f"{boundary_mismatch} boundary rows do not match candidate")
    distance_reaches, distance_nodes, max_distance_diff = (
        summarize_distance_formula_mismatches(con, candidate_reaches)
    )
    if distance_nodes:
        errors.append(
            f"{distance_nodes} node distance rows across {distance_reaches} reaches "
            f"do not match midpoint formula (max diff {max_distance_diff:.6f} m)"
        )
    return errors


def main() -> int:
    args = parse_args()
    if not args.db.exists():
        print(f"Database not found: {args.db}", file=sys.stderr)
        return 2

    con = duckdb.connect(str(args.db), read_only=not args.execute)
    affected_reaches = load_affected_reaches(con)
    candidate_nodes, candidate_reaches, issues = build_candidate(
        con, affected_reaches, args.tie_tolerance_m
    )
    summary = build_summary(con, args.db, candidate_nodes, candidate_reaches, issues)
    errors = validate_candidate(candidate_nodes, candidate_reaches, issues)
    write_outputs(
        args.output_dir,
        args.prefix,
        candidate_nodes,
        candidate_reaches,
        issues,
        summary,
    )

    if errors:
        print("Candidate validation failed:", file=sys.stderr)
        for error in errors:
            print(f"  - {error}", file=sys.stderr)
        return 1

    if args.execute:
        summary = apply_candidate(
            con,
            args.output_dir,
            args.prefix,
            candidate_nodes,
            candidate_reaches,
            summary,
        )
        post_errors = post_apply_check(con, candidate_nodes, candidate_reaches)
        write_outputs(
            args.output_dir,
            args.prefix,
            candidate_nodes,
            candidate_reaches,
            issues,
            summary,
        )
        if post_errors:
            print("Post-apply validation failed:", file=sys.stderr)
            for error in post_errors:
                print(f"  - {error}", file=sys.stderr)
            return 1

    print(json.dumps(asdict(summary), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
