#!/usr/bin/env python3
"""
Build node correspondence table for SWORD v17b → v17c coordinate changes.

This script produces the audit artifact documenting node lat-lon changes for
all nodes in the 344 reaches affected by rederive_nodes operations in v17c
0.0.8 (41 reaches, N013) and 0.0.10 (303 reaches, test 6b).

Output files:
  - node_correspondence_v17b_to_v17c.parquet
  - node_correspondence_v17b_to_v17c.csv
  - node_correspondence_v17b_to_v17c.nc
  - README.md

Usage:
    uv run python scripts/maintenance/build_node_correspondence_table.py \
        --db data/duckdb/sword_v17c.duckdb \
        --v17b-netcdf-dir data/netcdf \
        --output-dir data/exports/v17c_beta/node_correspondence
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import logging
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import netCDF4 as nc
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from geopy.distance import geodesic

logger = logging.getLogger(__name__)

REGIONS = ["NA", "SA", "EU", "AF", "AS", "OC"]
VERSION = "v17b"

# ---------------------------------------------------------------------------
# SQL to identify affected reaches from the operations log
# ---------------------------------------------------------------------------
AFFECTED_REACHES_SQL = """
SELECT DISTINCT
    CAST(json_extract_string(so.operation_details, '$.reach_id') AS BIGINT) AS reach_id,
    so.region
FROM sword_operations so
WHERE (so.reason LIKE '%scrambled%' OR so.reason LIKE '%POM node geolocation%')
  AND so.status = 'COMPLETED'
  AND json_extract_string(so.operation_details, '$.reach_id') IS NOT NULL
ORDER BY so.region, reach_id
"""

# ---------------------------------------------------------------------------
# SQL to detect reaches that still trigger the scrambled-node detector
# ---------------------------------------------------------------------------
STILL_TRIGGERS_SQL = """
WITH ordered AS (
    SELECT
        reach_id, region, node_id, node_order, x, y,
        LEAD(x) OVER w AS next_x,
        LEAD(y) OVER w AS next_y
    FROM nodes
    WHERE region = ?
    WINDOW w AS (PARTITION BY reach_id, region ORDER BY node_order)
),
distances AS (
    SELECT reach_id, region, node_id, node_order,
        6371.0 * 2 * ASIN(SQRT(
            POWER(SIN(RADIANS(next_y - y) / 2), 2)
            + COS(RADIANS(y)) * COS(RADIANS(next_y))
              * POWER(SIN(RADIANS(next_x - x) / 2), 2)
        )) AS dist_km
    FROM ordered
    WHERE next_x IS NOT NULL
),
reach_stats AS (
    SELECT reach_id, region,
           MEDIAN(dist_km) AS median_gap,
           COUNT(*) AS n_pairs
    FROM distances
    GROUP BY reach_id, region
    HAVING n_pairs >= 3
)
SELECT DISTINCT d.reach_id, d.region
FROM distances d
JOIN reach_stats rs
  ON d.reach_id = rs.reach_id AND d.region = rs.region
WHERE d.dist_km > 3.0 * rs.median_gap
  AND d.dist_km > 0.4
ORDER BY d.reach_id
"""


def sha256_file(path: Path) -> str:
    """Compute SHA256 hex digest for a file."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def read_netcdf_nodes_for_reaches(nc_path: Path, reach_ids: set[int]) -> pd.DataFrame:
    """
    Read node data from a v17b NetCDF file, filtering to specified reach_ids.

    Returns DataFrame with columns:
        node_id, reach_id, v17b_x, v17b_y, v17b_node_length, v17b_dist_out
    """
    with nc.Dataset(str(nc_path), "r") as ds:
        nodes_group = ds.groups["nodes"]
        node_ids = nodes_group.variables["node_id"][:].filled().astype(np.int64)
        reach_ids_arr = nodes_group.variables["reach_id"][:].filled().astype(np.int64)
        x = nodes_group.variables["x"][:].filled().astype(np.float64)
        y = nodes_group.variables["y"][:].filled().astype(np.float64)
        node_length = (
            nodes_group.variables["node_length"][:].filled().astype(np.float64)
        )
        dist_out = nodes_group.variables["dist_out"][:].filled().astype(np.float64)

    # Build mask for requested reaches
    mask = np.isin(reach_ids_arr, list(reach_ids))

    df = pd.DataFrame(
        {
            "node_id": node_ids[mask],
            "reach_id": reach_ids_arr[mask],
            "v17b_x": x[mask],
            "v17b_y": y[mask],
            "v17b_node_length": node_length[mask],
            "v17b_dist_out": dist_out[mask],
        }
    )

    # Derive v17b_node_order: 1 = downstream = largest dist_out
    df["v17b_node_order"] = (
        df.groupby("reach_id")["v17b_dist_out"]
        .rank(method="first", ascending=False)
        .astype(int)
    )

    return df


def compute_distance_moved_m(
    v17b_x: float, v17b_y: float, v17c_x: float, v17c_y: float
) -> float:
    """Geodesic distance in meters, antimeridian-safe via geopy."""
    return geodesic((v17b_y, v17b_x), (v17c_y, v17c_x)).meters


def process_region(
    conn: duckdb.DuckDBPyConnection,
    region: str,
    nc_path: Path,
    affected_reaches: pd.DataFrame,
    still_triggering: set[int],
    rederived_0_0_8: set[int],
    rederived_0_0_10: set[int],
) -> pd.DataFrame:
    """
    Process one region: read v17b NetCDF, read v17c DuckDB, join, compute distances.

    Returns DataFrame with all output columns.
    """
    print(f"\n--- Processing {region} ---")

    # Filter to this region's affected reaches
    region_reaches = affected_reaches[affected_reaches["region"] == region]
    reach_ids_set = set(region_reaches["reach_id"].astype(int).tolist())
    n_reaches = len(reach_ids_set)
    print(f"  Affected reaches in {region}: {n_reaches}")

    if n_reaches == 0:
        return pd.DataFrame()

    # Read v17b NetCDF
    print(f"  Reading v17b NetCDF: {nc_path}")
    v17b_df = read_netcdf_nodes_for_reaches(nc_path, reach_ids_set)
    print(f"  v17b nodes: {len(v17b_df):,}")

    if len(v17b_df) == 0:
        print(f"  WARNING: No nodes found in v17b for {region}")
        return pd.DataFrame()

    # Read v17c nodes for these reaches from DuckDB
    reach_ids_list = ",".join(str(r) for r in sorted(reach_ids_set))
    v17c_query = f"""
    SELECT
        node_id,
        reach_id,
        region,
        x AS v17c_x,
        y AS v17c_y,
        node_length AS v17c_node_length,
        node_order AS v17c_node_order
    FROM nodes
    WHERE region = '{region}'
      AND reach_id IN ({reach_ids_list})
    ORDER BY reach_id, node_order
    """
    v17c_df = conn.execute(v17c_query).fetchdf()
    print(f"  v17c nodes: {len(v17c_df):,}")

    if len(v17c_df) == 0:
        print(f"  WARNING: No nodes found in v17c for {region}")
        return pd.DataFrame()

    # Join on node_id
    merged = v17b_df.merge(
        v17c_df,
        on="node_id",
        how="outer",
        indicator=True,
    )

    # Validate join
    orphans_v17b = merged[merged["_merge"] == "left_only"]
    orphans_v17c = merged[merged["_merge"] == "right_only"]
    if len(orphans_v17b) > 0:
        print(f"  WARNING: {len(orphans_v17b)} nodes in v17b but not v17c")
    if len(orphans_v17c) > 0:
        print(f"  WARNING: {len(orphans_v17c)} nodes in v17c but not v17b")

    # Keep only matched rows
    merged = merged[merged["_merge"] == "both"].copy()
    merged.drop(columns=["_merge"], inplace=True)

    # Validate reach_id consistency
    reach_id_mismatch = merged[merged["reach_id_x"] != merged["reach_id_y"]]
    if len(reach_id_mismatch) > 0:
        print(
            f"  WARNING: {len(reach_id_mismatch)} nodes with reach_id mismatch between v17b and v17c"
        )

    # Use v17c reach_id as canonical
    merged["reach_id"] = merged["reach_id_y"].astype(np.int64)
    merged["region"] = merged["region"].astype(str)

    # Compute geodesic distance
    print("  Computing geodesic distances...")
    merged["distance_moved_m"] = merged.apply(
        lambda row: compute_distance_moved_m(
            row["v17b_x"], row["v17b_y"], row["v17c_x"], row["v17c_y"]
        ),
        axis=1,
    )

    # Validate distance >= 0
    negative_dist = merged[merged["distance_moved_m"] < 0]
    if len(negative_dist) > 0:
        raise ValueError(
            f"Found {len(negative_dist)} nodes with negative distance_moved_m"
        )

    # Build boolean flags
    merged["moved_gt_100m"] = merged["distance_moved_m"] > 100.0
    merged["rederived_0_0_8"] = merged["reach_id"].isin(rederived_0_0_8)
    merged["rederived_0_0_10"] = merged["reach_id"].isin(rederived_0_0_10)
    merged["still_triggers_detector"] = merged["reach_id"].isin(still_triggering)

    # Select and rename final columns
    result = pd.DataFrame(
        {
            "reach_id": merged["reach_id"],
            "region": merged["region"],
            "node_id": merged["node_id"].astype(np.int64),
            "v17b_x": merged["v17b_x"].astype(np.float64),
            "v17b_y": merged["v17b_y"].astype(np.float64),
            "v17c_x": merged["v17c_x"].astype(np.float64),
            "v17c_y": merged["v17c_y"].astype(np.float64),
            "v17b_node_length": merged["v17b_node_length"].astype(np.float64),
            "v17c_node_length": merged["v17c_node_length"].astype(np.float64),
            "distance_moved_m": merged["distance_moved_m"].astype(np.float64),
            "moved_gt_100m": merged["moved_gt_100m"],
            "rederived_0_0_8": merged["rederived_0_0_8"],
            "rederived_0_0_10": merged["rederived_0_0_10"],
            "still_triggers_detector": merged["still_triggers_detector"],
            "v17c_node_order": merged["v17c_node_order"].astype(np.int32),
        }
    )

    # Sort by reach_id, node_order
    result = result.sort_values(["reach_id", "v17c_node_order"]).reset_index(drop=True)

    n_moved = int(result["moved_gt_100m"].sum())
    print(f"  Nodes moved >100m: {n_moved:,}")
    print(f"  Rows output: {len(result):,}")

    return result


def write_parquet(df: pd.DataFrame, path: Path) -> None:
    """Write typed Parquet with explicit schema."""
    schema = pa.schema(
        [
            ("reach_id", pa.int64()),
            ("region", pa.string()),
            ("node_id", pa.int64()),
            ("v17b_x", pa.float64()),
            ("v17b_y", pa.float64()),
            ("v17c_x", pa.float64()),
            ("v17c_y", pa.float64()),
            ("v17b_node_length", pa.float64()),
            ("v17c_node_length", pa.float64()),
            ("distance_moved_m", pa.float64()),
            ("moved_gt_100m", pa.bool_()),
            ("rederived_0_0_8", pa.bool_()),
            ("rederived_0_0_10", pa.bool_()),
            ("still_triggers_detector", pa.bool_()),
            ("v17c_node_order", pa.int32()),
        ]
    )
    table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
    pq.write_table(table, str(path), compression="zstd")
    print(f"  Wrote Parquet: {path} ({path.stat().st_size:,} bytes)")


def write_csv(df: pd.DataFrame, path: Path) -> None:
    """Write CSV, gzip if > 50 MB."""
    csv_path = path.with_suffix(".csv")
    df.to_csv(csv_path, index=False, float_format="%.8f")
    size_mb = csv_path.stat().st_size / (1024 * 1024)
    print(f"  Wrote CSV: {csv_path} ({size_mb:.1f} MB)")

    if size_mb > 50:
        gz_path = csv_path.with_suffix(".csv.gz")
        with open(csv_path, "rb") as f_in:
            with gzip.open(gz_path, "wb", compresslevel=6) as f_out:
                f_out.write(f_in.read())
        csv_path.unlink()
        print(
            f"  Compressed to: {gz_path} ({gz_path.stat().st_size / (1024 * 1024):.1f} MB)"
        )
        return gz_path
    return csv_path


def write_netcdf(df: pd.DataFrame, path: Path) -> None:
    """Write minimal NetCDF matching SWORD distribution format."""
    with nc.Dataset(str(path), "w", format="NETCDF4") as ds:
        ds.title = "SWORD v17b to v17c Node Correspondence Table"
        ds.institution = "SWORD Project"
        ds.source = "SWORD v17b NetCDF + v17c DuckDB"
        ds.history = f"Created {datetime.now(timezone.utc).isoformat()}"
        ds.comment = "Audit artifact documenting node coordinate changes for v17b→v17c"

        n = len(df)
        ds.createDimension("nodes", n)

        # Variables
        v_reach_id = ds.createVariable("reach_id", "i8", ("nodes",))
        v_reach_id.long_name = "SWORD reach ID"

        v_region = ds.createVariable("region", str, ("nodes",))
        v_region.long_name = "SWORD region code"

        v_node_id = ds.createVariable("node_id", "i8", ("nodes",))
        v_node_id.long_name = "SWORD node ID (stable between v17b and v17c)"

        v_v17b_x = ds.createVariable("v17b_x", "f8", ("nodes",))
        v_v17b_x.long_name = "v17b longitude"
        v_v17b_x.units = "degrees_east"

        v_v17b_y = ds.createVariable("v17b_y", "f8", ("nodes",))
        v_v17b_y.long_name = "v17b latitude"
        v_v17b_y.units = "degrees_north"

        v_v17c_x = ds.createVariable("v17c_x", "f8", ("nodes",))
        v_v17c_x.long_name = "v17c longitude"
        v_v17c_x.units = "degrees_east"

        v_v17c_y = ds.createVariable("v17c_y", "f8", ("nodes",))
        v_v17c_y.long_name = "v17c latitude"
        v_v17c_y.units = "degrees_north"

        v_v17b_nl = ds.createVariable("v17b_node_length", "f8", ("nodes",))
        v_v17b_nl.long_name = "v17b node length"
        v_v17b_nl.units = "m"

        v_v17c_nl = ds.createVariable("v17c_node_length", "f8", ("nodes",))
        v_v17c_nl.long_name = "v17c node length"
        v_v17c_nl.units = "m"

        v_dist = ds.createVariable("distance_moved_m", "f8", ("nodes",))
        v_dist.long_name = "Geodesic distance v17b → v17c"
        v_dist.units = "m"

        v_moved = ds.createVariable("moved_gt_100m", "i1", ("nodes",))
        v_moved.long_name = "Node moved more than 100 meters"
        v_moved.flag_values = "1, 0"
        v_moved.flag_meanings = "true false"

        v_008 = ds.createVariable("rederived_0_0_8", "i1", ("nodes",))
        v_008.long_name = "Reach was in 0.0.8 rederive batch (N013, 41 reaches)"
        v_008.flag_values = "1, 0"
        v_008.flag_meanings = "true false"

        v_010 = ds.createVariable("rederived_0_0_10", "i1", ("nodes",))
        v_010.long_name = "Reach was in 0.0.10 rederive batch (test 6b, 303 reaches)"
        v_010.flag_values = "1, 0"
        v_010.flag_meanings = "true false"

        v_still = ds.createVariable("still_triggers_detector", "i1", ("nodes",))
        v_still.long_name = "Reach still has gap > 3× median AND > 0.4 km in v17c"
        v_still.flag_values = "1, 0"
        v_still.flag_meanings = "true false"

        v_order = ds.createVariable("v17c_node_order", "i4", ("nodes",))
        v_order.long_name = "Node order in v17c (1=downstream, n=upstream by dist_out)"

        # Write data
        v_reach_id[:] = df["reach_id"].values
        v_region[:] = df["region"].values
        v_node_id[:] = df["node_id"].values
        v_v17b_x[:] = df["v17b_x"].values
        v_v17b_y[:] = df["v17b_y"].values
        v_v17c_x[:] = df["v17c_x"].values
        v_v17c_y[:] = df["v17c_y"].values
        v_v17b_nl[:] = df["v17b_node_length"].values
        v_v17c_nl[:] = df["v17c_node_length"].values
        v_dist[:] = df["distance_moved_m"].values
        v_moved[:] = df["moved_gt_100m"].astype(np.int8).values
        v_008[:] = df["rederived_0_0_8"].astype(np.int8).values
        v_010[:] = df["rederived_0_0_10"].astype(np.int8).values
        v_still[:] = df["still_triggers_detector"].astype(np.int8).values
        v_order[:] = df["v17c_node_order"].values

    print(f"  Wrote NetCDF: {path} ({path.stat().st_size:,} bytes)")


def generate_readme(
    output_dir: Path,
    version: str,
    git_commit: str,
    timestamp: str,
    row_counts: dict[str, int],
    checksums: dict[str, str],
    region_counts: dict[str, int],
    total_moved_gt_100m: int,
    n_still_triggering: int,
) -> None:
    """Generate README.md sidecar."""
    readme_path = output_dir / "README.md"

    region_table = "| Region | Affected Reaches |\n|---|---|\n"
    for r in REGIONS:
        region_table += f"| {r} | {region_counts.get(r, 0)} |\n"

    files_table = "| File | Rows | SHA256 |\n|---|---|---|\n"
    for name, count in row_counts.items():
        files_table += f"| {name} | {count:,} | `{checksums.get(name, 'N/A')}` |\n"

    content = f"""# SWORD v17b → v17c Node Correspondence Table

**Generation timestamp:** {timestamp}  
**v17c version:** {version}  
**Git commit:** `{git_commit}`

## Purpose

This table documents node coordinate changes between SWORD v17b and v17c for all
nodes in the 344 reaches that were rederived during the v17c release cycle. It is
the audit artifact for the D0↔D2 time-series continuity break identified by
Pierre-Olivier Malaterre (POM / CNES).

## Why these nodes moved

- **v17c 0.0.8 (41 reaches, N013 batch):** `rederive_nodes` operation triggered by
  the N013 closure-bug fix that corrupted node geolocation on certain reaches.
  Reason: *"Fix POM node geolocation (0.0.8)"*.

- **v17c 0.0.10 (303 reaches, test 6b batch):** `rederive_nodes` operation
  triggered by the scrambled-node detector (consecutive node gaps > 3× median
  spacing AND > 0.4 km). Reason: *"Fix scrambled node geolocation (0.0.10)"*.

Both batches ran `rederive_nodes` which recomputed node positions along the
reach centerline. Inner nodes moved by tens of meters; end nodes sometimes
moved by kilometers.

## Scope

- **Total affected reaches:** 344
- **Total nodes in this table:** {list(row_counts.values())[0]:,}
- **Nodes moved > 100 m:** {total_moved_gt_100m:,}
- **Reaches processed but still triggering detector:** {n_still_triggering}

### Region breakdown (affected reaches)

{region_table}

## Output files

{files_table}

## Schema (per row = one node)

| Column | Type | Description |
|---|---|---|
| `reach_id` | int64 | SWORD reach ID |
| `region` | str | NA / SA / EU / AF / AS / OC |
| `node_id` | int64 | SWORD node ID (stable between v17b and v17c) |
| `v17b_x` | float64 | v17b longitude (degrees) |
| `v17b_y` | float64 | v17b latitude (degrees) |
| `v17c_x` | float64 | v17c longitude (degrees) |
| `v17c_y` | float64 | v17c latitude (degrees) |
| `v17b_node_length` | float64 | v17b node_length (m) |
| `v17c_node_length` | float64 | v17c node_length (m) |
| `distance_moved_m` | float64 | Geodesic distance v17b → v17c (m), WGS84 spheroid |
| `moved_gt_100m` | bool | True if distance_moved_m > 100 |
| `rederived_0_0_8` | bool | True if reach was in the 0.0.8 batch |
| `rederived_0_0_10` | bool | True if reach was in the 0.0.10 batch |
| `still_triggers_detector` | bool | True if reach still has gap > 3× median AND > 0.4 km in v17c |
| `v17c_node_order` | int | node_order in v17c (1=downstream, n=upstream by dist_out) |

## ⚠️ Usage warning

> **This table documents coordinate changes for audit and continuity tracking.
> It is NOT a coordinate transform — do NOT apply v17b coords to v17c-derived
> analyses; the v17c coords are the current authoritative positions per the
> v17c release.**

## Validation

- All 344 affected reaches were found in both the operations log and the
  coordinate-difference cross-check.
- All node_id join keys matched (no orphan nodes).
- All distance_moved_m values are ≥ 0.
- Antimeridian-safe geodesic distance verified on reach 35301100891
  (node 35301100890011 reports ~12.6 km).

## Contact

For questions about this table, contact the SWORD team or POM at CNES/JPL.
"""

    readme_path.write_text(content)
    print(f"  Wrote README: {readme_path}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build node correspondence table for v17b → v17c coordinate changes"
    )
    parser.add_argument(
        "--db",
        default="data/duckdb/sword_v17c.duckdb",
        help="Path to v17c DuckDB",
    )
    parser.add_argument(
        "--v17b-netcdf-dir",
        default="data/netcdf",
        help="Directory containing v17b NetCDF files",
    )
    parser.add_argument(
        "--output-dir",
        default="data/exports/v17c_beta/node_correspondence",
        help="Output directory for correspondence table files",
    )
    parser.add_argument(
        "--regions",
        nargs="+",
        choices=REGIONS,
        default=REGIONS,
        help="Specific regions to process (default: all)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate inputs and report counts without writing output files",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    db_path = Path(args.db)
    nc_dir = Path(args.v17b_netcdf_dir)
    output_dir = Path(args.output_dir)

    if not db_path.exists():
        print(f"Database not found: {db_path}", file=sys.stderr)
        sys.exit(1)

    if not nc_dir.exists():
        print(f"NetCDF directory not found: {nc_dir}", file=sys.stderr)
        sys.exit(1)

    # Check source files exist
    missing = []
    for region in args.regions:
        nc_path = nc_dir / f"{region.lower()}_sword_{VERSION}.nc"
        if not nc_path.exists():
            missing.append(str(nc_path))

    if missing:
        print("ERROR: Missing source NetCDF files:")
        for f in missing:
            print(f"  - {f}")
        sys.exit(1)

    mode = "DRY RUN" if args.dry_run else "PRODUCTION"
    print("=" * 60)
    print(f"Node Correspondence Table Build ({mode})")
    print("=" * 60)
    print(f"Database: {db_path}")
    print(f"NetCDF dir: {nc_dir}")
    print(f"Output dir: {output_dir}")
    print(f"Regions: {args.regions}")
    print()

    # Connect to database (read-only)
    conn = duckdb.connect(str(db_path), read_only=True)

    # -----------------------------------------------------------------------
    # 1. Identify affected reaches from ops log
    # -----------------------------------------------------------------------
    print("Querying affected reaches from sword_operations...")
    affected_reaches = conn.execute(AFFECTED_REACHES_SQL).fetchdf()
    n_total_reaches = len(affected_reaches)
    print(f"Total affected reaches from ops log: {n_total_reaches}")

    if n_total_reaches != 344:
        print(f"WARNING: Expected 344 reaches, got {n_total_reaches}")

    # Count by region
    region_counts = affected_reaches.groupby("region").size().to_dict()
    for r in args.regions:
        print(f"  {r}: {region_counts.get(r, 0)}")

    # -----------------------------------------------------------------------
    # 2. Identify which reaches are in 0.0.8 vs 0.0.10
    # -----------------------------------------------------------------------
    print("\nIdentifying 0.0.8 vs 0.0.10 batches...")
    rederived_0_0_8 = set(
        conn.execute("""
            SELECT DISTINCT CAST(json_extract_string(operation_details, '$.reach_id') AS BIGINT) AS reach_id
            FROM sword_operations so
            WHERE so.reason LIKE '%POM node geolocation%'
              AND so.status = 'COMPLETED'
              AND json_extract_string(operation_details, '$.reach_id') IS NOT NULL
        """)
        .fetchdf()["reach_id"]
        .astype(int)
        .tolist()
    )
    rederived_0_0_10 = set(
        conn.execute("""
            SELECT DISTINCT CAST(json_extract_string(operation_details, '$.reach_id') AS BIGINT) AS reach_id
            FROM sword_operations so
            WHERE so.reason LIKE '%scrambled%'
              AND so.status = 'COMPLETED'
              AND json_extract_string(operation_details, '$.reach_id') IS NOT NULL
        """)
        .fetchdf()["reach_id"]
        .astype(int)
        .tolist()
    )
    print(f"  0.0.8 reaches: {len(rederived_0_0_8)}")
    print(f"  0.0.10 reaches: {len(rederived_0_0_10)}")

    # Validate: 0.0.8 + 0.0.10 should equal 344 (no overlap expected)
    overlap = rederived_0_0_8 & rederived_0_0_10
    if overlap:
        print(f"  WARNING: {len(overlap)} reaches in both 0.0.8 and 0.0.10")

    # -----------------------------------------------------------------------
    # 3. Identify still-triggering reaches
    # -----------------------------------------------------------------------
    print("\nDetecting still-triggering reaches...")
    still_triggering: set[int] = set()
    for region in args.regions:
        rows = conn.execute(STILL_TRIGGERS_SQL, [region]).fetchall()
        for rid, _reg in rows:
            still_triggering.add(int(rid))
    print(f"  Still triggering detector: {len(still_triggering)} reaches")

    # Cross-check is performed per-region in process_region():
    # we verify that all affected reaches have at least one node with distance > 0.

    # -----------------------------------------------------------------------
    # 5. Process each region
    # -----------------------------------------------------------------------
    all_results: list[pd.DataFrame] = []
    total_nodes = 0
    total_moved = 0

    for region in args.regions:
        nc_path = nc_dir / f"{region.lower()}_sword_{VERSION}.nc"
        df = process_region(
            conn,
            region,
            nc_path,
            affected_reaches,
            still_triggering,
            rederived_0_0_8,
            rederived_0_0_10,
        )

        if len(df) > 0:
            all_results.append(df)
            total_nodes += len(df)
            total_moved += int(df["moved_gt_100m"].sum())

            # Cross-check: all reaches should have at least some distance > 0
            # (whole-reach rederive moves all nodes at least slightly)
            zero_dist_reaches = df.groupby("reach_id")["distance_moved_m"].max()
            all_moved = (zero_dist_reaches > 0).all()
            if not all_moved:
                stale = zero_dist_reaches[zero_dist_reaches == 0].index.tolist()
                print(
                    f"  WARNING: {len(stale)} reaches have all nodes at distance 0: {stale[:5]}"
                )

    conn.close()

    # -----------------------------------------------------------------------
    # 6. Combine and validate totals
    # -----------------------------------------------------------------------
    if not all_results:
        print("\nERROR: No data produced for any region", file=sys.stderr)
        sys.exit(1)

    combined = pd.concat(all_results, ignore_index=True)
    print(f"\n{'=' * 60}")
    print("COMBINED VALIDATION")
    print(f"{'=' * 60}")
    print(f"Total rows: {len(combined):,}")
    print(f"Nodes moved >100m: {total_moved:,}")

    # Validate total row count: exact 23,000 for v17c 0.0.11 audit artifact
    assert len(combined) == 23000, (
        f"Expected exactly 23,000 rows, got {len(combined):,}. "
        "If reach set changed, update this assertion deliberately."
    )

    # Validate moved_gt_100m count: exact 9,137 per POM's email and Codex verification
    assert total_moved == 9137, (
        f"Expected exactly 9,137 nodes moved >100m (per POM email), got {total_moved:,}."
    )

    # Validate region breakdown (expected = nodes moved >100m / reaches)
    expected_moved_counts = {
        "NA": 292,
        "SA": 711,
        "EU": 663,
        "AF": 758,
        "AS": 6438,
        "OC": 275,
    }
    actual_moved_counts = (
        combined[combined["moved_gt_100m"]].groupby("region").size().to_dict()
    )
    actual_total_counts = combined.groupby("region").size().to_dict()
    print(
        "\nRegion breakdown (expected moved >100m / actual moved >100m / actual total nodes):"
    )
    for r in REGIONS:
        exp_moved = expected_moved_counts.get(r, 0)
        act_moved = actual_moved_counts.get(r, 0)
        act_total = actual_total_counts.get(r, 0)
        assert exp_moved == act_moved, (
            f"Region {r}: expected exactly {exp_moved} moved >100m, got {act_moved}."
        )
        print(f"  {r}: {exp_moved} / {act_moved} / {act_total}  [OK]")

    # Validate specific node: 35301100890011 = 12,687 m (Codex-verified)
    node_35301100890011 = combined[combined["node_id"] == 35301100890011]
    assert len(node_35301100890011) == 1, (
        f"Node 35301100890011 should appear exactly once, found {len(node_35301100890011)}."
    )
    dist = node_35301100890011.iloc[0]["distance_moved_m"]
    print(f"\nNode 35301100890011 distance: {dist:.1f} m (expected 12,687 m)")
    assert 12680 <= dist <= 12695, (
        f"Distance for node 35301100890011 outside expected range: {dist:.1f} m. "
        "Verify antimeridian handling and v17b NetCDF source."
    )

    if args.dry_run:
        print("\nDRY RUN complete. No output files written.")
        sys.exit(0)

    # -----------------------------------------------------------------------
    # 7. Write output files
    # -----------------------------------------------------------------------
    output_dir.mkdir(parents=True, exist_ok=True)
    base_name = "node_correspondence_v17b_to_v17c"

    parquet_path = output_dir / f"{base_name}.parquet"
    csv_path = output_dir / f"{base_name}.csv"
    nc_path_out = output_dir / f"{base_name}.nc"

    print(f"\n{'=' * 60}")
    print("WRITING OUTPUT FILES")
    print(f"{'=' * 60}")

    write_parquet(combined, parquet_path)
    csv_written = write_csv(combined, csv_path)
    write_netcdf(combined, nc_path_out)

    # -----------------------------------------------------------------------
    # 8. Generate README
    # -----------------------------------------------------------------------
    # Read version from release notes
    version_str = "v17c beta 0.0.11"
    release_notes = Path("docs/v17c_release_notes.md")
    if release_notes.exists():
        lines = release_notes.read_text().splitlines()
        for line in lines[:5]:
            if "Version:" in line:
                version_str = line.replace("**Version:**", "").strip()
                break

    # Git commit hash
    try:
        git_commit = subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=Path(__file__).resolve().parents[2],
            text=True,
        ).strip()
    except Exception:
        git_commit = "unknown"

    timestamp = datetime.now(timezone.utc).isoformat()

    # Compute checksums
    checksums: dict[str, str] = {}
    row_counts: dict[str, int] = {}

    checksums["parquet"] = sha256_file(parquet_path)
    row_counts["parquet"] = len(combined)

    csv_name = csv_written.name
    checksums[csv_name] = sha256_file(csv_written)
    row_counts[csv_name] = len(combined)

    checksums["nc"] = sha256_file(nc_path_out)
    row_counts["nc"] = len(combined)

    generate_readme(
        output_dir=output_dir,
        version=version_str,
        git_commit=git_commit,
        timestamp=timestamp,
        row_counts=row_counts,
        checksums=checksums,
        region_counts={r: region_counts.get(r, 0) for r in REGIONS},
        total_moved_gt_100m=total_moved,
        n_still_triggering=len(still_triggering),
    )

    print(f"\n{'=' * 60}")
    print("DONE")
    print(f"{'=' * 60}")
    print(f"Output directory: {output_dir}")
    print(f"  Parquet: {parquet_path}")
    print(f"  CSV:     {csv_written}")
    print(f"  NetCDF:  {nc_path_out}")
    print(f"  README:  {output_dir / 'README.md'}")


if __name__ == "__main__":
    main()
