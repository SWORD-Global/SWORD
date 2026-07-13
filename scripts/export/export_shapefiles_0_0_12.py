"""Export SWORD v17c 0.0.12 shapefiles from the staged GeoPackages.

Shapefiles follow the v17b distribution convention: split by HydroBASINS
Pfafstetter level-2 basin (first two digits of reach_id/node_id) within
each region, because per-region node layers exceed the 2 GB DBF limit
(AS nodes alone would be ~7.4 GB).

DBF field names are limited to 10 characters, so columns are renamed via
an explicit mapping (no GDAL auto-laundering). The mapping is written to
shapefile_field_name_mapping.csv alongside the shapefiles. DOUBLE columns
are cast to numeric(17,8) and BIGINT to numeric(14,0) to keep the largest
basin (SA hb62, Amazon: 1.13M nodes) safely under the 2 GB DBF limit.

Usage:
    uv run python scripts/export/export_shapefiles_0_0_12.py [--region OC]
"""

from __future__ import annotations

import argparse
import csv
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import duckdb

STAGING = Path("data/exports/v17c_beta/zenodo_0.0.12")
GPKG_DIR = STAGING / "gpkg"
PARQUET_DIR = STAGING / "parquet"
OUT_DIR = STAGING / "shp"

REGIONS = ["AF", "AS", "EU", "NA", "OC", "SA"]

# reach_id: 11 digits (CBBBBBRRRRT), node_id: 14 digits (CBBBBBRRRRNNNT).
# First two digits = Pfafstetter level-2 basin (continent digit + basin).
REACH_HB_DIV = 10**9
NODE_HB_DIV = 10**12

# Explicit short names for columns that exceed 10 chars or would collide
# after truncation. Everything not listed and <= 10 chars keeps its name.
EXPLICIT_SHORT = {
    "dist_out_dijkstra": "dist_dijk",
    "hydro_dist_out": "hdist_out",
    "hydro_dist_hw": "hdist_hw",
    "rch_id_up_main": "rid_up_m",
    "rch_id_dn_main": "rid_dn_m",
    "river_name_en": "rivname_en",
    "river_name_local": "rivname_lo",
    "slope_obs_n_passes": "sobs_npass",
    "slope_obs_slopeF": "sobs_f",
    "slope_obs_reliable": "sobs_rel",
    "slope_obs_quality": "sobs_qual",
    "slope_obs_range": "sobs_range",
    "slope_obs_mad": "sobs_mad",
    "slope_obs_adj": "sobs_adj",
    "reach_length": "reach_len",
    "node_length": "node_len",
    "subnetwork_id": "subnet_id",
    "stream_order": "strm_order",
    "is_mainstem": "is_mainstm",
    "best_headwater": "best_hw",
    "best_outlet": "best_out",
    "pathlen_out": "pathln_out",
    "main_path_id": "main_path",
    "low_slope_flag": "low_slope",
    "swot_obs_source": "swot_src",
    "meander_length": "meander_ln",
    "ext_dist_coef": "ext_dist_c",
    "facc_quality": "facc_qual",
}
PREFIX_SHORT = [
    ("wse_obs_", "wseobs_"),
    ("width_obs_", "wobs_"),
    ("slope_obs_", "sobs_"),
    ("rch_id_up_", "rid_up_"),
    ("rch_id_dn_", "rid_dn_"),
]

# Explicit widths for string columns (DBF truncates silently at width;
# river_name_local reaches 81 chars, over the GDAL default of 80).
STRING_WIDTHS = {
    "river_name": 80,
    "river_name_en": 80,
    "river_name_local": 100,
    "edit_flag": 60,
    "swot_obs_source": 8,
    "facc_quality": 12,
    "slope_obs_quality": 24,
    "version": 16,
    "region": 4,
}


def short_name(col: str) -> str:
    if col in EXPLICIT_SHORT:
        return EXPLICIT_SHORT[col]
    for prefix, repl in PREFIX_SHORT:
        if col.startswith(prefix):
            return (repl + col[len(prefix):])[:10]
    return col[:10]


def build_select(layer: str, columns: list[tuple[str, str]]) -> tuple[str, dict]:
    """Return (select_list_sql, {original: short}) for a layer."""
    mapping: dict[str, str] = {}
    # Explicit field lists drop geometry in OGR SQL; select it explicitly
    # (GPKG geometry column is named "geom").
    parts = ["geom"]
    for col, dtype in columns:
        if col == "geometry":
            continue
        short = short_name(col)
        if short in mapping.values():
            raise ValueError(f"{layer}: short-name collision on {short} ({col})")
        if len(short) > 10:
            raise ValueError(f"{layer}: {short} exceeds 10 chars ({col})")
        mapping[col] = short
        if dtype == "DOUBLE":
            parts.append(f'CAST("{col}" AS numeric(17,8)) AS "{short}"')
        elif dtype == "BIGINT":
            parts.append(f'CAST("{col}" AS numeric(14,0)) AS "{short}"')
        elif dtype == "BOOLEAN":
            parts.append(f'CAST("{col}" AS integer) AS "{short}"')
        elif dtype == "VARCHAR":
            width = STRING_WIDTHS.get(col, 80)
            parts.append(f'CAST("{col}" AS character({width})) AS "{short}"')
        else:  # INTEGER and friends: GDAL default width is fine
            parts.append(f'"{col}" AS "{short}"')
    return ", ".join(parts), mapping


def layer_columns(con: duckdb.DuckDBPyConnection, region: str, layer: str):
    rows = con.execute(
        f"DESCRIBE SELECT * FROM '{PARQUET_DIR}/sword_{region}_v17c_0.0.12_{layer}.parquet'"
    ).fetchall()
    return [(r[0], r[1].split("(")[0]) for r in rows]


def region_basins(con: duckdb.DuckDBPyConnection, region: str) -> list[int]:
    rows = con.execute(
        f"SELECT DISTINCT reach_id // {REACH_HB_DIV} AS hb "
        f"FROM '{PARQUET_DIR}/sword_{region}_v17c_0.0.12_reaches.parquet' ORDER BY hb"
    ).fetchall()
    return [int(r[0]) for r in rows]


def export_one(region: str, layer: str, hb: int, select_sql: str) -> str:
    gpkg = GPKG_DIR / f"sword_{region}_v17c_0.0.12.gpkg"
    reg = region.lower()
    out = OUT_DIR / f"{reg}_sword_{layer}_hb{hb}_v17c_0.0.12.shp"
    id_col = "reach_id" if layer == "reaches" else "node_id"
    div = REACH_HB_DIV if layer == "reaches" else NODE_HB_DIV
    lo, hi = hb * div, (hb + 1) * div
    sql = f"SELECT {select_sql} FROM {layer} WHERE {id_col} >= {lo} AND {id_col} < {hi}"
    # OGRSQL dialect (not GPKG's native SQLite): SQLite ignores
    # numeric(w,p)/character(n) width specs and mistypes cast doubles.
    cmd = [
        "ogr2ogr", "-f", "ESRI Shapefile", str(out), str(gpkg),
        "-dialect", "OGRSQL", "-sql", sql,
        "-lco", "ENCODING=UTF-8", "-overwrite",
    ]
    res = subprocess.run(cmd, capture_output=True, text=True)
    if res.returncode != 0:
        raise RuntimeError(f"ogr2ogr failed for {out.name}:\n{res.stderr[-2000:]}")
    return out.name


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--region", choices=REGIONS, help="single region (default: all)")
    ap.add_argument("--workers", type=int, default=5)
    args = ap.parse_args()
    regions = [args.region] if args.region else REGIONS

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()

    # Build select lists + mapping from any one region (schemas are identical).
    selects, mappings = {}, {}
    for layer in ("reaches", "nodes"):
        cols = layer_columns(con, regions[0], layer)
        selects[layer], mappings[layer] = build_select(layer, cols)

    mapping_csv = OUT_DIR / "shapefile_field_name_mapping.csv"
    with open(mapping_csv, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["layer", "original_name", "shapefile_field"])
        for layer in ("reaches", "nodes"):
            for orig, short in mappings[layer].items():
                w.writerow([layer, orig, short])
    print(f"wrote {mapping_csv}")

    jobs = []
    for region in regions:
        for hb in region_basins(con, region):
            for layer in ("reaches", "nodes"):
                jobs.append((region, layer, hb))
    print(f"{len(jobs)} export jobs across {len(regions)} region(s)")

    failures = []
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = {
            ex.submit(export_one, r, layer, hb, selects[layer]): (r, layer, hb)
            for r, layer, hb in jobs
        }
        for i, fut in enumerate(as_completed(futs), 1):
            key = futs[fut]
            try:
                name = fut.result()
                print(f"[{i}/{len(jobs)}] {name}")
            except Exception as e:  # noqa: BLE001 - collect and report all failures
                failures.append((key, str(e)))
                print(f"[{i}/{len(jobs)}] FAILED {key}: {e}", file=sys.stderr)

    if failures:
        sys.exit(f"{len(failures)} export job(s) failed")

    # Verification: feature counts per region/layer vs parquet, DBF size guard.
    print("\nVerifying feature counts and DBF sizes...")
    bad = []
    for region in regions:
        reg = region.lower()
        for layer in ("reaches", "nodes"):
            expected = con.execute(
                f"SELECT COUNT(*) FROM '{PARQUET_DIR}/sword_{region}_v17c_0.0.12_{layer}.parquet'"
            ).fetchone()[0]
            total = 0
            for shp in sorted(OUT_DIR.glob(f"{reg}_sword_{layer}_hb*_v17c_0.0.12.shp")):
                out = subprocess.run(
                    ["ogrinfo", "-ro", "-so", str(shp), shp.stem],
                    capture_output=True, text=True,
                )
                for line in out.stdout.splitlines():
                    if line.startswith("Feature Count:"):
                        total += int(line.split(":")[1])
            status = "OK" if total == expected else "COUNT MISMATCH"
            if total != expected:
                bad.append((region, layer, expected, total))
            print(f"{region} {layer}: shp={total} parquet={expected} {status}")
    for dbf in sorted(OUT_DIR.glob("*.dbf")):
        size = dbf.stat().st_size
        if size > 1.9e9:
            bad.append((dbf.name, "dbf-size", size, None))
            print(f"WARNING: {dbf.name} = {size / 1e9:.2f} GB (near/over 2 GB limit)")
    if bad:
        sys.exit(f"verification failed: {bad}")
    print("All counts match; all DBFs under limit.")


if __name__ == "__main__":
    main()
