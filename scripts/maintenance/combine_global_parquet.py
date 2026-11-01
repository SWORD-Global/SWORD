"""Combine per-region SWORD v17c GeoParquet into global all-continent files.

Produces, for each layer (reaches, nodes):
  - one combined GeoParquet (streamed, geo metadata preserved with unioned bbox)
  - one DuckDB file with a native GEOMETRY column (WKB decoded via ST_GeomFromWKB)
"""

import json
import sys
from pathlib import Path

import duckdb
import pyarrow.parquet as pq

BUNDLE = Path(sys.argv[1]) if len(sys.argv) > 1 else sys.exit("usage: combine_global_parquet.py <bundle dir with parquet/>")
SRC = BUNDLE / "parquet"
OUT = BUNDLE / "global"
REGIONS = ["AF", "AS", "EU", "NA", "OC", "SA"]


def region_path(region: str, layer: str) -> Path:
    return SRC / f"sword_{region}_v17c_{layer}.parquet"


def union_bbox(boxes: list[list[float]]) -> list[float]:
    minx = min(b[0] for b in boxes)
    miny = min(b[1] for b in boxes)
    maxx = max(b[2] for b in boxes)
    maxy = max(b[3] for b in boxes)
    return [minx, miny, maxx, maxy]


def build_global_geo_meta(layer: str) -> bytes:
    """Take first region's geo metadata, union bbox across all regions."""
    base = json.loads(pq.read_metadata(region_path("AF", layer)).metadata[b"geo"])
    boxes = []
    for r in REGIONS:
        g = json.loads(pq.read_metadata(region_path(r, layer)).metadata[b"geo"])
        boxes.append(g["columns"]["geometry"]["bbox"])
    base["columns"]["geometry"]["bbox"] = union_bbox(boxes)
    return json.dumps(base).encode("utf-8")


def combine_parquet(layer: str) -> Path:
    out_path = OUT / f"sword_global_v17c_{layer}.parquet"
    if out_path.exists():
        print(f"  {out_path} exists, reusing")
        return out_path
    geo_meta = build_global_geo_meta(layer)

    first = pq.ParquetFile(region_path("AF", layer))
    schema = first.schema_arrow
    existing = dict(schema.metadata or {})
    existing[b"geo"] = geo_meta
    schema = schema.with_metadata(existing)

    total = 0
    with pq.ParquetWriter(out_path, schema, compression="zstd") as writer:
        for r in REGIONS:
            pf = pq.ParquetFile(region_path(r, layer))
            assert pf.schema_arrow.equals(first.schema_arrow, check_metadata=False), (
                f"{layer} schema mismatch for region {r}"
            )
            rows = 0
            for batch in pf.iter_batches(batch_size=100_000):
                writer.write_batch(batch)
                rows += batch.num_rows
            total += rows
            print(f"  {layer} {r}: {rows:,} rows")
    print(f"  -> {out_path} ({total:,} rows)")
    return out_path


def build_duckdb(layer: str, parquet_path: Path) -> Path:
    db_path = OUT / f"sword_global_v17c_{layer}.duckdb"
    if db_path.exists():
        db_path.unlink()
    con = duckdb.connect(str(db_path))
    con.execute("INSTALL spatial; LOAD spatial;")
    # decode WKB geometry blob -> native GEOMETRY, keep all other columns as-is
    # spatial extension reads the GeoParquet geometry column as native GEOMETRY
    con.execute(f"CREATE TABLE {layer} AS SELECT * FROM read_parquet('{parquet_path}')")
    n = con.execute(f"SELECT COUNT(*) FROM {layer}").fetchone()[0]
    con.execute(f"CREATE INDEX idx_{layer}_geom ON {layer} USING RTREE (geometry)")
    con.close()
    print(f"  -> {db_path} ({n:,} rows, GEOMETRY + RTREE)")
    return db_path


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    for layer in ["reaches", "nodes"]:
        print(f"[{layer}] combining GeoParquet...")
        pq_path = combine_parquet(layer)
        print(f"[{layer}] building DuckDB...")
        build_duckdb(layer, pq_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
