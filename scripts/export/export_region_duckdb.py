#!/usr/bin/env python3
"""Build the per-region DuckDB release files from the staged GeoParquet.

Each ``sword_{REGION}_v17c.duckdb`` holds ``reaches`` and ``nodes`` tables with
every GeoParquet column, the geometry as a native GEOMETRY column, and an RTREE
index on it. Run with the DuckDB version named in the release notes (1.3.x) so
the files open with that version or newer.

Usage:
    uv run --with duckdb==1.3.2 python scripts/export/export_region_duckdb.py \\
        data/exports/v17c_beta/<staging>
"""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb

REGIONS = ["AF", "AS", "EU", "NA", "OC", "SA"]
LAYERS = ["reaches", "nodes"]


def build(bundle: Path, region: str) -> Path:
    out = bundle / "duckdb" / f"sword_{region}_v17c.duckdb"
    if out.exists():
        out.unlink()
    con = duckdb.connect(str(out))
    con.execute("INSTALL spatial; LOAD spatial;")
    for layer in LAYERS:
        src = bundle / "parquet" / f"sword_{region}_v17c_{layer}.parquet"
        con.execute(f"CREATE TABLE {layer} AS SELECT * FROM read_parquet('{src}')")
        n_db = con.execute(f"SELECT count(*) FROM {layer}").fetchone()[0]
        n_src = con.execute(f"SELECT count(*) FROM read_parquet('{src}')").fetchone()[0]
        if n_db != n_src:
            raise RuntimeError(
                f"{region} {layer}: wrote {n_db} rows, parquet has {n_src}"
            )
        con.execute(f"CREATE INDEX idx_{layer}_geom ON {layer} USING RTREE (geometry)")
        print(f"  {region} {layer}: {n_db:,} rows")
    con.close()
    return out


def main() -> int:
    if len(sys.argv) != 2:
        sys.exit("usage: export_region_duckdb.py <bundle dir with parquet/>")
    bundle = Path(sys.argv[1])
    (bundle / "duckdb").mkdir(parents=True, exist_ok=True)
    print(f"DuckDB {duckdb.__version__}")
    for region in REGIONS:
        out = build(bundle, region)
        print(f"-> {out} ({out.stat().st_size / 1e9:.2f} GB)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
