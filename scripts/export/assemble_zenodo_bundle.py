"""Assemble the final Zenodo v17c bundle with clean v17c filenames.

Sources the verified bytes from the existing zenodo_0.0.12/ staging (created and
count-verified earlier) and re-stages them into zenodo_v17c/ with the "_0.0.12"
build suffix dropped, matching the v17b naming convention. Then packages one ZIP
per format (each embedding the four doc files), and writes two SHA256 manifests:
SHA256SUMS.txt (the uploaded ZIPs) and SHA256SUMS_files.txt (per-file, for
post-unzip verification).

Files are cloned with `cp -c` (APFS copy-on-write; instant, no extra disk) so
this is cheap despite the ~25 GB payload.

Usage:
    uv run python scripts/export/assemble_zenodo_bundle.py
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
SRC = REPO / "data/exports/v17c_beta/zenodo_0.0.12"
DST = REPO / "data/exports/v17c_beta/zenodo_v17c"
DOCS = REPO / "docs"

REGIONS = ["af", "as", "eu", "na", "oc", "sa"]
REGIONS_UP = ["AF", "AS", "EU", "NA", "OC", "SA"]

DOC_FILES = {
    "v17c_release_notes.md": DOCS / "v17c_release_notes.md",
    "v17c_release_notes.pdf": DOCS / "v17c_release_notes.pdf",
    "v17c_variable_reference.md": DOCS / "v17c_variable_reference.md",
    "v17c_variable_reference.pdf": DOCS / "v17c_variable_reference.pdf",
}
DOC_NAMES = list(DOC_FILES)


def clone(src: Path, dst: Path) -> None:
    if not src.is_file():
        sys.exit(f"missing source: {src}")
    dst.parent.mkdir(parents=True, exist_ok=True)
    r = subprocess.run(["cp", "-c", str(src), str(dst)], capture_output=True, text=True)
    if r.returncode != 0:
        sys.exit(f"cp -c failed: {src} -> {dst}\n{r.stderr}")


def strip_build(name: str) -> str:
    return name.replace("_0.0.12", "")


def main() -> None:
    if DST.exists():
        sys.exit(f"target already exists: {DST} (remove/rename it first to re-run)")
    if not SRC.is_dir():
        sys.exit(f"source staging missing: {SRC}")

    # 1. Clone per-format data files, dropping the build suffix.
    for reg in REGIONS:
        clone(SRC / f"netcdf/{reg}_sword_v17c_0.0.12.nc", DST / f"netcdf/{reg}_sword_v17c.nc")
    for reg in REGIONS_UP:
        clone(SRC / f"gpkg/sword_{reg}_v17c_0.0.12.gpkg", DST / f"gpkg/sword_{reg}_v17c.gpkg")
        clone(SRC / f"duckdb/sword_{reg}_v17c_0.0.12.duckdb", DST / f"duckdb/sword_{reg}_v17c.duckdb")
        for lyr in ("reaches", "nodes"):
            clone(SRC / f"parquet/sword_{reg}_v17c_0.0.12_{lyr}.parquet",
                  DST / f"parquet/sword_{reg}_v17c_{lyr}.parquet")
    for lyr in ("reaches", "nodes"):
        for ext in ("parquet", "duckdb"):
            clone(SRC / f"global/sword_global_v17c_0.0.12_{lyr}.{ext}",
                  DST / f"global/sword_global_v17c_{lyr}.{ext}")

    # 2. Shapefiles: clone every part, dropping the build suffix; plus mapping.
    shp_src = sorted((SRC / "shp").glob("*_0.0.12.*"))
    if not shp_src:
        sys.exit("no shapefiles found in source")
    for p in shp_src:
        clone(p, DST / "shp" / strip_build(p.name))
    clone(SRC / "shp/shapefile_field_name_mapping.csv",
          DST / "shp/shapefile_field_name_mapping.csv")

    # 3. Docs (regenerated) at bundle root.
    for name, src in DOC_FILES.items():
        clone(src, DST / name)

    # 4. Package one ZIP per format, each embedding the doc files.
    fmt_dirs = ["netcdf", "gpkg", "shp", "parquet", "duckdb", "global"]
    for fmt in fmt_dirs:
        zip_path = DST / f"SWORD_v17c_{fmt}.zip"
        cmd = ["zip", "-q", "-r", zip_path.name, fmt, *DOC_NAMES]
        r = subprocess.run(cmd, cwd=DST, capture_output=True, text=True)
        if r.returncode != 0:
            sys.exit(f"zip failed for {fmt}:\n{r.stderr}")
        print(f"built {zip_path.name} ({zip_path.stat().st_size / 1e9:.2f} GB)")

    # 5a. Per-file manifest (post-unzip verification).
    per_file = sorted(
        [p for p in DST.rglob("*")
         if p.is_file() and not p.name.startswith("SHA256SUMS")
         and p.suffix != ".zip"],
        key=lambda p: str(p.relative_to(DST)),
    )
    rels = [str(p.relative_to(DST)) for p in per_file]
    r = subprocess.run(["shasum", "-a", "256", *rels], cwd=DST, capture_output=True, text=True)
    if r.returncode != 0:
        sys.exit(f"per-file shasum failed:\n{r.stderr}")
    (DST / "SHA256SUMS_files.txt").write_text(r.stdout)

    # 5b. ZIP-level manifest (upload verification).
    zips = sorted(p.name for p in DST.glob("SWORD_v17c_*.zip"))
    r = subprocess.run(["shasum", "-a", "256", *zips], cwd=DST, capture_output=True, text=True)
    if r.returncode != 0:
        sys.exit(f"zip shasum failed:\n{r.stderr}")
    (DST / "SHA256SUMS.txt").write_text(r.stdout)

    print(f"\nper-file manifest: {len(rels)} files")
    print("zip manifest:\n" + r.stdout)


if __name__ == "__main__":
    main()
