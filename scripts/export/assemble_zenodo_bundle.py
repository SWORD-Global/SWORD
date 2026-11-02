"""Assemble a Zenodo v17c bundle from a staged export directory.

The source directory holds the per-format export directories with their final
filenames (netcdf/, gpkg/, shp/, parquet/, duckdb/, global/). This clones them
into the bundle directory, copies the four doc files to the bundle root,
packages one ZIP per format (each embedding the doc files), and writes two
SHA256 manifests: SHA256SUMS.txt (the uploaded ZIPs) and SHA256SUMS_files.txt
(per-file, for post-unzip verification).

Files are cloned with `cp -c` (APFS copy-on-write; instant, no extra disk) so
this is cheap despite the ~25 GB payload.

Usage:
    uv run python scripts/export/assemble_zenodo_bundle.py \\
        --src data/exports/v17c_beta/facc_release_20261027 \\
        --dst data/exports/v17c_beta/zenodo_v17c_facc_20261027
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
DOCS = REPO / "docs"
REGIONS = ["af", "as", "eu", "na", "oc", "sa"]
REGIONS_UP = ["AF", "AS", "EU", "NA", "OC", "SA"]
FORMATS = ["netcdf", "gpkg", "shp", "parquet", "duckdb", "global"]

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
    r = subprocess.run(
        ["cp", "-c", str(src), str(dst)], capture_output=True, text=True, check=False
    )
    if r.returncode != 0:
        sys.exit(f"cp -c failed: {src} -> {dst}\n{r.stderr}")


def expected_files(src: Path) -> list[Path]:
    """Every data file the bundle must contain, relative to src."""
    files = [Path(f"netcdf/{reg}_sword_v17c.nc") for reg in REGIONS]
    for reg in REGIONS_UP:
        files.append(Path(f"gpkg/sword_{reg}_v17c.gpkg"))
        files.append(Path(f"duckdb/sword_{reg}_v17c.duckdb"))
        for lyr in ("reaches", "nodes"):
            files.append(Path(f"parquet/sword_{reg}_v17c_{lyr}.parquet"))
    for lyr in ("reaches", "nodes"):
        for ext in ("parquet", "duckdb"):
            files.append(Path(f"global/sword_global_v17c_{lyr}.{ext}"))
    shp = sorted(p.relative_to(src) for p in (src / "shp").glob("*_v17c.*"))
    if not shp:
        sys.exit(f"no shapefiles found in {src / 'shp'}")
    files += shp
    files.append(Path("shp/shapefile_field_name_mapping.csv"))
    return files


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--src", required=True, type=Path, help="staged export directory")
    ap.add_argument(
        "--dst", required=True, type=Path, help="bundle directory to create"
    )
    args = ap.parse_args()
    src, dst = args.src.resolve(), args.dst.resolve()
    if dst.exists():
        sys.exit(f"target already exists: {dst} (remove/rename it first to re-run)")
    if not src.is_dir():
        sys.exit(f"source staging missing: {src}")

    # 1. Clone every data file.
    rels = expected_files(src)
    for rel in rels:
        clone(src / rel, dst / rel)
    print(f"cloned {len(rels)} data files")

    # 2. Docs at bundle root.
    for name, path in DOC_FILES.items():
        clone(path, dst / name)

    # 3. Package one ZIP per format, each embedding the doc files.
    for fmt in FORMATS:
        zip_path = dst / f"SWORD_v17c_{fmt}.zip"
        cmd = ["zip", "-q", "-r", zip_path.name, fmt, *DOC_NAMES]
        r = subprocess.run(cmd, cwd=dst, capture_output=True, text=True, check=False)
        if r.returncode != 0:
            sys.exit(f"zip failed for {fmt}:\n{r.stderr}")
        print(f"built {zip_path.name} ({zip_path.stat().st_size / 1e9:.2f} GB)")

    # 4a. Per-file manifest (post-unzip verification).
    per_file = sorted(
        [
            p
            for p in dst.rglob("*")
            if p.is_file()
            and not p.name.startswith("SHA256SUMS")
            and p.suffix != ".zip"
        ],
        key=lambda p: str(p.relative_to(dst)),
    )
    names = [str(p.relative_to(dst)) for p in per_file]
    r = subprocess.run(
        ["shasum", "-a", "256", *names],
        cwd=dst,
        capture_output=True,
        text=True,
        check=False,
    )
    if r.returncode != 0:
        sys.exit(f"per-file shasum failed:\n{r.stderr}")
    (dst / "SHA256SUMS_files.txt").write_text(r.stdout)

    # 4b. ZIP-level manifest (upload verification).
    zips = sorted(p.name for p in dst.glob("SWORD_v17c_*.zip"))
    r = subprocess.run(
        ["shasum", "-a", "256", *zips],
        cwd=dst,
        capture_output=True,
        text=True,
        check=False,
    )
    if r.returncode != 0:
        sys.exit(f"zip shasum failed:\n{r.stderr}")
    (dst / "SHA256SUMS.txt").write_text(r.stdout)

    print(f"\nper-file manifest: {len(names)} files")
    print("zip manifest:\n" + r.stdout)


if __name__ == "__main__":
    main()
