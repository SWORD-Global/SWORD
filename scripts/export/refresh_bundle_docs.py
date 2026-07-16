"""Refresh the bundled docs inside the zenodo_v17c ZIPs and regenerate manifests.

The six format ZIPs each embed the four doc files (release notes + variable
reference, md + pdf). When the docs change, re-running the full bundle assembly
is wasteful (it would re-clone ~25 GB of data). This script only updates the
doc copies in place and regenerates both SHA256 manifests.

Run this once after the docs are final and before uploading:
    uv run python scripts/export/refresh_bundle_docs.py

Filenames are passed to `zip` as an explicit list (not a shell variable) to
avoid zsh's no-word-split behavior silently dropping files.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
DST = REPO / "data/exports/v17c_beta/zenodo_v17c"
DOCS = REPO / "docs"

DOC_FILES = {
    "v17c_release_notes.md": DOCS / "v17c_release_notes.md",
    "v17c_release_notes.pdf": DOCS / "v17c_release_notes.pdf",
    "v17c_variable_reference.md": DOCS / "v17c_variable_reference.md",
    "v17c_variable_reference.pdf": DOCS / "v17c_variable_reference.pdf",
}
DOC_NAMES = list(DOC_FILES)
FORMATS = ["netcdf", "gpkg", "shp", "parquet", "duckdb", "global"]


def main() -> None:
    if not DST.is_dir():
        sys.exit(f"bundle staging not found: {DST}")
    for name, src in DOC_FILES.items():
        if not src.is_file():
            sys.exit(f"missing doc: {src}")
        subprocess.run(["cp", str(src), str(DST / name)], check=True)

    for fmt in FORMATS:
        zip_path = DST / f"SWORD_v17c_{fmt}.zip"
        if not zip_path.is_file():
            sys.exit(f"missing zip: {zip_path}")
        r = subprocess.run(["zip", "-q", zip_path.name, *DOC_NAMES], cwd=DST)
        if r.returncode != 0:
            sys.exit(f"zip refresh failed for {zip_path.name}")
        print(f"refreshed docs in {zip_path.name}")

    # Regenerate per-file manifest (excludes the manifests and the zips).
    per_file = sorted(
        [p for p in DST.rglob("*")
         if p.is_file() and not p.name.startswith("SHA256SUMS") and p.suffix != ".zip"],
        key=lambda p: str(p.relative_to(DST)),
    )
    rels = [str(p.relative_to(DST)) for p in per_file]
    r = subprocess.run(["shasum", "-a", "256", *rels], cwd=DST, capture_output=True, text=True)
    if r.returncode != 0:
        sys.exit(f"per-file shasum failed:\n{r.stderr}")
    (DST / "SHA256SUMS_files.txt").write_text(r.stdout)

    zips = sorted(p.name for p in DST.glob("SWORD_v17c_*.zip"))
    r = subprocess.run(["shasum", "-a", "256", *zips], cwd=DST, capture_output=True, text=True)
    if r.returncode != 0:
        sys.exit(f"zip shasum failed:\n{r.stderr}")
    (DST / "SHA256SUMS.txt").write_text(r.stdout)

    print(f"\nper-file manifest: {len(rels)} files; zip manifest: {len(zips)} zips")
    print("Docs refreshed and manifests regenerated.")


if __name__ == "__main__":
    main()
