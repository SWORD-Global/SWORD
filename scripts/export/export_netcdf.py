#!/usr/bin/env python3
"""Export validated published-layout SWORD NetCDFs through the shared writer."""

import argparse
from pathlib import Path
import sys

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
from sword_duckdb.netcdf import REGIONS, export_region, sha256_file
from sword_duckdb.netcdf_schema import validate_templates


def main():
    parser = argparse.ArgumentParser(description="Export SWORD v17c DuckDB to NetCDF")
    parser.add_argument(
        "--db", default="data/duckdb/sword_v17c.duckdb", help="Path to v17c DuckDB"
    )
    parser.add_argument(
        "--v17b-dir", default="data/netcdf", help="Directory with v17b NetCDF files"
    )
    parser.add_argument("--output-dir", default="data/exports", help="Output directory")
    parser.add_argument("--region", type=str, help="Single region to export (e.g. NA)")
    parser.add_argument("--all", action="store_true", help="Export all 6 regions")
    parser.add_argument(
        "--checksums",
        action="store_true",
        default=True,
        help="Generate SHA256 checksums",
    )
    parser.add_argument(
        "--overwrite", action="store_true", help="Replace validated output files"
    )
    args = parser.parse_args()

    if not args.region and not args.all:
        parser.error("Specify --region or --all")

    db_path = Path(args.db)
    v17b_dir = Path(args.v17b_dir)
    output_dir = Path(args.output_dir)

    if not db_path.exists():
        print(f"Database not found: {db_path}", file=sys.stderr)
        sys.exit(1)
    if not v17b_dir.exists():
        print(f"v17b directory not found: {v17b_dir}", file=sys.stderr)
        sys.exit(1)

    regions = REGIONS if args.all else [args.region.upper()]
    validate_templates(v17b_dir, regions)
    output_dir.mkdir(parents=True, exist_ok=True)

    output_files: list[Path] = []
    with duckdb.connect(str(db_path), read_only=True) as con:
        for region in regions:
            out_path = export_region(
                con, region, v17b_dir, output_dir, overwrite=args.overwrite
            )
            output_files.append(out_path)

    # Generate checksums
    if args.checksums and output_files:
        checksum_path = output_dir / "SHA256SUMS.txt"
        print(f"\nGenerating checksums → {checksum_path}")
        with open(checksum_path, "w") as f:
            for p in output_files:
                digest = sha256_file(p)
                f.write(f"{digest}  {p.name}\n")
                print(f"  {digest}  {p.name}")

    print(f"\nExport complete. {len(output_files)} files in {output_dir}")


if __name__ == "__main__":
    main()
