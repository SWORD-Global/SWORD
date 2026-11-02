#!/usr/bin/env python3
"""Require the published SWORD NetCDF schema and setfinder extraction contract."""

import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
from sword_duckdb.netcdf_schema import SCHEMA_ID, validate_file


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("files", nargs="+", type=Path)
    args = parser.parse_args()
    results = []
    for path in args.files:
        try:
            validate_file(path)
        except (ValueError, OSError, RuntimeError) as error:
            results.append({"file": str(path), "passed": False, "error": str(error)})
        else:
            results.append({"file": str(path), "passed": True})
    print(json.dumps({"schema_id": SCHEMA_ID, "files": results}, indent=2))
    return 0 if all(result["passed"] for result in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
