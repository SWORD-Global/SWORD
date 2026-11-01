#!/usr/bin/env python3
"""Drop the ``rederived_v3:`` prefix from facc_quality on reaches and nodes.

The release stores the plain source-method name (``anchor``, ``interpolated``,
...). Every current value must carry the prefix before the write and none
after it; the write goes through the same per-region transactions, index
handling and sword_operations rows as the facc install.

Usage:
    python scripts/maintenance/strip_facc_quality_prefix.py --db <v17c.duckdb> [--dry-run]
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent))
import apply_facc_0015_product as p0014
from apply_facc_rederivation import (
    ENTITIES,
    REGIONS,
    ApplyError,
    _one,
    check_no_other_holders,
    db_regions,
    table_indexes,
)

PREFIX = "rederived_v3:"
SOURCES = (
    "anchor",
    "anchor_proximity",
    "anchor_aligned",
    "interpolated",
    "lake_derived",
    "partitioned_arm",
    "fallback_fill",
    "fallback_prior",
)


def run(db: Path, dry_run: bool) -> dict:
    con = duckdb.connect(str(db), read_only=dry_run)
    try:
        con.execute("INSTALL spatial; LOAD spatial;")
        regions = db_regions(con)
        if sorted(regions) != sorted(REGIONS):
            raise ApplyError(f"Expected all six regions, found {regions}")
        names = ", ".join(f"'{s}'" for s in SOURCES)
        summary: dict = {"mode": "dry-run" if dry_run else "apply"}
        for entity, key in ENTITIES:
            bad = _one(
                con,
                f"SELECT count(*) FROM {entity} WHERE facc_quality IS NULL "
                f"OR facc_quality NOT IN ({', '.join(repr(PREFIX + s) for s in SOURCES)})",
            )[0]
            if bad:
                raise ApplyError(
                    f"{entity}: {bad} rows do not carry '{PREFIX}<source>'"
                )
            con.execute(
                f"CREATE OR REPLACE TEMP TABLE _stage_{entity} AS "
                f"SELECT {key}, region, replace(facc_quality, '{PREFIX}', '') AS facc_quality "
                f"FROM {entity}"
            )
            summary[entity] = dict(
                con.execute(
                    f"SELECT facc_quality, count(*) FROM _stage_{entity} GROUP BY 1 ORDER BY 1"
                ).fetchall()
            )
            if set(summary[entity]) - set(SOURCES):
                raise ApplyError(
                    f"{entity}: unexpected names {set(summary[entity]) - set(SOURCES)}"
                )
        if dry_run:
            return summary
        indexes_before = table_indexes(con)
        p0014.WRITTEN_COLUMNS[:] = ["facc_quality"]
        p0014.USER_ID = "jake"
        p0014.SESSION_ID = "facc_release_20261027"
        summary["operation_ids"] = p0014.run_write(
            con,
            regions,
            "_stage_",
            "FACC_QUALITY_PLAIN_NAMES",
            f"facc_release_20261027: drop the '{PREFIX}' prefix from facc_quality; "
            "values are the plain source-method names",
            lambda region: {"prefix_removed": PREFIX, "names": list(SOURCES)},
        )
        for entity, _key in ENTITIES:
            left = _one(
                con,
                f"SELECT count(*) FROM {entity} WHERE facc_quality LIKE '{PREFIX}%'",
            )[0]
            out = _one(
                con,
                f"SELECT count(*) FROM {entity} WHERE facc_quality NOT IN ({names})",
            )[0]
            if left or out:
                raise ApplyError(
                    f"{entity}: {left} prefixed, {out} unexpected values after write"
                )
        if sorted(table_indexes(con)) != sorted(indexes_before):
            raise ApplyError("Index set changed during the write")
        return summary
    finally:
        con.close()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--db", required=True, type=Path)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    try:
        if "v17b" in args.db.name.lower():
            raise ApplyError("Refusing to touch a v17b database")
        check_no_other_holders(args.db)
        summary = run(args.db, args.dry_run)
    except ApplyError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(summary, indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
