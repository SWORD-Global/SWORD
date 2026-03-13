#!/usr/bin/env python3
"""Apply reviewer fixes from GCS JSON sessions to local DuckDB.

Downloads lint fix sessions from gs://sword-qc-data/sword/lint_fixes/,
filters to non-undone fixes, and applies them to the v17c DuckDB.

Usage:
    python scripts/maintenance/apply_reviewer_fixes.py
    python scripts/maintenance/apply_reviewer_fixes.py --db data/duckdb/sword_v17c.duckdb
    python scripts/maintenance/apply_reviewer_fixes.py --dry-run
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import tempfile
from pathlib import Path

import duckdb

GCS_BUCKET = "gs://sword-qc-data/sword/lint_fixes/"


def download_fix_jsons(tmp_dir: Path) -> list[Path]:
    """Download all lint fix JSONs from GCS to a temp directory."""
    # List files first, then download individually (gsutil -m hangs on macOS)
    ls_result = subprocess.run(
        ["gsutil", "ls", f"{GCS_BUCKET}*.json"],
        capture_output=True,
        text=True,
    )
    if ls_result.returncode != 0:
        print(f"gsutil ls error: {ls_result.stderr}", file=sys.stderr)
        sys.exit(1)
    gcs_paths = [
        line.strip() for line in ls_result.stdout.strip().split("\n") if line.strip()
    ]
    for gcs_path in gcs_paths:
        result = subprocess.run(
            ["gsutil", "cp", gcs_path, str(tmp_dir)],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            print(f"gsutil cp error for {gcs_path}: {result.stderr}", file=sys.stderr)
            sys.exit(1)
    return sorted(tmp_dir.glob("*.json"))


def parse_fixes(json_path: Path) -> list[dict]:
    """Extract non-undone fixes from a session JSON.

    For duplicate (reach_id, column) pairs, keep the latest by fix_id.
    """
    with open(json_path) as f:
        data = json.load(f)

    fixes = [fix for fix in data.get("fixes", []) if not fix.get("undone", False)]

    # Deduplicate: keep latest fix_id per (reach_id, column)
    latest: dict[tuple, dict] = {}
    for fix in fixes:
        key = (fix["reach_id"], fix["column_changed"])
        if key not in latest or fix["fix_id"] > latest[key]["fix_id"]:
            latest[key] = fix

    # Filter out no-ops (old_value == new_value, from undo/redo cycles)
    return [fix for fix in latest.values() if fix["old_value"] != fix["new_value"]]


# Columns that exist on both reaches and nodes — propagate changes to nodes
NODE_PROPAGATE_COLS = {"lakeflag"}


def apply_fixes(
    con: duckdb.DuckDBPyConnection,
    fixes: list[dict],
    source_file: str,
    dry_run: bool = False,
) -> dict[str, int]:
    """Apply fixes to DuckDB. Returns counts of applied/skipped/already_correct."""
    applied = 0
    skipped_already = 0
    skipped_mismatch = 0
    nodes_updated = 0

    for fix in fixes:
        reach_id = fix["reach_id"]
        region = fix["region"]
        column = fix["column_changed"]
        new_value = fix["new_value"]
        old_value = fix["old_value"]

        # Check current value in DuckDB
        row = con.execute(
            f"SELECT {column} FROM reaches WHERE reach_id = ? AND region = ?",
            [reach_id, region],
        ).fetchone()

        if row is None:
            print(f"  WARN: reach {reach_id} not found in region {region}")
            continue

        current = row[0]

        if current == new_value:
            skipped_already += 1
            # Still propagate to nodes if reach is correct but nodes lag behind
            if column in NODE_PROPAGATE_COLS and not dry_run:
                n = _propagate_to_nodes(con, reach_id, region, column, new_value)
                nodes_updated += n
            continue

        if current != old_value:
            skipped_mismatch += 1
            print(
                f"  WARN: reach {reach_id} {column}: "
                f"expected old={old_value}, got current={current}, "
                f"target={new_value} (from {source_file})"
            )

        if not dry_run:
            con.execute(
                f"UPDATE reaches SET {column} = ? WHERE reach_id = ? AND region = ?",
                [new_value, reach_id, region],
            )
            if column in NODE_PROPAGATE_COLS:
                n = _propagate_to_nodes(con, reach_id, region, column, new_value)
                nodes_updated += n
        applied += 1

    return {
        "applied": applied,
        "already_correct": skipped_already,
        "old_value_mismatch": skipped_mismatch,
        "nodes_updated": nodes_updated,
    }


def _propagate_to_nodes(
    con: duckdb.DuckDBPyConnection,
    reach_id: int,
    region: str,
    column: str,
    new_value,
) -> int:
    """Update nodes on a reach to match the reach's new column value."""
    count = con.execute(
        f"SELECT COUNT(*) FROM nodes WHERE reach_id = ? AND region = ? AND {column} != ?",
        [reach_id, region, new_value],
    ).fetchone()[0]
    if count > 0:
        con.execute(
            f"UPDATE nodes SET {column} = ? WHERE reach_id = ? AND region = ? AND {column} != ?",
            [new_value, reach_id, region, new_value],
        )
    return count


def main():
    parser = argparse.ArgumentParser(
        description="Apply reviewer fixes from GCS to DuckDB"
    )
    parser.add_argument(
        "--db",
        default="data/duckdb/sword_v17c.duckdb",
        help="Path to v17c DuckDB (default: data/duckdb/sword_v17c.duckdb)",
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Show what would be applied"
    )
    parser.add_argument(
        "--local-dir",
        type=str,
        help="Use local directory of JSON files instead of downloading from GCS",
    )
    args = parser.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        print(f"Database not found: {db_path}", file=sys.stderr)
        sys.exit(1)

    print(f"Database: {db_path}")

    if args.local_dir:
        local_dir = Path(args.local_dir)
        json_files = sorted(local_dir.glob("*.json"))
        print(f"Using local files from {local_dir}")
    else:
        print(f"Downloading fix sessions from {GCS_BUCKET}...")
        tmp_dir_obj = tempfile.mkdtemp()
        json_files = download_fix_jsons(Path(tmp_dir_obj))

    print(f"Found {len(json_files)} session files")

    all_fixes: list[tuple[str, list[dict]]] = []
    for jf in json_files:
        fixes = parse_fixes(jf)
        all_fixes.append((jf.name, fixes))
        print(f"  {jf.name}: {len(fixes)} non-undone fixes")

    total_fixes = sum(len(f) for _, f in all_fixes)
    if total_fixes == 0:
        print("No fixes to apply.")
        return

    print(f"\nTotal fixes to apply: {total_fixes}")
    if args.dry_run:
        print("(DRY RUN - no changes will be made)")

    # Need RTREE pattern for updates
    con = duckdb.connect(str(db_path))
    con.execute("INSTALL spatial; LOAD spatial;")

    # Drop RTREE indexes before UPDATE
    indexes = con.execute(
        "SELECT index_name, table_name, sql FROM duckdb_indexes() WHERE sql LIKE '%RTREE%'"
    ).fetchall()
    for idx_name, _tbl, _sql in indexes:
        con.execute(f'DROP INDEX "{idx_name}"')

    for source_file, fixes in all_fixes:
        if not fixes:
            continue
        print(f"\nApplying {source_file}...")
        counts = apply_fixes(con, fixes, source_file, dry_run=args.dry_run)
        print(
            f"  applied={counts['applied']}, "
            f"already_correct={counts['already_correct']}, "
            f"old_value_mismatch={counts['old_value_mismatch']}, "
            f"nodes_updated={counts['nodes_updated']}"
        )

    # Recreate RTREE indexes
    for _idx_name, _tbl, sql in indexes:
        con.execute(sql)

    con.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
