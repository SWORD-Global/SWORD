"""Sync GCS reviewer session fixes into local DuckDB lint_fix_log.

Run after the NetCDF export finishes (or any time DuckDB is not write-locked).

Usage:
    python scripts/maintenance/sync_gcs_to_duckdb.py \
        --db data/duckdb/sword_v17c.duckdb \
        --gcs-dir /tmp/sword_recovery/gcs_sessions
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import duckdb


def main():
    parser = argparse.ArgumentParser(
        description="Sync GCS session fixes into DuckDB lint_fix_log"
    )
    parser.add_argument("--db", required=True, help="Path to sword_v17c.duckdb")
    parser.add_argument(
        "--gcs-dir",
        required=True,
        help="Directory containing lint_session_*.json files",
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    gcs_dir = Path(args.gcs_dir)
    json_files = sorted(gcs_dir.glob("lint_session_*.json"))
    if not json_files:
        print(f"No lint_session_*.json files in {gcs_dir}")
        return

    # Parse all GCS fixes and skips
    gcs_fixes: dict[tuple, dict] = {}
    gcs_skips: dict[tuple, dict] = {}
    for jf in json_files:
        try:
            data = json.loads(jf.read_text())
        except (json.JSONDecodeError, OSError) as e:
            print(f"  SKIP {jf.name}: {e}")
            continue
        for f in data.get("fixes", []):
            if not f.get("undone"):
                key = (f["reach_id"], f.get("region"), f.get("column_changed"))
                gcs_fixes[key] = f
        for s in data.get("skips", []):
            key = (s.get("reach_id"), s.get("region"), s.get("check_id"))
            gcs_skips[key] = s

    print(
        f"GCS: {len(gcs_fixes)} active fixes, {len(gcs_skips)} skips across {len(json_files)} files"
    )

    # Connect to DuckDB
    try:
        con = duckdb.connect(str(args.db))
    except duckdb.IOException as e:
        print(f"ERROR: Cannot open DuckDB (locked?): {e}", file=sys.stderr)
        sys.exit(1)

    # Get existing keys
    db_fix_keys = set()
    for row in con.execute(
        "SELECT reach_id, region, column_changed FROM lint_fix_log "
        "WHERE action != 'skip' AND NOT COALESCE(undone, false)"
    ).fetchall():
        db_fix_keys.add(tuple(row))

    db_skip_keys = set()
    for row in con.execute(
        "SELECT reach_id, region, check_id FROM lint_fix_log WHERE action = 'skip'"
    ).fetchall():
        db_skip_keys.add(tuple(row))

    max_id = con.execute(
        "SELECT COALESCE(MAX(fix_id), 0) FROM lint_fix_log"
    ).fetchone()[0]

    missing_fixes = {k: v for k, v in gcs_fixes.items() if k not in db_fix_keys}
    missing_skips = {k: v for k, v in gcs_skips.items() if k not in db_skip_keys}

    print(
        f"Missing from DuckDB: {len(missing_fixes)} fixes, {len(missing_skips)} skips"
    )

    if not missing_fixes and not missing_skips:
        print("Already in sync.")
        con.close()
        return

    if args.dry_run:
        print("(DRY RUN — no changes)")
        for key, fix in sorted(
            missing_fixes.items(), key=lambda kv: kv[1].get("timestamp", "")
        ):
            print(
                f"  WOULD INSERT fix: reach {key[0]} ({key[1]}) {key[2]}: {fix.get('old_value')} -> {fix.get('new_value')}"
            )
        con.close()
        return

    next_id = max_id + 1
    for key, fix in sorted(
        missing_fixes.items(), key=lambda kv: kv[1].get("timestamp", "")
    ):
        reach_id, region, col = key
        con.execute(
            "INSERT INTO lint_fix_log (fix_id, timestamp, check_id, reach_id, region, "
            "action, column_changed, old_value, new_value, notes, undone) "
            "VALUES (?, ?, ?, ?, ?, 'fix', ?, ?, ?, ?, false)",
            [
                next_id,
                fix.get("timestamp"),
                fix.get("check_id", "C001"),
                reach_id,
                region,
                col,
                str(fix.get("old_value", "")),
                str(fix.get("new_value", "")),
                "[gcs_recovery] " + (fix.get("notes") or ""),
            ],
        )
        next_id += 1

    for key, skip in sorted(
        missing_skips.items(), key=lambda kv: kv[1].get("timestamp", "")
    ):
        reach_id, region, check_id = key
        con.execute(
            "INSERT INTO lint_fix_log (fix_id, timestamp, check_id, reach_id, region, "
            "action, notes, undone) VALUES (?, ?, ?, ?, ?, 'skip', ?, false)",
            [
                next_id,
                skip.get("timestamp"),
                check_id or "C001",
                reach_id,
                region,
                "[gcs_recovery] " + (skip.get("notes") or ""),
            ],
        )
        next_id += 1

    con.close()
    print(
        f"Inserted {len(missing_fixes)} fixes, {len(missing_skips)} skips into DuckDB"
    )


if __name__ == "__main__":
    main()
