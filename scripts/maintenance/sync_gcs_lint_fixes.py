#!/usr/bin/env python3
"""
Sync GCS Lint Fixes to DuckDB
------------------------------
Download lint session files from the cloud reviewer app (GCS), parse fixes,
deduplicate against local lint_fix_log, and apply to sword_v17c.duckdb.

Idempotent — safe to re-run as new reviews come in.

Usage:
    # Dry-run (default) — show what would be applied
    python scripts/maintenance/sync_gcs_lint_fixes.py \
        --db data/duckdb/sword_v17c.duckdb

    # Apply fixes
    python scripts/maintenance/sync_gcs_lint_fixes.py \
        --db data/duckdb/sword_v17c.duckdb --apply

    # Custom bucket path
    python scripts/maintenance/sync_gcs_lint_fixes.py \
        --db data/duckdb/sword_v17c.duckdb \
        --bucket gs://sword-qc-data/sword/lint_fixes/
"""

import argparse
import json
import subprocess
import sys
import tempfile
from pathlib import Path

import duckdb

from src.sword_duckdb import SWORDWorkflow

GCS_DEFAULT_BUCKET = "gs://sword-qc-data/sword/lint_fixes/"

# Columns that the reviewer app can modify — reject anything else
ALLOWED_COLUMNS = {"lakeflag", "type"}

VALID_REGIONS = {"NA", "SA", "EU", "AF", "AS", "OC"}

LINT_FIX_LOG_DDL = """
    CREATE TABLE IF NOT EXISTS lint_fix_log (
        fix_id INTEGER PRIMARY KEY,
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        check_id VARCHAR,
        reach_id BIGINT,
        region VARCHAR,
        action VARCHAR,
        column_changed VARCHAR,
        old_value VARCHAR,
        new_value VARCHAR,
        notes VARCHAR,
        undone BOOLEAN DEFAULT FALSE
    )
"""

REQUIRED_FIX_KEYS = {"check_id", "reach_id", "column_changed", "new_value"}


def download_session_files(bucket_path: str, dest_dir: Path) -> list[Path]:
    """Download all lint_session_*.json files from GCS to dest_dir."""
    if not bucket_path.startswith("gs://"):
        print(f"ERROR: bucket_path must start with gs://, got: {bucket_path!r}")
        sys.exit(1)
    if not bucket_path.endswith("/"):
        bucket_path += "/"

    try:
        result = subprocess.run(
            ["gcloud", "storage", "ls", bucket_path],
            capture_output=True,
            text=True,
            check=True,
        )
    except FileNotFoundError:
        print(
            "ERROR: gcloud CLI not found. "
            "Install: https://cloud.google.com/sdk/docs/install"
        )
        sys.exit(1)
    except subprocess.CalledProcessError as e:
        print(f"ERROR listing GCS: {e.stderr.strip()}")
        sys.exit(1)

    all_files = [
        line.strip() for line in result.stdout.strip().split("\n") if line.strip()
    ]
    session_files = [
        f
        for f in all_files
        if f.rsplit("/", 1)[-1].startswith("lint_session_") and f.endswith(".json")
    ]

    if not session_files:
        return []

    print(f"Found {len(session_files)} session file(s) in GCS")

    downloaded = []
    for gcs_path in session_files:
        filename = gcs_path.rsplit("/", 1)[-1]
        local_path = dest_dir / filename
        subprocess.run(
            ["gcloud", "storage", "cp", gcs_path, str(local_path)],
            capture_output=True,
            text=True,
            check=True,
        )
        downloaded.append(local_path)
        print(f"  {filename}")

    return downloaded


def _validate_record(rec: dict) -> str | None:
    """Return error string if record is missing required keys, else None."""
    missing = REQUIRED_FIX_KEYS - rec.keys()
    if missing:
        return f"missing keys: {sorted(missing)}"
    return None


def parse_session_files(
    session_files: list[Path],
) -> tuple[list[dict], list[dict], int]:
    """Parse session files. Returns (active_fixes, active_skips, defer_count)."""
    all_fixes = []
    all_skips = []
    defer_count = 0

    # Sort by filename for deterministic ordering across runs
    for path in sorted(session_files, key=lambda p: p.name):
        try:
            with open(path) as f:
                session = json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            print(f"  WARNING: could not parse {path.name}: {e}")
            continue

        raw_fixes = [f for f in session.get("fixes", []) if not f.get("undone", False)]
        skips = [s for s in session.get("skips", []) if not s.get("undone", False)]
        defers = session.get("pending", [])

        fixes = []
        for fix in raw_fixes:
            err = _validate_record(fix)
            if err:
                print(f"  WARNING: skipping malformed fix in {path.name}: {err}")
                continue
            # Ensure reach_id is int (JSON may deserialize as float)
            try:
                fix["reach_id"] = int(fix["reach_id"])
            except (ValueError, TypeError):
                print(
                    f"  WARNING: skipping fix with invalid reach_id "
                    f"{fix['reach_id']!r} in {path.name}"
                )
                continue
            fixes.append(fix)

        all_fixes.extend(fixes)
        all_skips.extend(skips)
        defer_count += len(defers)

        print(
            f"  {path.name}: "
            f"{len(fixes)} fixes, {len(skips)} skips, {len(defers)} defers"
        )

    return all_fixes, all_skips, defer_count


def get_existing_log_keys(
    db_path: str,
) -> tuple[dict[tuple, str | None], dict[tuple, str | None]]:
    """Return dicts of {(check_id, reach_id): new_value} for fixes and skips in lint_fix_log."""
    con = duckdb.connect(db_path, read_only=True)
    try:
        tables = con.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_name = 'lint_fix_log'"
        ).fetchall()
        if not tables:
            return {}, {}

        fix_keys = {
            (r[0], r[1]): r[2]
            for r in con.execute(
                "SELECT check_id, reach_id, new_value FROM lint_fix_log "
                "WHERE action = 'fix' AND NOT undone"
            ).fetchall()
        }
        skip_keys = {
            (r[0], r[1]): r[2]
            for r in con.execute(
                "SELECT check_id, reach_id, new_value FROM lint_fix_log "
                "WHERE action = 'skip' AND NOT undone"
            ).fetchall()
        }
        return fix_keys, skip_keys
    finally:
        con.close()


def deduplicate(
    records: list[dict], existing_keys: dict[tuple, str | None]
) -> tuple[list[dict], list[dict]]:
    """Split records into (new, already_present) based on (check_id, reach_id) keys.

    Warns when an incoming record has a different new_value than the logged one.
    """
    new = []
    dupes = []
    for rec in records:
        key = (rec.get("check_id"), rec.get("reach_id"))
        if key in existing_keys:
            logged_val = existing_keys[key]
            incoming_val = (
                str(rec["new_value"]) if rec.get("new_value") is not None else None
            )
            if incoming_val != logged_val:
                print(
                    f"  WARNING: reach {rec.get('reach_id')} check {rec.get('check_id')}: "
                    f"incoming value ({incoming_val}) differs from logged value "
                    f"({logged_val}) — skipping (undo the old fix first)"
                )
            dupes.append(rec)
        else:
            new.append(rec)
    return new, dupes


def next_fix_id(con) -> int:
    return con.execute(
        "SELECT COALESCE(MAX(fix_id), 0) + 1 FROM lint_fix_log"
    ).fetchone()[0]


def log_to_lint_fix_log(con, records: list[dict], action: str):
    """Batch-insert records into lint_fix_log."""
    if not records:
        return
    fid = next_fix_id(con)
    for rec in records:
        notes = rec.get("notes", "") or ""
        notes = f"[gcs_sync] {notes}".strip()
        con.execute(
            """
            INSERT INTO lint_fix_log
                (fix_id, check_id, reach_id, region, action,
                 column_changed, old_value, new_value, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                fid,
                rec.get("check_id"),
                rec.get("reach_id"),
                rec.get("region"),
                action,
                rec.get("column_changed"),
                str(rec["old_value"]) if rec.get("old_value") is not None else None,
                str(rec["new_value"]) if rec.get("new_value") is not None else None,
                notes,
            ],
        )
        fid += 1


def main():
    parser = argparse.ArgumentParser(
        description="Sync lint fixes from GCS cloud reviewer to local DuckDB"
    )
    parser.add_argument("--db", required=True, help="Path to DuckDB database")
    parser.add_argument(
        "--apply", action="store_true", help="Apply fixes (dry-run without this)"
    )
    parser.add_argument(
        "--bucket",
        default=GCS_DEFAULT_BUCKET,
        help=f"GCS bucket path (default: {GCS_DEFAULT_BUCKET})",
    )
    args = parser.parse_args()

    # --- 1. Download ---
    print("=== Downloading session files from GCS ===")
    with tempfile.TemporaryDirectory() as tmpdir:
        session_files = download_session_files(args.bucket, Path(tmpdir))
        if not session_files:
            print("No session files found. Nothing to do.")
            return

        # --- 2. Parse ---
        print("\n=== Parsing session files ===")
        fixes, skips, defer_count = parse_session_files(session_files)
        print(f"\nTotals: {len(fixes)} fixes, {len(skips)} skips, {defer_count} defers")

    if not fixes and not skips:
        print("No fixes or skips to sync.")
        return

    # --- 3. Deduplicate ---
    print("\n=== Deduplicating against lint_fix_log ===")
    existing_fix_keys, existing_skip_keys = get_existing_log_keys(args.db)

    new_fixes, dupe_fixes = deduplicate(fixes, existing_fix_keys)
    new_skips, dupe_skips = deduplicate(skips, existing_skip_keys)

    print(f"  Fixes:  {len(new_fixes)} new, {len(dupe_fixes)} already applied")
    print(f"  Skips:  {len(new_skips)} new, {len(dupe_skips)} already logged")

    if not new_fixes and not new_skips:
        print("\nEverything already synced. Nothing to do.")
        return

    # --- 4. Dry-run ---
    if not args.apply:
        print("\n=== DRY RUN (pass --apply to commit) ===")
        if new_fixes:
            print(f"\nWould apply {len(new_fixes)} fixes:")
            for fix in sorted(
                new_fixes, key=lambda f: (f.get("region", ""), f.get("reach_id", 0))
            ):
                print(
                    f"  {fix['check_id']}: reach {fix['reach_id']} ({fix.get('region', '?')}): "
                    f"{fix.get('column_changed', '?')} "
                    f"{fix.get('old_value', '?')} -> {fix.get('new_value', '?')}"
                )
        if new_skips:
            print(f"\nWould log {len(new_skips)} skips (no reach modification)")
        return

    # --- 5. Apply fixes grouped by region ---
    print("\n=== Applying fixes ===")
    by_region: dict[str, list[dict]] = {}
    for fix in new_fixes:
        by_region.setdefault(fix.get("region", "UNKNOWN"), []).append(fix)

    applied_count = 0
    mismatch_count = 0

    for region, region_fixes in sorted(by_region.items()):
        if region not in VALID_REGIONS:
            print(f"\nSKIP region '{region}': not in {sorted(VALID_REGIONS)}")
            mismatch_count += len(region_fixes)
            continue

        print(f"\nRegion {region}: {len(region_fixes)} fixes")

        workflow = SWORDWorkflow(user_id="gcs_sync")
        try:
            workflow.load(args.db, region)
            con = workflow.sword.db.conn

            applied_this_region = []

            with workflow.transaction(
                f"GCS sync: {len(region_fixes)} fixes for {region}"
            ):
                for fix in region_fixes:
                    reach_id = fix["reach_id"]
                    column = fix["column_changed"]
                    old_value = fix.get("old_value")
                    new_value = fix["new_value"]

                    if column not in ALLOWED_COLUMNS:
                        print(
                            f"  SKIP reach {reach_id}: "
                            f"column '{column}' not in allowlist {ALLOWED_COLUMNS}"
                        )
                        continue

                    # Verify current DB value matches expected old_value
                    row = con.execute(
                        f"SELECT {column} FROM reaches "  # noqa: S608 — column validated against ALLOWED_COLUMNS above
                        "WHERE reach_id = ? AND region = ?",
                        [reach_id, region],
                    ).fetchone()

                    if row is None:
                        print(f"  SKIP reach {reach_id}: not found in region {region}")
                        continue

                    current = row[0]
                    if old_value is not None and str(current) != str(old_value):
                        print(
                            f"  SKIP reach {reach_id}: {column} is {current}, "
                            f"expected {old_value} (already modified elsewhere?)"
                        )
                        mismatch_count += 1
                        continue

                    # Cast value to appropriate type
                    try:
                        val = (
                            int(new_value)
                            if column in ("lakeflag", "type")
                            else new_value
                        )
                    except (ValueError, TypeError):
                        print(
                            f"  SKIP reach {reach_id}: "
                            f"cannot cast {new_value!r} to int for {column}"
                        )
                        mismatch_count += 1
                        continue

                    reason = f"{fix['check_id']}: gcs_sync"
                    notes = fix.get("notes", "")
                    if notes:
                        reason += f" - {notes}"

                    workflow.modify_reach(reach_id, reason=reason, **{column: val})
                    applied_this_region.append(fix)
                    applied_count += 1
                    print(f"  Applied: reach {reach_id}: {column} = {val}")

                # Log inside transaction scope so fixes and log entries
                # share the same error-handling boundary.
                if applied_this_region:
                    con.execute(LINT_FIX_LOG_DDL)
                    log_to_lint_fix_log(con, applied_this_region, "fix")

            print(f"  Region {region}: {len(applied_this_region)} applied")
        finally:
            workflow.close()

    # --- 6. Log skips ---
    if new_skips:
        print(f"\nLogging {len(new_skips)} skips to lint_fix_log...")
        log_con = duckdb.connect(args.db)
        try:
            log_con.execute(LINT_FIX_LOG_DDL)
            log_to_lint_fix_log(log_con, new_skips, "skip")
        finally:
            log_con.close()

    # --- 7. Summary ---
    print("\n=== Summary ===")
    print(f"  Fixes applied:           {applied_count}")
    print(f"  Fixes skipped (dedup):   {len(dupe_fixes)}")
    print(f"  Fixes skipped (mismatch): {mismatch_count}")
    print(f"  Skips logged:            {len(new_skips)}")
    print(f"  Skips skipped (dedup):   {len(dupe_skips)}")
    print(f"  Defers in GCS:           {defer_count}")


if __name__ == "__main__":
    main()
