"""Reclassify midstream ghost reaches (type=6) that have both upstream and downstream neighbors.

94 reaches in v17c have type=6 but sit midstream (n_rch_up > 0 AND n_rch_down > 0).
Three causes:
  - 67: v17b mislabeling (UNC original)
  - 15: lake sandwich fix bug — max(neighbor_types) picked type=6 from ghost neighbor
  - 12: v17c topology changes gave headwater ghosts new upstream neighbors

All 94 have path_freq=-9999, stream_order=-9999, path_segs=-9999 because the v17c
pipeline skips type=6. This causes 51 N016 (path_segs contiguity) violations.

New type is determined by lakeflag:
  lakeflag=0 → type=1 (river)
  lakeflag=1 → type=3 (lake_on_river)
  lakeflag=2 → type=1 (canal → river)
  lakeflag=3 → type=1 (tidal → river)

Tracks issue #199.

Usage:
  python scripts/maintenance/fix_midstream_ghosts.py --db data/duckdb/sword_v17c.duckdb
  python scripts/maintenance/fix_midstream_ghosts.py --db data/duckdb/sword_v17c.duckdb --apply
"""

from __future__ import annotations

import argparse

import duckdb

LAKEFLAG_TO_TYPE = {
    0: 1,  # river
    1: 3,  # lake_on_river
    2: 1,  # canal → river
    3: 1,  # tidal → river
}

LINT_FIX_LOG_DDL = """\
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

QUERY_MIDSTREAM_GHOSTS = """\
    SELECT reach_id, region, type, lakeflag, edit_flag
    FROM reaches
    WHERE type = 6
      AND n_rch_up > 0
      AND n_rch_down > 0
    ORDER BY region, reach_id
"""


def next_fix_id(con: duckdb.DuckDBPyConnection) -> int:
    return con.execute(
        "SELECT COALESCE(MAX(fix_id), 0) + 1 FROM lint_fix_log"
    ).fetchone()[0]


def find_midstream_ghosts(con: duckdb.DuckDBPyConnection) -> list[dict]:
    rows = con.execute(QUERY_MIDSTREAM_GHOSTS).fetchall()
    results = []
    for reach_id, region, old_type, lakeflag, edit_flag in rows:
        new_type = LAKEFLAG_TO_TYPE.get(lakeflag)
        if new_type is None:
            print(f"  SKIP reach {reach_id}: unexpected lakeflag={lakeflag}")
            continue
        results.append(
            {
                "reach_id": reach_id,
                "region": region,
                "old_type": old_type,
                "new_type": new_type,
                "lakeflag": lakeflag,
                "edit_flag": edit_flag,
            }
        )
    return results


def apply_fixes(con: duckdb.DuckDBPyConnection, targets: list[dict]) -> int:
    """Apply type reclassification and log to lint_fix_log. Returns count applied."""
    con.execute("INSTALL spatial; LOAD spatial;")

    # Drop RTREE indexes before UPDATE
    indexes = con.execute(
        "SELECT index_name, table_name, sql FROM duckdb_indexes() "
        "WHERE sql LIKE '%RTREE%'"
    ).fetchall()
    for idx_name, _tbl, _sql in indexes:
        con.execute(f'DROP INDEX "{idx_name}"')

    try:
        con.begin()
        con.execute(LINT_FIX_LOG_DDL)

        applied = 0
        fid = next_fix_id(con)

        for t in targets:
            reach_id = t["reach_id"]
            region = t["region"]
            new_type = t["new_type"]

            # Update type + edit_flag
            con.execute(
                """
                UPDATE reaches
                SET type = ?,
                    edit_flag = CASE
                        WHEN edit_flag IS NULL OR edit_flag = 'NaN'
                            THEN 'ghost_reclass'
                        ELSE edit_flag || ',ghost_reclass'
                    END
                WHERE reach_id = ? AND region = ?
                """,
                [new_type, reach_id, region],
            )

            # Log to lint_fix_log
            con.execute(
                """
                INSERT INTO lint_fix_log
                    (fix_id, check_id, reach_id, region, action,
                     column_changed, old_value, new_value, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    fid,
                    "C003",
                    reach_id,
                    region,
                    "fix",
                    "type",
                    str(t["old_type"]),
                    str(new_type),
                    f"ghost_reclass: midstream ghost lakeflag={t['lakeflag']} (issue #199)",
                ],
            )
            fid += 1
            applied += 1

        con.commit()
        return applied

    finally:
        # Recreate RTREE indexes
        idx_errors: list[str] = []
        for _idx_name, _tbl, idx_sql in indexes:
            try:
                con.execute(idx_sql)
            except Exception as idx_err:
                idx_errors.append(str(idx_err))
        if idx_errors:
            raise RuntimeError(
                f"Failed to recreate {len(idx_errors)} RTREE index(es): "
                + "; ".join(idx_errors)
            )


def main():
    parser = argparse.ArgumentParser(description="Fix midstream ghost reaches (type=6)")
    parser.add_argument("--db", required=True, help="Path to sword_v17c.duckdb")
    parser.add_argument(
        "--apply", action="store_true", help="Apply fixes (default: dry-run)"
    )
    args = parser.parse_args()

    read_only = not args.apply
    con = duckdb.connect(args.db, read_only=read_only)

    try:
        targets = find_midstream_ghosts(con)
        print(
            f"\nFound {len(targets)} midstream ghost reaches (type=6, n_rch_up>0, n_rch_down>0)\n"
        )

        # Summary by region
        by_region: dict[str, list[dict]] = {}
        for t in targets:
            by_region.setdefault(t["region"], []).append(t)

        for region in sorted(by_region):
            region_targets = by_region[region]
            type_counts: dict[int, int] = {}
            for t in region_targets:
                type_counts[t["new_type"]] = type_counts.get(t["new_type"], 0) + 1
            type_summary = ", ".join(
                f"type={k}: {v}" for k, v in sorted(type_counts.items())
            )
            print(f"  {region}: {len(region_targets)} reaches → {type_summary}")

        # Detailed listing
        print(
            f"\n{'reach_id':>12}  {'region':>6}  {'lakeflag':>8}  {'old_type':>8}  {'new_type':>8}  edit_flag"
        )
        print("-" * 80)
        for t in targets:
            ef = t["edit_flag"] or ""
            print(
                f"  {t['reach_id']:>10}  {t['region']:>6}  {t['lakeflag']:>8}  "
                f"{t['old_type']:>8}  {t['new_type']:>8}  {ef}"
            )

        if not args.apply:
            print(
                f"\nDry run — {len(targets)} reaches would be updated. Use --apply to commit."
            )
            return

        applied = apply_fixes(con, targets)
        print(f"\nApplied {applied} fixes to {args.db}")

    finally:
        con.close()


if __name__ == "__main__":
    main()
