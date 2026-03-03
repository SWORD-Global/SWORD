#!/usr/bin/env python3
"""
Import Backwater QC Fixes into SWORD v17c
------------------------------------------
Pulls reach_fixes.duckdb from GCS (or local path), flattens divergence
junctions via FixesDatabase.build_corrections_table(), deduplicates against
existing backwater_routing_fixes rows, determines region per fix from PFAF
first digit, applies reroute corrections via SWORDWorkflow.modify_reach,
and inserts records into the backwater_routing_fixes table.

Usage:
    # From GCS (default)
    python scripts/maintenance/import_backwater_fixes.py \
        --db data/duckdb/sword_v17c.duckdb

    # From local file
    python scripts/maintenance/import_backwater_fixes.py \
        --db data/duckdb/sword_v17c.duckdb \
        --source /tmp/reach_fixes.duckdb

    # Dry run
    python scripts/maintenance/import_backwater_fixes.py \
        --db data/duckdb/sword_v17c.duckdb --dry-run

    # Include confirm fixes (ground truth only, no reach modification)
    python scripts/maintenance/import_backwater_fixes.py \
        --db data/duckdb/sword_v17c.duckdb --fix-types reroute,confirm
"""

import argparse
import json
import os
import subprocess
import sys
import tempfile
import uuid
from pathlib import Path

import duckdb
import pandas as pd

# Reverse PFAF lookup: first digit of reach_id → region code
_DIGIT_TO_REGION = {
    1: "AF",
    2: "EU",
    3: "AS",
    4: "AS",
    5: "OC",
    6: "SA",
    7: "NA",
    8: "NA",
    9: "NA",
}

GCS_DEFAULT = "gs://backwater-qc-data/backwater/reach_fixes.duckdb"


def region_from_reach_id(reach_id: int) -> str | None:
    """Derive SWORD region from the Pfafstetter first digit of a reach_id."""
    first_digit = int(str(abs(reach_id))[0])
    return _DIGIT_TO_REGION.get(first_digit)


def pull_from_gcs(gcs_uri: str, local_path: str) -> None:
    """Download a file from GCS using gcloud CLI."""
    print(f"Downloading {gcs_uri} -> {local_path}")
    result = subprocess.run(
        ["gcloud", "storage", "cp", gcs_uri, local_path],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(f"gcloud error: {result.stderr}", file=sys.stderr)
        sys.exit(1)
    print(f"Downloaded ({os.path.getsize(local_path) / 1e6:.1f} MB)")


def load_fixes(source_path: str) -> pd.DataFrame:
    """Load active fixes from reach_fixes.duckdb using FixesDatabase.build_corrections_table."""
    # Import FixesDatabase from the backwater package
    try:
        from backwater.core.fixes_db import FixesDatabase
    except ImportError:
        # Fall back to direct DuckDB query if backwater isn't installed
        print("backwater package not installed, using direct DuckDB query")
        return _load_fixes_direct(source_path)

    with FixesDatabase(Path(source_path)) as db:
        all_fixes = db.list_all_fixes(active_only=True)

    if all_fixes.empty:
        print("No active fixes found in source database")
        return pd.DataFrame()

    fix_type_counts = all_fixes["fix_type"].value_counts()
    print(f"Active fixes by type:\n{fix_type_counts.to_string()}")

    corrections = FixesDatabase.build_corrections_table(all_fixes)
    print(f"Flattened to {len(corrections)} junction-level corrections")
    return corrections


def _load_fixes_direct(source_path: str) -> pd.DataFrame:
    """Fallback: load fixes directly from DuckDB without backwater package."""
    con = duckdb.connect(source_path, read_only=True)
    try:
        fixes_df = con.execute(
            """
            SELECT fix_id, outlet_id, fix_type, user_id, metadata, created_at
            FROM reach_fixes
            WHERE is_active = TRUE
        """
        ).fetchdf()
    finally:
        con.close()

    if fixes_df.empty:
        print("No active fixes found in source database")
        return pd.DataFrame()

    fix_type_counts = fixes_df["fix_type"].value_counts()
    print(f"Active fixes by type:\n{fix_type_counts.to_string()}")

    # Manually flatten divergence junctions (mirrors FixesDatabase.build_corrections_table)
    records = []
    for _, row in fixes_df.iterrows():
        meta_raw = row.get("metadata")
        if not meta_raw:
            continue
        meta = json.loads(meta_raw) if isinstance(meta_raw, str) else meta_raw
        for junc in meta.get("divergence_junctions", []):
            records.append(
                {
                    "outlet_id": row["outlet_id"],
                    "junction_reach_id": junc.get("junction"),
                    "old_rch_id_up_main": junc.get("old_branch"),
                    "new_rch_id_up_main": junc.get("new_branch"),
                    "old_branch_facc": junc.get("old_branch_facc"),
                    "new_branch_facc": junc.get("new_branch_facc"),
                    "old_branch_width": junc.get("old_branch_width"),
                    "new_branch_width": junc.get("new_branch_width"),
                    "fix_type": row["fix_type"],
                    "user_id": row["user_id"],
                    "fix_id": row["fix_id"],
                    "created_at": row["created_at"],
                }
            )

    if not records:
        print("No divergence junctions found in fix metadata")
        return pd.DataFrame()

    corrections = pd.DataFrame(records)
    print(f"Flattened to {len(corrections)} junction-level corrections")
    return corrections


def dedup_corrections(
    corrections: pd.DataFrame, existing_junctions: set[int]
) -> pd.DataFrame:
    """Remove corrections for junctions already in backwater_routing_fixes.

    When multiple fixes target the same junction, keep the newest by created_at
    (if column available), otherwise keep the last one.
    """
    before = len(corrections)

    # Dedup within the incoming batch: keep last per junction
    if "created_at" in corrections.columns:
        corrections = corrections.sort_values("created_at")
    corrections = corrections.drop_duplicates(subset="junction_reach_id", keep="last")

    # Dedup against existing
    corrections = corrections[
        ~corrections["junction_reach_id"].isin(existing_junctions)
    ]

    after = len(corrections)
    if before != after:
        print(
            f"Deduplication: {before} -> {after} corrections ({before - after} skipped)"
        )
    return corrections


def validate_corrections(
    corrections: pd.DataFrame, sword_conn: duckdb.DuckDBPyConnection
) -> pd.DataFrame:
    """Validate that junction_reach_ids exist and new_rch_id_up_main are topology neighbors."""
    valid_rows = []
    skipped = 0

    for _, row in corrections.iterrows():
        jid = row["junction_reach_id"]
        new_up = row["new_rch_id_up_main"]

        if pd.isna(jid) or pd.isna(new_up):
            print(f"  SKIP: NULL junction or new_up for outlet {row['outlet_id']}")
            skipped += 1
            continue

        jid = int(jid)
        new_up = int(new_up)

        # Check junction exists
        exists = sword_conn.execute(
            "SELECT 1 FROM reaches WHERE reach_id = ?", [jid]
        ).fetchone()
        if not exists:
            print(f"  SKIP: junction {jid} not found in reaches")
            skipped += 1
            continue

        # Check new_up is an actual topology neighbor
        is_neighbor = sword_conn.execute(
            """
            SELECT 1 FROM reach_topology
            WHERE reach_id = ? AND direction = 'up' AND neighbor_reach_id = ?
        """,
            [jid, new_up],
        ).fetchone()
        if not is_neighbor:
            print(f"  SKIP: {new_up} is not an upstream neighbor of junction {jid}")
            skipped += 1
            continue

        valid_rows.append(row)

    if skipped:
        print(f"Validation: {skipped} corrections skipped, {len(valid_rows)} valid")
    return pd.DataFrame(valid_rows)


def _ensure_backwater_table(conn: duckdb.DuckDBPyConnection) -> None:
    """Create backwater_routing_fixes table if it doesn't exist."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS backwater_routing_fixes (
            fix_id VARCHAR PRIMARY KEY,
            outlet_id BIGINT NOT NULL,
            fix_type VARCHAR NOT NULL,
            junction_reach_id BIGINT,
            old_rch_id_up_main BIGINT,
            new_rch_id_up_main BIGINT,
            old_branch_facc DOUBLE,
            new_branch_facc DOUBLE,
            old_branch_width DOUBLE,
            new_branch_width DOUBLE,
            user_id VARCHAR,
            region VARCHAR(2),
            network INTEGER,
            import_run_id VARCHAR NOT NULL,
            applied_at TIMESTAMP DEFAULT current_timestamp,
            pipeline_rerun BOOLEAN DEFAULT FALSE
        )
    """)


def import_fixes(
    db_path: str,
    corrections: pd.DataFrame,
    run_id: str,
    dry_run: bool = False,
    fix_types: set[str] | None = None,
) -> dict[str, int]:
    """Apply corrections to SWORD v17c and insert into backwater_routing_fixes.

    Returns summary stats dict.
    """
    fix_types = fix_types or {"reroute"}
    stats = {"reroutes_applied": 0, "confirms_inserted": 0, "errors": 0}

    # Add PYTHONPATH for SWORDWorkflow import
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    sys.path.insert(
        0,
        os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"
        ),
    )
    from sword_duckdb import SWORDWorkflow

    # Open a raw connection for table inserts and network lookups
    raw_conn = duckdb.connect(db_path)

    # Ensure backwater_routing_fixes table exists
    _ensure_backwater_table(raw_conn)

    # Enrich with region and network
    enriched = []
    for _, row in corrections.iterrows():
        jid = int(row["junction_reach_id"])
        region = region_from_reach_id(jid)
        if not region:
            print(f"  SKIP: cannot determine region for junction {jid}")
            stats["errors"] += 1
            continue

        network = raw_conn.execute(
            "SELECT network FROM reaches WHERE reach_id = ?", [jid]
        ).fetchone()
        network_val = int(network[0]) if network and network[0] is not None else None

        enriched.append(
            {
                **row.to_dict(),
                "region": region,
                "network": network_val,
                "junction_reach_id": jid,
                "new_rch_id_up_main": int(row["new_rch_id_up_main"]),
                "old_rch_id_up_main": (
                    int(row["old_rch_id_up_main"])
                    if pd.notna(row.get("old_rch_id_up_main"))
                    else None
                ),
            }
        )

    raw_conn.close()

    if not enriched:
        print("No valid corrections to apply")
        return stats

    enriched_df = pd.DataFrame(enriched)

    # Group by region for workflow operations
    for region in sorted(enriched_df["region"].unique()):
        region_fixes = enriched_df[enriched_df["region"] == region]
        reroutes = region_fixes[region_fixes["fix_type"] == "reroute"]
        confirms = region_fixes[region_fixes["fix_type"] == "confirm"]

        print(f"\nRegion {region}: {len(reroutes)} reroutes, {len(confirms)} confirms")

        if not reroutes.empty and "reroute" in fix_types:
            if dry_run:
                for _, fix in reroutes.iterrows():
                    print(
                        f"  DRY RUN: junction {fix['junction_reach_id']}: "
                        f"rch_id_up_main {fix.get('old_rch_id_up_main')} -> "
                        f"{fix['new_rch_id_up_main']}"
                    )
            else:
                workflow = SWORDWorkflow(user_id="backwater_import")
                workflow.load(db_path, region, reason="backwater QC import")

                with workflow.transaction(
                    f"Backwater QC: {len(reroutes)} reroutes for {region}"
                ):
                    for _, fix in reroutes.iterrows():
                        try:
                            workflow.modify_reach(
                                fix["junction_reach_id"],
                                rch_id_up_main=fix["new_rch_id_up_main"],
                                reason=(
                                    f"backwater QC reroute: outlet={fix['outlet_id']}, "
                                    f"user={fix.get('user_id', 'unknown')}"
                                ),
                            )
                            stats["reroutes_applied"] += 1
                        except Exception as e:
                            print(
                                f"  ERROR applying reroute at junction "
                                f"{fix['junction_reach_id']}: {e}"
                            )
                            stats["errors"] += 1

                workflow.close()

        # Insert all processed fixes into backwater_routing_fixes
        insert_conn = duckdb.connect(db_path)
        try:
            for _, fix in region_fixes.iterrows():
                ft = fix["fix_type"]
                if ft not in fix_types:
                    continue

                fix_id = fix.get("fix_id", str(uuid.uuid4()))

                if dry_run:
                    if ft == "confirm":
                        print(
                            f"  DRY RUN: confirm junction {fix['junction_reach_id']} "
                            f"(ground truth, no reach modification)"
                        )
                    continue

                insert_conn.execute(
                    """
                    INSERT OR IGNORE INTO backwater_routing_fixes (
                        fix_id, outlet_id, fix_type, junction_reach_id,
                        old_rch_id_up_main, new_rch_id_up_main,
                        old_branch_facc, new_branch_facc,
                        old_branch_width, new_branch_width,
                        user_id, region, network, import_run_id
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    [
                        str(fix_id),
                        int(fix["outlet_id"]),
                        ft,
                        int(fix["junction_reach_id"]),
                        fix.get("old_rch_id_up_main"),
                        int(fix["new_rch_id_up_main"]) if ft == "reroute" else None,
                        fix.get("old_branch_facc"),
                        fix.get("new_branch_facc"),
                        fix.get("old_branch_width"),
                        fix.get("new_branch_width"),
                        fix.get("user_id"),
                        region,
                        fix.get("network"),
                        run_id,
                    ],
                )
                if ft == "confirm":
                    stats["confirms_inserted"] += 1
        finally:
            insert_conn.close()

    return stats


def main():
    parser = argparse.ArgumentParser(
        description="Import backwater QC fixes into SWORD v17c"
    )
    parser.add_argument("--db", required=True, help="Path to sword_v17c.duckdb")
    parser.add_argument(
        "--source",
        default=GCS_DEFAULT,
        help=f"GCS URI or local path to reach_fixes.duckdb (default: {GCS_DEFAULT})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without applying",
    )
    parser.add_argument(
        "--fix-types",
        default="reroute",
        help="Comma-separated fix types to process (default: reroute)",
    )
    args = parser.parse_args()

    if not os.path.exists(args.db):
        print(f"ERROR: Database not found: {args.db}", file=sys.stderr)
        sys.exit(1)

    fix_types = {ft.strip() for ft in args.fix_types.split(",")}
    run_id = str(uuid.uuid4())[:8]
    print(f"Import run: {run_id}")
    print(f"Fix types: {fix_types}")

    # Resolve source
    if args.source.startswith("gs://"):
        local_path = os.path.join(tempfile.gettempdir(), "reach_fixes.duckdb")
        pull_from_gcs(args.source, local_path)
        source_path = local_path
    else:
        source_path = args.source
        if not os.path.exists(source_path):
            print(f"ERROR: Source not found: {source_path}", file=sys.stderr)
            sys.exit(1)

    # Load and flatten fixes
    corrections = load_fixes(source_path)
    if corrections.empty:
        print("No corrections to import")
        return

    # Filter to requested fix types
    corrections = corrections[corrections["fix_type"].isin(fix_types)]
    if corrections.empty:
        print(f"No corrections match fix types: {fix_types}")
        return

    print(f"\n{len(corrections)} corrections for types: {fix_types}")

    # Get existing junctions to dedup
    sword_conn = duckdb.connect(args.db)
    try:
        # Ensure table exists
        _ensure_backwater_table(sword_conn)

        existing = sword_conn.execute(
            "SELECT junction_reach_id FROM backwater_routing_fixes"
        ).fetchdf()
        existing_junctions = set(existing["junction_reach_id"].astype(int))
        print(f"Existing fixes in DB: {len(existing_junctions)} junctions")
    except Exception:
        existing_junctions = set()
    finally:
        sword_conn.close()

    corrections = dedup_corrections(corrections, existing_junctions)
    if corrections.empty:
        print("All corrections already exist in database")
        return

    # Validate corrections against SWORD topology
    sword_conn = duckdb.connect(args.db, read_only=True)
    try:
        reroutes = corrections[corrections["fix_type"] == "reroute"]
        confirms = corrections[corrections["fix_type"] == "confirm"]

        if not reroutes.empty:
            valid_reroutes = validate_corrections(reroutes, sword_conn)
        else:
            valid_reroutes = pd.DataFrame()

        # Confirms don't need topology validation
        corrections = pd.concat([valid_reroutes, confirms], ignore_index=True)
    finally:
        sword_conn.close()

    if corrections.empty:
        print("No valid corrections after validation")
        return

    print(f"\n{len(corrections)} corrections ready to import")

    # Apply
    stats = import_fixes(
        args.db, corrections, run_id, dry_run=args.dry_run, fix_types=fix_types
    )

    print(f"\nImport complete (run_id={run_id}):")
    for k, v in stats.items():
        print(f"  {k}: {v}")

    if not args.dry_run and stats["reroutes_applied"] > 0:
        print("\nNext step: re-run v17c pipeline to propagate routing changes:")
        print(
            f"  PYTHONPATH=src python -m src.sword_v17c_pipeline.v17c_pipeline "
            f"--db {args.db} --all --skip-swot --skip-facc --skip-path-vars"
        )


if __name__ == "__main__":
    main()
