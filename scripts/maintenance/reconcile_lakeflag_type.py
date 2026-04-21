#!/usr/bin/env python3
"""Reconcile lakeflag and type columns on SWORD v17c DuckDB database.

This script performs two main tasks:

Part 1: Retroactive node propagation for GCS sync fixes
    The GCS sync script previously applied ~970 lakeflag changes to the reaches
    table but did NOT propagate them to the nodes table. This part queries the
    lint_fix_log for all lakeflag fixes and propagates them to nodes.

Part 2: Apply classifier predictions
    Load classifier predictions from a parquet file and apply high-confidence
    predictions (pred_proba > 0.8 → lake, pred_proba < 0.2 → river).

Usage:
    # Dry run (default)
    uv run python scripts/maintenance/reconcile_lakeflag_type.py \
        --db data/duckdb/sword_v17c.duckdb \
        --predictions /tmp/lake_classifier_predictions.parquet

    # Apply changes
    uv run python scripts/maintenance/reconcile_lakeflag_type.py \
        --db data/duckdb/sword_v17c.duckdb \
        --predictions /tmp/lake_classifier_predictions.parquet \
        --apply
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb
import pandas as pd

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


def get_next_fix_id(conn: duckdb.DuckDBPyConnection) -> int:
    """Get the next sequential fix_id for lint_fix_log."""
    result = conn.execute("SELECT COALESCE(MAX(fix_id), 0) + 1 FROM lint_fix_log").fetchone()
    return result[0]


def get_rtree_indexes(conn: duckdb.DuckDBPyConnection) -> list[tuple]:
    """Query all RTREE indexes from the database."""
    conn.execute("INSTALL spatial; LOAD spatial;")
    indexes = conn.execute(
        "SELECT index_name, table_name, sql FROM duckdb_indexes() WHERE sql LIKE '%RTREE%'"
    ).fetchall()
    return indexes


def drop_rtree_indexes(conn: duckdb.DuckDBPyConnection, indexes: list[tuple]) -> None:
    """Drop all RTREE indexes."""
    for idx_name, _tbl, _sql in indexes:
        conn.execute(f'DROP INDEX "{idx_name}"')


def recreate_rtree_indexes(conn: duckdb.DuckDBPyConnection, indexes: list[tuple]) -> None:
    """Recreate all RTREE indexes. Raises RuntimeError on failure."""
    idx_errors: list[str] = []
    for _idx_name, _tbl, idx_sql in indexes:
        try:
            conn.execute(idx_sql)
        except Exception as idx_err:
            idx_errors.append(str(idx_err))
    if idx_errors:
        raise RuntimeError(
            f"Failed to recreate {len(idx_errors)} RTREE index(es): " + "; ".join(idx_errors)
        )


def get_gcs_lakeflag_fixes(conn: duckdb.DuckDBPyConnection) -> list[dict]:
    """Query lint_fix_log for all lakeflag fixes from GCS sync.
    
    Returns list of dicts with reach_id, region, new_value (lakeflag value).
    """
    rows = conn.execute(
        """
        SELECT reach_id, region, new_value
        FROM lint_fix_log
        WHERE column_changed = 'lakeflag'
          AND action = 'fix'
          AND notes LIKE '%gcs_sync%'
          AND NOT undone
        """
    ).fetchall()
    
    fixes = []
    for reach_id, region, new_value in rows:
        fixes.append({
            "reach_id": reach_id,
            "region": region,
            "lakeflag": int(new_value) if new_value is not None else None,
        })
    return fixes


def get_nodes_needing_update(
    conn: duckdb.DuckDBPyConnection, fixes: list[dict]
) -> list[dict]:
    """Filter fixes to only those where nodes need updating.
    
    Returns list of dicts where the node's lakeflag doesn't match the reach's lakeflag.
    """
    if not fixes:
        return []
    
    # Create temp table of fixes
    conn.execute("DROP TABLE IF EXISTS _gcs_fixes")
    conn.execute("CREATE TEMP TABLE _gcs_fixes (reach_id BIGINT, region VARCHAR, lakeflag INTEGER)")
    conn.executemany(
        "INSERT INTO _gcs_fixes VALUES (?, ?, ?)",
        [(f["reach_id"], f["region"], f["lakeflag"]) for f in fixes],
    )
    
    # Find nodes that need updating
    rows = conn.execute(
        """
        SELECT n.reach_id, n.region, f.lakeflag as new_lakeflag, n.lakeflag as old_lakeflag
        FROM nodes n
        JOIN _gcs_fixes f ON n.reach_id = f.reach_id AND n.region = f.region
        WHERE n.lakeflag != f.lakeflag
        """
    ).fetchall()
    
    conn.execute("DROP TABLE IF EXISTS _gcs_fixes")
    
    updates = []
    for reach_id, region, new_lakeflag, old_lakeflag in rows:
        updates.append({
            "reach_id": reach_id,
            "region": region,
            "new_lakeflag": new_lakeflag,
            "old_lakeflag": old_lakeflag,
        })
    return updates


def apply_node_propagation(
    conn: duckdb.DuckDBPyConnection, updates: list[dict], next_fix_id: int
) -> tuple[int, int]:
    """Apply lakeflag propagation to nodes table.
    
    Returns (nodes_updated, next_fix_id).
    """
    if not updates:
        return 0, next_fix_id
    
    # Create temp table of updates
    conn.execute("DROP TABLE IF EXISTS _node_updates")
    conn.execute(
        """
        CREATE TEMP TABLE _node_updates (
            reach_id BIGINT,
            region VARCHAR,
            new_lakeflag INTEGER,
            old_lakeflag INTEGER
        )
        """
    )
    conn.executemany(
        "INSERT INTO _node_updates VALUES (?, ?, ?, ?)",
        [(u["reach_id"], u["region"], u["new_lakeflag"], u["old_lakeflag"]) for u in updates],
    )
    
    # Update nodes
    conn.execute(
        """
        UPDATE nodes
        SET lakeflag = u.new_lakeflag
        FROM _node_updates u
        WHERE nodes.reach_id = u.reach_id AND nodes.region = u.region
        """
    )
    nodes_updated = conn.execute("SELECT COUNT(*) FROM _node_updates").fetchone()[0]
    
    # Log to lint_fix_log (one entry per reach for lakeflag column)
    fix_id = next_fix_id
    for update in updates:
        conn.execute(
            """
            INSERT INTO lint_fix_log
                (fix_id, check_id, reach_id, region, action, column_changed, old_value, new_value, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                fix_id,
                "GCS_SYNC",
                update["reach_id"],
                update["region"],
                "fix",
                "lakeflag",
                str(update["old_lakeflag"]),
                str(update["new_lakeflag"]),
                "[clf_reconcile] retroactive node propagation for GCS sync fix",
            ],
        )
        fix_id += 1
    
    conn.execute("DROP TABLE IF EXISTS _node_updates")
    return nodes_updated, fix_id


def load_classifier_predictions(predictions_path: Path) -> pd.DataFrame:
    """Load classifier predictions from parquet file."""
    df = pd.read_parquet(predictions_path)
    required_cols = {"reach_id", "cur_lakeflag", "cur_type", "pred_lake", "pred_proba", "region"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Predictions file missing required columns: {missing}")
    return df


def classify_predictions(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Classify predictions into lake, river, and skip categories.
    
    Returns (lake_df, river_df, skip_df).
    """
    lake_df = df[df["pred_proba"] > 0.8].copy()
    river_df = df[df["pred_proba"] < 0.2].copy()
    skip_df = df[(df["pred_proba"] >= 0.2) & (df["pred_proba"] <= 0.8)].copy()
    return lake_df, river_df, skip_df


def get_current_reach_values(
    conn: duckdb.DuckDBPyConnection, reach_ids: list[int], region: str
) -> dict[int, tuple[int, int]]:
    """Get current lakeflag and type values for reaches.
    
    Returns dict mapping reach_id -> (lakeflag, type).
    """
    if not reach_ids:
        return {}
    
    # Create temp table
    conn.execute("DROP TABLE IF EXISTS _reach_ids")
    conn.execute("CREATE TEMP TABLE _reach_ids (reach_id BIGINT)")
    conn.executemany(
        "INSERT INTO _reach_ids VALUES (?)",
        [(rid,) for rid in reach_ids],
    )
    
    rows = conn.execute(
        """
        SELECT r.reach_id, r.lakeflag, r.type
        FROM reaches r
        JOIN _reach_ids ri ON r.reach_id = ri.reach_id
        WHERE r.region = ?
        """,
        [region],
    ).fetchall()
    
    conn.execute("DROP TABLE IF EXISTS _reach_ids")
    
    return {rid: (lakeflag, rtype) for rid, lakeflag, rtype in rows}


def apply_classifier_predictions(
    conn: duckdb.DuckDBPyConnection,
    lake_df: pd.DataFrame,
    river_df: pd.DataFrame,
    next_fix_id: int,
    dry_run: bool = True,
) -> tuple[dict, int]:
    """Apply classifier predictions to reaches and propagate to nodes.
    
    Returns (stats, next_fix_id).
    """
    stats = {
        "lake_applied": 0,
        "lake_skipped_mismatch": 0,
        "river_applied": 0,
        "river_skipped_mismatch": 0,
        "by_region": {},
    }
    
    fix_id = next_fix_id
    
    # Process lake predictions (lakeflag=1, type=3)
    for region in VALID_REGIONS:
        region_lake_df = lake_df[lake_df["region"] == region]
        region_river_df = river_df[river_df["region"] == region]
        
        region_lake_applied = 0
        region_lake_mismatch = 0
        region_river_applied = 0
        region_river_mismatch = 0
        
        # Process lake predictions
        if len(region_lake_df) > 0:
            reach_ids = region_lake_df["reach_id"].tolist()
            current_values = get_current_reach_values(conn, reach_ids, region)
            
            for _, row in region_lake_df.iterrows():
                reach_id = int(row["reach_id"])
                cur_lakeflag = int(row["cur_lakeflag"])
                cur_type = int(row["cur_type"])
                pred_proba = float(row["pred_proba"])
                
                # Verify current values match expected
                if reach_id not in current_values:
                    print(f"  WARNING: reach {reach_id} not found in region {region}")
                    region_lake_mismatch += 1
                    continue
                
                actual_lakeflag, actual_type = current_values[reach_id]
                if actual_lakeflag != cur_lakeflag or actual_type != cur_type:
                    print(
                        f"  WARNING: reach {reach_id} current values ({actual_lakeflag}, {actual_type}) "
                        f"don't match expected ({cur_lakeflag}, {cur_type}) - skipping"
                    )
                    region_lake_mismatch += 1
                    continue
                
                if not dry_run:
                    # Update reaches.lakeflag
                    conn.execute(
                        "UPDATE reaches SET lakeflag = 1 WHERE reach_id = ? AND region = ?",
                        [reach_id, region],
                    )
                    # Update reaches.type
                    conn.execute(
                        "UPDATE reaches SET type = 3 WHERE reach_id = ? AND region = ?",
                        [reach_id, region],
                    )
                    # Propagate lakeflag to nodes
                    conn.execute(
                        "UPDATE nodes SET lakeflag = 1 WHERE reach_id = ? AND region = ?",
                        [reach_id, region],
                    )
                    # Update edit_flag
                    conn.execute(
                        """
                        UPDATE reaches SET edit_flag = CASE
                            WHEN edit_flag IS NULL OR edit_flag = '' OR edit_flag = 'NaN'
                                THEN 'clf_reconcile'
                            ELSE edit_flag || ',clf_reconcile'
                        END
                        WHERE reach_id = ? AND region = ?
                        """,
                        [reach_id, region],
                    )
                    # Log lakeflag change
                    conn.execute(
                        """
                        INSERT INTO lint_fix_log
                            (fix_id, check_id, reach_id, region, action, column_changed, old_value, new_value, notes)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        [
                            fix_id,
                            "CLF",
                            reach_id,
                            region,
                            "fix",
                            "lakeflag",
                            str(cur_lakeflag),
                            "1",
                            f"[clf_reconcile] classifier p={pred_proba:.3f}",
                        ],
                    )
                    fix_id += 1
                    # Log type change
                    conn.execute(
                        """
                        INSERT INTO lint_fix_log
                            (fix_id, check_id, reach_id, region, action, column_changed, old_value, new_value, notes)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        [
                            fix_id,
                            "CLF",
                            reach_id,
                            region,
                            "fix",
                            "type",
                            str(cur_type),
                            "3",
                            f"[clf_reconcile] classifier p={pred_proba:.3f}",
                        ],
                    )
                    fix_id += 1
                
                region_lake_applied += 1
        
        # Process river predictions (lakeflag=0, type=1)
        if len(region_river_df) > 0:
            reach_ids = region_river_df["reach_id"].tolist()
            current_values = get_current_reach_values(conn, reach_ids, region)
            
            for _, row in region_river_df.iterrows():
                reach_id = int(row["reach_id"])
                cur_lakeflag = int(row["cur_lakeflag"])
                cur_type = int(row["cur_type"])
                pred_proba = float(row["pred_proba"])
                
                # Verify current values match expected
                if reach_id not in current_values:
                    print(f"  WARNING: reach {reach_id} not found in region {region}")
                    region_river_mismatch += 1
                    continue
                
                actual_lakeflag, actual_type = current_values[reach_id]
                if actual_lakeflag != cur_lakeflag or actual_type != cur_type:
                    print(
                        f"  WARNING: reach {reach_id} current values ({actual_lakeflag}, {actual_type}) "
                        f"don't match expected ({cur_lakeflag}, {cur_type}) - skipping"
                    )
                    region_river_mismatch += 1
                    continue
                
                if not dry_run:
                    # Update reaches.lakeflag
                    conn.execute(
                        "UPDATE reaches SET lakeflag = 0 WHERE reach_id = ? AND region = ?",
                        [reach_id, region],
                    )
                    # Update reaches.type
                    conn.execute(
                        "UPDATE reaches SET type = 1 WHERE reach_id = ? AND region = ?",
                        [reach_id, region],
                    )
                    # Propagate lakeflag to nodes
                    conn.execute(
                        "UPDATE nodes SET lakeflag = 0 WHERE reach_id = ? AND region = ?",
                        [reach_id, region],
                    )
                    # Update edit_flag
                    conn.execute(
                        """
                        UPDATE reaches SET edit_flag = CASE
                            WHEN edit_flag IS NULL OR edit_flag = '' OR edit_flag = 'NaN'
                                THEN 'clf_reconcile'
                            ELSE edit_flag || ',clf_reconcile'
                        END
                        WHERE reach_id = ? AND region = ?
                        """,
                        [reach_id, region],
                    )
                    # Log lakeflag change
                    conn.execute(
                        """
                        INSERT INTO lint_fix_log
                            (fix_id, check_id, reach_id, region, action, column_changed, old_value, new_value, notes)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        [
                            fix_id,
                            "CLF",
                            reach_id,
                            region,
                            "fix",
                            "lakeflag",
                            str(cur_lakeflag),
                            "0",
                            f"[clf_reconcile] classifier p={pred_proba:.3f}",
                        ],
                    )
                    fix_id += 1
                    # Log type change
                    conn.execute(
                        """
                        INSERT INTO lint_fix_log
                            (fix_id, check_id, reach_id, region, action, column_changed, old_value, new_value, notes)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        [
                            fix_id,
                            "CLF",
                            reach_id,
                            region,
                            "fix",
                            "type",
                            str(cur_type),
                            "1",
                            f"[clf_reconcile] classifier p={pred_proba:.3f}",
                        ],
                    )
                    fix_id += 1
                
                region_river_applied += 1
        
        # Update stats
        stats["lake_applied"] += region_lake_applied
        stats["lake_skipped_mismatch"] += region_lake_mismatch
        stats["river_applied"] += region_river_applied
        stats["river_skipped_mismatch"] += region_river_mismatch
        
        if region not in stats["by_region"]:
            stats["by_region"][region] = {}
        stats["by_region"][region]["lake_applied"] = region_lake_applied
        stats["by_region"][region]["lake_mismatch"] = region_lake_mismatch
        stats["by_region"][region]["river_applied"] = region_river_applied
        stats["by_region"][region]["river_mismatch"] = region_river_mismatch
    
    return stats, fix_id


def print_dry_run_summary(
    node_updates: list[dict],
    lake_df: pd.DataFrame,
    river_df: pd.DataFrame,
    skip_df: pd.DataFrame,
    stats: dict,
) -> None:
    """Print dry run summary."""
    print("\n=== DRY RUN SUMMARY ===")
    
    print(f"\nPart 1: Node Propagation (GCS Sync Retroactive)")
    print(f"  Node propagation fixes needed: {len(node_updates)}")
    
    print(f"\nPart 2: Classifier Predictions")
    print(f"  Lake predictions (p > 0.8):    {len(lake_df)}")
    print(f"  River predictions (p < 0.2):     {len(river_df)}")
    print(f"  Uncertain (skipped):             {len(skip_df)}")
    
    print(f"\nChanges by Region:")
    for region in VALID_REGIONS:
        region_stats = stats["by_region"].get(region, {})
        lake = region_stats.get("lake_applied", 0)
        river = region_stats.get("river_applied", 0)
        if lake > 0 or river > 0:
            print(f"  {region}: {lake} lake, {river} river")
    
    print(f"\nSample of first 10 changes:")
    sample_count = 0
    for df, label, new_vals in [(lake_df, "lake", "lakeflag=1, type=3"), (river_df, "river", "lakeflag=0, type=1")]:
        for _, row in df.head(5).iterrows():
            if sample_count >= 10:
                break
            print(
                f"  {label}: reach {row['reach_id']} ({row['region']}) "
                f"cur=({row['cur_lakeflag']}, {row['cur_type']}) -> {new_vals} "
                f"p={row['pred_proba']:.3f}"
            )
            sample_count += 1


def main():
    parser = argparse.ArgumentParser(
        description="Reconcile lakeflag and type columns on SWORD v17c DuckDB"
    )
    parser.add_argument(
        "--db",
        default="data/duckdb/sword_v17c.duckdb",
        help="Path to v17c DuckDB",
    )
    parser.add_argument(
        "--predictions",
        required=True,
        help="Path to classifier predictions parquet file",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply changes (default: dry-run only)",
    )
    args = parser.parse_args()
    
    db_path = Path(args.db)
    predictions_path = Path(args.predictions)
    
    if not db_path.exists():
        print(f"ERROR: Database not found: {db_path}", file=sys.stderr)
        sys.exit(1)
    
    if not predictions_path.exists():
        print(f"ERROR: Predictions file not found: {predictions_path}", file=sys.stderr)
        sys.exit(1)
    
    mode = "APPLY" if args.apply else "DRY RUN"
    print(f"=== Lakeflag/Type Reconciliation ({mode}) ===")
    print(f"Database: {db_path}")
    print(f"Predictions: {predictions_path}")
    print()
    
    # Load predictions
    print("Loading classifier predictions...")
    try:
        predictions_df = load_classifier_predictions(predictions_path)
        print(f"  Loaded {len(predictions_df)} predictions")
    except ValueError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
    
    # Classify predictions
    lake_df, river_df, skip_df = classify_predictions(predictions_df)
    print(f"  Lake predictions (p > 0.8): {len(lake_df)}")
    print(f"  River predictions (p < 0.2): {len(river_df)}")
    print(f"  Uncertain (skip): {len(skip_df)}")
    print()
    
    # Connect to database
    conn = duckdb.connect(str(db_path), read_only=not args.apply)
    
    try:
        # Ensure lint_fix_log exists (only in apply mode)
        if args.apply:
            conn.execute(LINT_FIX_LOG_DDL)
        
        # Part 1: Get GCS lakeflag fixes that need node propagation
        print("Part 1: Checking for GCS sync fixes needing node propagation...")
        gcs_fixes = get_gcs_lakeflag_fixes(conn)
        print(f"  Found {len(gcs_fixes)} GCS lakeflag fixes in lint_fix_log")
        
        node_updates = get_nodes_needing_update(conn, gcs_fixes)
        print(f"  Nodes needing update: {len(node_updates)}")
        print()
        
        # Part 2: Pre-calculate classifier changes (for dry-run reporting)
        print("Part 2: Analyzing classifier predictions...")
        # Get stats in dry-run mode first (for reporting)
        stats, _ = apply_classifier_predictions(conn, lake_df, river_df, 0, dry_run=True)
        print()
        
        # Print dry-run summary
        if not args.apply:
            print_dry_run_summary(node_updates, lake_df, river_df, skip_df, stats)
            print("\nDRY RUN complete. Use --apply to commit changes.")
            conn.close()
            return
        
        # APPLY MODE
        print("=== APPLYING CHANGES ===")
        
        # Get next fix_id
        next_fix_id = get_next_fix_id(conn)
        print(f"Starting fix_id: {next_fix_id}")
        
        # Get RTREE indexes (must drop before updates)
        indexes = get_rtree_indexes(conn)
        print(f"Found {len(indexes)} RTREE indexes to preserve")
        
        # Drop RTREE indexes
        drop_rtree_indexes(conn, indexes)
        print("RTREE indexes dropped")
        
        try:
            # Part 1: Apply node propagation
            if node_updates:
                print(f"\nApplying {len(node_updates)} node propagation updates...")
                nodes_updated, next_fix_id = apply_node_propagation(
                    conn, node_updates, next_fix_id
                )
                print(f"  Nodes updated: {nodes_updated}")
                print(f"  Logged {nodes_updated} entries to lint_fix_log")
            
            # Part 2: Apply classifier predictions
            print(f"\nApplying classifier predictions...")
            stats, next_fix_id = apply_classifier_predictions(
                conn, lake_df, river_df, next_fix_id, dry_run=False
            )
            
            total_applied = stats["lake_applied"] + stats["river_applied"]
            total_mismatch = stats["lake_skipped_mismatch"] + stats["river_skipped_mismatch"]
            print(f"  Lake predictions applied: {stats['lake_applied']}")
            print(f"  River predictions applied: {stats['river_applied']}")
            print(f"  Skipped (mismatch): {total_mismatch}")
            print(f"  Total lint_fix_log entries: {total_applied * 2}")  # 2 entries per reach (lakeflag + type)
            
            print(f"\nChanges by Region:")
            for region in VALID_REGIONS:
                region_stats = stats["by_region"].get(region, {})
                lake = region_stats.get("lake_applied", 0)
                river = region_stats.get("river_applied", 0)
                if lake > 0 or river > 0:
                    print(f"  {region}: {lake} lake, {river} river")
            
            print("\nCommitting changes...")
            conn.commit()
            print("Changes committed successfully")
            
        except Exception as e:
            print(f"\nERROR during updates: {e}")
            conn.rollback()
            raise
        finally:
            # ALWAYS recreate RTREE indexes
            print("\nRecreating RTREE indexes...")
            try:
                recreate_rtree_indexes(conn, indexes)
                print("RTREE indexes recreated successfully")
            except RuntimeError as e:
                print(f"CRITICAL ERROR: {e}", file=sys.stderr)
                raise
    
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        conn.close()
        sys.exit(1)
    
    conn.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
