#!/usr/bin/env python3
"""
Fix reversed flow directions in SWORD v17c.

Section-based flipping with multi-signal scoring and global path
verification.  Uses existing v17c pipeline infrastructure for graph
construction and derived-attribute rebuild.

Usage
-----
    # Dry-run (score and report only):
    python scripts/topology/fix_reversed_sections.py \
        --db data/duckdb/sword_v17c.duckdb --region NA --dry-run

    # Apply approved flips:
    python scripts/topology/fix_reversed_sections.py \
        --db data/duckdb/sword_v17c.duckdb --all --apply

    # Rollback a previous run:
    python scripts/topology/fix_reversed_sections.py \
        --db data/duckdb/sword_v17c.duckdb --region NA --rollback RUN_ID
"""

import argparse
import os
import sys

# Add project root to path
sys.path.insert(
    0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

from src.sword_v17c_pipeline.flow_verification import run_flow_verification
from src.sword_v17c_pipeline.stages._logging import log

REGIONS = ["NA", "SA", "EU", "AF", "AS", "OC"]


def main():
    parser = argparse.ArgumentParser(
        description="Fix reversed flow directions in SWORD v17c"
    )
    parser.add_argument("--db", required=True, help="Path to sword_v17c.duckdb")
    parser.add_argument(
        "--region", help="Single region to process (NA, SA, EU, AF, AS, OC)"
    )
    parser.add_argument("--all", action="store_true", help="Process all regions")
    parser.add_argument(
        "--dry-run", action="store_true", help="Score and report only, no DB writes"
    )
    parser.add_argument(
        "--apply", action="store_true", help="Apply approved flips to database"
    )
    parser.add_argument(
        "--rollback", metavar="RUN_ID", help="Rollback a previous run, then exit"
    )
    parser.add_argument(
        "--max-group-size",
        type=int,
        default=30,
        help="Max sections per flip group (default: 30)",
    )
    parser.add_argument(
        "--min-section-size",
        type=int,
        default=2,
        help="Min reaches per section to be eligible (default: 2)",
    )
    parser.add_argument(
        "--skip-rebuild",
        action="store_true",
        help="Skip derived-attribute rebuild after flipping (debugging only)",
    )

    args = parser.parse_args()

    # Determine regions
    if args.all:
        regions = REGIONS
    elif args.region:
        regions = [args.region.upper()]
    else:
        parser.error("Either --region or --all must be specified")

    if not os.path.exists(args.db):
        print(f"ERROR: Database not found: {args.db}")
        sys.exit(1)

    # Rollback mode
    if args.rollback:
        import duckdb

        from src.sword_v17c_pipeline.flow_direction import rollback_flow_corrections

        conn = duckdb.connect(args.db)
        for region in regions:
            rollback_flow_corrections(conn, region, args.rollback)
        conn.close()
        log("Rollback complete")
        sys.exit(0)

    # Need either --dry-run or --apply
    if not args.dry_run and not args.apply:
        parser.error("Either --dry-run or --apply is required")

    dry_run = args.dry_run

    all_results = []
    for region in regions:
        log(f"\n{'=' * 60}")
        log(f"Region: {region}")
        log(f"{'=' * 60}")

        result = run_flow_verification(
            db_path=args.db,
            region=region,
            dry_run=dry_run,
            max_group_size=args.max_group_size,
            min_section_size=args.min_section_size,
            skip_rebuild=args.skip_rebuild,
        )
        all_results.append(result)

    # Print summary
    log(f"\n{'=' * 60}")
    log("OVERALL SUMMARY")
    log(f"{'=' * 60}")
    total_approved = 0
    total_rejected = 0
    total_low = 0
    for r in all_results:
        approved = r.get("n_approved", 0)
        rejected = r.get("n_rejected", 0)
        low = r.get("n_low", 0)
        total_approved += approved
        total_rejected += rejected
        total_low += low
        log(
            f"  {r['region']}: {approved} approved, "
            f"{rejected} rejected, {low} low-confidence"
        )

    log(
        f"\nTotal: {total_approved} approved, {total_rejected} rejected, {total_low} low"
    )
    if dry_run:
        log("Mode: DRY RUN (no changes applied)")
    else:
        log("Mode: APPLIED")


if __name__ == "__main__":
    main()
