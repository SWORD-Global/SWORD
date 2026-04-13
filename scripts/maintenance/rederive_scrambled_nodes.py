"""Rederive nodes for reaches with mislocalized nodes (v17c 0.0.10).

Finds reaches where consecutive node gaps exceed 3x the reach's median
spacing AND > 0.4 km absolute, then runs rederive_nodes to fix them.

Usage:
    uv run python scripts/maintenance/rederive_scrambled_nodes.py \
        --db data/duckdb/sword_v17c.duckdb --all
    uv run python scripts/maintenance/rederive_scrambled_nodes.py \
        --db data/duckdb/sword_v17c.duckdb --region AS
    uv run python scripts/maintenance/rederive_scrambled_nodes.py \
        --db data/duckdb/sword_v17c.duckdb --all --dry-run
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import duckdb

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

REGIONS = ["NA", "SA", "EU", "AF", "AS", "OC"]

DETECT_SQL = """
WITH ordered AS (
    SELECT
        reach_id, region, node_id, node_order, x, y,
        LEAD(x) OVER w AS next_x,
        LEAD(y) OVER w AS next_y
    FROM nodes
    WHERE region = ?
    WINDOW w AS (PARTITION BY reach_id, region ORDER BY node_order)
),
distances AS (
    SELECT reach_id, region, node_id, node_order,
        6371.0 * 2 * ASIN(SQRT(
            POWER(SIN(RADIANS(next_y - y) / 2), 2)
            + COS(RADIANS(y)) * COS(RADIANS(next_y))
              * POWER(SIN(RADIANS(next_x - x) / 2), 2)
        )) AS dist_km
    FROM ordered
    WHERE next_x IS NOT NULL
),
reach_stats AS (
    SELECT reach_id, region,
           MEDIAN(dist_km) AS median_gap,
           COUNT(*) AS n_pairs
    FROM distances
    GROUP BY reach_id, region
    HAVING n_pairs >= 3
)
SELECT DISTINCT d.reach_id
FROM distances d
JOIN reach_stats rs
  ON d.reach_id = rs.reach_id AND d.region = rs.region
WHERE d.dist_km > 3.0 * rs.median_gap
  AND d.dist_km > 0.4
ORDER BY d.reach_id
"""


def detect_scrambled(db_path: str, region: str) -> list[int]:
    """Return reach_ids with mislocalized nodes in *region*."""
    con = duckdb.connect(db_path, read_only=True)
    try:
        rows = con.execute(DETECT_SQL, [region]).fetchall()
        return [r[0] for r in rows]
    finally:
        con.close()


def rederive_region(
    db_path: str, region: str, reach_ids: list[int], dry_run: bool
) -> dict:
    """Run rederive_nodes for one region via SWORDWorkflow."""
    from sword_duckdb import SWORDWorkflow

    wf = SWORDWorkflow(user_id="jake")
    wf.load(db_path, region)
    try:
        result = wf.rederive_nodes(
            reach_ids=reach_ids,
            region=region,
            dry_run=dry_run,
            reason="Fix scrambled node geolocation (0.0.10)",
        )
        return result
    finally:
        wf.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Rederive scrambled nodes")
    parser.add_argument("--db", required=True, help="Path to sword_v17c.duckdb")
    parser.add_argument("--all", action="store_true", help="All 6 regions")
    parser.add_argument("--region", help="Single region (e.g. AS)")
    parser.add_argument("--dry-run", action="store_true", help="Detect only")
    args = parser.parse_args()

    if not args.all and not args.region:
        parser.error("Specify --all or --region")

    regions = REGIONS if args.all else [args.region.upper()]

    total_detected = 0
    total_processed = 0

    for region in regions:
        logger.info(f"--- {region} ---")
        reach_ids = detect_scrambled(args.db, region)
        logger.info(f"  Detected {len(reach_ids)} reaches with scrambled nodes")
        total_detected += len(reach_ids)

        if not reach_ids:
            continue

        result = rederive_region(args.db, region, reach_ids, args.dry_run)
        processed = result["reaches_processed"]
        total_processed += processed

        skipped = [
            d for d in result["details"] if d["status"] != "ok" and d["status"] != "dry_run"
        ]
        if skipped:
            logger.warning(f"  Skipped {len(skipped)} reaches:")
            for s in skipped:
                logger.warning(f"    {s['reach_id']}: {s['status']}")

        logger.info(f"  Processed {processed}/{len(reach_ids)}")

    logger.info(f"=== Total: detected {total_detected}, processed {total_processed} ===")

    if args.dry_run:
        logger.info("(dry-run — no changes written)")
    else:
        logger.info("Done. Verify with --dry-run or POM test 6b.")


if __name__ == "__main__":
    # Ensure src/ is importable
    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
    main()
