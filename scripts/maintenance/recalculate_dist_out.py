#!/usr/bin/env python3
"""
Recalculate dist_out for all reaches and nodes.

Ensures continuity across reach boundaries by accumulating reach lengths
upstream from outlets.

Formula:
  Reach Outlets (n_rch_down == 0): dist_out = reach_length
  Other Reaches: dist_out = max(downstream neighbors' dist_out) + reach_length
  Nodes: dist_out = (base reach dist_out - reach_length) + cumulative node lengths
"""

import argparse
import sys
import logging
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from sword_duckdb.sword_db import SWORDDatabase
from sword_duckdb.workflow import SWORDWorkflow

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Recalculate dist_out values")
    parser.add_argument("--db", required=True, help="Path to DuckDB file")
    parser.add_argument("--region", help="Specific region to process (default: all)")
    args = parser.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        logger.error(f"Database not found: {db_path}")
        sys.exit(1)

    # Determine regions to process
    if args.region:
        regions = [args.region.upper()]
    else:
        # Get regions from database
        db = SWORDDatabase(str(db_path))
        conn = db.connect()
        regions = [
            r[0] for r in conn.execute("SELECT DISTINCT region FROM reaches").fetchall()
        ]
        db.close()

    logger.info(f"Processing regions: {regions}")

    workflow = SWORDWorkflow(user_id="dist_out_fix", enable_provenance=True)

    for region in regions:
        logger.info(f"--- Processing Region: {region} ---")
        try:
            workflow.load(str(db_path), region)

            # 1. Recalculate reach_length from node_length first to ensure consistency
            logger.info("Recalculating reach_length from nodes...")
            conn = workflow._sword.db.connect()
            conn.execute(
                f"""
                UPDATE reaches
                SET reach_length = sub.total_len
                FROM (
                    SELECT reach_id, region, SUM(node_length) as total_len
                    FROM nodes
                    WHERE region = '{region}'
                    GROUP BY reach_id, region
                ) sub
                WHERE reaches.reach_id = sub.reach_id
                  AND reaches.region = sub.region
            """
            )

            # 2. Recalculate dist_out
            result = workflow.calculate_dist_out(
                update_nodes=True,
                reason="Global recalculation to fix N006 discontinuities",
            )

            if result["success"]:
                logger.info(
                    f"Successfully updated {result['reaches_updated']} reaches and {result['nodes_updated']} nodes."
                )
            else:
                logger.warning(
                    f"Recalculation incomplete for {region}. {len(result['unfilled_reaches'])} reaches skipped."
                )

            workflow.close()
        except Exception as e:
            logger.error(f"Error processing region {region}: {e}")
            if workflow.is_loaded:
                workflow.close()

    logger.info("Done.")


if __name__ == "__main__":
    main()
