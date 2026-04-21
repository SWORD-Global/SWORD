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


def propagate_dist_out_for_reaches(
    conn, region: str, reach_ids: list[int]
) -> int:
    """Recalculate node-level dist_out for specified reaches.

    After rederive_nodes changes node positions/order, the node dist_out
    values must be re-interpolated from the reach using the midpoint
    convention:  reach_dist_out - reach_length + cumsum(node_length) - 0.5*node_length

    Without this step, node_order changes leave dist_out at stale positions,
    causing N004 (dist_out monotonicity) violations.

    Uses the caller's connection to stay within the same transaction.
    """
    updated = 0
    for rid in reach_ids:
        r = conn.execute(
            "SELECT dist_out, reach_length FROM reaches "
            "WHERE reach_id = ? AND region = ?",
            [rid, region],
        ).fetchone()
        if r is None or r[1] == 0:
            continue
        reach_do, reach_len = r

        nodes = conn.execute(
            "SELECT node_id, node_order, node_length, dist_out "
            "FROM nodes WHERE reach_id = ? AND region = ? "
            "ORDER BY node_order",
            [rid, region],
        ).fetchall()

        cumsum = 0.0
        batch = []
        for node_id, _order, node_length, old_do in nodes:
            if node_length <= 0:
                node_length = 0.01  # guard against zero-length nodes
            cumsum += node_length
            new_do = reach_do - reach_len + cumsum - 0.5 * node_length
            if abs(new_do - old_do) > 0.01:
                batch.append((new_do, node_id))

        if batch:
            conn.executemany(
                f"UPDATE nodes SET dist_out = ? "
                f"WHERE node_id = ? AND region = '{region}'",
                batch,
            )
            updated += len(batch)

    return updated


def verify_node_lengths(conn, region: str, reach_ids: list[int]) -> list[int]:
    """Check sum(node_length) == reach_length for processed reaches.

    Returns list of reach_ids where the two diverge by more than 1%.
    Guards against the historical N013 closure bug in rederive_nodes()
    that could corrupt node_length on unrelated reaches.
    """
    bad = []
    for rid in reach_ids:
        r = conn.execute(
            "SELECT r.reach_length, SUM(n.node_length) "
            "FROM reaches r "
            "JOIN nodes n ON r.reach_id = n.reach_id AND r.region = n.region "
            "WHERE r.reach_id = ? AND r.region = ? "
            "GROUP BY r.reach_length",
            [rid, region],
        ).fetchone()
        if r and r[0] > 0:
            pct = abs(r[0] - r[1]) / r[0] * 100
            if pct > 1.0:
                bad.append(rid)
    return bad


def verify_dist_out_monotonicity(
    conn, region: str, reach_ids: list[int]
) -> list[int]:
    """Check dist_out increases with node_order for processed reaches.

    Returns list of reach_ids with N004 violations after propagation.
    """
    bad = []
    for rid in reach_ids:
        violations = conn.execute(
            """
            SELECT COUNT(*) FROM (
                SELECT node_order, dist_out,
                    LAG(dist_out) OVER (ORDER BY node_order) AS prev_do
                FROM nodes WHERE reach_id = ? AND region = ?
            ) WHERE prev_do IS NOT NULL AND dist_out < prev_do
            """,
            [rid, region],
        ).fetchone()[0]
        if violations > 0:
            bad.append(rid)
    return bad


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

        if not dry_run:
            ok_ids = [
                d["reach_id"]
                for d in result["details"]
                if d["status"] == "ok"
            ]
            if not ok_ids:
                return result

            # Use the workflow's DB connection for all post-rederive
            # operations so everything stays in one transaction.
            conn = wf.sword.db.conn

            # After rederive, recalculate node dist_out for affected
            # reaches to prevent N004 violations from stale positions.
            n_updated = propagate_dist_out_for_reaches(conn, region, ok_ids)
            logger.info(
                f"  Recalculated dist_out on {n_updated} nodes "
                f"across {len(ok_ids)} reaches"
            )

            # Verify dist_out monotonicity (N004).
            bad_n004 = verify_dist_out_monotonicity(conn, region, ok_ids)
            if bad_n004:
                logger.error(
                    f"  N004 ALERT: {len(bad_n004)} reaches have "
                    f"non-monotonic dist_out after propagation: {bad_n004}"
                )

            # Verify node_length integrity (G002) to catch any residual
            # N013 closure-bug damage.
            bad_g002 = verify_node_lengths(conn, region, ok_ids)
            if bad_g002:
                logger.error(
                    f"  G002 ALERT: {len(bad_g002)} reaches have "
                    f"node_length != reach_length after rederive: {bad_g002}"
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
