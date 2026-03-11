#!/usr/bin/env python3
"""Fix self-intersecting reach geometries (G004) in SWORD v17c.

Two categories of self-intersecting geometries:

1. **v17c-introduced** (79 reaches): Caused by overlap vertex addition in
   ``rebuild_reach_geometry.py``. The prepended/appended overlap vertex
   doubles back past the original endpoint, creating a bowtie crossing.
   Fix: remove the offending overlap vertex(es) by comparing to v17b.

2. **v17b-inherited** (395 reaches): Upstream data quality issues where
   near-adjacent centerline segments zigzag and cross. Most are single
   crossings between segments 2-3 apart.
   Fix: iteratively remove interior vertices between crossing segments.
   For touching-point duplicates, remove the duplicate vertex.
   For edge cases, try single-vertex removal brute force.

Usage
-----
    python scripts/maintenance/fix_self_intersections.py \
        --v17c data/duckdb/sword_v17c.duckdb \
        --v17b data/duckdb/sword_v17b.duckdb

    # Dry run (report only, no writes)
    python scripts/maintenance/fix_self_intersections.py \
        --v17c data/duckdb/sword_v17c.duckdb \
        --v17b data/duckdb/sword_v17b.duckdb \
        --dry-run
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

import duckdb
from shapely import wkb
from shapely.geometry import LineString

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

BATCH_SIZE = 500


# ---------------------------------------------------------------------------
# Fix strategies
# ---------------------------------------------------------------------------


def fix_v17c_introduced(
    con_c: duckdb.DuckDBPyConnection,
    con_b: duckdb.DuckDBPyConnection,
    introduced_ids: list[int],
) -> tuple[list[tuple[int, bytes]], list[int]]:
    """Fix v17c-introduced self-intersections by removing overlap vertices.

    Compares v17c geometry to v17b geometry. The overlap vertex is the
    first or last coordinate that was prepended/appended by
    rebuild_reach_geometry.py. Removing it restores simplicity.

    Returns (fixed, unfixed) where fixed is [(reach_id, wkb_bytes), ...].
    """
    fixed: list[tuple[int, bytes]] = []
    unfixed: list[int] = []

    for rid in introduced_ids:
        v17c_row = con_c.execute(
            "SELECT ST_AsWKB(geom) FROM reaches WHERE reach_id = ?", [rid]
        ).fetchone()
        v17b_row = con_b.execute(
            "SELECT ST_AsWKB(geom) FROM reaches WHERE reach_id = ?", [rid]
        ).fetchone()

        if not v17c_row or not v17c_row[0]:
            unfixed.append(rid)
            continue

        g_c = wkb.loads(bytes(v17c_row[0]))
        c_coords = list(g_c.coords)

        if not v17b_row or not v17b_row[0]:
            # No v17b geometry to compare -- skip
            unfixed.append(rid)
            continue

        g_b = wkb.loads(bytes(v17b_row[0]))
        b_coords = list(g_b.coords)

        # Detect which end has the overlap vertex
        prepended = c_coords[0] != b_coords[0]
        appended = c_coords[-1] != b_coords[-1]

        new_coords = list(c_coords)
        if prepended:
            new_coords = new_coords[1:]
        if appended:
            new_coords = new_coords[:-1]

        if len(new_coords) >= 2:
            new_g = LineString(new_coords)
            if new_g.is_simple:
                fixed.append((rid, new_g.wkb))
                continue

        # Fallback: revert to v17b geometry if it was simple
        if g_b.is_simple:
            fixed.append((rid, g_b.wkb))
            continue

        unfixed.append(rid)

    return fixed, unfixed


def _find_crossings(coords: list[tuple]) -> list[tuple[int, int]]:
    """Find pairs of non-adjacent segments that cross each other."""
    n = len(coords)
    crossings = []
    for i in range(n - 1):
        seg1 = LineString([coords[i], coords[i + 1]])
        for j in range(i + 2, n - 1):
            seg2 = LineString([coords[j], coords[j + 1]])
            if seg1.crosses(seg2):
                crossings.append((i, j))
    return crossings


def _find_touching_segments(coords: list[tuple]) -> list[tuple[int, int]]:
    """Find non-adjacent segments that touch (share a point without crossing)."""
    n = len(coords)
    touches = []
    for i in range(n - 1):
        seg1 = LineString([coords[i], coords[i + 1]])
        for j in range(i + 2, n - 1):
            seg2 = LineString([coords[j], coords[j + 1]])
            if seg1.touches(seg2):
                touches.append((i, j))
    return touches


def _iterative_crossing_removal(
    coords: list[tuple], max_iters: int = 20
) -> list[tuple] | None:
    """Iteratively remove vertices between crossing segments until simple.

    Returns new coords if successful, None if not.
    """
    modified = list(coords)
    for _ in range(max_iters):
        crossings = _find_crossings(modified)
        if not crossings:
            g = LineString(modified) if len(modified) >= 2 else None
            if g and g.is_simple:
                return modified
            break

        # Remove vertices between the first crossing pair
        ci, cj = crossings[0]
        to_remove = set(range(ci + 1, cj + 1))
        modified = [c for idx, c in enumerate(modified) if idx not in to_remove]

        if len(modified) < 2:
            return None

    return modified if len(modified) >= 2 and LineString(modified).is_simple else None


def _touching_point_removal(coords: list[tuple]) -> list[tuple] | None:
    """Fix touching-point self-intersections (duplicate consecutive vertices).

    These appear as non-adjacent segments that share an endpoint because
    the same coordinate appears twice with one vertex between them.
    """
    touches = _find_touching_segments(coords)
    if not touches:
        return None

    to_remove = set()
    for ci, cj in touches:
        # Remove vertices between the two touching segments
        for k in range(ci + 1, cj + 1):
            to_remove.add(k)

    new_coords = [c for idx, c in enumerate(coords) if idx not in to_remove]
    if len(new_coords) >= 2 and LineString(new_coords).is_simple:
        return new_coords
    return None


def _brute_force_single_removal(coords: list[tuple]) -> list[tuple] | None:
    """Try removing each vertex one at a time to find which one fixes the issue."""
    n = len(coords)
    for i in range(n):
        test_coords = coords[:i] + coords[i + 1 :]
        if len(test_coords) >= 2:
            test_g = LineString(test_coords)
            if test_g.is_simple:
                return test_coords
    return None


def fix_v17b_inherited(
    con_c: duckdb.DuckDBPyConnection,
    inherited_ids: list[int],
) -> tuple[list[tuple[int, bytes]], list[int]]:
    """Fix v17b-inherited self-intersections.

    Strategy cascade:
    1. Iterative crossing removal (handles 387/395)
    2. Touching-point duplicate removal (handles 4 more)
    3. Brute-force single vertex removal (handles remaining edge cases)

    Returns (fixed, unfixed) where fixed is [(reach_id, wkb_bytes), ...].
    """
    fixed: list[tuple[int, bytes]] = []
    unfixed: list[int] = []

    for idx, rid in enumerate(inherited_ids):
        if (idx + 1) % 50 == 0:
            logger.info(f"  Processing inherited {idx + 1}/{len(inherited_ids)}...")

        row = con_c.execute(
            "SELECT ST_AsWKB(geom) FROM reaches WHERE reach_id = ?", [rid]
        ).fetchone()
        if not row or not row[0]:
            unfixed.append(rid)
            continue

        g = wkb.loads(bytes(row[0]))
        coords = list(g.coords)

        # Strategy 1: iterative crossing removal
        result = _iterative_crossing_removal(coords)
        if result is not None:
            fixed.append((rid, LineString(result).wkb))
            continue

        # Strategy 2: touching-point removal
        result = _touching_point_removal(coords)
        if result is not None:
            fixed.append((rid, LineString(result).wkb))
            continue

        # Strategy 3: brute-force single vertex removal
        result = _brute_force_single_removal(coords)
        if result is not None:
            fixed.append((rid, LineString(result).wkb))
            continue

        unfixed.append(rid)

    return fixed, unfixed


# ---------------------------------------------------------------------------
# Write fixes to database
# ---------------------------------------------------------------------------


def apply_fixes(
    con: duckdb.DuckDBPyConnection,
    fixes: list[tuple[int, bytes]],
) -> int:
    """Write fixed geometries back to the reaches table.

    Also recomputes bbox/centroid columns for affected reaches.
    """
    if not fixes:
        return 0

    updated = 0
    for i in range(0, len(fixes), BATCH_SIZE):
        batch = fixes[i : i + BATCH_SIZE]
        con.executemany(
            "UPDATE reaches SET geom = ST_GeomFromWKB($1) WHERE reach_id = $2",
            [(wkb_bytes, rid) for rid, wkb_bytes in batch],
        )
        updated += len(batch)
        logger.info(f"  Written {updated}/{len(fixes)} fixes")

    # Recompute bbox/centroid for affected reaches
    reach_ids = [rid for rid, _ in fixes]
    placeholders = ",".join(str(r) for r in reach_ids)
    con.execute(f"""
        UPDATE reaches SET
            x     = ST_X(ST_Centroid(geom)),
            y     = ST_Y(ST_Centroid(geom)),
            x_min = ST_XMin(geom),
            x_max = ST_XMax(geom),
            y_min = ST_YMin(geom),
            y_max = ST_YMax(geom)
        WHERE reach_id IN ({placeholders})
    """)

    return updated


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(
        description="Fix self-intersecting reach geometries (G004)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--v17c",
        required=True,
        help="Path to v17c DuckDB database",
    )
    parser.add_argument(
        "--v17b",
        required=True,
        help="Path to v17b DuckDB database (read-only reference)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report fixes without writing to database",
    )
    args = parser.parse_args()

    v17c_path = Path(args.v17c)
    v17b_path = Path(args.v17b)
    if not v17c_path.exists():
        logger.error(f"v17c database not found: {v17c_path}")
        sys.exit(1)
    if not v17b_path.exists():
        logger.error(f"v17b database not found: {v17b_path}")
        sys.exit(1)

    t0 = time.time()

    # Open connections
    con_c = duckdb.connect(str(v17c_path), read_only=args.dry_run)
    con_c.execute("INSTALL spatial; LOAD spatial;")
    con_b = duckdb.connect(str(v17b_path), read_only=True)
    con_b.execute("INSTALL spatial; LOAD spatial;")

    # Identify self-intersecting reaches
    logger.info("Identifying self-intersecting reaches in v17c...")
    v17c_bad = con_c.execute("""
        SELECT reach_id FROM reaches
        WHERE geom IS NOT NULL AND ST_IsSimple(geom) = FALSE
        ORDER BY reach_id
    """).fetchall()
    v17c_ids = set(r[0] for r in v17c_bad)
    logger.info(f"Found {len(v17c_ids)} self-intersecting reaches in v17c")

    logger.info("Identifying self-intersecting reaches in v17b...")
    v17b_bad = con_b.execute("""
        SELECT reach_id FROM reaches
        WHERE geom IS NOT NULL AND ST_IsSimple(geom) = FALSE
        ORDER BY reach_id
    """).fetchall()
    v17b_ids = set(r[0] for r in v17b_bad)
    logger.info(f"Found {len(v17b_ids)} self-intersecting reaches in v17b")

    inherited_ids = sorted(v17c_ids & v17b_ids)
    introduced_ids = sorted(v17c_ids - v17b_ids)
    logger.info(f"v17c-introduced: {len(introduced_ids)}")
    logger.info(f"v17b-inherited:  {len(inherited_ids)}")

    # Fix v17c-introduced
    logger.info("--- Fixing v17c-introduced self-intersections ---")
    intro_fixed, intro_unfixed = fix_v17c_introduced(con_c, con_b, introduced_ids)
    logger.info(
        f"v17c-introduced: {len(intro_fixed)} fixed, {len(intro_unfixed)} unfixed"
    )
    if intro_unfixed:
        logger.warning(f"Unfixed v17c-introduced: {intro_unfixed}")

    # Fix v17b-inherited
    logger.info("--- Fixing v17b-inherited self-intersections ---")
    inher_fixed, inher_unfixed = fix_v17b_inherited(con_c, inherited_ids)
    logger.info(
        f"v17b-inherited: {len(inher_fixed)} fixed, {len(inher_unfixed)} unfixed"
    )
    if inher_unfixed:
        logger.warning(f"Unfixed v17b-inherited: {inher_unfixed}")

    all_fixes = intro_fixed + inher_fixed

    # Apply fixes
    if args.dry_run:
        logger.info(f"DRY RUN: Would fix {len(all_fixes)} reaches total")
    else:
        # RTREE pattern: drop indexes before UPDATE
        rtree_indexes = con_c.execute(
            "SELECT index_name, table_name, sql FROM duckdb_indexes() "
            "WHERE sql LIKE '%RTREE%' AND table_name = 'reaches'"
        ).fetchall()
        for idx_name, _, _ in rtree_indexes:
            logger.info(f"Dropping RTREE index: {idx_name}")
            con_c.execute(f'DROP INDEX "{idx_name}"')

        logger.info(f"Applying {len(all_fixes)} geometry fixes...")
        n_updated = apply_fixes(con_c, all_fixes)
        logger.info(f"Updated {n_updated} reaches")

        # Recreate RTREE indexes
        for idx_name, _, create_sql in rtree_indexes:
            logger.info(f"Recreating RTREE index: {idx_name}")
            con_c.execute(create_sql)

        # Verify
        remaining = con_c.execute("""
            SELECT COUNT(*) FROM reaches
            WHERE geom IS NOT NULL AND ST_IsSimple(geom) = FALSE
        """).fetchone()[0]
        logger.info(f"Verification: {remaining} self-intersecting reaches remaining")

    # Summary
    elapsed = time.time() - t0
    print()
    print("=" * 60)
    print("G004 Self-Intersection Fix Summary")
    print("=" * 60)
    print(
        f"v17c-introduced:  {len(intro_fixed):>4} fixed / {len(introduced_ids):>4} total"
    )
    print(
        f"v17b-inherited:   {len(inher_fixed):>4} fixed / {len(inherited_ids):>4} total"
    )
    print(f"TOTAL:            {len(all_fixes):>4} fixed / {len(v17c_ids):>4} total")
    if intro_unfixed:
        print(f"Unfixed (introduced): {intro_unfixed}")
    if inher_unfixed:
        print(f"Unfixed (inherited):  {inher_unfixed}")
    if not args.dry_run:
        print(f"Remaining after fix:  {remaining}")
    print(f"Elapsed: {elapsed:.1f}s")
    print("=" * 60)

    con_b.close()
    con_c.close()


if __name__ == "__main__":
    main()
