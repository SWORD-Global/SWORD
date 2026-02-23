#!/usr/bin/env python3
"""Revert the incomplete break_reaches() split of OC reach 51111300061.

Background:
    An ad-hoc break_reaches() call split reach 51111300061 (OC, 19.7km, 586 CLs)
    into 4 segments. Only 2 of 4 reach records were created. 434 centerlines and
    73 nodes are orphaned. The geometry on 51111300061 is stale (covers full
    19.7km but the reach only owns 81 CLs / 2.7km).

    This script reverts to v17b state for this reach while preserving v17c-only
    columns on the reach record and SWOT percentile data on the 14 nodes that
    have it.

Usage:
    python scripts/maintenance/revert_oc_reach_split.py --dry-run
    python scripts/maintenance/revert_oc_reach_split.py --execute
    python scripts/maintenance/revert_oc_reach_split.py --verify
"""

import argparse
import sys

import duckdb

V17B_PATH = "data/duckdb/sword_v17b.duckdb"
V17C_PATH = "data/duckdb/sword_v17c.duckdb"

ORIGINAL_REACH = 51111300061
SPURIOUS_REACH = 51111300391
ORPHAN_REACHES = [51111300371, 51111300381]
ALL_SPLIT_REACHES = [SPURIOUS_REACH] + ORPHAN_REACHES
REGION = "OC"

# Neighbors whose n_rch_up/n_rch_down were bumped by the Feb 7 topology fix
NEIGHBOR_051 = 51111300051  # n_rch_up: 1 -> 2
NEIGHBOR_071 = 51111300071  # n_rch_down: 1 -> 2


def precondition_checks(
    con_c: duckdb.DuckDBPyConnection, con_b: duckdb.DuckDBPyConnection
):
    """Verify the database is in the expected broken state before proceeding."""
    errors = []

    # 1. Reach 51111300391 exists in v17c
    n = con_c.execute(
        f"SELECT COUNT(*) FROM reaches WHERE reach_id = {SPURIOUS_REACH}"
    ).fetchone()[0]
    if n != 1:
        errors.append(f"Expected reach {SPURIOUS_REACH} to exist in v17c, found {n}")

    # 2. Orphan reaches do NOT exist in v17c reaches table
    for rid in ORPHAN_REACHES:
        n = con_c.execute(
            f"SELECT COUNT(*) FROM reaches WHERE reach_id = {rid}"
        ).fetchone()[0]
        if n != 0:
            errors.append(f"Expected reach {rid} NOT in v17c reaches, found {n}")

    # 3. Orphan centerlines exist
    for rid in ORPHAN_REACHES:
        n = con_c.execute(
            f"SELECT COUNT(*) FROM centerlines WHERE reach_id = {rid} AND region = '{REGION}'"
        ).fetchone()[0]
        if n == 0:
            errors.append(f"Expected orphan CLs for {rid}, found 0")

    # 4. v17b has the clean state
    n = con_b.execute(
        f"SELECT COUNT(*) FROM centerlines WHERE reach_id = {ORIGINAL_REACH} AND region = '{REGION}'"
    ).fetchone()[0]
    if n != 586:
        errors.append(f"Expected 586 v17b CLs for {ORIGINAL_REACH}, found {n}")

    n = con_b.execute(
        f"SELECT COUNT(*) FROM nodes WHERE reach_id = {ORIGINAL_REACH} AND region = '{REGION}'"
    ).fetchone()[0]
    if n != 99:
        errors.append(f"Expected 99 v17b nodes for {ORIGINAL_REACH}, found {n}")

    return errors


def dry_run(con_c: duckdb.DuckDBPyConnection, con_b: duckdb.DuckDBPyConnection):
    """Show what the revert would do without changing anything."""
    errors = precondition_checks(con_c, con_b)
    if errors:
        print("PRECONDITION FAILURES:")
        for e in errors:
            print(f"  - {e}")
        return False

    # Count what will be deleted/restored
    orphan_cls = con_c.execute(
        f"SELECT COUNT(*) FROM centerlines WHERE reach_id IN ({','.join(str(r) for r in ALL_SPLIT_REACHES)}) AND region = '{REGION}'"
    ).fetchone()[0]
    orphan_nodes = con_c.execute(
        f"SELECT COUNT(*) FROM nodes WHERE reach_id IN ({','.join(str(r) for r in ALL_SPLIT_REACHES)}) AND region = '{REGION}'"
    ).fetchone()[0]
    current_cls = con_c.execute(
        f"SELECT COUNT(*) FROM centerlines WHERE reach_id = {ORIGINAL_REACH} AND region = '{REGION}'"
    ).fetchone()[0]
    current_nodes = con_c.execute(
        f"SELECT COUNT(*) FROM nodes WHERE reach_id = {ORIGINAL_REACH} AND region = '{REGION}'"
    ).fetchone()[0]
    topo_entries = con_c.execute(
        f"SELECT COUNT(*) FROM reach_topology WHERE reach_id = {SPURIOUS_REACH} OR neighbor_reach_id = {SPURIOUS_REACH}"
    ).fetchone()[0]

    print("DRY RUN — Changes that would be made:")
    print(f"  DELETE {orphan_cls} orphan centerlines (reaches {ALL_SPLIT_REACHES})")
    print(f"  DELETE {orphan_nodes} orphan nodes (reaches {ALL_SPLIT_REACHES})")
    print(
        f"  DELETE {current_cls} current CLs for {ORIGINAL_REACH}, replace with 586 from v17b"
    )
    print(
        f"  DELETE {current_nodes} current nodes for {ORIGINAL_REACH}, insert 85 missing from v17b"
    )
    print(f"  DELETE reach record for {SPURIOUS_REACH}")
    print(f"  DELETE {topo_entries} topology entries for {SPURIOUS_REACH}")
    print(f"  UPDATE n_rch_up on {NEIGHBOR_051}: 2 -> 1")
    print(f"  UPDATE n_rch_down on {NEIGHBOR_071}: 2 -> 1")
    print(
        f"  UPDATE reach {ORIGINAL_REACH} metadata from v17b (x, y, bbox, reach_length, n_nodes)"
    )
    print(f"  REBUILD geometry for {ORIGINAL_REACH} from restored nodes")
    print("\nPreconditions: PASS")
    return True


def execute(con_c: duckdb.DuckDBPyConnection, con_b: duckdb.DuckDBPyConnection):
    """Execute the revert.

    Uses ATTACH to read v17b geometry columns directly (avoids BLOB->GEOMETRY
    cast failure when round-tripping through pandas DataFrames).
    """
    errors = precondition_checks(con_c, con_b)
    if errors:
        print("PRECONDITION FAILURES — aborting:")
        for e in errors:
            print(f"  - {e}")
        return False

    con_c.execute("INSTALL spatial; LOAD spatial;")

    # --- Step 1: Handle RTREE indexes (before attaching v17b) ---
    print("Step 1: Drop RTREE indexes...")
    indexes = con_c.execute(
        "SELECT index_name, table_name, sql FROM duckdb_indexes() WHERE sql LIKE '%RTREE%'"
    ).fetchall()
    for idx_name, _tbl, _sql in indexes:
        con_c.execute(f'DROP INDEX "{idx_name}"')
    print(f"  Dropped {len(indexes)} RTREE indexes")

    # Attach v17b for direct cross-DB queries (avoids BLOB->GEOMETRY issue)
    con_c.execute(f"ATTACH '{V17B_PATH}' AS v17b (READ_ONLY)")

    # --- Step 2: Delete orphan centerlines and nodes ---
    split_ids = ",".join(str(r) for r in ALL_SPLIT_REACHES)
    print("Step 2: Delete orphan data...")

    n = con_c.execute(
        f"DELETE FROM centerlines WHERE reach_id IN ({split_ids}) AND region = '{REGION}'"
    ).fetchone()[0]
    print(f"  Deleted {n} orphan centerlines")

    n = con_c.execute(
        f"DELETE FROM nodes WHERE reach_id IN ({split_ids}) AND region = '{REGION}'"
    ).fetchone()[0]
    print(f"  Deleted {n} orphan nodes")

    # --- Step 3: Restore centerlines from v17b ---
    print("Step 3: Restore centerlines from v17b...")
    n = con_c.execute(
        f"DELETE FROM centerlines WHERE reach_id = {ORIGINAL_REACH} AND region = '{REGION}'"
    ).fetchone()[0]
    print(f"  Deleted {n} current v17c centerlines")

    v17b_cl_cols = [r[0] for r in con_c.execute("DESCRIBE v17b.centerlines").fetchall()]
    v17c_cl_cols = [r[0] for r in con_c.execute("DESCRIBE centerlines").fetchall()]
    shared_cl_cols = [c for c in v17b_cl_cols if c in v17c_cl_cols]
    cols_str = ", ".join(shared_cl_cols)

    n = con_c.execute(f"""
        INSERT INTO centerlines ({cols_str})
        SELECT {cols_str} FROM v17b.centerlines
        WHERE reach_id = {ORIGINAL_REACH} AND region = '{REGION}'
    """).fetchone()[0]
    print(f"  Inserted {n} v17b centerlines")

    # --- Step 4: Restore missing nodes from v17b ---
    print("Step 4: Restore missing nodes from v17b...")

    existing_nids = (
        con_c.execute(
            f"SELECT node_id FROM nodes WHERE reach_id = {ORIGINAL_REACH} AND region = '{REGION}'"
        )
        .fetchdf()["node_id"]
        .tolist()
    )
    print(f"  Keeping {len(existing_nids)} existing nodes (have SWOT data)")

    v17b_node_cols = [r[0] for r in con_c.execute("DESCRIBE v17b.nodes").fetchall()]
    v17c_node_cols = [r[0] for r in con_c.execute("DESCRIBE nodes").fetchall()]
    shared_node_cols = [c for c in v17b_node_cols if c in v17c_node_cols]
    node_cols_str = ", ".join(shared_node_cols)

    nid_list = ",".join(str(nid) for nid in existing_nids)
    n = con_c.execute(f"""
        INSERT INTO nodes ({node_cols_str})
        SELECT {node_cols_str} FROM v17b.nodes
        WHERE reach_id = {ORIGINAL_REACH} AND region = '{REGION}'
          AND node_id NOT IN ({nid_list})
    """).fetchone()[0]
    print(f"  Inserted {n} missing v17b nodes")

    # --- Step 5: Delete spurious reach ---
    print("Step 5: Delete spurious reach and topology...")

    for tbl in ["reach_swot_orbits", "reach_ice_flags"]:
        try:
            n = con_c.execute(
                f"DELETE FROM {tbl} WHERE reach_id = {SPURIOUS_REACH}"
            ).fetchone()[0]
            if n > 0:
                print(f"  Deleted {n} rows from {tbl}")
        except Exception:
            pass

    n = con_c.execute(
        f"DELETE FROM reach_topology WHERE reach_id = {SPURIOUS_REACH} OR neighbor_reach_id = {SPURIOUS_REACH}"
    ).fetchone()[0]
    print(f"  Deleted {n} topology entries for {SPURIOUS_REACH}")

    n = con_c.execute(
        f"DELETE FROM reaches WHERE reach_id = {SPURIOUS_REACH}"
    ).fetchone()[0]
    print(f"  Deleted {n} reach record")

    # --- Step 6: Restore neighbor counts ---
    print("Step 6: Restore neighbor counts...")
    con_c.execute(f"UPDATE reaches SET n_rch_up = 1 WHERE reach_id = {NEIGHBOR_051}")
    con_c.execute(f"UPDATE reaches SET n_rch_down = 1 WHERE reach_id = {NEIGHBOR_071}")
    print(f"  {NEIGHBOR_051}: n_rch_up -> 1")
    print(f"  {NEIGHBOR_071}: n_rch_down -> 1")

    # --- Step 7: Restore reach metadata from v17b ---
    print("Step 7: Restore reach metadata...")
    con_c.execute(f"""
        UPDATE reaches SET
            x = v.x, y = v.y,
            x_min = v.x_min, x_max = v.x_max,
            y_min = v.y_min, y_max = v.y_max,
            reach_length = v.reach_length, n_nodes = v.n_nodes
        FROM v17b.reaches v
        WHERE reaches.reach_id = {ORIGINAL_REACH} AND v.reach_id = {ORIGINAL_REACH}
    """)
    print("  Updated x, y, bbox, reach_length, n_nodes from v17b")

    # --- Step 8: Rebuild geometry from nodes ---
    print("Step 8: Rebuild geometry...")
    con_c.execute(f"""
        UPDATE reaches SET geom = (
            SELECT ST_MakeLine(LIST(ST_Point(n.x, n.y) ORDER BY n.dist_out DESC))
            FROM nodes n
            WHERE n.reach_id = {ORIGINAL_REACH} AND n.region = '{REGION}'
        )
        WHERE reach_id = {ORIGINAL_REACH}
    """)
    n_pts = con_c.execute(
        f"SELECT ST_NPoints(geom) FROM reaches WHERE reach_id = {ORIGINAL_REACH}"
    ).fetchone()[0]
    print(f"  Rebuilt geometry with {n_pts} points")

    # --- Step 9: Recreate RTREE indexes ---
    print("Step 9: Recreate RTREE indexes...")
    con_c.execute("DETACH v17b")
    for idx_name, _tbl, sql in indexes:
        con_c.execute(sql)
    print(f"  Recreated {len(indexes)} RTREE indexes")

    print("\nRevert complete.")
    return True


def verify(con_c: duckdb.DuckDBPyConnection):
    """Verify the revert was successful."""
    print("Verification:")
    errors = []

    # 1. Reach 51111300391 should NOT exist
    n = con_c.execute(
        f"SELECT COUNT(*) FROM reaches WHERE reach_id = {SPURIOUS_REACH}"
    ).fetchone()[0]
    status = "PASS" if n == 0 else "FAIL"
    print(f"  [{status}] Spurious reach {SPURIOUS_REACH} deleted: {n} rows")
    if n != 0:
        errors.append("spurious reach still exists")

    # 2. No orphan centerlines
    n = con_c.execute(
        "SELECT COUNT(*) FROM centerlines c LEFT JOIN reaches r ON c.reach_id = r.reach_id WHERE r.reach_id IS NULL"
    ).fetchone()[0]
    status = "PASS" if n == 0 else "FAIL"
    print(f"  [{status}] Orphan centerlines: {n}")
    if n != 0:
        errors.append(f"{n} orphan centerlines remain")

    # 3. No orphan nodes
    n = con_c.execute(
        "SELECT COUNT(*) FROM nodes n LEFT JOIN reaches r ON n.reach_id = r.reach_id WHERE r.reach_id IS NULL"
    ).fetchone()[0]
    status = "PASS" if n == 0 else "FAIL"
    print(f"  [{status}] Orphan nodes: {n}")
    if n != 0:
        errors.append(f"{n} orphan nodes remain")

    # 4. Reach 51111300061 has 586 CLs
    n = con_c.execute(
        f"SELECT COUNT(*) FROM centerlines WHERE reach_id = {ORIGINAL_REACH} AND region = '{REGION}'"
    ).fetchone()[0]
    status = "PASS" if n == 586 else "FAIL"
    print(f"  [{status}] CLs for {ORIGINAL_REACH}: {n} (expected 586)")
    if n != 586:
        errors.append(f"expected 586 CLs, got {n}")

    # 5. Reach 51111300061 has 99 nodes
    n = con_c.execute(
        f"SELECT COUNT(*) FROM nodes WHERE reach_id = {ORIGINAL_REACH} AND region = '{REGION}'"
    ).fetchone()[0]
    status = "PASS" if n == 99 else "FAIL"
    print(f"  [{status}] Nodes for {ORIGINAL_REACH}: {n} (expected 99)")
    if n != 99:
        errors.append(f"expected 99 nodes, got {n}")

    # 6. reach_length restored
    rl = con_c.execute(
        f"SELECT reach_length FROM reaches WHERE reach_id = {ORIGINAL_REACH}"
    ).fetchone()[0]
    status = "PASS" if abs(rl - 19703.04) < 1 else "FAIL"
    print(f"  [{status}] reach_length: {rl:.1f} (expected ~19703)")
    if abs(rl - 19703.04) > 1:
        errors.append(f"reach_length wrong: {rl}")

    # 7. Geometry point count reasonable
    con_c.execute("INSTALL spatial; LOAD spatial;")
    n_pts = con_c.execute(
        f"SELECT ST_NPoints(geom) FROM reaches WHERE reach_id = {ORIGINAL_REACH}"
    ).fetchone()[0]
    status = "PASS" if n_pts == 99 else "WARN"
    print(f"  [{status}] Geometry points: {n_pts} (expected 99 from nodes)")

    # 8. C005 check — centroid distance
    dist = con_c.execute(f"""
        WITH cl_centroid AS (
            SELECT AVG(x) as cx, AVG(y) as cy
            FROM centerlines WHERE reach_id = {ORIGINAL_REACH} AND region = '{REGION}'
        )
        SELECT 111000.0 * SQRT(
            POWER((r.x - c.cx) * COS(RADIANS((r.y + c.cy) / 2.0)), 2)
            + POWER(r.y - c.cy, 2)
        )
        FROM reaches r, cl_centroid c
        WHERE r.reach_id = {ORIGINAL_REACH}
    """).fetchone()[0]
    status = "PASS" if dist < 5000 else "FAIL"
    print(f"  [{status}] C005 centroid distance: {dist:.0f}m (threshold: 5000m)")
    if dist >= 5000:
        errors.append(f"C005 still fails: {dist:.0f}m")

    # 9. Neighbor counts
    for rid, col, expected in [
        (NEIGHBOR_051, "n_rch_up", 1),
        (NEIGHBOR_071, "n_rch_down", 1),
    ]:
        val = con_c.execute(
            f"SELECT {col} FROM reaches WHERE reach_id = {rid}"
        ).fetchone()[0]
        status = "PASS" if val == expected else "FAIL"
        print(f"  [{status}] {rid}.{col}: {val} (expected {expected})")
        if val != expected:
            errors.append(f"{rid}.{col} = {val}, expected {expected}")

    # 10. No topology references to spurious reach
    n = con_c.execute(
        f"SELECT COUNT(*) FROM reach_topology WHERE reach_id = {SPURIOUS_REACH} OR neighbor_reach_id = {SPURIOUS_REACH}"
    ).fetchone()[0]
    status = "PASS" if n == 0 else "FAIL"
    print(f"  [{status}] Topology refs to {SPURIOUS_REACH}: {n}")
    if n != 0:
        errors.append(f"topology refs remain for {SPURIOUS_REACH}")

    # 11. SWOT data preserved on first 14 nodes
    n_swot = con_c.execute(f"""
        SELECT COUNT(*) FROM nodes
        WHERE reach_id = {ORIGINAL_REACH} AND region = '{REGION}'
          AND wse_obs_p50 IS NOT NULL
    """).fetchone()[0]
    status = "PASS" if n_swot == 14 else "WARN"
    print(f"  [{status}] Nodes with SWOT data: {n_swot} (expected 14)")

    if errors:
        print(f"\nVERIFICATION FAILED: {len(errors)} issues")
        return False
    print("\nVERIFICATION PASSED")
    return True


def main():
    parser = argparse.ArgumentParser(description="Revert OC reach 51111300061 split")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--dry-run", action="store_true", help="Show what would change")
    group.add_argument("--execute", action="store_true", help="Execute the revert")
    group.add_argument("--verify", action="store_true", help="Verify post-revert state")
    args = parser.parse_args()

    if args.verify:
        con_c = duckdb.connect(V17C_PATH, read_only=True)
        ok = verify(con_c)
        con_c.close()
        sys.exit(0 if ok else 1)

    con_b = duckdb.connect(V17B_PATH, read_only=True)

    if args.dry_run:
        con_c = duckdb.connect(V17C_PATH, read_only=True)
        ok = dry_run(con_c, con_b)
    else:
        con_c = duckdb.connect(V17C_PATH, read_only=False)
        ok = execute(con_c, con_b)

    con_c.close()
    con_b.close()
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
