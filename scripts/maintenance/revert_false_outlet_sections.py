"""Revert topology for sections that created false outlets.

Flow correction flipped 15 sections (10 oscillating + 5 LOW confidence) whose
boundary junctions lost all downstream connections, becoming false outlets.
This script reverts their topology to v17b state and updates derived columns.

Usage:
    uv run python scripts/maintenance/revert_false_outlet_sections.py --dry-run
    uv run python scripts/maintenance/revert_false_outlet_sections.py
"""

import argparse
import json
import sys

import duckdb

# Sections to revert: 10 oscillating + 5 LOW + 8 MEDIUM/HIGH (all create false outlets)
REVERT_SECTIONS = {
    # Oscillating (2+ flips, ambiguous WSE evidence)
    823, 1436, 1671, 1834, 1891, 2268, 4131, 4835, 4961, 6142,
    # LOW confidence (single flip, weak signal)
    1399, 1999, 5055, 9626, 12690,
    # MEDIUM/HIGH but still create false outlets (boundary junction loses all downstream)
    14759, 9307, 8924, 2132, 5608, 2965, 16507, 7644,
}

# Boundary junction reaches (false outlets) from SYNC batch
BOUNDARY_JUNCTIONS = {
    # Oscillating
    53110600311, 12228400013, 73114000681, 91259000355,
    72588000023, 84209100173, 83110800271, 71382000513,
    71224300353, 61309001475,
    # LOW
    32268900241, 34100003161, 35417400181, 33129500143, 28364000011,
    # MEDIUM/HIGH
    33125000631, 44403900233, 34100002071, 29461000381,
    32214001531, 31298000013, 43427000591, 32216200473,
}


def get_section_reach_ids(v17c: duckdb.DuckDBPyConnection) -> set[int]:
    """Get all reach IDs from the interior of sections to revert."""
    rows = v17c.execute(
        """
        SELECT reach_ids_flipped FROM v17c_flow_corrections
        WHERE section_id = ANY(?)
          AND action = 'flip' AND n_reaches_flipped > 0
    """,
        [list(REVERT_SECTIONS)],
    ).fetchall()
    ids = set()
    for (rids_json,) in rows:
        ids.update(json.loads(rids_json))
    return ids


def revert_topology(
    v17c: duckdb.DuckDBPyConnection,
    v17b: duckdb.DuckDBPyConnection,
    reach_ids: set[int],
    dry_run: bool,
) -> None:
    """Replace v17c topology entries with v17b for given reaches."""
    id_list = list(reach_ids)
    id_csv = ",".join(str(r) for r in id_list)

    # Read v17b topology for these reaches
    # v17b has no topology_suspect/topology_approved columns — add defaults
    v17b_topo = v17b.execute(
        f"""
        SELECT reach_id, region, direction, neighbor_rank,
               neighbor_reach_id,
               FALSE AS topology_suspect,
               FALSE AS topology_approved
        FROM reach_topology
        WHERE reach_id IN ({id_csv})
    """
    ).fetchdf()

    # Read v17c topology for comparison
    v17c_topo = v17c.execute(
        f"""
        SELECT reach_id, direction, neighbor_reach_id
        FROM reach_topology
        WHERE reach_id IN ({id_csv})
    """
    ).fetchdf()

    # Count actual differences
    v17c_set = set(
        zip(v17c_topo["reach_id"], v17c_topo["direction"], v17c_topo["neighbor_reach_id"])
    )
    v17b_set = set(
        zip(v17b_topo["reach_id"], v17b_topo["direction"], v17b_topo["neighbor_reach_id"])
    )
    only_v17c = v17c_set - v17b_set
    only_v17b = v17b_set - v17c_set
    changed = set(r[0] for r in only_v17c) | set(r[0] for r in only_v17b)

    print(f"Topology entries to remove (v17c-only): {len(only_v17c)}")
    print(f"Topology entries to add (v17b-only):    {len(only_v17b)}")
    print(f"Reaches with actual changes:            {len(changed)}")

    if dry_run:
        print("[DRY RUN] No changes made.")
        return

    # Delete v17c entries
    n_before = v17c.execute(
        f"SELECT COUNT(*) FROM reach_topology WHERE reach_id IN ({id_csv})"
    ).fetchone()[0]
    v17c.execute(f"DELETE FROM reach_topology WHERE reach_id IN ({id_csv})")
    print(f"Deleted {n_before} v17c topology entries")

    # Insert v17b entries
    v17c.register("_v17b_topo", v17b_topo)
    v17c.execute("INSERT INTO reach_topology SELECT * FROM _v17b_topo")
    v17c.unregister("_v17b_topo")
    print(f"Inserted {len(v17b_topo)} v17b topology entries")


def update_derived_columns(
    v17c: duckdb.DuckDBPyConnection,
    reach_ids: set[int],
    dry_run: bool,
) -> None:
    """Recompute n_rch_up, n_rch_down, end_reach from reverted topology."""
    id_csv = ",".join(str(r) for r in reach_ids)

    # Compute from topology
    derived = v17c.execute(
        f"""
        SELECT
            r.reach_id,
            COALESCE(u.n_up, 0) AS n_rch_up,
            COALESCE(d.n_dn, 0) AS n_rch_down,
            CASE
                WHEN COALESCE(u.n_up, 0) = 0 AND COALESCE(d.n_dn, 0) = 0 THEN 0
                WHEN COALESCE(u.n_up, 0) = 0 THEN 1
                WHEN COALESCE(d.n_dn, 0) = 0 THEN 2
                WHEN COALESCE(u.n_up, 0) > 1 OR COALESCE(d.n_dn, 0) > 1 THEN 3
                ELSE 0
            END AS end_reach
        FROM reaches r
        LEFT JOIN (
            SELECT reach_id, COUNT(*) AS n_up
            FROM reach_topology
            WHERE direction = 'up' AND reach_id IN ({id_csv})
            GROUP BY reach_id
        ) u ON r.reach_id = u.reach_id
        LEFT JOIN (
            SELECT reach_id, COUNT(*) AS n_dn
            FROM reach_topology
            WHERE direction = 'down' AND reach_id IN ({id_csv})
            GROUP BY reach_id
        ) d ON r.reach_id = d.reach_id
        WHERE r.reach_id IN ({id_csv})
    """
    ).fetchdf()

    print(f"\nDerived column updates for {len(derived)} reaches:")
    print("  end_reach distribution:", derived["end_reach"].value_counts().to_dict())

    if dry_run:
        print("[DRY RUN] No changes made.")
        return

    # RTREE safety: drop indexes, update, recreate
    v17c.execute("INSTALL spatial; LOAD spatial;")
    indexes = v17c.execute(
        "SELECT index_name, table_name, sql FROM duckdb_indexes() "
        "WHERE sql LIKE '%RTREE%' AND table_name = 'reaches'"
    ).fetchall()
    for idx_name, _, _ in indexes:
        v17c.execute(f'DROP INDEX "{idx_name}"')
        print(f"  Dropped RTREE index: {idx_name}")

    try:
        v17c.register("_derived", derived)
        v17c.execute(
            """
            UPDATE reaches SET
                n_rch_up = d.n_rch_up,
                n_rch_down = d.n_rch_down,
                end_reach = d.end_reach
            FROM _derived d
            WHERE reaches.reach_id = d.reach_id
        """
        )
        v17c.unregister("_derived")
        print(f"Updated {len(derived)} reaches")
    finally:
        for idx_name, _, sql in indexes:
            v17c.execute(sql)
            print(f"  Recreated RTREE index: {idx_name}")


def update_rch_id_columns(
    v17c: duckdb.DuckDBPyConnection,
    reach_ids: set[int],
    dry_run: bool,
) -> None:
    """Sync rch_id_up_1..4 / rch_id_dn_1..4 from reach_topology for reverted reaches."""
    id_csv = ",".join(str(r) for r in reach_ids)

    sync_sql = f"""
        WITH ranked AS (
            SELECT reach_id, direction, neighbor_reach_id,
                   ROW_NUMBER() OVER (
                       PARTITION BY reach_id, direction ORDER BY neighbor_rank
                   ) AS rn
            FROM reach_topology
            WHERE reach_id IN ({id_csv})
        ),
        pivoted AS (
            SELECT
                reach_id,
                MAX(CASE WHEN direction='up'   AND rn=1 THEN neighbor_reach_id ELSE 0 END) AS up1,
                MAX(CASE WHEN direction='up'   AND rn=2 THEN neighbor_reach_id ELSE 0 END) AS up2,
                MAX(CASE WHEN direction='up'   AND rn=3 THEN neighbor_reach_id ELSE 0 END) AS up3,
                MAX(CASE WHEN direction='up'   AND rn=4 THEN neighbor_reach_id ELSE 0 END) AS up4,
                MAX(CASE WHEN direction='down' AND rn=1 THEN neighbor_reach_id ELSE 0 END) AS dn1,
                MAX(CASE WHEN direction='down' AND rn=2 THEN neighbor_reach_id ELSE 0 END) AS dn2,
                MAX(CASE WHEN direction='down' AND rn=3 THEN neighbor_reach_id ELSE 0 END) AS dn3,
                MAX(CASE WHEN direction='down' AND rn=4 THEN neighbor_reach_id ELSE 0 END) AS dn4
            FROM ranked
            GROUP BY reach_id
        )
        SELECT * FROM pivoted
    """
    pivoted = v17c.execute(sync_sql).fetchdf()

    if dry_run:
        print(f"[DRY RUN] Would sync rch_id_up/dn_1..4 for {len(pivoted)} reaches")
        return

    v17c.register("_pivoted", pivoted)
    v17c.execute(
        """
        UPDATE reaches SET
            rch_id_up_1 = p.up1, rch_id_up_2 = p.up2,
            rch_id_up_3 = p.up3, rch_id_up_4 = p.up4,
            rch_id_dn_1 = p.dn1, rch_id_dn_2 = p.dn2,
            rch_id_dn_3 = p.dn3, rch_id_dn_4 = p.dn4
        FROM _pivoted p
        WHERE reaches.reach_id = p.reach_id
    """
    )
    v17c.unregister("_pivoted")
    print(f"Synced rch_id_up/dn_1..4 for {len(pivoted)} reaches")


def verify(v17c: duckdb.DuckDBPyConnection, v17b: duckdb.DuckDBPyConnection, reach_ids: set[int]) -> bool:
    """Verify reverted topology matches v17b."""
    id_csv = ",".join(str(r) for r in reach_ids)

    v17c_topo = v17c.execute(
        f"SELECT reach_id, direction, neighbor_reach_id FROM reach_topology WHERE reach_id IN ({id_csv})"
    ).fetchdf()
    v17b_topo = v17b.execute(
        f"SELECT reach_id, direction, neighbor_reach_id FROM reach_topology WHERE reach_id IN ({id_csv})"
    ).fetchdf()

    v17c_set = set(zip(v17c_topo["reach_id"], v17c_topo["direction"], v17c_topo["neighbor_reach_id"]))
    v17b_set = set(zip(v17b_topo["reach_id"], v17b_topo["direction"], v17b_topo["neighbor_reach_id"]))

    if v17c_set == v17b_set:
        print("PASS: All 74 reaches match v17b topology")
        return True
    else:
        diff = v17c_set.symmetric_difference(v17b_set)
        print(f"FAIL: {len(diff)} topology entries still differ")
        return False


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="Show what would change without modifying DB")
    parser.add_argument("--v17c", default="data/duckdb/sword_v17c.duckdb", help="Path to v17c database")
    parser.add_argument("--v17b", default="data/duckdb/sword_v17b.duckdb", help="Path to v17b database")
    args = parser.parse_args()

    v17b = duckdb.connect(args.v17b, read_only=True)
    v17c = duckdb.connect(args.v17c, read_only=args.dry_run)

    # Collect all reach IDs
    section_ids = get_section_reach_ids(v17c)
    all_ids = section_ids | BOUNDARY_JUNCTIONS
    print(f"Section interior reaches: {len(section_ids)}")
    print(f"Boundary junction reaches: {len(BOUNDARY_JUNCTIONS)}")
    print(f"Total reaches to revert: {len(all_ids)}")

    # Step 1: Revert topology
    print("\n--- Step 1: Revert topology ---")
    revert_topology(v17c, v17b, all_ids, args.dry_run)

    # Step 2: Update derived columns
    print("\n--- Step 2: Update n_rch_up, n_rch_down, end_reach ---")
    update_derived_columns(v17c, all_ids, args.dry_run)

    # Step 3: Sync rch_id_up/dn columns
    print("\n--- Step 3: Sync rch_id_up/dn_1..4 ---")
    update_rch_id_columns(v17c, all_ids, args.dry_run)

    # Step 4: Verify
    if not args.dry_run:
        print("\n--- Step 4: Verify ---")
        ok = verify(v17c, v17b, all_ids)
        if not ok:
            print("ERROR: Verification failed!", file=sys.stderr)
            sys.exit(1)

    # Check false outlets resolved
    false_outlet_check = v17c.execute(
        f"""
        SELECT reach_id, end_reach, n_rch_up, n_rch_down
        FROM reaches
        WHERE reach_id IN ({','.join(str(r) for r in BOUNDARY_JUNCTIONS)})
        ORDER BY reach_id
    """
    ).fetchdf()
    still_outlets = false_outlet_check[false_outlet_check["end_reach"] == 2]
    print(f"\nBoundary junctions still outlet (end_reach=2): {len(still_outlets)}")
    if len(still_outlets) > 0:
        print(still_outlets.to_string())

    v17c.close()
    v17b.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
