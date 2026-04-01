"""Fix the 2 remaining F006 (facc junction conservation) violations.

F006 checks that at junctions (n_upstream >= 2), downstream facc >= sum(upstream facc).
Two reaches violate this:
  - 53130100215 (OC): facc=555.00, sum_upstream=806.87, deficit=251.87 km²
  - 45311901585 (AS): facc=1865.37, sum_upstream=2669.99, deficit=804.62 km²

Both are type=5 (unreliable). Fix: set downstream facc = sum(upstream facc).
"""

import duckdb


def main():
    db_path = "data/duckdb/sword_v17c.duckdb"
    con = duckdb.connect(db_path, read_only=False)
    con.execute("INSTALL spatial; LOAD spatial;")

    # Drop RTREE indexes
    indexes = con.execute("""
        SELECT index_name, table_name, sql
        FROM duckdb_indexes()
        WHERE sql LIKE '%RTREE%' AND database_name = 'sword_v17c'
    """).fetchall()
    print(f"Dropping {len(indexes)} RTREE indexes...")
    for idx_name, _tbl, _sql in indexes:
        con.execute(f'DROP INDEX "{idx_name}"')

    try:
        fixes = [
            (53130100215, "OC", [53130100225, 53130101065]),
            (45311901585, "AS", [45311901625, 45311901595]),
        ]

        for reach_id, region, upstream_ids in fixes:
            # Fetch current downstream facc
            old_facc = con.execute(
                "SELECT facc FROM reaches WHERE reach_id = ?", [reach_id]
            ).fetchone()[0]

            # Compute sum of upstream facc
            placeholders = ",".join("?" * len(upstream_ids))
            new_facc = con.execute(
                f"SELECT SUM(facc) FROM reaches WHERE reach_id IN ({placeholders})",
                upstream_ids,
            ).fetchone()[0]

            print(f"\n{region} reach {reach_id}: facc {old_facc:.4f} -> {new_facc:.4f}")
            print(f"  deficit was {new_facc - old_facc:.2f} km²")

            # Update reaches
            con.execute(
                "UPDATE reaches SET facc = ? WHERE reach_id = ?", [new_facc, reach_id]
            )

            # Update nodes
            n_nodes = con.execute(
                "SELECT COUNT(*) FROM nodes WHERE reach_id = ?", [reach_id]
            ).fetchone()[0]
            con.execute(
                "UPDATE nodes SET facc = ? WHERE reach_id = ?", [new_facc, reach_id]
            )
            print(f"  updated {n_nodes} nodes")

            # Tag edit_flag
            con.execute(
                """
                UPDATE reaches
                SET edit_flag = CASE
                    WHEN edit_flag IS NULL OR edit_flag = '' THEN 'facc_f006_fix'
                    ELSE edit_flag || ',facc_f006_fix'
                END
                WHERE reach_id = ?
                """,
                [reach_id],
            )

        # Verify F006 = 0
        print("\n=== Verifying F006 ===")
        violations = con.execute("""
            WITH upstream_facc AS (
                SELECT rt.reach_id, rt.region,
                       SUM(r_up.facc) as upstream_facc_sum,
                       COUNT(*) as n_upstream
                FROM reach_topology rt
                JOIN reaches r_up ON rt.neighbor_reach_id = r_up.reach_id
                    AND rt.region = r_up.region
                WHERE rt.direction = 'up'
                  AND r_up.facc > 0 AND r_up.facc != -9999
                GROUP BY rt.reach_id, rt.region
            )
            SELECT r.reach_id, r.region, r.facc, uf.upstream_facc_sum,
                   uf.upstream_facc_sum - r.facc as conservation_deficit
            FROM reaches r
            JOIN upstream_facc uf ON r.reach_id = uf.reach_id AND r.region = uf.region
            WHERE r.facc > 0 AND r.facc != -9999
              AND uf.n_upstream >= 2
              AND r.facc < uf.upstream_facc_sum
              AND (uf.upstream_facc_sum - r.facc) > 1.0
            ORDER BY (uf.upstream_facc_sum - r.facc) DESC
        """).fetchdf()
        print(f"F006 violations remaining: {len(violations)}")
        if len(violations) > 0:
            print(violations.to_string())
        else:
            print("PASS - 0 violations!")

        # Show final state
        print("\n=== Final state ===")
        for reach_id, _, _ in fixes:
            r = con.execute(
                "SELECT reach_id, region, facc, edit_flag FROM reaches WHERE reach_id = ?",
                [reach_id],
            ).fetchdf()
            print(r.to_string())

    finally:
        # Always recreate RTREE indexes
        print("\nRecreating RTREE indexes...")
        for idx_name, _tbl, sql in indexes:
            con.execute(sql)
        print("Done.")

    con.close()


if __name__ == "__main__":
    main()
