#!/usr/bin/env python3
"""Export a slim DuckDB for the Cloud Run reviewer.

Only includes:
  - reaches (16 columns the reviewer actually queries)
  - reach_topology (full)
  - lint_fix_log (created empty if missing)

Usage:
    python export_slim.py                          # default paths
    python export_slim.py --source path/to/v17c.duckdb --out slim.duckdb
"""
import argparse
import duckdb
import os

REACH_COLS = [
    "reach_id", "region", "facc", "width", "river_name", "x", "y",
    "lakeflag", "type", "slope", "facc_quality", "edit_flag",
    "n_rch_up", "n_rch_down", "reach_length", "network",
    "dist_out", "path_freq", "end_reach", "geom",
]


def export(source: str, out: str):
    if os.path.exists(out):
        os.remove(out)

    dst = duckdb.connect(out)
    dst.execute("INSTALL spatial; LOAD spatial;")
    dst.execute(f"ATTACH '{source}' AS src (READ_ONLY)")

    # reaches — only needed columns (simplify geom to ~50m tolerance)
    non_geom = [c for c in REACH_COLS if c != "geom"]
    cols = ", ".join(non_geom) + ", ST_Simplify(geom, 0.0005) AS geom"
    print(f"Exporting reaches ({len(REACH_COLS)} cols, simplified geom)...")
    dst.execute(f"CREATE TABLE reaches AS SELECT {cols} FROM src.reaches")
    cnt = dst.execute("SELECT COUNT(*) FROM reaches").fetchone()[0]
    print(f"  {cnt:,} reaches")

    # reach_topology — full table
    print("Exporting reach_topology...")
    dst.execute("CREATE TABLE reach_topology AS SELECT * FROM src.reach_topology")
    cnt = dst.execute("SELECT COUNT(*) FROM reach_topology").fetchone()[0]
    print(f"  {cnt:,} topology edges")

    # lint_fix_log — copy if exists, else create empty
    print("Exporting lint_fix_log...")
    try:
        dst.execute("CREATE TABLE lint_fix_log AS SELECT * FROM src.lint_fix_log")
        cnt = dst.execute("SELECT COUNT(*) FROM lint_fix_log").fetchone()[0]
        print(f"  {cnt:,} fix log entries")
    except Exception:
        dst.execute("""
            CREATE TABLE lint_fix_log (
                fix_id INTEGER PRIMARY KEY,
                check_id VARCHAR,
                reach_id BIGINT,
                region VARCHAR,
                action VARCHAR,
                column_changed VARCHAR,
                old_value VARCHAR,
                new_value VARCHAR,
                notes VARCHAR,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                undone BOOLEAN DEFAULT FALSE
            )
        """)
        print("  Created empty lint_fix_log")

    dst.execute("DETACH src")

    # Add indexes for reviewer performance
    print("Adding indexes...")
    dst.execute("CREATE INDEX idx_reaches_region ON reaches(region)")
    dst.execute("CREATE INDEX idx_reaches_pk ON reaches(reach_id, region)")
    dst.execute("CREATE INDEX idx_topo_reach ON reach_topology(reach_id, region)")
    dst.execute("CREATE INDEX idx_topo_neighbor ON reach_topology(neighbor_reach_id, region)")
    dst.execute("CREATE INDEX idx_fixlog_region ON lint_fix_log(region, check_id)")

    dst.close()

    size_mb = os.path.getsize(out) / 1024 / 1024
    print(f"\nDone: {out} ({size_mb:.0f} MB)")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--source", default="../../data/duckdb/sword_v17c.duckdb")
    p.add_argument("--out", default="sword_reviewer.duckdb")
    args = p.parse_args()
    export(args.source, args.out)
