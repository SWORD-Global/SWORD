#!/usr/bin/env python3
"""Ingest DL-GROD obstruction data into SWORD v17c reaches.

Reads barrier_reach_mapping.csv from the swot_obstructions project (He et al. 2025),
normalizes obstruction types, resolves multi-obstruction reaches by priority, and
bulk-updates reaches.obstr_type and reaches.dl_grod_id.

<<<<<<< HEAD
v17c obstr_type encoding (intentionally extends v17b):
    0 = no obstruction
    1 = dam (GROD / DL-GROD)
    2 = low-head dam (DL-GROD; NOTE: v17b used 2=lock — v17c redefines this slot)
    3 = lock (GROD / DL-GROD; NOTE: v17b used 3=low-perm — v17c redefines this slot)
    4 = waterfall (HydroFALLS; NULL dl_grod_id)
    5 = partial dam (DL-GROD, He et al. 2025)

Priority for reaches with multiple DL-GROD features (highest wins):
    Dam > Lock > Low-head Dam > Partial Dam > Waterfall
=======
Type mapping (DL-GROD → SWORD obstr_type):
    Dam              → 1
    Low-head Dam     → 2
    Lock             → 3
    Waterfall        → 4  (NULL dl_grod_id — HydroFALLS source, not DL-GROD)
    Partial Dam      → 5

Priority for reaches with multiple DL-GROD features (highest wins):
    Dam(1) > Lock(3) > Low-head Dam(2) > Partial Dam(5) > Waterfall(4)
>>>>>>> ad53e4b (feat: add DL-GROD ingestion and obstruction lint checks (#127))

DL-GROD wins over existing GROD assignments (GROD ⊂ DL-GROD).

Usage:
<<<<<<< HEAD
    python scripts/maintenance/ingest_dl_grod.py --mapping /path/to/barrier_reach_mapping.csv
    python scripts/maintenance/ingest_dl_grod.py --mapping /path/to/barrier_reach_mapping.csv --dry-run
    python scripts/maintenance/ingest_dl_grod.py --mapping /path/to/barrier_reach_mapping.csv --db data/duckdb/sword_v17c.duckdb
=======
    python scripts/maintenance/ingest_dl_grod.py
    python scripts/maintenance/ingest_dl_grod.py --dry-run
    python scripts/maintenance/ingest_dl_grod.py --mapping /path/to/barrier_reach_mapping.csv
>>>>>>> ad53e4b (feat: add DL-GROD ingestion and obstruction lint checks (#127))
"""

from __future__ import annotations

import argparse
import logging
<<<<<<< HEAD
import re
=======
>>>>>>> ad53e4b (feat: add DL-GROD ingestion and obstruction lint checks (#127))
import sys
from pathlib import Path

import duckdb
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

<<<<<<< HEAD
DEFAULT_DB = Path("data/duckdb/sword_v17c.duckdb")
=======
DEFAULT_MAPPING = Path(
    "/Users/jakegearon/projects/swot_obstructions/exports/fragmentation/barrier_reach_mapping.csv"
)
DEFAULT_DB = Path("/Users/jakegearon/projects/SWORD/data/duckdb/sword_v17c.duckdb")
>>>>>>> ad53e4b (feat: add DL-GROD ingestion and obstruction lint checks (#127))

# obstr_type values
TYPE_DAM = 1
TYPE_LOW_HEAD = 2
TYPE_LOCK = 3
TYPE_WATERFALL = 4
TYPE_PARTIAL = 5

# Priority order (lower = higher priority)
TYPE_PRIORITY = {
    TYPE_DAM: 0,
    TYPE_LOCK: 1,
    TYPE_LOW_HEAD: 2,
    TYPE_PARTIAL: 3,
    TYPE_WATERFALL: 4,
}

# Normalized type string → obstr_type
_RAW_TO_OBSTR: dict[str, int] = {
    "dam": TYPE_DAM,
    "low-head dam": TYPE_LOW_HEAD,
    "lock": TYPE_LOCK,
    "waterfall": TYPE_WATERFALL,
    "partial dam": TYPE_PARTIAL,
}


def normalize_type(raw: str) -> int | None:
<<<<<<< HEAD
    key = re.sub(r"\s+", " ", raw.strip()).lower()
=======
    key = raw.strip().lower().replace("  ", " ")
>>>>>>> ad53e4b (feat: add DL-GROD ingestion and obstruction lint checks (#127))
    return _RAW_TO_OBSTR.get(key)


def load_mapping(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    required = {"ID", "Type", "reach_id"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns in mapping file: {missing}")

    # Drop unmatched rows (no reach_id)
    df = df[df["reach_id"].notna()].copy()
    df["reach_id"] = df["reach_id"].astype("int64")
    df["ID"] = df["ID"].astype("int64")

    # Normalize type
    df["obstr_type"] = df["Type"].map(normalize_type)
    unknown = df[df["obstr_type"].isna()]["Type"].unique()
    if len(unknown) > 0:
        log.warning("Unknown DL-GROD types (will be skipped): %s", list(unknown))
    df = df[df["obstr_type"].notna()].copy()
    df["obstr_type"] = df["obstr_type"].astype("int8")

    log.info("Loaded %d matched DL-GROD features", len(df))
    return df


def resolve_per_reach(df: pd.DataFrame) -> pd.DataFrame:
    """For reaches with multiple DL-GROD features, keep highest-priority type.

    Among ties, keep the closest (minimum distance_m). Waterfalls get NULL
    dl_grod_id since their source is HydroFALLS, not DL-GROD.
    """
    df = df.copy()
    df["priority"] = df["obstr_type"].map(TYPE_PRIORITY)

    # Sort: lowest priority number first (= highest importance), then distance
    dist_col = "distance_m" if "distance_m" in df.columns else None
    sort_cols = ["reach_id", "priority"] + ([dist_col] if dist_col else [])
    df = df.sort_values(sort_cols).drop_duplicates(subset="reach_id", keep="first")

    # dl_grod_id is NULL for waterfalls (sourced from HydroFALLS, not DL-GROD)
    df["dl_grod_id"] = df.apply(
        lambda r: r["ID"] if r["obstr_type"] != TYPE_WATERFALL else pd.NA,
        axis=1,
    )
    df["dl_grod_id"] = pd.array(df["dl_grod_id"], dtype=pd.Int64Dtype())

    return df[["reach_id", "obstr_type", "dl_grod_id"]]


def add_dl_grod_column(con: duckdb.DuckDBPyConnection) -> None:
    """Add dl_grod_id column if it doesn't exist."""
    existing = {
        r[0]
        for r in con.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name='reaches'"
        ).fetchall()
    }
    if "dl_grod_id" not in existing:
        con.execute("ALTER TABLE reaches ADD COLUMN dl_grod_id BIGINT")
        log.info("Added dl_grod_id column to reaches")
    else:
        log.info("dl_grod_id column already exists")


def update_reaches(
    con: duckdb.DuckDBPyConnection,
    resolved: pd.DataFrame,
    dry_run: bool,
) -> int:
    if dry_run:
        log.info("[dry-run] Would update %d reaches", len(resolved))
        by_type = resolved.groupby("obstr_type").size()
        for t, n in by_type.items():
            log.info("  obstr_type=%d: %d reaches", t, n)
        return len(resolved)

<<<<<<< HEAD
    # reaches has an RTREE spatial index; must drop before UPDATE, recreate after.
    # See CLAUDE.md Known Issues: "RTREE Update Pattern".
    con.execute("INSTALL spatial; LOAD spatial;")
    rtree_indexes = con.execute(
        "SELECT index_name, table_name, sql FROM duckdb_indexes() WHERE sql LIKE '%RTREE%'"
    ).fetchall()
    for idx_name, _tbl, _sql in rtree_indexes:
        con.execute(f'DROP INDEX "{idx_name}"')

    try:
        con.register("_dl_grod_updates", resolved)
        # Clear grod_id when reclassifying to waterfall (obstr_type=4) to avoid
        # stale GROD IDs triggering O002 lint violations.
        con.execute("""
            UPDATE reaches
            SET obstr_type = u.obstr_type,
                dl_grod_id = u.dl_grod_id,
                grod_id = CASE WHEN u.obstr_type = 4 THEN NULL ELSE reaches.grod_id END
            FROM _dl_grod_updates u
            WHERE reaches.reach_id = u.reach_id
        """)
        con.unregister("_dl_grod_updates")
    finally:
        for idx_name, _tbl, sql in rtree_indexes:
            con.execute(sql)

=======
    con.register("_dl_grod_updates", resolved)
    con.execute("""
        UPDATE reaches
        SET obstr_type = u.obstr_type,
            dl_grod_id = u.dl_grod_id
        FROM _dl_grod_updates u
        WHERE reaches.reach_id = u.reach_id
    """)
    con.unregister("_dl_grod_updates")
>>>>>>> ad53e4b (feat: add DL-GROD ingestion and obstruction lint checks (#127))
    return len(resolved)


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest DL-GROD into SWORD reaches")
<<<<<<< HEAD
    parser.add_argument(
        "--mapping",
        required=True,
        help="Path to barrier_reach_mapping.csv from swot_obstructions project",
    )
=======
    parser.add_argument("--mapping", default=str(DEFAULT_MAPPING))
>>>>>>> ad53e4b (feat: add DL-GROD ingestion and obstruction lint checks (#127))
    parser.add_argument("--db", default=str(DEFAULT_DB))
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    mapping_path = Path(args.mapping)
    db_path = Path(args.db)

    if not mapping_path.exists():
        log.error("Mapping file not found: %s", mapping_path)
        sys.exit(1)
    if not db_path.exists():
        log.error("DuckDB not found: %s", db_path)
        sys.exit(1)

    df = load_mapping(mapping_path)
    resolved = resolve_per_reach(df)

<<<<<<< HEAD
    if len(resolved) == 0:
        log.warning(
            "No matched reaches after resolving mapping — check CSV path and column names."
        )
        sys.exit(1)

=======
>>>>>>> ad53e4b (feat: add DL-GROD ingestion and obstruction lint checks (#127))
    log.info("Resolved to %d unique reaches", len(resolved))
    by_type = resolved.groupby("obstr_type").size()
    for t, n in by_type.items():
        log.info("  obstr_type=%d: %d reaches", t, n)

    con = duckdb.connect(str(db_path), read_only=args.dry_run)
<<<<<<< HEAD
    try:
        if not args.dry_run:
            add_dl_grod_column(con)

        n = update_reaches(con, resolved, args.dry_run)
        log.info(
            "%s %d reaches", "[dry-run] Would update" if args.dry_run else "Updated", n
        )
    finally:
        con.close()
=======

    if not args.dry_run:
        add_dl_grod_column(con)

    n = update_reaches(con, resolved, args.dry_run)
    log.info(
        "%s %d reaches", "[dry-run] Would update" if args.dry_run else "Updated", n
    )

    con.close()
>>>>>>> ad53e4b (feat: add DL-GROD ingestion and obstruction lint checks (#127))
    log.info("Done.")


if __name__ == "__main__":
    main()
