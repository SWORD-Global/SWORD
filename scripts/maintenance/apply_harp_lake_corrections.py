#!/usr/bin/env python3
"""Apply HarP-derived lake classification corrections to v17c DuckDB.

Uses the Intersected SWORD-PLD dataset (HarP v1.1, Sikder et al. 2024) to
identify reaches that should have lakeflag=1 but currently have lakeflag=0.

HarP was built on SWORD v16; reach IDs are translated to v17b (= v17c) via
the UNC translation tables.

Correction tiers (highest to lowest confidence):
  T1: In-lake      — reach is geometrically inside a PLD lake polygon
  T2: Through-lake  — PLD lake runs entirely through the reach
  T3: Inflow/Outflow — reach is both a lake inlet and outlet (connector)

Excluded from correction:
  - type=6 (ghost) — ghost typing takes priority
  - type=4 (dam) — dam typing takes priority
  - reaches already lakeflag != 0

Usage:
    # Dry run (default) — report only
    python scripts/maintenance/apply_harp_lake_corrections.py

    # Apply corrections
    python scripts/maintenance/apply_harp_lake_corrections.py --apply

    # Single tier only
    python scripts/maintenance/apply_harp_lake_corrections.py --apply --tiers T1 T2

    # Custom DB path
    python scripts/maintenance/apply_harp_lake_corrections.py --apply \
        --db data/duckdb/sword_v17c.duckdb
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import fiona

logger = logging.getLogger(__name__)

REGIONS = ["NA", "SA", "EU", "AF", "AS", "OC"]
SKIP_TYPES = {4, 6}  # dam, ghost
TIER_REACH_TYPES = {
    "T1": ["In-lake"],
    "T2": ["Through-lake"],
    "T3": ["Inflow/Outflow"],
}
ALL_TIERS = ["T1", "T2", "T3"]

GDB_PATH = Path(
    "data/Jida_Safat_Lake_QC/HarP/Intersected_SWORD_PLD_dataset.gdb"
)
TRANSLATION_DIR = Path("data/Jida_Safat_Lake_QC/v17b_v16_translation/v17b")


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------


def load_v16_to_v17b_translation(
    translation_dir: Path,
) -> dict[int, list[int]]:
    """Load v16 -> v17b reach ID mappings from UNC translation CSVs.

    Returns dict mapping v16_reach_id -> [v17b_reach_id, ...].
    """
    v16_to_v17b: dict[int, list[int]] = {}
    for region in REGIONS:
        fpath = translation_dir / f"{region}_ReachIDs_v17b_vs_v16.csv"
        if not fpath.exists():
            logger.warning("Translation file missing: %s", fpath)
            continue
        with open(fpath) as f:
            reader = csv.DictReader(f)
            for row in reader:
                v16_str = row["v16_reach_id"].strip()
                if not v16_str or int(v16_str) == 0:
                    continue
                v16 = int(v16_str)
                v17 = int(row["v17_reach_id"])
                v16_to_v17b.setdefault(v16, []).append(v17)
    logger.info(
        "Loaded %d unique v16 reach IDs from translation",
        len(v16_to_v17b),
    )
    return v16_to_v17b


def load_harp_reach_flags(gdb_path: Path) -> dict[int, dict]:
    """Load HarP reach-level flags from all pfaf layers in the GDB.

    Returns dict mapping v16_reach_id -> {Reach_type, In_lak_flg, ...}.
    """
    layers = fiona.listlayers(str(gdb_path))
    reach_layers = [l for l in layers if l.startswith("Intersected_SWORD_reaches")]

    harp: dict[int, dict] = {}
    for lyr in reach_layers:
        with fiona.open(str(gdb_path), layer=lyr) as src:
            for feat in src:
                p = feat["properties"]
                rid = int(p["reach_id"])
                harp[rid] = {
                    "Reach_type": p.get("Reach_type"),
                    "In_lak_flg": int(p.get("In_lak_flg", 0)),
                    "In_lake_id": p.get("In_lake_id"),
                    "Tr_lake_id": p.get("Tr_lake_id"),
                }
    logger.info("Loaded %d HarP intersected reaches", len(harp))
    return harp


def build_correction_candidates(
    harp: dict[int, dict],
    v16_to_v17b: dict[int, list[int]],
    tiers: list[str],
) -> dict[int, dict]:
    """Map HarP flags to v17b IDs and filter to requested tiers.

    Returns dict mapping v17b_reach_id -> {tier, Reach_type, ...}.
    """
    allowed_types: set[str] = set()
    tier_lookup: dict[str, str] = {}
    for tier in tiers:
        for rt in TIER_REACH_TYPES[tier]:
            allowed_types.add(rt)
            tier_lookup[rt] = tier

    candidates: dict[int, dict] = {}
    unmatched = 0
    for v16_id, flags in harp.items():
        rt = flags["Reach_type"]
        if rt not in allowed_types:
            continue
        if v16_id not in v16_to_v17b:
            unmatched += 1
            continue
        tier = tier_lookup[rt]
        for v17b_id in v16_to_v17b[v16_id]:
            candidates[v17b_id] = {
                "tier": tier,
                "Reach_type": rt,
                "v16_reach_id": v16_id,
                "In_lake_id": flags.get("In_lake_id"),
                "Tr_lake_id": flags.get("Tr_lake_id"),
            }

    if unmatched:
        logger.warning(
            "%d HarP v16 reaches had no v17b translation (expected for v16-only reaches)",
            unmatched,
        )
    logger.info(
        "Built %d v17b correction candidates across tiers %s",
        len(candidates),
        tiers,
    )
    return candidates


# ---------------------------------------------------------------------------
# DB operations
# ---------------------------------------------------------------------------


def filter_against_db(
    conn: duckdb.DuckDBPyConnection,
    candidates: dict[int, dict],
) -> tuple[list[dict], dict[str, int]]:
    """Filter candidates against current v17c state.

    Keeps only reaches where lakeflag=0 and type not in SKIP_TYPES.
    Returns (corrections, skip_stats).
    """
    reach_ids = list(candidates.keys())

    # Batch query — load all candidate reaches in one shot
    conn.execute("DROP TABLE IF EXISTS _harp_candidates")
    conn.execute("CREATE TEMP TABLE _harp_candidates (reach_id BIGINT PRIMARY KEY)")
    conn.executemany(
        "INSERT INTO _harp_candidates VALUES (?)",
        [(int(rid),) for rid in reach_ids],
    )

    rows = conn.execute(
        """
        SELECT r.reach_id, r.lakeflag, r.type, r.region, r.width
        FROM reaches r
        JOIN _harp_candidates c ON r.reach_id = c.reach_id
        """
    ).fetchall()

    conn.execute("DROP TABLE IF EXISTS _harp_candidates")

    stats = {
        "already_lake": 0,
        "skip_ghost": 0,
        "skip_dam": 0,
        "skip_type5": 0,
        "no_match_in_db": 0,
        "eligible": 0,
    }

    db_lookup = {}
    for reach_id, lakeflag, rtype, region, width in rows:
        db_lookup[reach_id] = (lakeflag, rtype, region, width)

    corrections = []
    for reach_id, cand in candidates.items():
        if reach_id not in db_lookup:
            stats["no_match_in_db"] += 1
            continue

        lakeflag, rtype, region, width = db_lookup[reach_id]

        if lakeflag != 0:
            stats["already_lake"] += 1
            continue
        if rtype == 6:
            stats["skip_ghost"] += 1
            continue
        if rtype == 4:
            stats["skip_dam"] += 1
            continue
        if rtype == 5:
            stats["skip_type5"] += 1
            continue

        stats["eligible"] += 1
        corrections.append(
            {
                "reach_id": reach_id,
                "region": region,
                "old_lakeflag": lakeflag,
                "new_lakeflag": 1,
                "type": rtype,
                "width": width,
                "tier": cand["tier"],
                "harp_reach_type": cand["Reach_type"],
                "v16_reach_id": cand["v16_reach_id"],
            }
        )

    return corrections, stats


def apply_corrections(
    conn: duckdb.DuckDBPyConnection,
    corrections: list[dict],
) -> dict[str, int]:
    """Write lakeflag corrections + edit_flag to DB. RTREE-safe."""
    if not corrections:
        return {"reaches_updated": 0, "nodes_updated": 0}

    conn.execute("INSTALL spatial; LOAD spatial;")

    # Drop RTREE indexes
    indexes = conn.execute(
        "SELECT index_name, table_name, sql FROM duckdb_indexes() "
        "WHERE sql LIKE '%RTREE%'"
    ).fetchall()
    for idx_name, _tbl, _sql in indexes:
        conn.execute(f'DROP INDEX "{idx_name}"')

    # Stage corrections into temp table
    conn.execute("DROP TABLE IF EXISTS _harp_lake_fixes")
    conn.execute(
        """
        CREATE TEMP TABLE _harp_lake_fixes (
            reach_id BIGINT PRIMARY KEY,
            region VARCHAR,
            tier VARCHAR,
            harp_reach_type VARCHAR
        )
        """
    )
    conn.executemany(
        "INSERT INTO _harp_lake_fixes VALUES (?, ?, ?, ?)",
        [
            (c["reach_id"], c["region"], c["tier"], c["harp_reach_type"])
            for c in corrections
        ],
    )

    # Update reaches: lakeflag -> 1
    conn.execute(
        """
        UPDATE reaches SET lakeflag = 1
        FROM _harp_lake_fixes h
        WHERE reaches.reach_id = h.reach_id
          AND reaches.region = h.region
        """
    )

    # Tag edit_flag (append to existing)
    conn.execute(
        """
        UPDATE reaches SET edit_flag = CASE
            WHEN edit_flag IS NULL OR edit_flag = '' OR edit_flag = 'NaN'
                THEN 'harp_lake'
            ELSE edit_flag || ',harp_lake'
        END
        FROM _harp_lake_fixes h
        WHERE reaches.reach_id = h.reach_id
          AND reaches.region = h.region
        """
    )

    reaches_updated = conn.execute(
        "SELECT COUNT(*) FROM _harp_lake_fixes"
    ).fetchone()[0]

    # Propagate lakeflag to nodes
    nodes_updated = conn.execute(
        """
        UPDATE nodes SET lakeflag = 1
        FROM _harp_lake_fixes h
        WHERE nodes.reach_id = h.reach_id
          AND nodes.region = h.region
          AND nodes.lakeflag != 1
        """
    ).fetchone()[0]

    conn.execute("DROP TABLE IF EXISTS _harp_lake_fixes")

    # Recreate RTREE indexes
    for _idx_name, _tbl, sql in indexes:
        conn.execute(sql)

    return {"reaches_updated": reaches_updated, "nodes_updated": nodes_updated}


def save_report(corrections: list[dict], output_path: Path) -> None:
    """Save corrections CSV for audit trail."""
    if not corrections:
        return
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=corrections[0].keys())
        writer.writeheader()
        writer.writerows(corrections)
    logger.info("Saved %d corrections to %s", len(corrections), output_path)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Apply HarP-derived lake classification corrections to v17c"
    )
    parser.add_argument(
        "--db",
        default="data/duckdb/sword_v17c.duckdb",
        help="Path to v17c DuckDB",
    )
    parser.add_argument(
        "--gdb",
        default=str(GDB_PATH),
        help="Path to HarP Intersected_SWORD_PLD GDB",
    )
    parser.add_argument(
        "--translation-dir",
        default=str(TRANSLATION_DIR),
        help="Directory containing v17b-to-v16 translation CSVs",
    )
    parser.add_argument(
        "--tiers",
        nargs="+",
        choices=ALL_TIERS,
        default=ALL_TIERS,
        help="Which tiers to apply (default: all)",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually write corrections to DB (default: dry run)",
    )
    parser.add_argument(
        "--output",
        default="output/harp_lake_corrections/corrections.csv",
        help="Path for corrections report CSV",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    db_path = Path(args.db)
    gdb_path = Path(args.gdb)
    trans_dir = Path(args.translation_dir)

    if not db_path.exists():
        print(f"Database not found: {db_path}", file=sys.stderr)
        sys.exit(1)
    if not gdb_path.exists():
        print(f"HarP GDB not found: {gdb_path}", file=sys.stderr)
        sys.exit(1)
    if not trans_dir.exists():
        print(f"Translation dir not found: {trans_dir}", file=sys.stderr)
        sys.exit(1)

    mode = "APPLY" if args.apply else "DRY RUN"
    print(f"=== HarP Lake Corrections ({mode}) ===")
    print(f"Database: {db_path}")
    print(f"HarP GDB: {gdb_path}")
    print(f"Tiers:    {args.tiers}")
    print()

    # 1. Load data
    print("Loading v16->v17b translation...")
    v16_to_v17b = load_v16_to_v17b_translation(trans_dir)

    print("Loading HarP reach flags from GDB...")
    harp = load_harp_reach_flags(gdb_path)

    print("Building correction candidates...")
    candidates = build_correction_candidates(harp, v16_to_v17b, args.tiers)

    # 2. Filter against current DB state
    print("Filtering against v17c database...")
    conn = duckdb.connect(str(db_path), read_only=not args.apply)
    corrections, stats = filter_against_db(conn, candidates)

    # 3. Report
    print(f"\n--- Filter Results ---")
    print(f"  HarP candidates (v17b):     {len(candidates)}")
    print(f"  Already lakeflag != 0:       {stats['already_lake']}")
    print(f"  Skipped ghost (type=6):      {stats['skip_ghost']}")
    print(f"  Skipped dam (type=4):        {stats['skip_dam']}")
    print(f"  Skipped unreliable (type=5): {stats['skip_type5']}")
    print(f"  No match in v17c DB:         {stats['no_match_in_db']}")
    print(f"  ELIGIBLE for correction:     {stats['eligible']}")

    # Breakdown by tier
    from collections import Counter

    tier_counts = Counter(c["tier"] for c in corrections)
    region_counts = Counter(c["region"] for c in corrections)
    type_counts = Counter(c["type"] for c in corrections)

    print(f"\n--- Eligible by Tier ---")
    for tier in ALL_TIERS:
        rt_names = ", ".join(TIER_REACH_TYPES[tier])
        print(f"  {tier} ({rt_names}): {tier_counts.get(tier, 0)}")

    print(f"\n--- Eligible by Region ---")
    for region in REGIONS:
        print(f"  {region}: {region_counts.get(region, 0)}")

    print(f"\n--- Eligible by v17c type ---")
    for t in sorted(type_counts):
        print(f"  type={t}: {type_counts[t]}")

    # Width stats
    if corrections:
        widths = [c["width"] for c in corrections if c["width"] is not None]
        if widths:
            widths.sort()
            n = len(widths)
            print(f"\n--- Width Distribution ---")
            print(f"  min={widths[0]:.0f}  p25={widths[n//4]:.0f}  "
                  f"median={widths[n//2]:.0f}  p75={widths[3*n//4]:.0f}  "
                  f"max={widths[-1]:.0f}")

    # 4. Save report
    output_path = Path(args.output)
    save_report(corrections, output_path)
    if corrections:
        print(f"\nCorrections CSV: {output_path}")

    # 5. Apply or skip
    if not args.apply:
        print(f"\nDRY RUN complete. Use --apply to write {len(corrections)} corrections.")
        conn.close()
        return

    if not corrections:
        print("\nNo corrections to apply.")
        conn.close()
        return

    print(f"\nApplying {len(corrections)} corrections...")
    result = apply_corrections(conn, corrections)
    conn.close()

    print(f"  Reaches updated: {result['reaches_updated']}")
    print(f"  Nodes updated:   {result['nodes_updated']}")
    print("Done.")


if __name__ == "__main__":
    main()
