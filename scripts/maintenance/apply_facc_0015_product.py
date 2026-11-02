#!/usr/bin/env python3
"""Apply facc product 0021 (rule set 0.0.21) to the working v17c DuckDB (v17c-0.0.14).

Writes three product columns to ``reaches`` and ``nodes`` and refreshes the
provenance string:

    facc          per-channel drainage area (km2), overwritten in place
    facc_total    the river's total drainage area (km2), new DOUBLE column
    facc_flag     one categorical label per reach, new VARCHAR column
    facc_quality  rederived_v2:<facc_source>

``facc_flag`` is derived from the product's reach flags; first match wins:

    same_river   same_river_arm OR same_river is set (a channel that reads the
                 whole river, or a junction exit with duplicate inflows)
    suspect      suspect_high OR suspect_swap
    delta_arm    partition_unvalidated OR channel_capped > 0
    braid_share  cls IN (side_channel, distributary)
                 OR facc_source LIKE 'partition%'
    filled       facc_source LIKE 'fallback%'
                 OR fallback_reason = 'trunk_copy_rejected'
    ok           everything else

Nodes take facc / facc_total from the node product file (linear in dist_out
between the reach's head and tail) and facc_flag / facc_quality from their reach.

Input (``--product-dir``): ``sword_{R}_v17c_facc_0015.parquet`` (reaches) and
``sword_{R}_v17c_facc_nodes_0015.parquet`` (nodes) for every region. The file
names keep 0015; the values are rule set 0.0.21 (``--product-dir``
/Users/jakegearon/projects/sword-mhv-facc-rule/outputs/product).

Safety (same conventions as apply_facc_rederivation.py, whose helpers are reused):
  * refuses while another process holds the database, and refuses v17b paths;
  * baseline guard: the database's facc / facc_quality must equal the pre-0.0.13
    backup tables facc_backup_0012_{nodes,reaches}. Run
    ``apply_facc_rederivation.py --rollback`` first if 0.0.13 is applied;
  * writes per-region parquet backups of the current values to ``--backup-dir``
    (facc_before_0.0.14_{nodes,reaches}_{R}.parquet; existing files are verified,
    never overwritten). The backup tables keep their 0012 names, so ``--rollback``
    returns the database to the pre-0.0.13 state and clears facc_total / facc_flag;
  * RTREE indexes and any index on a facc column are dropped and recreated around
    the write in try/finally; the two columns are added if missing, stale
    per-region ``SELECT *`` views are re-bound;
  * one transaction per region: UPDATE reaches, UPDATE nodes and the
    sword_operations row commit together.

Usage:
    python scripts/maintenance/apply_facc_0015_product.py --dry-run
    python scripts/maintenance/apply_facc_0015_product.py
    python scripts/maintenance/apply_facc_0015_product.py --rollback
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime
from pathlib import Path

import duckdb
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent))
from apply_facc_rederivation import (  # noqa: E402
    BACKUP_NODES,
    BACKUP_REACHES,
    ENTITIES,
    REGIONS,
    ApplyError,
    _one,
    backup_mismatches,
    check_no_other_holders,
    check_region_relations,
    db_regions,
    guard_paths,
    indexes_to_drop,
    log,
    rebind_statements,
    sha256_file,
    sql_path,
    stale_views,
    table_exists,
    table_indexes,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DB = PROJECT_ROOT / "data" / "duckdb" / "sword_v17c.duckdb"
DEFAULT_PRODUCT_DIR = Path("/Users/jakegearon/projects/sword-mhv-facc/outputs/product")
DEFAULT_BACKUP_DIR = (
    PROJECT_ROOT / "output" / "facc_detection" / "release_0.0.14" / "backup"
)

RELEASE = "v17c-0.0.14"
PRODUCT = "facc product 0021 (rule set 0.0.21)"
OPERATION_KIND = "FACC_PRODUCT_0015"
ROLLBACK_KIND = "FACC_PRODUCT_0015_ROLLBACK"
SESSION_ID = "v17c_0_0_14_facc_product_0015"
USER_ID = "jake"
QUALITY_PREFIX = "rederived_v2"

SOURCES = (
    "anchor",
    "anchor_proximity",
    "anchor_aligned",
    "interpolated",
    "lake_derived",
    "partition_rejoin_conserving",
    "fallback_fill",
    "fallback_prior",
)
CLASSES = ("skeleton", "side_channel", "distributary")
FLAGS = ("ok", "same_river", "braid_share", "delta_arm", "filled", "suspect")
NEW_COLUMNS = (("facc_total", "DOUBLE"), ("facc_flag", "VARCHAR"))
WRITTEN_COLUMNS = ["facc", "facc_total", "facc_flag", "facc_quality"]
REL_TOL = 1e-6

FLAG_SQL = """
    CASE
        WHEN same_river_arm OR (same_river IS NOT NULL AND same_river <> '')
            THEN 'same_river'
        WHEN coalesce(suspect_high, false) OR coalesce(suspect_swap, false)
            THEN 'suspect'
        WHEN coalesce(partition_unvalidated, false) OR coalesce(channel_capped, 0) > 0
            THEN 'delta_arm'
        WHEN cls IN ('side_channel', 'distributary')
             OR facc_source LIKE 'partition%'
            THEN 'braid_share'
        WHEN facc_source LIKE 'fallback%'
             OR coalesce(fallback_reason, '') = 'trunk_copy_rejected'
            THEN 'filled'
        ELSE 'ok'
    END
"""


def product_files(product_dir: Path, region: str) -> dict[str, Path]:
    return {
        "reaches": product_dir / f"sword_{region}_v17c_facc_0015.parquet",
        "nodes": product_dir / f"sword_{region}_v17c_facc_nodes_0015.parquet",
    }


# ---------------------------------------------------------------------------
# Staging and validation
# ---------------------------------------------------------------------------


def stage_inputs(con, product_dir: Path, regions: list[str]) -> dict:
    meta = {}
    reach_parts, node_parts = [], []
    for region in regions:
        files = product_files(product_dir, region)
        for kind, path in files.items():
            if not path.exists():
                raise ApplyError(f"Missing product file: {path}")
            meta[path.name] = {
                "sha256": sha256_file(path),
                "rows": pq.ParquetFile(path).metadata.num_rows,
            }
        reach_parts.append(
            "SELECT reach_id, region, facc, facc_total, facc_source, cls, share, "
            "suspect_high, suspect_swap, partition_unvalidated, channel_capped, "
            f"same_river, same_river_arm, fallback_reason, '{region}' AS file_region "
            f"FROM read_parquet({sql_path(files['reaches'])})"
        )
        node_parts.append(
            "SELECT node_id, reach_id, region, facc, facc_total, facc_source, "
            f"'{region}' AS file_region FROM read_parquet({sql_path(files['nodes'])})"
        )
    con.execute(
        f"CREATE OR REPLACE TEMP TABLE _in_reaches AS {' UNION ALL '.join(reach_parts)}"
    )
    con.execute(
        f"CREATE OR REPLACE TEMP TABLE _in_nodes AS {' UNION ALL '.join(node_parts)}"
    )
    return meta


def validate_inputs(con) -> dict:
    report: dict = {}
    errors: list[str] = []
    sources = ", ".join(f"'{s}'" for s in SOURCES)
    classes = ", ".join(f"'{c}'" for c in CLASSES)

    for entity, key in ENTITIES:
        t = f"_in_{entity}"
        n_rows, n_ids = _one(con, f"SELECT count(*), count(DISTINCT {key}) FROM {t}")
        checks = {
            "missing_from_input": f"SELECT count(*) FROM {entity} d ANTI JOIN {t} i USING ({key})",
            "not_in_db": f"SELECT count(*) FROM {t} i ANTI JOIN {entity} d USING ({key})",
            "region_mismatch": (
                f"SELECT count(*) FROM {entity} d JOIN {t} i USING ({key}) "
                "WHERE d.region IS DISTINCT FROM i.region "
                "OR i.region IS DISTINCT FROM i.file_region"
            ),
            "bad_values": (
                f"SELECT count(*) FROM {t} WHERE facc IS NULL OR facc_total IS NULL "
                "OR NOT isfinite(facc) OR NOT isfinite(facc_total) "
                "OR facc < 1 OR facc_total < 1"
            ),
            "facc_above_total": f"SELECT count(*) FROM {t} WHERE facc > facc_total",
            "bad_source": (
                f"SELECT count(*) FROM {t} WHERE facc_source IS NULL "
                f"OR facc_source NOT IN ({sources})"
            ),
        }
        if entity == "reaches":
            checks["bad_cls"] = (
                f"SELECT count(*) FROM {t} WHERE cls IS NULL OR cls NOT IN ({classes})"
            )
            checks["null_same_river_arm"] = (
                f"SELECT count(*) FROM {t} WHERE same_river_arm IS NULL"
            )
        else:
            checks["reach_id_mismatch"] = (
                f"SELECT count(*) FROM nodes d JOIN {t} i USING (node_id) "
                "WHERE d.reach_id IS DISTINCT FROM i.reach_id"
            )
            checks["source_differs_from_reach"] = (
                f"SELECT count(*) FROM {t} n JOIN _in_reaches r USING (reach_id) "
                "WHERE n.facc_source IS DISTINCT FROM r.facc_source"
            )
        res = {name: _one(con, sql)[0] for name, sql in checks.items()}
        res["input_rows"] = n_rows
        res["duplicate_ids"] = n_rows - n_ids
        report[entity] = res
        for name, n in res.items():
            if name != "input_rows" and n:
                errors.append(f"{entity}: {name} = {n}")

    for col in ("facc", "facc_total"):
        n = _one(
            con,
            f"""
            WITH m AS (SELECT reach_id, max({col}) AS node_max FROM _in_nodes GROUP BY 1)
            SELECT count(*) FROM _in_reaches r LEFT JOIN m USING (reach_id)
            WHERE m.node_max IS NULL
               OR abs(r.{col} - m.node_max) > {REL_TOL} * greatest(r.{col}, m.node_max)
            """,
        )[0]
        report[f"reach_{col}_ne_max_node"] = n
        if n:
            errors.append(f"{n} reaches whose {col} != max(node {col})")

    for entity, key in ENTITIES:
        table = BACKUP_NODES if entity == "nodes" else BACKUP_REACHES
        if not table_exists(con, table):
            errors.append(f"backup table {table} missing")
            continue
        drift = backup_mismatches(con, entity, key, table)
        report[entity]["db_differs_from_pre_0013_backup"] = drift
        if drift:
            errors.append(
                f"{entity}: database differs from {table} on {drift} rows. "
                "The database is not in the pre-0.0.13 state; run "
                "apply_facc_rederivation.py --rollback (or this script's --rollback) first."
            )

    if errors:
        raise ApplyError("Input validation failed:\n  - " + "\n  - ".join(errors))
    return report


def build_stage(con) -> dict:
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE _stage_reaches AS
        SELECT reach_id, region, facc, facc_total, {FLAG_SQL} AS facc_flag,
               '{QUALITY_PREFIX}:' || facc_source AS facc_quality
        FROM _in_reaches
        """
    )
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE _stage_nodes AS
        SELECT n.node_id, n.region, n.facc, n.facc_total, r.facc_flag, r.facc_quality
        FROM _in_nodes n JOIN _stage_reaches r USING (reach_id)
        """
    )
    stats: dict = {}
    for entity, _key in ENTITIES:
        stats[entity] = {
            "rows": _one(con, f"SELECT count(*) FROM _stage_{entity}")[0],
            "by_flag": dict(
                con.execute(
                    f"SELECT facc_flag, count(*) FROM _stage_{entity} GROUP BY 1 ORDER BY 1"
                ).fetchall()
            ),
            "by_region_flag": {
                f"{r}:{f}": n
                for r, f, n in con.execute(
                    f"SELECT region, facc_flag, count(*) FROM _stage_{entity} "
                    "GROUP BY 1, 2 ORDER BY 1, 2"
                ).fetchall()
            },
        }
    return stats


def write_backup_files(
    con, backup_dir: Path, regions: list[str], dry_run: bool
) -> dict:
    """Per-region parquet copies of the values this apply replaces."""
    out = {}
    for entity, key in ENTITIES:
        for region in regions:
            path = backup_dir / f"facc_before_0.0.14_{entity}_{region}.parquet"
            relation = f"(SELECT * FROM {entity} WHERE region = '{region}')"
            if path.exists():
                bad = _one(
                    con,
                    f"""
                    SELECT count(*) FROM {relation} d
                    FULL OUTER JOIN read_parquet({sql_path(path)}) b USING ({key})
                    WHERE d.{key} IS NULL OR b.{key} IS NULL
                       OR d.facc IS DISTINCT FROM b.facc
                       OR d.facc_quality IS DISTINCT FROM b.facc_quality
                    """,
                )[0]
                if bad:
                    raise ApplyError(
                        f"Backup file {path} exists but differs from the DB on {bad} rows"
                    )
                out[path.name] = {
                    "status": "verified_existing",
                    "sha256": sha256_file(path),
                }
            elif dry_run:
                out[path.name] = {"status": "would_create"}
            else:
                backup_dir.mkdir(parents=True, exist_ok=True)
                tmp = path.with_suffix(".parquet.tmp")
                con.execute(
                    f"COPY (SELECT {key}, region, facc, facc_quality FROM {entity} "
                    f"WHERE region = '{region}' ORDER BY {key}) "
                    f"TO {sql_path(tmp)} (FORMAT PARQUET, COMPRESSION ZSTD)"
                )
                tmp.rename(path)
                out[path.name] = {"status": "created", "sha256": sha256_file(path)}
    return out


# ---------------------------------------------------------------------------
# Write
# ---------------------------------------------------------------------------


def missing_columns(con) -> list[tuple[str, str, str]]:
    out = []
    for entity, _key in ENTITIES:
        have = {r[0] for r in con.execute(f"DESCRIBE {entity}").fetchall()}
        out += [(entity, c, t) for c, t in NEW_COLUMNS if c not in have]
    return out


def fingerprint(con, entity: str, key: str, region: str | None = None) -> str:
    cols = ", ".join([key, "region", *WRITTEN_COLUMNS])
    where = f" WHERE region = '{region}'" if region else ""
    value = _one(con, f"SELECT bit_xor(hash({cols})) FROM {entity}{where}")[0]
    return f"{entity}:{int(value or 0):016x}"


def insert_operation(
    con,
    region: str,
    kind: str,
    reason: str,
    details: dict,
    started_at: datetime,
    before: str,
    source_op,
) -> int:
    op_id = _one(
        con, "SELECT COALESCE(max(operation_id), 0) + 1 FROM sword_operations"
    )[0]
    after = ";".join(fingerprint(con, e, k, region) for e, k in ENTITIES)
    con.execute(
        """
        INSERT INTO sword_operations (
            operation_id, operation_type, table_name, entity_ids, region,
            user_id, session_id, started_at, completed_at, operation_details,
            affected_columns, reason, source_operation_id, status,
            before_checksum, after_checksum
        ) VALUES (?, 'UPDATE', 'reaches,nodes', NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                  'COMPLETED', ?, ?)
        """,
        [
            op_id,
            region,
            USER_ID,
            SESSION_ID,
            started_at,
            datetime.now(),
            json.dumps({**details, "operation_kind": kind}, default=str),
            WRITTEN_COLUMNS,
            reason,
            source_op,
            before,
            after,
        ],
    )
    return int(op_id)


def run_write(
    con,
    regions: list[str],
    source_prefix: str,
    kind: str,
    reason: str,
    details_for,
    source_op=None,
) -> dict[str, int]:
    """Drop indexes, add columns, then one transaction per region; recreate indexes."""
    views = check_region_relations(con)
    dropped = indexes_to_drop(table_indexes(con))
    for name, table, _sql in dropped:
        log(f"Dropping index {name} on {table}")
        con.execute(f'DROP INDEX "{name}"')
    op_ids: dict[str, int] = {}
    try:
        for entity, col, typ in missing_columns(con):
            log(f"ALTER TABLE {entity} ADD COLUMN {col} {typ}")
            con.execute(f"ALTER TABLE {entity} ADD COLUMN {col} {typ}")
        for stmt in rebind_statements(con, stale_views(con, views)):
            con.execute(stmt)
        for region in regions:
            started_at = datetime.now()
            before = ";".join(fingerprint(con, e, k, region) for e, k in ENTITIES)
            con.execute("BEGIN TRANSACTION")
            try:
                for entity, key in ENTITIES:
                    rel = f"{source_prefix}{entity}"
                    sets = ", ".join(f"{c} = s.{c}" for c in WRITTEN_COLUMNS)
                    con.execute(
                        f"UPDATE {entity} SET {sets} FROM {rel} s "
                        f"WHERE {entity}.{key} = s.{key} AND {entity}.region = s.region "
                        f"AND s.region = '{region}'"
                    )
                op_ids[region] = insert_operation(
                    con,
                    region,
                    kind,
                    reason,
                    details_for(region),
                    started_at,
                    before,
                    source_op,
                )
                con.execute("COMMIT")
            except Exception:
                con.execute("ROLLBACK")
                raise
            log(f"[{region}] committed operation {op_ids[region]}")
    finally:
        for name, table, sql in dropped:
            log(f"Recreating index {name} on {table}")
            con.execute(sql)
    return op_ids


# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------


def verify_apply(con, counts_before: dict, indexes_before: list, stage: dict) -> dict:
    out: dict = {}
    for entity, key in ENTITIES:
        n = _one(con, f"SELECT count(*) FROM {entity}")[0]
        if n != counts_before[entity]:
            raise ApplyError(
                f"{entity}: row count changed {counts_before[entity]} -> {n}"
            )
        diff = _one(
            con,
            f"SELECT count(*) FROM {entity} d FULL OUTER JOIN _stage_{entity} s USING ({key}) "
            "WHERE d."
            + key
            + " IS NULL OR s."
            + key
            + " IS NULL OR "
            + " OR ".join(f"d.{c} IS DISTINCT FROM s.{c}" for c in WRITTEN_COLUMNS),
        )[0]
        if diff:
            raise ApplyError(f"{entity}: {diff} rows differ from the staged values")
        by_flag = dict(
            con.execute(
                f"SELECT facc_flag, count(*) FROM {entity} GROUP BY 1"
            ).fetchall()
        )
        if by_flag != stage[entity]["by_flag"]:
            raise ApplyError(
                f"{entity}: label counts {by_flag} != staged {stage[entity]['by_flag']}"
            )
        out[entity] = {"rows": n, "staged_mismatches": diff, "by_flag": by_flag}
    if sorted(table_indexes(con)) != sorted(indexes_before):
        raise ApplyError("Index set changed during the write")
    return out


# ---------------------------------------------------------------------------
# Modes
# ---------------------------------------------------------------------------


def do_apply(args) -> dict:
    t0 = time.time()
    db_path, product_dir, backup_dir = (
        Path(args.db),
        Path(args.product_dir),
        Path(args.backup_dir),
    )
    con = duckdb.connect(str(db_path), read_only=args.dry_run)
    try:
        con.execute("INSTALL spatial; LOAD spatial;")
        regions = db_regions(con)
        if sorted(regions) != sorted(REGIONS):
            raise ApplyError(f"Expected all six regions in the DB, found {regions}")
        log(f"Database {db_path} ({'read-only' if args.dry_run else 'read-write'})")
        file_meta = stage_inputs(con, product_dir, regions)
        validation = validate_inputs(con)
        log("Input valid; staging")
        stage = build_stage(con)
        counts_before = {
            e: _one(con, f"SELECT count(*) FROM {e}")[0] for e, _ in ENTITIES
        }
        indexes_before = table_indexes(con)
        backups = write_backup_files(con, backup_dir, regions, args.dry_run)
        summary = {
            "mode": "dry-run" if args.dry_run else "apply",
            "db": str(db_path),
            "product_dir": str(product_dir),
            "inputs": file_meta,
            "validation": validation,
            "stage": stage,
            "backup_files": backups,
            "columns_to_add": missing_columns(con),
            "indexes_dropped_for_update": [
                n for n, _t, _s in indexes_to_drop(indexes_before)
            ],
        }
        if args.dry_run:
            summary["elapsed_s"] = round(time.time() - t0, 1)
            return summary

        def details_for(region: str) -> dict:
            return {
                "release": RELEASE,
                "product": PRODUCT,
                "product_dir": str(product_dir.resolve()),
                "source_files": {
                    p.name: file_meta[p.name]
                    for p in product_files(product_dir, region).values()
                },
                "quality_vocabulary": f"{QUALITY_PREFIX}:<facc_source>",
                "facc_flag_rule": " ".join(FLAG_SQL.split()),
                "reaches_by_flag": {
                    k.split(":", 1)[1]: v
                    for k, v in stage["reaches"]["by_region_flag"].items()
                    if k.startswith(f"{region}:")
                },
                "backup_tables": [BACKUP_NODES, BACKUP_REACHES],
                "backup_files": {
                    k: v for k, v in backups.items() if k.endswith(f"_{region}.parquet")
                },
            }

        reason = (
            f"{RELEASE} facc: {PRODUCT} from MERIT-Basins tree matching; facc per "
            "channel, facc_total river total, facc_flag label"
        )
        op_ids = run_write(con, regions, "_stage_", OPERATION_KIND, reason, details_for)
        summary["operation_ids"] = op_ids
        summary["verification"] = verify_apply(
            con, counts_before, indexes_before, stage
        )
        summary["elapsed_s"] = round(time.time() - t0, 1)
        return summary
    finally:
        con.close()


def do_rollback(args) -> dict:
    """Restore facc / facc_quality from the pre-0.0.13 tables; clear the new columns."""
    t0 = time.time()
    con = duckdb.connect(str(args.db), read_only=args.dry_run)
    try:
        con.execute("INSTALL spatial; LOAD spatial;")
        regions = db_regions(con)
        summary: dict = {"mode": "rollback-dry-run" if args.dry_run else "rollback"}
        for entity, key in ENTITIES:
            table = BACKUP_NODES if entity == "nodes" else BACKUP_REACHES
            if not table_exists(con, table):
                raise ApplyError(f"Backup table {table} not found")
            summary[entity] = {
                "rows_changing": backup_mismatches(con, entity, key, table)
            }
        if args.dry_run:
            return summary
        if missing_columns(con):
            raise ApplyError(
                "facc_total / facc_flag columns absent; nothing to roll back"
            )
        for entity, key in ENTITIES:
            table = BACKUP_NODES if entity == "nodes" else BACKUP_REACHES
            con.execute(
                f"CREATE OR REPLACE TEMP TABLE _bak_{entity} AS SELECT {key}, region, facc, "
                "facc_quality, NULL::DOUBLE AS facc_total, NULL::VARCHAR AS facc_flag "
                f"FROM {table}"
            )
        last_apply = _one(
            con,
            "SELECT max(operation_id) FROM sword_operations WHERE status = 'COMPLETED' "
            "AND json_extract_string(operation_details, '$.operation_kind') = "
            f"'{OPERATION_KIND}'",
        )[0]
        op_ids = run_write(
            con,
            regions,
            "_bak_",
            ROLLBACK_KIND,
            f"{RELEASE} facc rollback: restore pre-0.0.13 facc/facc_quality, clear "
            "facc_total/facc_flag",
            lambda region: {
                "release": RELEASE,
                "restored_from": [BACKUP_NODES, BACKUP_REACHES],
            },
            source_op=last_apply,
        )
        for entity, key in ENTITIES:
            table = BACKUP_NODES if entity == "nodes" else BACKUP_REACHES
            bad = backup_mismatches(con, entity, key, table)
            if bad:
                raise ApplyError(
                    f"Rollback left {bad} {entity} rows differing from {table}"
                )
        summary["operation_ids"] = op_ids
        summary["elapsed_s"] = round(time.time() - t0, 1)
        return summary
    finally:
        con.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--db", default=str(DEFAULT_DB))
    parser.add_argument("--product-dir", default=str(DEFAULT_PRODUCT_DIR))
    parser.add_argument("--backup-dir", default=str(DEFAULT_BACKUP_DIR))
    parser.add_argument(
        "--dry-run", action="store_true", help="Validate and report only"
    )
    parser.add_argument(
        "--rollback",
        action="store_true",
        help="Restore pre-0.0.13 facc/facc_quality, clear facc_total/facc_flag",
    )
    args = parser.parse_args()
    try:
        guard_paths(Path(args.db), Path(args.product_dir), Path(args.backup_dir))
        check_no_other_holders(Path(args.db))
        summary = do_rollback(args) if args.rollback else do_apply(args)
    except ApplyError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(summary, indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
