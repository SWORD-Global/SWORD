#!/usr/bin/env python3
"""Apply the v17c-0.0.13 facc rederivation to the working v17c DuckDB.

Replaces ``nodes.facc`` and ``reaches.facc`` with the validated rederivation
(MERIT Hydro UPA read by section-level D8 matching, network-constrained fill
where matching fails) and sets ``facc_quality`` to a provenance string:

    rederived_v1:<source>:<tier>[:review]

``source`` is engine | soft | bridge | fill and ``tier`` is A | A2 | B | C | D | E,
both taken from the input row. ``:review`` is appended when the row's
``junction_inconsistent`` or ``topology_suspect`` flag is set. Node rows use the
node file, reach rows the reach file (the reach value is the junction-aware
reach value, not recomputed here).

Input: ``final_nodes_{R}.parquet`` / ``final_reaches_{R}.parquet`` in
``--source-dir`` for every region present in the database. Inputs may be a
superset of the database only with ``--allow-superset`` (used for the test
fixture); otherwise the id sets must match exactly.

Safety:
  * refuses to run while another process holds the target database (lsof),
    and refuses any path that names the read-only v17b database;
  * refuses unless the database's current facc equals the input's ``v17c``
    column, i.e. the database is in the state the rederivation was built from
    (after an apply, run --rollback before applying a new input);
  * backs up the current values before writing, to parquet files in
    ``--backup-dir`` and to the DuckDB tables facc_backup_0012_nodes /
    facc_backup_0012_reaches. Existing backups are verified against the
    database and never overwritten. Nothing is written to ``--source-dir``;
  * RTREE indexes (and any index on a facc column) on nodes/reaches are dropped
    and recreated around the UPDATE in try/finally;
  * the UPDATEs and the sword_operations row commit in one transaction.

Usage:
    # validate and report only (read-only connection, nothing written)
    python scripts/maintenance/apply_facc_rederivation.py --dry-run

    # apply to data/duckdb/sword_v17c.duckdb
    python scripts/maintenance/apply_facc_rederivation.py

    # restore the pre-0.0.13 values from the backup tables
    python scripts/maintenance/apply_facc_rederivation.py --rollback

    # test fixture copy (synthetic superset input)
    python scripts/maintenance/apply_facc_rederivation.py --db /tmp/fixture.duckdb \\
        --source-dir /tmp/fixture_source --backup-dir /tmp/fixture_backup \\
        --allow-superset
"""

from __future__ import annotations

import argparse
import hashlib
import json
import random
import re
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

import duckdb
import pyarrow.parquet as pq

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DB = PROJECT_ROOT / "data" / "duckdb" / "sword_v17c.duckdb"
V17B_DB = PROJECT_ROOT / "data" / "duckdb" / "sword_v17b.duckdb"
REVIEW_ROOT = PROJECT_ROOT / "output" / "facc_detection" / "fable_review_20261001"
DEFAULT_SOURCE_DIR = REVIEW_ROOT / "integrate_global" / "outputs"
DEFAULT_BACKUP_DIR = REVIEW_ROOT / "release_0.0.13" / "backup"

REGIONS = ("AF", "AS", "EU", "NA", "OC", "SA")
SOURCES = ("engine", "soft", "bridge", "fill")
TIERS = ("A", "A2", "B", "C", "D", "E")
QUALITY_PREFIX = "rederived_v1"

RELEASE = "v17c-0.0.13"
OPERATION_KIND = "FACC_REDERIVATION_V1"
ROLLBACK_KIND = "FACC_REDERIVATION_ROLLBACK"
SESSION_ID = "v17c_0_0_13_facc_rederivation"
USER_ID = "jake"
BACKUP_NODES = "facc_backup_0012_nodes"
BACKUP_REACHES = "facc_backup_0012_reaches"

REL_TOL = 1e-6
MAX_REACH_MAX_EXCEPTION_FRAC = 0.001
SPOT_CHECK_REACHES = 20
SPOT_CHECK_SEED = 20261002

# (entity, id column) for the two updated tables
ENTITIES = (("nodes", "node_id"), ("reaches", "reach_id"))


class ApplyError(RuntimeError):
    """A precondition or verification failed; nothing further is attempted."""


def log(msg: str) -> None:
    print(f"[{datetime.now():%H:%M:%S}] {msg}", flush=True)


def sql_path(path: Path) -> str:
    return "'" + str(path).replace("'", "''") + "'"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


# ---------------------------------------------------------------------------
# Guards
# ---------------------------------------------------------------------------


def guard_paths(db_path: Path, source_dir: Path, backup_dir: Path) -> None:
    db = db_path.resolve()
    if db == V17B_DB.resolve() or "v17b" in db.name.lower():
        raise ApplyError(
            f"Refusing to touch {db}: v17b is the read-only reference. "
            "Point --db at the v17c working database."
        )
    if not db.exists():
        raise ApplyError(f"Database not found: {db}")
    if not source_dir.is_dir():
        raise ApplyError(f"Source directory not found: {source_dir}")
    src = source_dir.resolve()
    bak = backup_dir.resolve()
    if bak == src or src in bak.parents:
        raise ApplyError(
            f"--backup-dir {bak} is inside --source-dir {src}; the source outputs "
            "must not be modified. Choose a backup directory outside it."
        )


def check_no_other_holders(db_path: Path) -> None:
    """Refuse if any process has the database (or its WAL) open."""
    holders = []
    for path in (db_path, db_path.with_name(db_path.name + ".wal")):
        if not path.exists():
            continue
        try:
            proc = subprocess.run(
                ["lsof", "-F", "pc", str(path)],
                capture_output=True,
                text=True,
                check=False,
            )
        except FileNotFoundError as exc:
            raise ApplyError("lsof not found; cannot verify the DB is free") from exc
        if proc.returncode not in (0, 1):
            raise ApplyError(f"lsof failed on {path}: {proc.stderr.strip()}")
        pid = None
        for line in proc.stdout.splitlines():
            if line.startswith("p"):
                pid = line[1:]
            elif line.startswith("c") and pid is not None:
                holders.append(f"pid {pid} ({line[1:]}) on {path.name}")
    if holders:
        raise ApplyError(
            "Another process holds the database: "
            + "; ".join(holders)
            + ". Stop it (Streamlit reviewer, notebooks, exports) and re-run."
        )


# ---------------------------------------------------------------------------
# Input staging and validation
# ---------------------------------------------------------------------------


def db_regions(con) -> list[str]:
    rows = con.execute("SELECT DISTINCT region FROM reaches ORDER BY 1").fetchall()
    regions = [r[0] for r in rows]
    unknown = sorted(set(regions) - set(REGIONS))
    if unknown:
        raise ApplyError(f"Unexpected region codes in reaches: {unknown}")
    return regions


def input_files(source_dir: Path, regions: list[str]) -> dict[str, Path]:
    files = {}
    missing = []
    for region in regions:
        for kind in ("nodes", "reaches"):
            path = source_dir / f"final_{kind}_{region}.parquet"
            if path.exists():
                files[path.name] = path
            else:
                missing.append(path.name)
    if missing:
        raise ApplyError(f"Missing input files in {source_dir}: {missing}")
    return files


def stage_inputs(con, source_dir: Path, regions: list[str]) -> None:
    """Load the input rows for the database's regions into temp tables."""
    node_parts = []
    reach_parts = []
    for region in regions:
        node_path = sql_path(source_dir / f"final_nodes_{region}.parquet")
        reach_path = sql_path(source_dir / f"final_reaches_{region}.parquet")
        node_parts.append(
            "SELECT node_id, reach_id, facc_final, source, tier_final, "
            "junction_inconsistent, topology_suspect, v17c, "
            f"'{region}' AS region FROM read_parquet({node_path})"
        )
        reach_parts.append(
            "SELECT reach_id, region, facc_final, source, tier_final, "
            "junction_inconsistent, topology_suspect, v17c, "
            f"'{region}' AS file_region FROM read_parquet({reach_path})"
        )
    con.execute("DROP TABLE IF EXISTS _in_nodes")
    con.execute("DROP TABLE IF EXISTS _in_reaches")
    con.execute(f"CREATE TEMP TABLE _in_nodes AS {' UNION ALL '.join(node_parts)}")
    con.execute(f"CREATE TEMP TABLE _in_reaches AS {' UNION ALL '.join(reach_parts)}")


def _one(con, sql: str):
    return con.execute(sql).fetchone()


def validate_inputs(con, allow_superset: bool) -> dict:
    """Validate staged input against the database. Raises ApplyError on failure."""
    report: dict = {}
    sources = ", ".join(f"'{s}'" for s in SOURCES)
    tiers = ", ".join(f"'{t}'" for t in TIERS)
    errors: list[str] = []

    for entity, key in ENTITIES:
        table = f"_in_{entity}"
        n_rows, n_ids = _one(
            con, f"SELECT count(*), count(DISTINCT {key}) FROM {table}"
        )
        n_db = _one(con, f"SELECT count(*) FROM {entity}")[0]
        bad_value = _one(
            con,
            f"SELECT count(*) FROM {table} WHERE facc_final IS NULL "
            "OR NOT isfinite(facc_final) OR facc_final <= 0",
        )[0]
        bad_label = _one(
            con,
            f"SELECT count(*) FROM {table} WHERE source IS NULL OR tier_final IS NULL "
            f"OR source NOT IN ({sources}) OR tier_final NOT IN ({tiers})",
        )[0]
        null_flags = _one(
            con,
            f"SELECT count(*) FROM {table} WHERE junction_inconsistent IS NULL "
            "OR topology_suspect IS NULL",
        )[0]
        missing = _one(
            con,
            f"SELECT count(*) FROM {entity} d ANTI JOIN {table} i USING ({key})",
        )[0]
        extra = _one(
            con,
            f"SELECT count(*) FROM {table} i ANTI JOIN {entity} d USING ({key})",
        )[0]
        region_mismatch = _one(
            con,
            f"SELECT count(*) FROM {entity} d JOIN {table} i USING ({key}) "
            "WHERE d.region IS DISTINCT FROM i.region",
        )[0]
        baseline_drift = _one(
            con,
            f"SELECT count(*) FROM {entity} d JOIN {table} i USING ({key}) "
            "WHERE d.facc IS DISTINCT FROM i.v17c",
        )[0]
        report[entity] = {
            "input_rows": n_rows,
            "db_rows": n_db,
            "db_rows_missing_from_input": missing,
            "input_rows_not_in_db": extra,
            "duplicate_ids": n_rows - n_ids,
            "non_finite_or_non_positive": bad_value,
            "unknown_source_or_tier": bad_label,
            "null_flags": null_flags,
            "region_mismatch": region_mismatch,
            "db_facc_differs_from_input_v17c": baseline_drift,
        }
        if n_rows != n_ids:
            errors.append(f"{entity}: {n_rows - n_ids} duplicate {key} values in input")
        if bad_value:
            errors.append(
                f"{entity}: {bad_value} input facc_final values not finite and > 0"
            )
        if bad_label:
            errors.append(
                f"{entity}: {bad_label} rows with source/tier outside {SOURCES}/{TIERS}"
            )
        if null_flags:
            errors.append(
                f"{entity}: {null_flags} rows with NULL junction/topology flags"
            )
        if missing:
            errors.append(f"{entity}: {missing} database rows have no input row")
        if extra and not allow_superset:
            errors.append(
                f"{entity}: {extra} input rows are not in the database "
                "(row counts must match; --allow-superset only for test fixtures)"
            )
        if region_mismatch:
            errors.append(
                f"{entity}: {region_mismatch} rows whose region differs from the DB"
            )
        if baseline_drift:
            errors.append(
                f"{entity}: database facc differs from the input's v17c column on "
                f"{baseline_drift} rows. The database is not in the state the "
                "rederivation was built from (already applied? run --rollback first)."
            )

    reach_region_col = _one(
        con,
        "SELECT count(*) FROM _in_reaches WHERE region IS DISTINCT FROM file_region",
    )[0]
    node_reach_mismatch = _one(
        con,
        "SELECT count(*) FROM nodes d JOIN _in_nodes i USING (node_id) "
        "WHERE d.reach_id IS DISTINCT FROM i.reach_id",
    )[0]
    report["reach_file_region_mismatch"] = reach_region_col
    report["node_reach_id_mismatch"] = node_reach_mismatch
    if reach_region_col:
        errors.append(
            f"reaches: {reach_region_col} rows whose region column != file region"
        )
    if node_reach_mismatch:
        errors.append(
            f"nodes: {node_reach_mismatch} rows whose reach_id differs from the DB"
        )

    if errors:
        raise ApplyError("Input validation failed:\n  - " + "\n  - ".join(errors))

    exceptions = reach_max_exceptions(con, "_in_reaches", "_in_nodes", "facc_final")
    n_reaches = report["reaches"]["db_rows"]
    report["reach_max_node_exceptions"] = len(exceptions)
    report["reach_max_node_exception_frac"] = len(exceptions) / n_reaches
    report["reach_max_node_exception_rows"] = exceptions
    if len(exceptions) > MAX_REACH_MAX_EXCEPTION_FRAC * n_reaches:
        raise ApplyError(
            f"{len(exceptions)} of {n_reaches} reaches differ from the max of their "
            f"nodes by more than {REL_TOL:g} relative (limit "
            f"{MAX_REACH_MAX_EXCEPTION_FRAC:.1%}); inputs are inconsistent."
        )
    return report


def reach_max_exceptions(
    con, reach_table: str, node_table: str, value_col: str
) -> list:
    """Reaches (in the DB) whose value differs from max(node value) beyond REL_TOL."""
    rows = con.execute(
        f"""
        WITH m AS (
            SELECT n.reach_id, max(n.{value_col}) AS node_max
            FROM {node_table} n JOIN nodes d USING (node_id)
            GROUP BY n.reach_id
        )
        SELECT r.reach_id, r.region, r.{value_col} AS reach_value, m.node_max
        FROM {reach_table} r JOIN reaches d USING (reach_id)
        LEFT JOIN m USING (reach_id)
        WHERE m.node_max IS NULL
           OR abs(r.{value_col} - m.node_max)
              > {REL_TOL} * greatest(abs(r.{value_col}), abs(m.node_max))
        ORDER BY r.reach_id
        """
    ).fetchall()
    return [
        {"reach_id": int(r[0]), "region": r[1], "reach": r[2], "node_max": r[3]}
        for r in rows
    ]


def build_stage(con) -> dict:
    """Create the update tables (DB-matched rows only) and return counts."""
    quality = (
        f"'{QUALITY_PREFIX}:' || i.source || ':' || i.tier_final || "
        "CASE WHEN i.junction_inconsistent OR i.topology_suspect "
        "THEN ':review' ELSE '' END"
    )
    stats = {}
    for entity, key in ENTITIES:
        con.execute(f"DROP TABLE IF EXISTS _stage_{entity}")
        con.execute(
            f"""
            CREATE TEMP TABLE _stage_{entity} AS
            SELECT i.{key}, i.region, i.facc_final AS facc, {quality} AS facc_quality,
                   i.source
            FROM _in_{entity} i JOIN {entity} d USING ({key})
            """
        )
        by_quality = dict(
            con.execute(
                f"SELECT facc_quality, count(*) FROM _stage_{entity} GROUP BY 1 ORDER BY 1"
            ).fetchall()
        )
        by_source = dict(
            con.execute(
                f"SELECT source, count(*) FROM _stage_{entity} GROUP BY 1 ORDER BY 1"
            ).fetchall()
        )
        changed = _one(
            con,
            f"SELECT count(*) FROM {entity} d JOIN _stage_{entity} s USING ({key}) "
            "WHERE d.facc IS DISTINCT FROM s.facc "
            "OR d.facc_quality IS DISTINCT FROM s.facc_quality",
        )[0]
        stats[entity] = {
            "rows": sum(by_quality.values()),
            "rows_changing": changed,
            "by_source": by_source,
            "by_quality": by_quality,
            "review": sum(v for k, v in by_quality.items() if k.endswith(":review")),
        }
    return stats


# ---------------------------------------------------------------------------
# Backups
# ---------------------------------------------------------------------------


def table_exists(con, name: str) -> bool:
    return bool(
        _one(
            con,
            "SELECT count(*) FROM information_schema.tables "
            f"WHERE table_schema = 'main' AND table_name = '{name}'",
        )[0]
    )


def backup_mismatches(con, entity: str, key: str, relation: str) -> int:
    """Rows where the DB and a backup relation disagree (either direction)."""
    return _one(
        con,
        f"""
        SELECT count(*) FROM {entity} d FULL OUTER JOIN {relation} b USING ({key})
        WHERE d.{key} IS NULL OR b.{key} IS NULL
           OR d.region IS DISTINCT FROM b.region
           OR d.facc IS DISTINCT FROM b.facc
           OR d.facc_quality IS DISTINCT FROM b.facc_quality
        """,
    )[0]


def backup_file(backup_dir: Path, entity: str, region: str) -> Path:
    return backup_dir / f"facc_before_0.0.13_{entity}_{region}.parquet"


def ensure_backups(con, backup_dir: Path, regions: list[str], dry_run: bool) -> dict:
    """Create or verify the parquet and DuckDB-table backups of the current values."""
    rederived = sum(
        _one(
            con,
            f"SELECT count(*) FROM {e} WHERE facc_quality LIKE '{QUALITY_PREFIX}:%'",
        )[0]
        for e, _ in ENTITIES
    )
    tables = {
        e: (BACKUP_NODES if e == "nodes" else BACKUP_REACHES) for e, _ in ENTITIES
    }
    result: dict = {"tables": {}, "files": {}}

    for entity, key in ENTITIES:
        table = tables[entity]
        if table_exists(con, table):
            bad = backup_mismatches(con, entity, key, table)
            if bad:
                raise ApplyError(
                    f"Backup table {table} exists but differs from {entity} on {bad} rows; "
                    "refusing to continue (it would no longer describe the current state)."
                )
            result["tables"][table] = "verified_existing"
        elif dry_run:
            result["tables"][table] = "would_create"
        else:
            if rederived:
                raise ApplyError(
                    f"{rederived} rows already carry {QUALITY_PREFIX} values; refusing to "
                    "create a backup of rederived values."
                )
            con.execute(
                f"CREATE TABLE {table} AS SELECT {key}, region, facc, facc_quality "
                f"FROM {entity} ORDER BY {key}"
            )
            bad = backup_mismatches(con, entity, key, table)
            if bad:
                raise ApplyError(
                    f"Backup table {table} differs from {entity} on {bad} rows"
                )
            result["tables"][table] = "created"

        for region in regions:
            path = backup_file(backup_dir, entity, region)
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
                        f"Backup file {path} exists but differs from the DB on {bad} rows; "
                        "refusing to overwrite it."
                    )
                result["files"][path.name] = {
                    "status": "verified_existing",
                    "sha256": sha256_file(path),
                }
            elif dry_run:
                result["files"][path.name] = {"status": "would_create"}
            else:
                backup_dir.mkdir(parents=True, exist_ok=True)
                tmp = path.with_suffix(".parquet.tmp")
                con.execute(
                    f"COPY (SELECT {key}, region, facc, facc_quality FROM {entity} "
                    f"WHERE region = '{region}' ORDER BY {key}) "
                    f"TO {sql_path(tmp)} (FORMAT PARQUET, COMPRESSION ZSTD)"
                )
                tmp.rename(path)
                result["files"][path.name] = {
                    "status": "created",
                    "sha256": sha256_file(path),
                }
    return result


# ---------------------------------------------------------------------------
# Index handling and the write transaction
# ---------------------------------------------------------------------------

FACC_COL_RE = re.compile(r"\bfacc(_quality)?\b", re.IGNORECASE)


def table_indexes(con) -> list[tuple[str, str, str]]:
    return con.execute(
        """
        SELECT index_name, table_name, sql FROM duckdb_indexes()
        WHERE database_name = current_database() AND schema_name = 'main'
          AND table_name IN ('nodes', 'reaches')
        ORDER BY table_name, index_name
        """
    ).fetchall()


def indexes_to_drop(indexes: list[tuple[str, str, str]]) -> list[tuple[str, str, str]]:
    """RTREE indexes and any index on a facc column must not exist during UPDATE."""
    out = []
    for name, table, sql in indexes:
        on_clause = sql.split(" ON ", 1)[-1] if sql else ""
        if "RTREE" in (sql or "").upper() or FACC_COL_RE.search(on_clause):
            out.append((name, table, sql))
    return out


def fingerprint(con, entity: str, key: str) -> str:
    value = _one(
        con, f"SELECT bit_xor(hash({key}, region, facc, facc_quality)) FROM {entity}"
    )[0]
    return f"{entity}:{int(value or 0):016x}"


def run_write(con, statements: list[str], op_row: dict) -> int:
    """Drop indexes, run UPDATEs + provenance insert in one transaction, recreate."""
    dropped = indexes_to_drop(table_indexes(con))
    for name, table, _sql in dropped:
        log(f"Dropping index {name} on {table} for the update")
        con.execute(f'DROP INDEX "{name}"')
    try:
        con.execute("BEGIN TRANSACTION")
        try:
            for stmt in statements:
                con.execute(stmt)
            op_id = _one(
                con, "SELECT COALESCE(max(operation_id), 0) + 1 FROM sword_operations"
            )[0]
            con.execute(
                """
                INSERT INTO sword_operations (
                    operation_id, operation_type, table_name, entity_ids, region,
                    user_id, session_id, started_at, completed_at, operation_details,
                    affected_columns, reason, source_operation_id, status,
                    before_checksum, after_checksum
                ) VALUES (?, 'UPDATE', 'reaches,nodes', NULL, ?, ?, ?, ?, ?, ?,
                          ['facc', 'facc_quality'], ?, ?, 'COMPLETED', ?, ?)
                """,
                [
                    op_id,
                    op_row["region"],
                    USER_ID,
                    SESSION_ID,
                    op_row["started_at"],
                    datetime.now(),
                    json.dumps(op_row["details"], default=str),
                    op_row["reason"],
                    op_row.get("source_operation_id"),
                    op_row["before_checksum"],
                    ";".join(fingerprint(con, e, k) for e, k in ENTITIES),
                ],
            )
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
    finally:
        for name, table, sql in dropped:
            log(f"Recreating index {name} on {table}")
            con.execute(sql)
    return int(op_id)


def update_statements(source_prefix: str) -> list[str]:
    """UPDATE ... FROM for nodes and reaches from <prefix>nodes / <prefix>reaches."""
    stmts = []
    for entity, key in ENTITIES:
        rel = f"{source_prefix}{entity}"
        stmts.append(
            f"UPDATE {entity} SET facc = s.facc, facc_quality = s.facc_quality "
            f"FROM {rel} s WHERE {entity}.{key} = s.{key} AND {entity}.region = s.region"
        )
    return stmts


# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------


def check_region_relations(con) -> dict:
    """Per-region relations ({r}_nodes, {r}_reaches) must be views over the base tables."""
    found = {}
    for region in REGIONS:
        for entity, _ in ENTITIES:
            name = f"{region.lower()}_{entity}"
            row = con.execute(
                "SELECT table_type FROM information_schema.tables "
                "WHERE table_schema = 'main' AND table_name = ?",
                [name],
            ).fetchone()
            if row is None:
                continue
            if row[0] != "VIEW":
                raise ApplyError(
                    f"{name} is a {row[0]}, not a view; this script only updates the base "
                    "tables. Update it explicitly or convert it to a view first."
                )
            found[name] = "VIEW"
    return found


def stale_views(con, views: dict) -> list[str]:
    """Views whose stored binding no longer matches their base table's columns.

    DuckDB binds ``SELECT *`` views at creation; columns added to the base table
    later make every query on the view fail with "Contents of view were altered".
    """
    stale = []
    for name in views:
        try:
            con.execute(f"SELECT * FROM {name} LIMIT 0").fetchall()
        except duckdb.BinderException as exc:
            if "Contents of view were altered" not in str(exc):
                raise
            stale.append(name)
    return stale


def rebind_statements(con, names: list[str]) -> list[str]:
    """Re-run each view's own stored CREATE VIEW statement as CREATE OR REPLACE."""
    stmts = []
    for name in names:
        sql = con.execute(
            "SELECT sql FROM duckdb_views() WHERE database_name = current_database() "
            "AND schema_name = 'main' AND view_name = ?",
            [name],
        ).fetchone()[0]
        prefix = "CREATE VIEW "
        if not sql.upper().startswith(prefix):
            raise ApplyError(f"Unexpected definition for view {name}: {sql}")
        stmts.append("CREATE OR REPLACE VIEW " + sql[len(prefix) :])
    return stmts


def verify_views(con, views: dict) -> dict:
    out = {}
    for name in views:
        region, entity = name.split("_", 1)
        key = dict(ENTITIES)[entity]
        agg = f"count(*), bit_xor(hash({key}, region, facc, facc_quality))"
        view_row = _one(con, f"SELECT {agg} FROM {name}")
        base_row = _one(
            con, f"SELECT {agg} FROM {entity} WHERE region = '{region.upper()}'"
        )
        if view_row != base_row:
            raise ApplyError(
                f"View {name} disagrees with {entity}: {view_row} vs {base_row}"
            )
        out[name] = view_row[0]
    return out


def spot_check(con, source_dir: Path, regions: list[str]) -> list[dict]:
    """Compare 20 random reaches (and their nodes) with the parquet, read independently."""
    reach_ids = [
        int(r[0])
        for r in con.execute(
            "SELECT reach_id FROM _stage_reaches ORDER BY reach_id"
        ).fetchall()
    ]
    rng = random.Random(SPOT_CHECK_SEED)
    sample = sorted(rng.sample(reach_ids, min(SPOT_CHECK_REACHES, len(reach_ids))))

    def expected_quality(row) -> str:
        review = bool(row["junction_inconsistent"]) or bool(row["topology_suspect"])
        return f"{QUALITY_PREFIX}:{row['source']}:{row['tier_final']}" + (
            ":review" if review else ""
        )

    columns = [
        "facc_final",
        "source",
        "tier_final",
        "junction_inconsistent",
        "topology_suspect",
    ]
    results = []
    failures = []
    for region in regions:
        rch = pq.read_table(
            source_dir / f"final_reaches_{region}.parquet",
            columns=["reach_id", *columns],
            filters=[("reach_id", "in", sample)],
        ).to_pandas()
        if rch.empty:
            continue
        nod = pq.read_table(
            source_dir / f"final_nodes_{region}.parquet",
            columns=["node_id", "reach_id", *columns],
            filters=[("reach_id", "in", rch["reach_id"].tolist())],
        ).to_pandas()
        for _, row in rch.iterrows():
            rid = int(row["reach_id"])
            db_facc, db_q = _one(
                con, f"SELECT facc, facc_quality FROM reaches WHERE reach_id = {rid}"
            )
            ok = db_facc == row["facc_final"] and db_q == expected_quality(row)
            node_rows = nod[nod["reach_id"] == rid]
            db_nodes = dict(
                (int(n), (f, q))
                for n, f, q in con.execute(
                    "SELECT node_id, facc, facc_quality FROM nodes WHERE reach_id = ?",
                    [rid],
                ).fetchall()
            )
            node_bad = 0
            for _, n in node_rows.iterrows():
                got = db_nodes.get(int(n["node_id"]))
                if (
                    got is None
                    or got[0] != n["facc_final"]
                    or got[1] != expected_quality(n)
                ):
                    node_bad += 1
            if len(db_nodes) != len(node_rows):
                node_bad += abs(len(db_nodes) - len(node_rows))
            entry = {
                "reach_id": rid,
                "region": region,
                "facc": db_facc,
                "facc_quality": db_q,
                "nodes": len(node_rows),
                "node_mismatches": node_bad,
                "ok": bool(ok and node_bad == 0),
            }
            results.append(entry)
            if not entry["ok"]:
                failures.append(rid)
    if len(results) != len(sample):
        raise ApplyError(
            f"Spot check found {len(results)} of {len(sample)} sampled reaches"
        )
    if failures:
        raise ApplyError(f"Spot check failed for reaches {failures}")
    return results


def verify_apply(
    con,
    source_dir: Path,
    regions: list[str],
    counts_before: dict,
    indexes_before: list,
    input_exceptions: list,
    op_id: int,
    views: dict,
) -> dict:
    out: dict = {}
    for entity, key in ENTITIES:
        n = _one(con, f"SELECT count(*) FROM {entity}")[0]
        if n != counts_before[entity]:
            raise ApplyError(
                f"{entity}: row count changed {counts_before[entity]} -> {n}"
            )
        bad = _one(
            con, f"SELECT count(*) FROM {entity} WHERE facc IS NULL OR facc <= 0"
        )[0]
        if bad:
            raise ApplyError(
                f"{entity}: {bad} rows with NULL or non-positive facc after apply"
            )
        diff = _one(
            con,
            f"SELECT count(*) FROM {entity} d JOIN _stage_{entity} s USING ({key}) "
            "WHERE d.facc IS DISTINCT FROM s.facc OR d.facc_quality IS DISTINCT FROM s.facc_quality",
        )[0]
        if diff:
            raise ApplyError(
                f"{entity}: {diff} rows differ from the staged values after apply"
            )
        not_rederived = _one(
            con,
            f"SELECT count(*) FROM {entity} WHERE facc_quality IS NULL "
            f"OR facc_quality NOT LIKE '{QUALITY_PREFIX}:%'",
        )[0]
        if not_rederived:
            raise ApplyError(
                f"{entity}: {not_rederived} rows without a {QUALITY_PREFIX} tag"
            )
        out[entity] = {"rows": n, "staged_mismatches": diff}

    db_exceptions = reach_max_exceptions(con, "reaches", "nodes", "facc")
    if [e["reach_id"] for e in db_exceptions] != [
        e["reach_id"] for e in input_exceptions
    ]:
        raise ApplyError(
            f"reach = max(node) exceptions after apply ({len(db_exceptions)}) do not match "
            f"the input's ({len(input_exceptions)})"
        )
    out["reach_max_node_exceptions"] = len(db_exceptions)

    out["spot_check"] = spot_check(con, source_dir, regions)

    indexes_after = table_indexes(con)
    if sorted(indexes_after) != sorted(indexes_before):
        raise ApplyError(
            f"Index set changed: before {indexes_before}, after {indexes_after}"
        )
    out["indexes"] = [name for name, _t, _s in indexes_after]
    out["rtree_indexes"] = [
        name for name, _t, s in indexes_after if "RTREE" in (s or "").upper()
    ]

    row = con.execute(
        "SELECT status, region, affected_columns FROM sword_operations WHERE operation_id = ?",
        [op_id],
    ).fetchone()
    if row is None or row[0] != "COMPLETED":
        raise ApplyError(
            f"sword_operations row {op_id} missing or not COMPLETED: {row}"
        )
    out["operation"] = {"operation_id": op_id, "status": row[0], "region": row[1]}
    out["views"] = verify_views(con, views)
    return out


# ---------------------------------------------------------------------------
# Modes
# ---------------------------------------------------------------------------


def operation_region(regions: list[str]) -> str:
    return "ALL" if sorted(regions) == sorted(REGIONS) else ",".join(regions)


def do_apply(args) -> dict:
    t0 = time.time()
    started_at = datetime.now()
    db_path = Path(args.db)
    source_dir = Path(args.source_dir)
    backup_dir = Path(args.backup_dir)

    con = duckdb.connect(str(db_path), read_only=args.dry_run)
    try:
        con.execute("INSTALL spatial; LOAD spatial;")
        regions = db_regions(con)
        log(
            f"Database {db_path} ({'read-only' if args.dry_run else 'read-write'}), regions {regions}"
        )
        files = input_files(source_dir, regions)
        file_meta = {}
        for name, path in sorted(files.items()):
            file_meta[name] = {
                "sha256": sha256_file(path),
                "rows": pq.ParquetFile(path).metadata.num_rows,
            }
        summary_json = source_dir / "INTEGRATION_SUMMARY.json"
        if summary_json.exists():
            file_meta[summary_json.name] = {"sha256": sha256_file(summary_json)}

        views = check_region_relations(con)
        views_to_rebind = stale_views(con, views)
        stage_inputs(con, source_dir, regions)
        validation = validate_inputs(con, args.allow_superset)
        log(
            "Input valid: "
            f"{validation['nodes']['db_rows']:,} nodes, {validation['reaches']['db_rows']:,} "
            f"reaches; {validation['reach_max_node_exceptions']} reach != max(node) "
            "exceptions (within limit)"
        )
        stage = build_stage(con)
        counts_before = {
            e: _one(con, f"SELECT count(*) FROM {e}")[0] for e, _ in ENTITIES
        }
        indexes_before = table_indexes(con)
        before_checksum = ";".join(fingerprint(con, e, k) for e, k in ENTITIES)

        backups = ensure_backups(con, backup_dir, regions, args.dry_run)

        summary = {
            "mode": "dry-run" if args.dry_run else "apply",
            "db": str(db_path),
            "source_dir": str(source_dir),
            "backup_dir": str(backup_dir),
            "regions": regions,
            "inputs": file_meta,
            "validation": {
                k: v
                for k, v in validation.items()
                if k != "reach_max_node_exception_rows"
            },
            "reach_max_node_exception_rows": validation[
                "reach_max_node_exception_rows"
            ],
            "stage": stage,
            "backups": backups,
            "region_views": sorted(views),
            "stale_region_views_rebound": views_to_rebind,
            "indexes_before": [n for n, _t, _s in indexes_before],
            "indexes_dropped_for_update": [
                n for n, _t, _s in indexes_to_drop(indexes_before)
            ],
            "before_checksum": before_checksum,
        }
        if args.dry_run:
            summary["elapsed_s"] = round(time.time() - t0, 1)
            return summary

        details = {
            "release": RELEASE,
            "operation_kind": OPERATION_KIND,
            "quality_vocabulary": f"{QUALITY_PREFIX}:<source>:<tier>[:review]",
            "review_rule": "junction_inconsistent OR topology_suspect",
            "source_dir": str(source_dir.resolve()),
            "source_files": file_meta,
            "nodes_updated": stage["nodes"]["rows"],
            "reaches_updated": stage["reaches"]["rows"],
            "nodes_by_source": stage["nodes"]["by_source"],
            "reaches_by_source": stage["reaches"]["by_source"],
            "nodes_by_quality": stage["nodes"]["by_quality"],
            "reaches_by_quality": stage["reaches"]["by_quality"],
            "reach_max_node_exceptions": validation["reach_max_node_exception_rows"],
            "input_rows_not_in_db": {
                e: validation[e]["input_rows_not_in_db"] for e, _ in ENTITIES
            },
            "backup_tables": [BACKUP_NODES, BACKUP_REACHES],
            "backup_dir": str(backup_dir.resolve()),
            "backup_files": backups["files"],
            "stale_region_views_rebound": views_to_rebind,
        }
        op_row = {
            "region": operation_region(regions),
            "started_at": started_at,
            "details": details,
            "reason": (
                f"{RELEASE} facc rederivation: MERIT Hydro UPA by section-level D8 "
                "matching with network-constrained fill (rederived_v1)"
            ),
            "before_checksum": before_checksum,
        }
        log(
            "Writing: UPDATE nodes, UPDATE reaches, sword_operations row (one transaction)"
        )
        t_write = time.time()
        statements = update_statements("_stage_") + rebind_statements(
            con, views_to_rebind
        )
        op_id = run_write(con, statements, op_row)
        log(f"Committed operation {op_id} in {time.time() - t_write:.1f}s; verifying")
        summary["operation_id"] = op_id
        summary["write_s"] = round(time.time() - t_write, 1)
        summary["verification"] = verify_apply(
            con,
            source_dir,
            regions,
            counts_before,
            indexes_before,
            validation["reach_max_node_exception_rows"],
            op_id,
            views,
        )
        summary["after_checksum"] = ";".join(
            fingerprint(con, e, k) for e, k in ENTITIES
        )
        summary["elapsed_s"] = round(time.time() - t0, 1)
        return summary
    finally:
        con.close()


def do_rollback(args) -> dict:
    t0 = time.time()
    started_at = datetime.now()
    db_path = Path(args.db)
    con = duckdb.connect(str(db_path), read_only=args.dry_run)
    try:
        con.execute("INSTALL spatial; LOAD spatial;")
        regions = db_regions(con)
        views = check_region_relations(con)
        views_to_rebind = stale_views(con, views)
        summary: dict = {
            "mode": "rollback-dry-run" if args.dry_run else "rollback",
            "db": str(db_path),
            "stale_region_views_rebound": views_to_rebind,
        }
        for entity, key in ENTITIES:
            table = BACKUP_NODES if entity == "nodes" else BACKUP_REACHES
            if not table_exists(con, table):
                raise ApplyError(
                    f"Backup table {table} not found; nothing to roll back from"
                )
            n_db = _one(con, f"SELECT count(*) FROM {entity}")[0]
            n_bak, n_ids = _one(
                con, f"SELECT count(*), count(DISTINCT {key}) FROM {table}"
            )
            uncovered = _one(
                con,
                f"SELECT count(*) FROM {entity} d ANTI JOIN {table} b USING ({key})",
            )[0]
            if n_bak != n_ids or n_bak != n_db or uncovered:
                raise ApplyError(
                    f"{table} does not cover {entity} one-to-one "
                    f"(backup {n_bak} rows / {n_ids} ids, db {n_db}, uncovered {uncovered})"
                )
            summary[entity] = {
                "rows": n_db,
                "rows_changing": backup_mismatches(con, entity, key, table),
            }
        last_apply = con.execute(
            "SELECT max(operation_id) FROM sword_operations WHERE status = 'COMPLETED' "
            "AND json_extract_string(operation_details, '$.operation_kind') = ?",
            [OPERATION_KIND],
        ).fetchone()[0]
        summary["source_operation_id"] = last_apply
        if args.dry_run:
            summary["elapsed_s"] = round(time.time() - t0, 1)
            return summary

        con.execute(
            f"CREATE OR REPLACE TEMP VIEW _bak_nodes AS SELECT * FROM {BACKUP_NODES}"
        )
        con.execute(
            f"CREATE OR REPLACE TEMP VIEW _bak_reaches AS SELECT * FROM {BACKUP_REACHES}"
        )
        indexes_before = table_indexes(con)
        before_checksum = ";".join(fingerprint(con, e, k) for e, k in ENTITIES)
        op_row = {
            "region": operation_region(regions),
            "started_at": started_at,
            "details": {
                "release": RELEASE,
                "operation_kind": ROLLBACK_KIND,
                "restored_from": [BACKUP_NODES, BACKUP_REACHES],
                "rows_changing": {e: summary[e]["rows_changing"] for e, _ in ENTITIES},
                "stale_region_views_rebound": views_to_rebind,
            },
            "reason": f"{RELEASE} facc rollback: restore facc and facc_quality from backup tables",
            "source_operation_id": last_apply,
            "before_checksum": before_checksum,
        }
        statements = update_statements("_bak_") + rebind_statements(
            con, views_to_rebind
        )
        op_id = run_write(con, statements, op_row)
        for entity, key in ENTITIES:
            table = BACKUP_NODES if entity == "nodes" else BACKUP_REACHES
            bad = backup_mismatches(con, entity, key, table)
            if bad:
                raise ApplyError(
                    f"Rollback left {bad} {entity} rows differing from {table}"
                )
        if sorted(table_indexes(con)) != sorted(indexes_before):
            raise ApplyError("Index set changed during rollback")
        status = _one(
            con, f"SELECT status FROM sword_operations WHERE operation_id = {op_id}"
        )[0]
        if status != "COMPLETED":
            raise ApplyError(f"Rollback operation {op_id} status {status}")
        summary["operation_id"] = op_id
        summary["views"] = verify_views(con, views)
        summary["after_checksum"] = ";".join(
            fingerprint(con, e, k) for e, k in ENTITIES
        )
        summary["elapsed_s"] = round(time.time() - t0, 1)
        return summary
    finally:
        con.close()


def print_summary(summary: dict) -> None:
    print("\n=== facc rederivation summary ===")
    print(f"mode: {summary['mode']}  db: {summary['db']}")
    print(
        f"stale per-region views re-bound: {summary.get('stale_region_views_rebound')}"
    )
    if "stage" in summary:
        for entity, _ in ENTITIES:
            st = summary["stage"][entity]
            print(
                f"{entity}: {st['rows']:,} rows, {st['rows_changing']:,} changing, "
                f"{st['review']:,} with :review"
            )
            for src, n in st["by_source"].items():
                print(f"    {src:7s} {n:>10,} ({100 * n / st['rows']:.1f}%)")
        print(
            f"reach != max(node) exceptions: {summary['validation']['reach_max_node_exceptions']}"
        )
        print(f"backups: {summary['backups']['tables']}")
    for entity in ("nodes", "reaches"):
        if "rows_changing" in summary.get(entity, {}):
            print(f"{entity}: {summary[entity]['rows_changing']:,} rows to restore")
    if "verification" in summary:
        ver = summary["verification"]
        ok = sum(1 for s in ver["spot_check"] if s["ok"])
        print(
            f"verification: counts ok, spot check {ok}/{len(ver['spot_check'])}, "
            f"indexes {len(ver['indexes'])} (RTREE {ver['rtree_indexes']}), "
            f"operation {ver['operation']}"
        )
    if "operation_id" in summary:
        print(f"sword_operations.operation_id = {summary['operation_id']}")
    print(f"elapsed: {summary.get('elapsed_s')} s")
    print("\nJSON:")
    print(json.dumps(summary, indent=2, default=str))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--db", default=str(DEFAULT_DB), help="v17c DuckDB to update")
    parser.add_argument(
        "--source-dir",
        default=str(DEFAULT_SOURCE_DIR),
        help="Directory with final_{nodes,reaches}_{R}.parquet",
    )
    parser.add_argument(
        "--backup-dir",
        default=str(DEFAULT_BACKUP_DIR),
        help="Directory for facc_before_0.0.13_{nodes,reaches}_{R}.parquet",
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Validate and report only"
    )
    parser.add_argument(
        "--rollback",
        action="store_true",
        help="Restore facc/facc_quality from backup tables",
    )
    parser.add_argument(
        "--allow-superset",
        action="store_true",
        help="Accept input rows that are not in the DB (test fixtures only)",
    )
    args = parser.parse_args()

    try:
        guard_paths(Path(args.db), Path(args.source_dir), Path(args.backup_dir))
        check_no_other_holders(Path(args.db))
        summary = do_rollback(args) if args.rollback else do_apply(args)
    except ApplyError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    print_summary(summary)
    return 0


if __name__ == "__main__":
    sys.exit(main())
