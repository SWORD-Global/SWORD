#!/usr/bin/env python3
"""Install the adopted facc release products into the working v17c DuckDB.

``config/facc_adopted.json`` (schema 2) names two products by manifest and hash:

    numeric    facc, facc_total   per-channel and whole-river drainage area (km2)
    metadata   facc_flag, facc_quality   one label per reach, copied to its nodes

Only those four columns are written, on ``reaches`` and ``nodes``. The other
product fields (facc_head, facc_total_head, facc_source, facc_class,
facc_review) are not installed.

Safety (same conventions as apply_facc_0015_product.py, whose write path is reused):
  * each manifest must hash to the value pinned in the config, and every
    product file must match its manifest's sha256 and row count;
  * every reach and node is covered exactly once by both products; regions and
    node->reach links agree with the database; values are finite, >= 1 and
    facc <= facc_total (values within float rounding of those bounds are
    snapped onto them, and the count is recorded); labels come from the
    documented vocabulary; every node carries its reach's labels;
  * the database must already carry facc_total and facc_flag;
  * per-region parquet backups of the four replaced columns go to
    ``--backup-dir`` (existing files are verified, never overwritten);
  * RTREE and facc indexes are dropped and recreated around the write;
    one transaction per region with its sword_operations row.

Usage:
    python scripts/maintenance/install_facc_release.py --db <v17c.duckdb> \\
        --data-root <checkout with output/> --release-config config/facc_adopted.json \\
        --backup-dir <dir> [--dry-run]
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent))
import apply_facc_0015_product as p0014  # noqa: E402
from apply_facc_rederivation import (  # noqa: E402
    ENTITIES,
    REGIONS,
    ApplyError,
    _one,
    check_no_other_holders,
    db_regions,
    guard_paths,
    log,
    sha256_file,
    sql_path,
    table_indexes,
)

OPERATION_KIND = "FACC_RELEASE_20261027"
WRITTEN_COLUMNS = ["facc", "facc_total", "facc_flag", "facc_quality"]
# Product values sit within float rounding of the bounds (0.9999999999999999 km2,
# facc above facc_total by 1e-14); staging snaps them onto facc_total >= facc >= 1.
TOL = 1e-9
FLAGS = ("ok", "same_river", "braid_share", "delta_arm", "filled", "suspect")
QUALITY_PREFIX = "rederived_v3"
SOURCES = (
    "anchor",
    "anchor_proximity",
    "anchor_aligned",
    "interpolated",
    "lake_derived",
    "partitioned_arm",
    "fallback_fill",
    "fallback_prior",
)
PRODUCT_COLUMNS = {
    "numeric": {
        "reaches": "reach_id, region, facc, facc_total",
        "nodes": "node_id, reach_id, region, facc, facc_total",
    },
    "metadata": {
        "reaches": "reach_id, region, facc_flag, facc_quality",
        "nodes": "node_id, reach_id, facc_flag, facc_quality",
    },
}


def load_release(config_path: Path, root: Path) -> tuple[dict, dict[str, tuple[dict, Path]]]:
    config = json.loads(config_path.read_text())
    if config.get("schema_version") != 2:
        raise ApplyError(f"{config_path}: expected schema_version 2")
    products = {}
    for name in ("numeric", "metadata"):
        spec = config["products"][name]
        manifest_path = root / spec["manifest"]
        got = sha256_file(manifest_path)
        if got != spec["manifest_sha256"]:
            raise ApplyError(
                f"{name} manifest hash {got} != config {spec['manifest_sha256']}"
            )
        products[name] = (json.loads(manifest_path.read_text()), manifest_path.parent)
    return config, products


def stage_inputs(con, products: dict[str, tuple[dict, Path]]) -> dict:
    meta = {}
    for name, (manifest, product_dir) in products.items():
        parts: dict[str, list[str]] = {"reaches": [], "nodes": []}
        for f in manifest["files"]:
            path = product_dir / f["name"]
            digest = sha256_file(path)
            if digest != f["sha256"]:
                raise ApplyError(f"{path.name}: sha256 {digest} != manifest {f['sha256']}")
            meta[f"{name}/{f['name']}"] = {"sha256": digest, "rows": f["rows"]}
            cols = PRODUCT_COLUMNS[name][f["entity"]]
            parts[f["entity"]].append(
                f"SELECT {cols}, '{f['region']}' AS file_region, {f['rows']} AS manifest_rows "
                f"FROM read_parquet({sql_path(path)})"
            )
        for entity in ("reaches", "nodes"):
            if not parts[entity]:
                raise ApplyError(f"{name} manifest lists no {entity} files")
            con.execute(
                f"CREATE OR REPLACE TEMP TABLE _in_{name}_{entity} AS "
                + " UNION ALL ".join(parts[entity])
            )
    return meta


def validate_inputs(con) -> dict:
    report: dict = {}
    errors: list[str] = []
    flags = ", ".join(f"'{f}'" for f in FLAGS)
    qualities = ", ".join(f"'{QUALITY_PREFIX}:{s}'" for s in SOURCES)
    informational = {"input_rows", "facc_changing", "facc_total_changing",
                     "facc_flag_changing", "facc_quality_changing"}

    for name in ("numeric", "metadata"):
        for entity, key in ENTITIES:
            t = f"_in_{name}_{entity}"
            n_rows, n_ids = _one(con, f"SELECT count(*), count(DISTINCT {key}) FROM {t}")
            checks = {
                "rows_ne_manifest": (
                    f"SELECT count(*) FROM (SELECT file_region, count(*) AS n, "
                    f"any_value(manifest_rows) AS m FROM {t} GROUP BY 1) WHERE n <> m"
                ),
                "missing_from_input": f"SELECT count(*) FROM {entity} d ANTI JOIN {t} i USING ({key})",
                "not_in_db": f"SELECT count(*) FROM {t} i ANTI JOIN {entity} d USING ({key})",
                "db_missing_0014_product": (
                    f"SELECT count(*) FROM {entity} WHERE facc_total IS NULL OR facc_flag IS NULL"
                ),
            }
            if "region" in PRODUCT_COLUMNS[name][entity]:
                checks["region_mismatch"] = (
                    f"SELECT count(*) FROM {entity} d JOIN {t} i USING ({key}) "
                    "WHERE d.region IS DISTINCT FROM i.region "
                    "OR i.region IS DISTINCT FROM i.file_region"
                )
            else:
                checks["region_mismatch"] = (
                    f"SELECT count(*) FROM {entity} d JOIN {t} i USING ({key}) "
                    "WHERE d.region IS DISTINCT FROM i.file_region"
                )
            if entity == "nodes":
                checks["reach_id_mismatch"] = (
                    f"SELECT count(*) FROM nodes d JOIN {t} i USING (node_id) "
                    "WHERE d.reach_id IS DISTINCT FROM i.reach_id"
                )
            if name == "numeric":
                checks["bad_values"] = (
                    f"SELECT count(*) FROM {t} WHERE facc IS NULL OR facc_total IS NULL "
                    "OR NOT isfinite(facc) OR NOT isfinite(facc_total) "
                    f"OR facc < 1 - {TOL} OR facc_total < 1 - {TOL}"
                )
                checks["facc_above_total"] = (
                    f"SELECT count(*) FROM {t} WHERE facc > facc_total * (1 + {TOL})"
                )
            else:
                checks["bad_flag"] = (
                    f"SELECT count(*) FROM {t} WHERE facc_flag IS NULL OR facc_flag NOT IN ({flags})"
                )
                checks["bad_quality"] = (
                    f"SELECT count(*) FROM {t} WHERE facc_quality IS NULL "
                    f"OR facc_quality NOT IN ({qualities})"
                )
                if entity == "nodes":
                    checks["node_labels_differ_from_reach"] = (
                        f"SELECT count(*) FROM {t} n JOIN _in_metadata_reaches r USING (reach_id) "
                        "WHERE n.facc_flag IS DISTINCT FROM r.facc_flag "
                        "OR n.facc_quality IS DISTINCT FROM r.facc_quality"
                    )
            res = {c: _one(con, sql)[0] for c, sql in checks.items()}
            res["input_rows"] = n_rows
            res["duplicate_ids"] = n_rows - n_ids
            for col in PRODUCT_COLUMNS[name][entity].split(", "):
                if col in WRITTEN_COLUMNS:
                    res[f"{col}_changing"] = _one(
                        con,
                        f"SELECT count(*) FROM {entity} d JOIN {t} i USING ({key}) "
                        f"WHERE d.{col} IS DISTINCT FROM i.{col}",
                    )[0]
            report[f"{name}/{entity}"] = res
            for c, n in res.items():
                if n and c not in informational:
                    errors.append(f"{name}/{entity}: {c} = {n}")
    if errors:
        raise ApplyError("Input validation failed:\n  - " + "\n  - ".join(errors))
    return report


def build_stage(con) -> dict:
    stats = {}
    for entity, key in ENTITIES:
        con.execute(
            f"CREATE OR REPLACE TEMP TABLE _stage_{entity} AS "
            f"SELECT n.{key}, n.region, "
            "least(greatest(n.facc, 1.0), greatest(n.facc_total, 1.0)) AS facc, "
            "greatest(n.facc_total, 1.0) AS facc_total, m.facc_flag, m.facc_quality "
            f"FROM _in_numeric_{entity} n JOIN _in_metadata_{entity} m USING ({key})"
        )
        rows, snapped = _one(
            con,
            f"SELECT count(*), count(*) FILTER (WHERE s.facc <> i.facc "
            f"OR s.facc_total <> i.facc_total) FROM _stage_{entity} s "
            f"JOIN _in_numeric_{entity} i USING ({key})",
        )
        stats[entity] = {
            "rows": rows,
            "snapped_to_bounds": snapped,
            "by_flag": dict(
                con.execute(
                    f"SELECT facc_flag, count(*) FROM _stage_{entity} GROUP BY 1 ORDER BY 1"
                ).fetchall()
            ),
        }
    return stats


def write_backup_files(con, backup_dir: Path, regions: list[str], dry_run: bool) -> dict:
    out = {}
    cols = ", ".join(WRITTEN_COLUMNS)
    for entity, key in ENTITIES:
        for region in regions:
            path = backup_dir / f"facc_before_release_20261027_{entity}_{region}.parquet"
            if path.exists():
                bad = _one(
                    con,
                    f"""
                    SELECT count(*) FROM (SELECT * FROM {entity} WHERE region = '{region}') d
                    FULL OUTER JOIN read_parquet({sql_path(path)}) b USING ({key})
                    WHERE d.{key} IS NULL OR b.{key} IS NULL OR
                    """
                    + " OR ".join(f"d.{c} IS DISTINCT FROM b.{c}" for c in WRITTEN_COLUMNS),
                )[0]
                if bad:
                    raise ApplyError(f"Backup {path} exists but differs from the DB on {bad} rows")
                out[path.name] = {"status": "verified_existing", "sha256": sha256_file(path)}
            elif dry_run:
                out[path.name] = {"status": "would_create"}
            else:
                backup_dir.mkdir(parents=True, exist_ok=True)
                tmp = path.with_suffix(".parquet.tmp")
                con.execute(
                    f"COPY (SELECT {key}, region, {cols} FROM {entity} "
                    f"WHERE region = '{region}' ORDER BY {key}) "
                    f"TO {sql_path(tmp)} (FORMAT PARQUET, COMPRESSION ZSTD)"
                )
                tmp.rename(path)
                out[path.name] = {"status": "created", "sha256": sha256_file(path)}
    return out


def verify_apply(con, counts_before: dict, indexes_before: list, stage: dict) -> dict:
    out: dict = {}
    for entity, key in ENTITIES:
        n = _one(con, f"SELECT count(*) FROM {entity}")[0]
        if n != counts_before[entity]:
            raise ApplyError(f"{entity}: row count changed {counts_before[entity]} -> {n}")
        diff = _one(
            con,
            f"SELECT count(*) FROM {entity} d FULL OUTER JOIN _stage_{entity} s USING ({key}) "
            f"WHERE d.{key} IS NULL OR s.{key} IS NULL OR "
            + " OR ".join(f"d.{c} IS DISTINCT FROM s.{c}" for c in WRITTEN_COLUMNS),
        )[0]
        if diff:
            raise ApplyError(f"{entity}: {diff} rows differ from the staged values")
        by_flag = dict(
            con.execute(f"SELECT facc_flag, count(*) FROM {entity} GROUP BY 1 ORDER BY 1").fetchall()
        )
        if by_flag != stage[entity]["by_flag"]:
            raise ApplyError(f"{entity}: label counts {by_flag} != staged {stage[entity]['by_flag']}")
        out[entity] = {"rows": n, "staged_mismatches": diff, "by_flag": by_flag}
    if sorted(table_indexes(con)) != sorted(indexes_before):
        raise ApplyError("Index set changed during the write")
    return out


def do_apply(args) -> dict:
    t0 = time.time()
    config, products = load_release(Path(args.release_config), Path(args.data_root))
    con = duckdb.connect(str(args.db), read_only=args.dry_run)
    try:
        con.execute("INSTALL spatial; LOAD spatial;")
        regions = db_regions(con)
        if sorted(regions) != sorted(REGIONS):
            raise ApplyError(f"Expected all six regions in the DB, found {regions}")
        log(f"Database {args.db} ({'read-only' if args.dry_run else 'read-write'})")
        file_meta = stage_inputs(con, products)
        validation = validate_inputs(con)
        log("Input valid; staging")
        stage = build_stage(con)
        counts_before = {e: _one(con, f"SELECT count(*) FROM {e}")[0] for e, _ in ENTITIES}
        indexes_before = table_indexes(con)
        backups = write_backup_files(con, Path(args.backup_dir), regions, args.dry_run)
        summary = {
            "mode": "dry-run" if args.dry_run else "apply",
            "db": str(args.db),
            "release": config["id"],
            "inputs": file_meta,
            "validation": validation,
            "stage": stage,
            "backup_files": backups,
        }
        if args.dry_run:
            summary["elapsed_s"] = round(time.time() - t0, 1)
            return summary

        def details_for(region: str) -> dict:
            return {
                "release": config["id"],
                "release_config": str(Path(args.release_config).resolve()),
                "products": {
                    name: {
                        "manifest": config["products"][name]["manifest"],
                        "manifest_sha256": config["products"][name]["manifest_sha256"],
                        "files": {
                            f["name"]: file_meta[f"{name}/{f['name']}"]
                            for f in manifest["files"]
                            if f["region"] == region
                        },
                    }
                    for name, (manifest, _dir) in products.items()
                },
                "quality_vocabulary": f"{QUALITY_PREFIX}:<facc_source>",
                "snapped_to_bounds": {e: stage[e]["snapped_to_bounds"] for e, _ in ENTITIES},
                "backup_files": {
                    k: v for k, v in backups.items() if k.endswith(f"_{region}.parquet")
                },
            }

        reason = (
            f"{config['id']}: conservative bifurcation facc, unsplit facc_total propagation, "
            "and reassessed facc_flag/facc_quality from config/facc_adopted.json"
        )
        p0014.WRITTEN_COLUMNS[:] = WRITTEN_COLUMNS
        p0014.USER_ID = "jake"
        p0014.SESSION_ID = config["id"]
        op_ids = p0014.run_write(con, regions, "_stage_", OPERATION_KIND, reason, details_for)
        summary["operation_ids"] = op_ids
        summary["verification"] = verify_apply(con, counts_before, indexes_before, stage)
        summary["elapsed_s"] = round(time.time() - t0, 1)
        return summary
    finally:
        con.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--db", required=True)
    parser.add_argument(
        "--data-root",
        required=True,
        help="Checkout holding the product directories (config paths are relative to it)",
    )
    parser.add_argument("--release-config", required=True)
    parser.add_argument("--backup-dir", required=True)
    parser.add_argument("--dry-run", action="store_true", help="Validate and report only")
    args = parser.parse_args()
    try:
        guard_paths(
            Path(args.db),
            Path(args.data_root) / "output" / "facc_detection",
            Path(args.backup_dir),
        )
        check_no_other_holders(Path(args.db))
        summary = do_apply(args)
    except ApplyError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(summary, indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
