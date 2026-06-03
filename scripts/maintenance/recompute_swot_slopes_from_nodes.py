"""Recompute reach SWOT slope observations from node-level RiverSP WSE.

The older v17c slope aggregation used already-signed RiverSP reach slopes.
Those signs can inherit the pre-0.0.11 NetCDF node order convention.  This
script recomputes reach slope observations from node-level smoothed WSE
(``wse_sm`` when available) joined to the current SWORD nodes table, using
current ``nodes.dist_out`` as the regression axis.  Since ``dist_out``
increases upstream in v17c beta 0.0.12, positive slopes mean upstream WSE is
higher than downstream WSE.

Dry run by default.  Pass ``--apply`` to update ``reaches.slope_obs_*``.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

import duckdb

from sword_duckdb.swot_filters import SLOPE_REF_UNCERTAINTY, build_node_filter_sql

DB_PATH = Path("/Users/jakegearon/projects/SWORD/data/duckdb/sword_v17c.duckdb")
SWOT_NODE_DIR = Path("/Volumes/SWORD_DATA/data/swot/RiverSP_D_parq/node")
OUTPUT_DIR = Path("/Users/jakegearon/projects/SWORD/outputs")
REGIONS = ("AF", "AS", "EU", "NA", "OC", "SA")
SWOT_CONTINENTS = ("AF", "AR", "AS", "AU", "EU", "GR", "NA", "SA", "SI")

NODE_RANGES = {
    "AF": (11000000000000, 19999999999999),
    "EU": (21000000000000, 29999999999999),
    "AS": (31000000000000, 49999999999999),
    "OC": (51000000000000, 59999999999999),
    "SA": (61000000000000, 69999999999999),
    "NA": (71000000000000, 99999999999999),
}

SLOPE_COLUMNS = (
    "slope_obs_p10",
    "slope_obs_p20",
    "slope_obs_p30",
    "slope_obs_p40",
    "slope_obs_p50",
    "slope_obs_p60",
    "slope_obs_p70",
    "slope_obs_p80",
    "slope_obs_p90",
    "slope_obs_range",
    "slope_obs_mad",
    "slope_obs_n",
    "slope_obs_n_passes",
    "slope_obs_q",
    "slope_obs_adj",
    "slope_obs_slopeF",
    "slope_obs_reliable",
    "slope_obs_quality",
)


@dataclass
class RegionSummary:
    region: str
    source_files_checked: int = 0
    pass_slopes: int = 0
    reaches_with_new_slopes: int = 0
    reaches_with_current_slopes: int = 0
    p50_changed: int = 0
    p50_sign_changed: int = 0
    current_negative_p50: int = 0
    new_negative_p50: int = 0
    updated_reaches: int = 0
    elapsed_seconds: float = 0.0


@dataclass
class RunSummary:
    db: str
    swot_node_dir: str
    apply: bool
    regions: list[str]
    reach_ids: list[int] = field(default_factory=list)
    min_nodes_per_pass: int = 3
    min_dist_range_m: float = 100.0
    max_abs_slope: float = 0.1
    operation_id: int | None = None
    started_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    regions_summary: list[RegionSummary] = field(default_factory=list)


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def fetchone_int(cursor: duckdb.DuckDBPyConnection) -> int:
    row = cursor.fetchone()
    if row is None:
        raise RuntimeError("Expected DuckDB query to return one row")
    return int(row[0])


def detect_columns(con: duckdb.DuckDBPyConnection, files: list[Path]) -> set[str]:
    if not files:
        return set()
    sample = files[0]
    return set(
        c.lower()
        for c in con.execute(f"SELECT * FROM read_parquet('{sample}') LIMIT 1")
        .fetchdf()
        .columns.tolist()
    )


def node_wse_column(colnames: set[str]) -> str:
    """Prefer RiverSP smoothed node WSE for along-reach slope fitting."""
    if "wse_sm" in colnames:
        return "wse_sm"
    if "wse" in colnames:
        return "wse"
    raise RuntimeError("SWOT node parquet must contain wse_sm or wse")


def find_node_files(node_dir: Path) -> list[Path]:
    if not node_dir.exists():
        raise FileNotFoundError(f"SWOT node directory not found: {node_dir}")
    files = sorted(
        p for p in node_dir.rglob("*.parquet") if not p.name.startswith("._")
    )
    if not files:
        raise FileNotFoundError(f"No parquet files found under {node_dir}")
    return files


def source_expr(colnames: set[str], *candidates: str) -> str | None:
    present = [name for name in candidates if name.lower() in colnames]
    if not present:
        return None
    if len(present) == 1:
        return quote_ident(present[0])
    return "COALESCE(" + ", ".join(quote_ident(name) for name in present) + ")"


def pass_key_expr(colnames: set[str]) -> str:
    cycle = source_expr(colnames, "cycle")
    swot_pass = source_expr(colnames, "pass", "pass_id", "pass_tile")
    time_str = source_expr(colnames, "time_str")
    if cycle and swot_pass:
        return (
            "COALESCE(CAST("
            + cycle
            + " AS VARCHAR), '') || ':' || COALESCE(CAST("
            + swot_pass
            + " AS VARCHAR), '')"
        )
    if time_str:
        return "CAST(" + time_str + " AS VARCHAR)"
    if swot_pass:
        return "CAST(" + swot_pass + " AS VARCHAR)"
    return "'all_observations'"


def register_target_reaches(
    con: duckdb.DuckDBPyConnection, reach_ids: list[int]
) -> None:
    con.execute("DROP TABLE IF EXISTS _target_reaches")
    if not reach_ids:
        return
    con.execute("CREATE TEMP TABLE _target_reaches(reach_id BIGINT)")
    con.executemany(
        "INSERT INTO _target_reaches VALUES (?)",
        [(int(reach_id),) for reach_id in reach_ids],
    )


def prepare_region_nodes(
    con: duckdb.DuckDBPyConnection, region: str, reach_ids: list[int]
) -> None:
    con.execute("DROP TABLE IF EXISTS _sword_nodes")
    target_join = (
        "JOIN _target_reaches tr ON n.reach_id = tr.reach_id" if reach_ids else ""
    )
    con.execute(
        f"""
        CREATE TEMP TABLE _sword_nodes AS
        SELECT n.node_id, n.reach_id, n.region, n.dist_out
        FROM nodes n
        {target_join}
        WHERE n.region = ?
          AND n.dist_out IS NOT NULL
        """,
        [region],
    )
    con.execute("CREATE INDEX _sword_nodes_node_idx ON _sword_nodes(node_id)")


def create_pass_slope_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("DROP TABLE IF EXISTS _pass_slopes")
    con.execute(
        """
        CREATE TEMP TABLE _pass_slopes (
            reach_id BIGINT,
            pass_key VARCHAR,
            n_nodes INTEGER,
            slope DOUBLE
        )
        """
    )


def parquet_globs(node_dir: Path) -> list[str]:
    globs = [
        str(node_dir / f"SWOT*_{continent}_*.parquet") for continent in SWOT_CONTINENTS
    ]
    direct_files = sorted(
        p for p in node_dir.glob("*.parquet") if not p.name.startswith("._")
    )
    if direct_files:
        globs.append(str(node_dir / "*.parquet"))
    return globs


def insert_pass_slopes(
    con: duckdb.DuckDBPyConnection,
    node_dir: Path,
    colnames: set[str],
    region: str,
    chunk_idx: int,
    n_chunks: int,
    min_nodes_per_pass: int,
    min_dist_range_m: float,
    max_abs_slope: float,
) -> int:
    id_min, id_max = NODE_RANGES[region]
    chunk_size = (id_max - id_min + 1) // n_chunks
    sub_min = id_min + chunk_idx * chunk_size
    sub_max = id_min + (chunk_idx + 1) * chunk_size - 1
    if chunk_idx == n_chunks - 1:
        sub_max = id_max

    where_clause, _wse_col = build_node_filter_sql(colnames)
    wse_col = node_wse_column(colnames)
    wse = quote_ident(wse_col)
    pass_key = pass_key_expr(colnames)
    reach_col = source_expr(colnames, "reach_id")
    node_col = source_expr(colnames, "node_id")
    if reach_col is None or node_col is None:
        raise RuntimeError(
            "SWOT node parquet must contain reach_id and node_id columns"
        )

    total_inserted = 0
    for glob in parquet_globs(node_dir):
        try:
            cursor = con.execute(
                f"""
                INSERT INTO _pass_slopes
                WITH filtered AS (
                    SELECT
                        TRY_CAST({reach_col} AS BIGINT) AS reach_id,
                        TRY_CAST({node_col} AS BIGINT) AS node_id,
                        CAST({wse} AS DOUBLE) AS wse,
                        {pass_key} AS pass_key
                    FROM read_parquet('{glob}', union_by_name=true)
                    WHERE TRY_CAST({node_col} AS BIGINT) BETWEEN {sub_min} AND {sub_max}
                      AND {where_clause}
                ),
                joined AS (
                    SELECT s.reach_id, f.pass_key, s.dist_out, f.wse
                    FROM filtered f
                    JOIN _sword_nodes s
                      ON f.node_id = s.node_id
                     AND f.reach_id = s.reach_id
                    WHERE f.wse IS NOT NULL
                      AND isfinite(f.wse)
                ),
                pass_fit AS (
                    SELECT
                        reach_id,
                        pass_key,
                        COUNT(*) AS n_nodes,
                        MAX(dist_out) - MIN(dist_out) AS dist_range_m,
                        REGR_SLOPE(wse, dist_out) AS slope
                    FROM joined
                    GROUP BY reach_id, pass_key
                )
                SELECT reach_id, pass_key, CAST(n_nodes AS INTEGER), slope
                FROM pass_fit
                WHERE n_nodes >= {min_nodes_per_pass}
                  AND dist_range_m >= {min_dist_range_m}
                  AND slope IS NOT NULL
                  AND isfinite(slope)
                  AND ABS(slope) <= {max_abs_slope}
                """
            )
        except duckdb.IOException:
            continue
        total_inserted += fetchone_int(cursor)
    return total_inserted


def aggregate_pass_slopes(con: duckdb.DuckDBPyConnection) -> None:
    ref_u = SLOPE_REF_UNCERTAINTY
    con.execute("DROP TABLE IF EXISTS _slope_agg")
    con.execute(
        f"""
        CREATE TEMP TABLE _slope_agg AS
        WITH pct AS (
            SELECT
                reach_id,
                QUANTILE_CONT(slope, 0.1) AS slope_obs_p10,
                QUANTILE_CONT(slope, 0.2) AS slope_obs_p20,
                QUANTILE_CONT(slope, 0.3) AS slope_obs_p30,
                QUANTILE_CONT(slope, 0.4) AS slope_obs_p40,
                QUANTILE_CONT(slope, 0.5) AS slope_obs_p50,
                QUANTILE_CONT(slope, 0.6) AS slope_obs_p60,
                QUANTILE_CONT(slope, 0.7) AS slope_obs_p70,
                QUANTILE_CONT(slope, 0.8) AS slope_obs_p80,
                QUANTILE_CONT(slope, 0.9) AS slope_obs_p90,
                MAX(slope) - MIN(slope) AS slope_obs_range,
                CAST(SUM(n_nodes) AS INTEGER) AS slope_obs_n,
                CAST(COUNT(*) AS INTEGER) AS slope_obs_n_passes,
                SUM(n_nodes * CASE WHEN slope > 0 THEN 1 WHEN slope < 0 THEN -1 ELSE 0 END)
                    / NULLIF(SUM(n_nodes), 0) AS slope_obs_slopeF
            FROM _pass_slopes
            GROUP BY reach_id
        )
        SELECT
            reach_id,
            slope_obs_p10,
            slope_obs_p20,
            slope_obs_p30,
            slope_obs_p40,
            slope_obs_p50,
            slope_obs_p60,
            slope_obs_p70,
            slope_obs_p80,
            slope_obs_p90,
            slope_obs_range,
            (slope_obs_p80 - slope_obs_p20) * 0.4010 AS slope_obs_mad,
            slope_obs_n,
            slope_obs_n_passes,
            (CASE WHEN slope_obs_p50 < -{ref_u} THEN 1 ELSE 0 END)
            + (CASE WHEN slope_obs_n_passes < 10 THEN 2 ELSE 0 END)
            + (CASE WHEN (slope_obs_p80 - slope_obs_p20) * 0.4010 > 2 * ABS(slope_obs_p50) THEN 4 ELSE 0 END)
            + (CASE WHEN ABS(slope_obs_p50) > 0.05 THEN 8 ELSE 0 END)
            + (CASE WHEN ABS(slope_obs_p50) <= {ref_u} THEN 16 ELSE 0 END)
                AS slope_obs_q,
            GREATEST(slope_obs_p50, 0.0) AS slope_obs_adj,
            slope_obs_slopeF,
            CASE WHEN ABS(slope_obs_slopeF) > 0.5
                   AND ABS(slope_obs_p50) > {ref_u}
                 THEN TRUE ELSE FALSE END AS slope_obs_reliable,
            CASE
                WHEN slope_obs_p50 < -{ref_u} THEN 'negative'
                WHEN ABS(slope_obs_p50) <= {ref_u} THEN 'below_ref_uncertainty'
                WHEN ABS(slope_obs_slopeF) <= 0.5 THEN 'high_uncertainty'
                ELSE 'reliable'
            END AS slope_obs_quality
        FROM pct
        """
    )


def summarize_region(con: duckdb.DuckDBPyConnection, region: str) -> RegionSummary:
    row = con.execute(
        """
        WITH current AS (
            SELECT r.region, r.reach_id, r.slope_obs_p50 AS current_p50
            FROM reaches r
            WHERE r.region = ?
        ),
        joined AS (
            SELECT c.current_p50, a.slope_obs_p50 AS new_p50
            FROM current c
            FULL OUTER JOIN _slope_agg a USING (reach_id)
        )
        SELECT
            (SELECT COUNT(*) FROM _pass_slopes) AS pass_slopes,
            (SELECT COUNT(*) FROM _slope_agg) AS reaches_with_new_slopes,
            COUNT(*) FILTER (WHERE current_p50 IS NOT NULL) AS reaches_with_current_slopes,
            COUNT(*) FILTER (
                WHERE current_p50 IS DISTINCT FROM new_p50
                  AND new_p50 IS NOT NULL
            ) AS p50_changed,
            COUNT(*) FILTER (
                WHERE current_p50 IS NOT NULL
                  AND new_p50 IS NOT NULL
                  AND SIGN(current_p50) IS DISTINCT FROM SIGN(new_p50)
            ) AS p50_sign_changed,
            COUNT(*) FILTER (WHERE current_p50 < 0) AS current_negative_p50,
            COUNT(*) FILTER (WHERE new_p50 < 0) AS new_negative_p50
        FROM joined
        """,
        [region],
    ).fetchone()
    if row is None:
        raise RuntimeError("Region summary query returned no row")
    return RegionSummary(
        region=region,
        pass_slopes=int(row[0] or 0),
        reaches_with_new_slopes=int(row[1] or 0),
        reaches_with_current_slopes=int(row[2] or 0),
        p50_changed=int(row[3] or 0),
        p50_sign_changed=int(row[4] or 0),
        current_negative_p50=int(row[5] or 0),
        new_negative_p50=int(row[6] or 0),
    )


def apply_region_update(
    con: duckdb.DuckDBPyConnection,
    region: str,
    operation_id: int,
) -> int:
    con.execute("INSTALL spatial; LOAD spatial;")
    rtrees = con.execute(
        """
        SELECT index_name, table_name, sql
        FROM duckdb_indexes()
        WHERE sql LIKE '%RTREE%'
          AND table_name = 'reaches'
        """
    ).fetchall()
    for index_name, _table_name, _sql in rtrees:
        con.execute(f'DROP INDEX IF EXISTS "{index_name}"')

    try:
        con.execute("BEGIN TRANSACTION")
        cursor = con.execute(
            """
            UPDATE reaches
            SET slope_obs_p10 = a.slope_obs_p10,
                slope_obs_p20 = a.slope_obs_p20,
                slope_obs_p30 = a.slope_obs_p30,
                slope_obs_p40 = a.slope_obs_p40,
                slope_obs_p50 = a.slope_obs_p50,
                slope_obs_p60 = a.slope_obs_p60,
                slope_obs_p70 = a.slope_obs_p70,
                slope_obs_p80 = a.slope_obs_p80,
                slope_obs_p90 = a.slope_obs_p90,
                slope_obs_range = a.slope_obs_range,
                slope_obs_mad = a.slope_obs_mad,
                slope_obs_n = a.slope_obs_n,
                slope_obs_n_passes = a.slope_obs_n_passes,
                slope_obs_q = a.slope_obs_q,
                slope_obs_adj = a.slope_obs_adj,
                slope_obs_slopeF = a.slope_obs_slopeF,
                slope_obs_reliable = a.slope_obs_reliable,
                slope_obs_quality = a.slope_obs_quality
            FROM _slope_agg a
            WHERE reaches.reach_id = a.reach_id
              AND reaches.region = ?
            """,
            [region],
        )
        updated = fetchone_int(cursor)
        con.execute(
            """
            UPDATE sword_operations
            SET entity_ids = list_concat(entity_ids, (
                    SELECT LIST(reach_id ORDER BY reach_id) FROM _slope_agg
                ))
            WHERE operation_id = ?
            """,
            [operation_id],
        )
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    finally:
        for _index_name, _table_name, sql in rtrees:
            con.execute(sql)
    return updated


def create_operation(
    con: duckdb.DuckDBPyConnection,
    summary: RunSummary,
    output_path: Path,
) -> int:
    operation_id = fetchone_int(
        con.execute("SELECT COALESCE(MAX(operation_id), 0) + 1 FROM sword_operations")
    )
    started = datetime.now(timezone.utc).replace(tzinfo=None)
    details = {
        "operation_kind": "SWOT_SLOPE_OBS_FROM_NODE_WSE",
        "release": "v17c-0.0.12",
        "swot_node_dir": summary.swot_node_dir,
        "regions": summary.regions,
        "reach_ids": summary.reach_ids,
        "min_nodes_per_pass": summary.min_nodes_per_pass,
        "min_dist_range_m": summary.min_dist_range_m,
        "max_abs_slope": summary.max_abs_slope,
        "dry_run_summary": str(output_path),
    }
    con.execute(
        """
        INSERT INTO sword_operations (
            operation_id, operation_type, table_name, entity_ids, region,
            user_id, session_id, started_at, operation_details,
            affected_columns, reason, source_operation_id, status
        )
        VALUES (?, 'UPDATE', 'reaches', [], 'ALL', 'jake',
                'v17c_0_0_12_swot_slope_obs_from_nodes', ?, ?,
                ?, ?,
                NULL, 'PENDING')
        """,
        [
            operation_id,
            started,
            json.dumps(details),
            [f"reaches.{column}" for column in SLOPE_COLUMNS],
            "v17c-0.0.12 recompute SWOT slope_obs from node-level RiverSP WSE "
            "using current SWORD node dist_out",
        ],
    )
    return operation_id


def complete_operation(
    con: duckdb.DuckDBPyConnection, operation_id: int, output_path: Path
) -> None:
    completed = datetime.now(timezone.utc).replace(tzinfo=None)
    row = con.execute(
        "SELECT operation_details FROM sword_operations WHERE operation_id = ?",
        [operation_id],
    ).fetchone()
    details = json.loads(row[0] or "{}") if row else {}
    details["completed_summary"] = str(output_path)
    con.execute(
        """
        UPDATE sword_operations
        SET completed_at = ?,
            status = 'COMPLETED',
            operation_details = ?
        WHERE operation_id = ?
        """,
        [completed, json.dumps(details), operation_id],
    )


def write_summary(summary: RunSummary, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    suffix = "apply" if summary.apply else "dry_run"
    output_path = output_dir / f"swot_slope_node_recompute_{suffix}_{stamp}.json"
    output_path.write_text(json.dumps(asdict(summary), indent=2) + "\n")
    return output_path


def run(args: argparse.Namespace) -> RunSummary:
    node_dir = Path(args.swot_node_dir)
    files = find_node_files(node_dir)
    regions = [args.region] if args.region else list(REGIONS)
    reach_ids = [int(reach_id) for reach_id in args.reach_id]
    summary = RunSummary(
        db=str(Path(args.db)),
        swot_node_dir=str(node_dir),
        apply=args.apply,
        regions=regions,
        reach_ids=reach_ids,
        min_nodes_per_pass=args.min_nodes_per_pass,
        min_dist_range_m=args.min_dist_range_m,
        max_abs_slope=args.max_abs_slope,
    )

    con = duckdb.connect(str(args.db))
    try:
        con.execute(f"SET threads={args.threads}")
        con.execute(f"SET memory_limit='{args.memory_limit}'")
        con.execute("SET preserve_insertion_order=false")
        if args.temp_directory:
            con.execute(f"SET temp_directory='{args.temp_directory}'")

        colnames = detect_columns(con, files)
        if "node_id" not in colnames or "reach_id" not in colnames:
            raise RuntimeError("SWOT node files must include node_id and reach_id")
        register_target_reaches(con, reach_ids)
        operation_id = None

        for region in regions:
            t0 = time.monotonic()
            print(f"\n=== {region} ===", flush=True)
            prepare_region_nodes(con, region, reach_ids)
            create_pass_slope_tables(con)
            inserted_total = 0
            for chunk_idx in range(args.chunks):
                inserted = insert_pass_slopes(
                    con,
                    node_dir,
                    colnames,
                    region,
                    chunk_idx,
                    args.chunks,
                    args.min_nodes_per_pass,
                    args.min_dist_range_m,
                    args.max_abs_slope,
                )
                inserted_total += inserted
                print(
                    f"  chunk {chunk_idx + 1}/{args.chunks}: "
                    f"{inserted_total:,} pass slopes",
                    flush=True,
                )

            aggregate_pass_slopes(con)
            region_summary = summarize_region(con, region)
            region_summary.source_files_checked = len(files)
            region_summary.elapsed_seconds = round(time.monotonic() - t0, 3)

            if args.apply:
                if operation_id is None:
                    dry_summary_path = write_summary(summary, Path(args.output_dir))
                    operation_id = create_operation(con, summary, dry_summary_path)
                    summary.operation_id = operation_id
                region_summary.updated_reaches = apply_region_update(
                    con, region, operation_id
                )
                con.execute("CHECKPOINT")

            summary.regions_summary.append(region_summary)
            print(json.dumps(asdict(region_summary), indent=2), flush=True)

        output_path = write_summary(summary, Path(args.output_dir))
        if args.apply and summary.operation_id is not None:
            complete_operation(con, summary.operation_id, output_path)
            con.execute("CHECKPOINT")
        print(f"\nSummary: {output_path}", flush=True)
        return summary
    finally:
        con.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Recompute reach slope_obs_* from node-level RiverSP WSE."
    )
    parser.add_argument("--db", type=Path, default=DB_PATH)
    parser.add_argument("--swot-node-dir", type=Path, default=SWOT_NODE_DIR)
    parser.add_argument("--region", choices=REGIONS)
    parser.add_argument("--reach-id", type=int, action="append", default=[])
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--chunks", type=int, default=27)
    parser.add_argument("--min-nodes-per-pass", type=int, default=3)
    parser.add_argument("--min-dist-range-m", type=float, default=100.0)
    parser.add_argument("--max-abs-slope", type=float, default=0.1)
    parser.add_argument("--threads", type=int, default=2)
    parser.add_argument("--memory-limit", default="32GB")
    parser.add_argument("--temp-directory")
    parser.add_argument("--output-dir", type=Path, default=OUTPUT_DIR)
    return parser.parse_args()


if __name__ == "__main__":
    try:
        run(parse_args())
    except (FileNotFoundError, RuntimeError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
