"""
SWORD Lint - v17c-Specific Checks (V0xx)

Validates v17c new attributes: hydro_dist_out, is_mainstem_edge,
best_headwater, best_outlet, pathlen_hw, pathlen_out.
"""

from typing import Optional

import duckdb
import pandas as pd

from ..core import (
    register_check,
    Category,
    Severity,
    CheckResult,
)


@register_check(
    "V001",
    Category.V17C,
    Severity.ERROR,
    "hydro_dist_out must decrease downstream (min of all downstream neighbors)",
    default_threshold=100.0,  # tolerance in meters
)
def check_hydro_dist_out_monotonicity(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """
    Check that hydro_dist_out decreases downstream.

    Like T001 for dist_out, but for the v17c hydrologic distance metric.
    hydro_dist_out is computed via Dijkstra from all outlets.

    For reaches with multiple downstream neighbors (bifurcations), at least
    one path should lead to a nearer outlet. We check the MINIMUM downstream
    hydro_dist_out, which handles multi-outlet networks correctly.
    """
    tolerance = threshold if threshold is not None else 100.0
    where_clause = f"AND r1.region = '{region}'" if region else ""

    # Check if column exists
    try:
        conn.execute("SELECT hydro_dist_out FROM reaches LIMIT 1")
    except duckdb.CatalogException:
        return CheckResult(
            check_id="V001",
            name="hydro_dist_out_monotonicity",
            severity=Severity.ERROR,
            passed=True,
            total_checked=0,
            issues_found=0,
            issue_pct=0,
            details=pd.DataFrame(),
            description="Column hydro_dist_out not found (v17c pipeline not run)",
        )

    # Check MINIMUM downstream hydro_dist_out - at bifurcations, one path may go
    # to a more distant outlet, which is expected network behavior
    query = f"""
    WITH min_downstream AS (
        SELECT
            r1.reach_id,
            r1.region,
            r1.hydro_dist_out as dist_up,
            MIN(r2.hydro_dist_out) as min_dist_down,
            r1.river_name,
            r1.x, r1.y,
            r1.n_rch_down
        FROM reaches r1
        JOIN reach_topology rt ON r1.reach_id = rt.reach_id AND r1.region = rt.region
        JOIN reaches r2 ON rt.neighbor_reach_id = r2.reach_id AND rt.region = r2.region
        WHERE rt.direction = 'down'
            AND r1.hydro_dist_out IS NOT NULL
            AND r2.hydro_dist_out IS NOT NULL
            {where_clause}
        GROUP BY r1.reach_id, r1.region, r1.hydro_dist_out, r1.river_name, r1.x, r1.y, r1.n_rch_down
    )
    SELECT
        reach_id, region, river_name, x, y,
        dist_up, min_dist_down,
        (min_dist_down - dist_up) as dist_increase,
        n_rch_down
    FROM min_downstream
    WHERE min_dist_down > dist_up + {tolerance}
    ORDER BY dist_increase DESC
    """

    issues = conn.execute(query).fetchdf()

    total_query = f"""
    SELECT COUNT(*) FROM reaches r1
    WHERE hydro_dist_out IS NOT NULL
    {where_clause.replace("r1.", "")}
    """
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="V001",
        name="hydro_dist_out_monotonicity",
        severity=Severity.ERROR,
        passed=len(issues) == 0,
        total_checked=total,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total if total > 0 else 0,
        details=issues,
        description="Reaches where min(downstream hydro_dist_out) increases (flow direction error)",
        threshold=tolerance,
    )


@register_check(
    "V002",
    Category.V17C,
    Severity.INFO,
    "hydro_dist_out vs pathlen_out difference tracking",
)
def check_hydro_dist_vs_pathlen(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """
    Track difference between hydro_dist_out and pathlen_out.

    These use different algorithms:
    - hydro_dist_out: Dijkstra to ANY outlet
    - pathlen_out: Path to SPECIFIC best_outlet

    Differences are EXPECTED, this check documents them.
    """
    where_clause = f"AND r.region = '{region}'" if region else ""

    # Check if columns exist
    try:
        conn.execute("SELECT hydro_dist_out, pathlen_out FROM reaches LIMIT 1")
    except duckdb.CatalogException:
        return CheckResult(
            check_id="V002",
            name="hydro_dist_vs_pathlen",
            severity=Severity.INFO,
            passed=True,
            total_checked=0,
            issues_found=0,
            issue_pct=0,
            details=pd.DataFrame(),
            description="Columns not found (v17c pipeline not run)",
        )

    query = f"""
    SELECT
        r.reach_id, r.region, r.river_name, r.x, r.y,
        r.hydro_dist_out,
        r.pathlen_out,
        ABS(r.hydro_dist_out - r.pathlen_out) as diff,
        CASE
            WHEN r.hydro_dist_out > 0
            THEN 100.0 * ABS(r.hydro_dist_out - r.pathlen_out) / r.hydro_dist_out
            ELSE 0
        END as diff_pct
    FROM reaches r
    WHERE r.hydro_dist_out IS NOT NULL
        AND r.pathlen_out IS NOT NULL
        AND ABS(r.hydro_dist_out - r.pathlen_out) > 1000  -- >1km difference
        {where_clause}
    ORDER BY diff DESC
    LIMIT 1000
    """

    issues = conn.execute(query).fetchdf()

    total_query = f"""
    SELECT COUNT(*) FROM reaches r
    WHERE hydro_dist_out IS NOT NULL AND pathlen_out IS NOT NULL
    {where_clause}
    """
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="V002",
        name="hydro_dist_vs_pathlen",
        severity=Severity.INFO,
        passed=True,  # Informational, always passes
        total_checked=total,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total if total > 0 else 0,
        details=issues,
        description=f"Reaches with >1km difference between hydro_dist_out and pathlen_out ({len(issues)} found)",
    )


@register_check(
    "V004",
    Category.V17C,
    Severity.WARNING,
    "is_mainstem_edge continuity check",
)
def check_mainstem_continuity(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """
    Check that is_mainstem_edge forms continuous paths.

    Mainstem reaches should have at least one mainstem neighbor
    (except headwaters and outlets).
    """
    where_clause = f"AND r.region = '{region}'" if region else ""

    # Check if column exists
    try:
        conn.execute("SELECT is_mainstem_edge FROM reaches LIMIT 1")
    except duckdb.CatalogException:
        return CheckResult(
            check_id="V004",
            name="mainstem_continuity",
            severity=Severity.WARNING,
            passed=True,
            total_checked=0,
            issues_found=0,
            issue_pct=0,
            details=pd.DataFrame(),
            description="Column is_mainstem_edge not found (v17c pipeline not run)",
        )

    query = f"""
    WITH mainstem_neighbors AS (
        SELECT
            r.reach_id, r.region,
            SUM(CASE WHEN rt.direction = 'up' AND r2.is_mainstem_edge THEN 1 ELSE 0 END) as ms_up,
            SUM(CASE WHEN rt.direction = 'down' AND r2.is_mainstem_edge THEN 1 ELSE 0 END) as ms_down
        FROM reaches r
        JOIN reach_topology rt ON r.reach_id = rt.reach_id AND r.region = rt.region
        JOIN reaches r2 ON rt.neighbor_reach_id = r2.reach_id AND rt.region = r2.region
        WHERE r.is_mainstem_edge = TRUE
            {where_clause}
        GROUP BY r.reach_id, r.region
    )
    SELECT
        mn.reach_id, mn.region, r.river_name, r.x, r.y,
        r.n_rch_up, r.n_rch_down,
        mn.ms_up, mn.ms_down,
        CASE
            WHEN mn.ms_up = 0 AND r.n_rch_up > 0 THEN 'missing_upstream_mainstem'
            WHEN mn.ms_down = 0 AND r.n_rch_down > 0 THEN 'missing_downstream_mainstem'
            WHEN mn.ms_up = 0 AND mn.ms_down = 0 THEN 'isolated_mainstem'
        END as issue_type
    FROM mainstem_neighbors mn
    JOIN reaches r ON mn.reach_id = r.reach_id AND mn.region = r.region
    WHERE (mn.ms_up = 0 AND r.n_rch_up > 0)  -- Has upstream but no mainstem upstream
       OR (mn.ms_down = 0 AND r.n_rch_down > 0)  -- Has downstream but no mainstem downstream
    ORDER BY mn.reach_id
    """

    issues = conn.execute(query).fetchdf()

    total_query = f"""
    SELECT COUNT(*) FROM reaches r
    WHERE is_mainstem_edge = TRUE
    {where_clause}
    """
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="V004",
        name="mainstem_continuity",
        severity=Severity.WARNING,
        passed=len(issues) == 0,
        total_checked=total,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total if total > 0 else 0,
        details=issues,
        description="Mainstem reaches without continuous mainstem path",
    )


@register_check(
    "V005",
    Category.V17C,
    Severity.ERROR,
    "No NULL hydro_dist_out for connected reaches",
)
def check_hydro_dist_out_coverage(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """
    Check that all connected reaches have hydro_dist_out values.

    NULL hydro_dist_out indicates disconnected reaches or pipeline failure.
    """
    where_clause = f"AND r.region = '{region}'" if region else ""

    # Check if column exists
    try:
        conn.execute("SELECT hydro_dist_out FROM reaches LIMIT 1")
    except duckdb.CatalogException:
        return CheckResult(
            check_id="V005",
            name="hydro_dist_out_coverage",
            severity=Severity.ERROR,
            passed=True,
            total_checked=0,
            issues_found=0,
            issue_pct=0,
            details=pd.DataFrame(),
            description="Column hydro_dist_out not found (v17c pipeline not run)",
        )

    query = f"""
    SELECT
        r.reach_id, r.region, r.river_name, r.x, r.y,
        r.n_rch_up, r.n_rch_down, r.network, r.type
    FROM reaches r
    WHERE r.hydro_dist_out IS NULL
        AND (r.n_rch_up > 0 OR r.n_rch_down > 0)  -- Connected reach
        AND r.type NOT IN (5, 6)  -- Exclude unreliable/ghost
        {where_clause}
    ORDER BY r.reach_id
    """

    issues = conn.execute(query).fetchdf()

    total_query = f"""
    SELECT COUNT(*) FROM reaches r
    WHERE (n_rch_up > 0 OR n_rch_down > 0) AND type NOT IN (5, 6)
    {where_clause}
    """
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="V005",
        name="hydro_dist_out_coverage",
        severity=Severity.ERROR,
        passed=len(issues) == 0,
        total_checked=total,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total if total > 0 else 0,
        details=issues,
        description="Connected reaches missing hydro_dist_out (pipeline failure or disconnection)",
    )


@register_check(
    "V006",
    Category.V17C,
    Severity.INFO,
    "is_mainstem_edge coverage statistics",
)
def check_mainstem_coverage(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """
    Report is_mainstem_edge coverage statistics.

    Expected: 96-99% of reaches should be on mainstem paths.
    """
    where_clause = f"AND r.region = '{region}'" if region else ""

    # Check if column exists
    try:
        conn.execute("SELECT is_mainstem_edge FROM reaches LIMIT 1")
    except duckdb.CatalogException:
        return CheckResult(
            check_id="V006",
            name="mainstem_coverage",
            severity=Severity.INFO,
            passed=True,
            total_checked=0,
            issues_found=0,
            issue_pct=0,
            details=pd.DataFrame(),
            description="Column is_mainstem_edge not found (v17c pipeline not run)",
        )

    query = f"""
    SELECT
        region,
        COUNT(*) as total_reaches,
        SUM(CASE WHEN is_mainstem_edge = TRUE THEN 1 ELSE 0 END) as mainstem_reaches,
        ROUND(100.0 * SUM(CASE WHEN is_mainstem_edge = TRUE THEN 1 ELSE 0 END) / COUNT(*), 2) as mainstem_pct
    FROM reaches r
    WHERE type NOT IN (5, 6)
        {where_clause}
    GROUP BY region
    ORDER BY region
    """

    stats = conn.execute(query).fetchdf()

    total_query = f"""
    SELECT COUNT(*) FROM reaches r WHERE type NOT IN (5, 6) {where_clause}
    """
    total = conn.execute(total_query).fetchone()[0]

    mainstem_count = stats["mainstem_reaches"].sum() if len(stats) > 0 else 0
    mainstem_pct = 100 * mainstem_count / total if total > 0 else 0

    return CheckResult(
        check_id="V006",
        name="mainstem_coverage",
        severity=Severity.INFO,
        passed=True,  # Informational
        total_checked=total,
        issues_found=int(total - mainstem_count),
        issue_pct=100 - mainstem_pct,
        details=stats,
        description=f"Mainstem coverage: {mainstem_pct:.1f}% ({int(mainstem_count)}/{total} reaches)",
    )


@register_check(
    "V007",
    Category.V17C,
    Severity.WARNING,
    "best_headwater must be an actual headwater",
)
def check_best_headwater_validity(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """
    Check that best_headwater points to actual headwater reaches.

    A headwater has n_rch_up = 0 (no upstream neighbors).
    """
    where_clause = f"AND r.region = '{region}'" if region else ""

    # Check if column exists
    try:
        conn.execute("SELECT best_headwater FROM reaches LIMIT 1")
    except duckdb.CatalogException:
        return CheckResult(
            check_id="V007",
            name="best_headwater_validity",
            severity=Severity.WARNING,
            passed=True,
            total_checked=0,
            issues_found=0,
            issue_pct=0,
            details=pd.DataFrame(),
            description="Column best_headwater not found (v17c pipeline not run)",
        )

    query = f"""
    SELECT
        r.reach_id, r.region, r.river_name, r.x, r.y,
        r.best_headwater,
        hw.n_rch_up as hw_n_rch_up,
        hw.river_name as hw_river_name
    FROM reaches r
    JOIN reaches hw ON r.best_headwater = hw.reach_id AND r.region = hw.region
    WHERE r.best_headwater IS NOT NULL
        AND hw.n_rch_up > 0  -- Not actually a headwater
        {where_clause}
    ORDER BY hw.n_rch_up DESC
    """

    issues = conn.execute(query).fetchdf()

    total_query = f"""
    SELECT COUNT(*) FROM reaches r
    WHERE best_headwater IS NOT NULL
    {where_clause}
    """
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="V007",
        name="best_headwater_validity",
        severity=Severity.WARNING,
        passed=len(issues) == 0,
        total_checked=total,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total if total > 0 else 0,
        details=issues,
        description="Reaches where best_headwater is not an actual headwater (n_rch_up > 0)",
    )


@register_check(
    "V008",
    Category.V17C,
    Severity.WARNING,
    "best_outlet must be an actual outlet",
)
def check_best_outlet_validity(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """
    Check that best_outlet points to actual outlet reaches.

    An outlet has n_rch_down = 0 (no downstream neighbors).
    """
    where_clause = f"AND r.region = '{region}'" if region else ""

    # Check if column exists
    try:
        conn.execute("SELECT best_outlet FROM reaches LIMIT 1")
    except duckdb.CatalogException:
        return CheckResult(
            check_id="V008",
            name="best_outlet_validity",
            severity=Severity.WARNING,
            passed=True,
            total_checked=0,
            issues_found=0,
            issue_pct=0,
            details=pd.DataFrame(),
            description="Column best_outlet not found (v17c pipeline not run)",
        )

    query = f"""
    SELECT
        r.reach_id, r.region, r.river_name, r.x, r.y,
        r.best_outlet,
        out.n_rch_down as out_n_rch_down,
        out.river_name as out_river_name
    FROM reaches r
    JOIN reaches out ON r.best_outlet = out.reach_id AND r.region = out.region
    WHERE r.best_outlet IS NOT NULL
        AND out.n_rch_down > 0  -- Not actually an outlet
        {where_clause}
    ORDER BY out.n_rch_down DESC
    """

    issues = conn.execute(query).fetchdf()

    total_query = f"""
    SELECT COUNT(*) FROM reaches r
    WHERE best_outlet IS NOT NULL
    {where_clause}
    """
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="V008",
        name="best_outlet_validity",
        severity=Severity.WARNING,
        passed=len(issues) == 0,
        total_checked=total,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total if total > 0 else 0,
        details=issues,
        description="Reaches where best_outlet is not an actual outlet (n_rch_down > 0)",
    )


@register_check(
    "V011",
    Category.V17C,
    Severity.WARNING,
    "Unexpected river_name_local change along rch_id_dn_main chain on 1:1 links",
)
def check_osm_name_continuity(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """
    Flag reaches where river_name_local changes along rch_id_dn_main on 1:1 links.

    Name changes at junctions are expected — only flags 1:1 links where
    n_rch_down = 1 AND the downstream reach has n_rch_up = 1.
    """
    where_clause = f"AND r1.region = '{region}'" if region else ""

    # Column existence guard
    try:
        conn.execute("SELECT river_name_local, rch_id_dn_main FROM reaches LIMIT 1")
    except (duckdb.CatalogException, duckdb.BinderException):
        return CheckResult(
            check_id="V011",
            name="osm_name_continuity",
            severity=Severity.WARNING,
            passed=True,
            total_checked=0,
            issues_found=0,
            issue_pct=0,
            details=pd.DataFrame(),
            description="Column river_name_local or rch_id_dn_main not found (OSM enrichment or v17c pipeline not run)",
        )

    query = f"""
    SELECT
        r1.reach_id,
        r1.region,
        r1.x,
        r1.y,
        r1.river_name_local AS name_up,
        r2.river_name_local AS name_down,
        r1.rch_id_dn_main
    FROM reaches r1
    JOIN reaches r2
        ON r1.rch_id_dn_main = r2.reach_id
        AND r1.region = r2.region
    WHERE r1.river_name_local IS NOT NULL
        AND r2.river_name_local IS NOT NULL
        AND r1.river_name_local != r2.river_name_local
        AND r1.n_rch_down = 1
        AND r2.n_rch_up = 1
        {where_clause}
    ORDER BY r1.reach_id
    """

    issues = conn.execute(query).fetchdf()

    total_query = f"""
    SELECT COUNT(*) FROM reaches r1
    JOIN reaches r2
        ON r1.rch_id_dn_main = r2.reach_id
        AND r1.region = r2.region
    WHERE r1.river_name_local IS NOT NULL
        AND r2.river_name_local IS NOT NULL
        AND r1.n_rch_down = 1
        AND r2.n_rch_up = 1
        {where_clause}
    """
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="V011",
        name="osm_name_continuity",
        severity=Severity.WARNING,
        passed=len(issues) == 0,
        total_checked=total,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total if total > 0 else 0,
        details=issues,
        description="Reaches where river_name_local changes on 1:1 link (not at junction)",
    )


@register_check(
    "V013",
    Category.V17C,
    Severity.WARNING,
    "main_path_id must map to a single (best_headwater, best_outlet) tuple globally",
)
def check_main_path_id_global_uniqueness(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """
    Check that each main_path_id maps to exactly one (best_headwater, best_outlet) pair.

    Collisions indicate the per-region counter wasn't offset, so different
    networks in different regions share the same main_path_id.
    """
    # Check if column exists
    try:
        conn.execute("SELECT main_path_id FROM reaches LIMIT 1")
    except duckdb.CatalogException:
        return CheckResult(
            check_id="V013",
            name="main_path_id_global_uniqueness",
            severity=Severity.WARNING,
            passed=True,
            total_checked=0,
            issues_found=0,
            issue_pct=0,
            details=pd.DataFrame(),
            description="Column main_path_id not found (v17c pipeline not run)",
        )

    # When checking a single region, collisions won't appear (they're cross-region).
    # Still run the query — it acts as a within-region sanity check too.
    where_clause = f"WHERE r.region = '{region}'" if region else ""

    query = f"""
    SELECT
        main_path_id,
        COUNT(DISTINCT (best_headwater, best_outlet)) as n_tuples,
        COUNT(*) as n_reaches,
        ARRAY_AGG(DISTINCT region) as regions,
        ARRAY_AGG(DISTINCT best_headwater) as headwaters,
        ARRAY_AGG(DISTINCT best_outlet) as outlets
    FROM reaches r
    {where_clause}
    {"AND" if where_clause else "WHERE"} main_path_id IS NOT NULL
    GROUP BY main_path_id
    HAVING COUNT(DISTINCT (best_headwater, best_outlet)) > 1
    ORDER BY n_tuples DESC
    LIMIT 500
    """

    issues = conn.execute(query).fetchdf()

    total_query = f"""
    SELECT COUNT(DISTINCT main_path_id) FROM reaches r
    {where_clause}
    {"AND" if where_clause else "WHERE"} main_path_id IS NOT NULL
    """
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="V013",
        name="main_path_id_global_uniqueness",
        severity=Severity.WARNING,
        passed=len(issues) == 0,
        total_checked=total,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total if total > 0 else 0,
        details=issues,
        description=f"main_path_id values mapping to multiple (best_headwater, best_outlet) tuples ({len(issues)} collisions)",
    )
