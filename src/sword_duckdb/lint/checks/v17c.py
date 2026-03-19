"""
SWORD Lint - v17c-Specific Checks (V0xx)

Validates v17c new attributes: hydro_dist_out, is_mainstem,
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
    "hydro_dist_out vs (pathlen_out + reach_length) difference tracking",
)
def check_hydro_dist_vs_pathlen(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """
    Track difference between hydro_dist_out and (pathlen_out + reach_length).

    hydro_dist_out walks rch_id_dn_main accumulating reach_length (including
    self), so it should equal pathlen_out + reach_length for all reaches.
    Large differences indicate rch_id_dn_main chain diverges from the
    best_outlet path used by pathlen_out.
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
        r.reach_length,
        r.pathlen_out + r.reach_length as expected,
        ABS(r.hydro_dist_out - (r.pathlen_out + r.reach_length)) as diff,
        CASE
            WHEN r.hydro_dist_out > 0
            THEN 100.0 * ABS(r.hydro_dist_out - (r.pathlen_out + r.reach_length)) / r.hydro_dist_out
            ELSE 0
        END as diff_pct
    FROM reaches r
    WHERE r.hydro_dist_out IS NOT NULL
        AND r.pathlen_out IS NOT NULL
        AND ABS(r.hydro_dist_out - (r.pathlen_out + r.reach_length)) > 1000  -- >1km difference
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
        description=f"Reaches with >1km difference between hydro_dist_out and pathlen_out + reach_length ({len(issues)} found)",
    )


@register_check(
    "V004",
    Category.V17C,
    Severity.WARNING,
    "is_mainstem continuity check",
)
def check_mainstem_continuity(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """
    Check that is_mainstem forms continuous paths.

    Mainstem reaches should have at least one mainstem neighbor
    (except headwaters and outlets).
    """
    where_clause = f"AND r.region = '{region}'" if region else ""

    # Check if column exists
    try:
        conn.execute("SELECT is_mainstem FROM reaches LIMIT 1")
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
            description="Column is_mainstem not found (v17c pipeline not run)",
        )

    query = f"""
    WITH mainstem_neighbors AS (
        SELECT
            r.reach_id, r.region,
            SUM(CASE WHEN rt.direction = 'up' AND r2.is_mainstem THEN 1 ELSE 0 END) as ms_up,
            SUM(CASE WHEN rt.direction = 'down' AND r2.is_mainstem THEN 1 ELSE 0 END) as ms_down
        FROM reaches r
        JOIN reach_topology rt ON r.reach_id = rt.reach_id AND r.region = rt.region
        JOIN reaches r2 ON rt.neighbor_reach_id = r2.reach_id AND rt.region = r2.region
        WHERE r.is_mainstem = TRUE
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
    WHERE is_mainstem = TRUE
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
    "is_mainstem coverage statistics",
)
def check_mainstem_coverage(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """
    Report is_mainstem coverage statistics.

    Expected: 96-99% of reaches should be on mainstem paths.
    """
    where_clause = f"AND r.region = '{region}'" if region else ""

    # Check if column exists
    try:
        conn.execute("SELECT is_mainstem FROM reaches LIMIT 1")
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
            description="Column is_mainstem not found (v17c pipeline not run)",
        )

    query = f"""
    SELECT
        region,
        COUNT(*) as total_reaches,
        SUM(CASE WHEN is_mainstem = TRUE THEN 1 ELSE 0 END) as mainstem_reaches,
        ROUND(100.0 * SUM(CASE WHEN is_mainstem = TRUE THEN 1 ELSE 0 END) / COUNT(*), 2) as mainstem_pct
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


@register_check(
    "V009",
    Category.V17C,
    Severity.WARNING,
    "dist_out_dijkstra must decrease downstream (min of all downstream neighbors)",
    default_threshold=100.0,
)
def check_dist_out_dijkstra_monotonicity(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """
    Check that dist_out_dijkstra decreases downstream.

    Dijkstra computes shortest path to ANY outlet, so violations are
    expected at multi-outlet bifurcations where one branch leads to a
    more distant outlet. ~1,210 violations expected globally.
    """
    tolerance = threshold if threshold is not None else 100.0
    where_clause = f"AND r1.region = '{region}'" if region else ""

    try:
        conn.execute("SELECT dist_out_dijkstra FROM reaches LIMIT 1")
    except duckdb.CatalogException:
        return CheckResult(
            check_id="V009",
            name="dist_out_dijkstra_monotonicity",
            severity=Severity.WARNING,
            passed=True,
            total_checked=0,
            issues_found=0,
            issue_pct=0,
            details=pd.DataFrame(),
            description="Column dist_out_dijkstra not found (v17c pipeline not run)",
        )

    query = f"""
    WITH min_downstream AS (
        SELECT
            r1.reach_id,
            r1.region,
            r1.dist_out_dijkstra as dist_up,
            MIN(r2.dist_out_dijkstra) as min_dist_down,
            r1.river_name,
            r1.x, r1.y,
            r1.n_rch_down
        FROM reaches r1
        JOIN reach_topology rt ON r1.reach_id = rt.reach_id AND r1.region = rt.region
        JOIN reaches r2 ON rt.neighbor_reach_id = r2.reach_id AND rt.region = r2.region
        WHERE rt.direction = 'down'
            AND r1.dist_out_dijkstra IS NOT NULL
            AND r2.dist_out_dijkstra IS NOT NULL
            {where_clause}
        GROUP BY r1.reach_id, r1.region, r1.dist_out_dijkstra, r1.river_name, r1.x, r1.y, r1.n_rch_down
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
    WHERE dist_out_dijkstra IS NOT NULL
    {where_clause.replace("r1.", "")}
    """
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="V009",
        name="dist_out_dijkstra_monotonicity",
        severity=Severity.WARNING,
        passed=len(issues) == 0,
        total_checked=total,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total if total > 0 else 0,
        details=issues,
        description="Reaches where min(downstream dist_out_dijkstra) increases (expected at multi-outlet bifurcations)",
        threshold=tolerance,
    )


@register_check(
    "V010",
    Category.V17C,
    Severity.ERROR,
    "rch_id_up_main/dn_main must reference valid neighboring reaches",
)
def check_main_connection_integrity(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """
    Validate rch_id_up_main/rch_id_dn_main referential and topologic integrity.

    Checks:
    - Referenced reach exists in same region
    - Reference is not self
    - Reference appears in reach_topology with matching direction
    """
    where_clause = f"AND r.region = '{region}'" if region else ""

    try:
        conn.execute("SELECT rch_id_up_main, rch_id_dn_main FROM reaches LIMIT 1")
    except duckdb.CatalogException:
        return CheckResult(
            check_id="V010",
            name="main_connection_integrity",
            severity=Severity.ERROR,
            passed=True,
            total_checked=0,
            issues_found=0,
            issue_pct=0,
            details=pd.DataFrame(),
            description="Columns rch_id_up_main/rch_id_dn_main not found (v17c pipeline not run)",
        )

    query = f"""
    WITH issues AS (
        SELECT
            r.reach_id, r.region, r.river_name, r.x, r.y,
            'up_main_invalid_fk' as issue_type,
            r.rch_id_up_main as ref_reach_id
        FROM reaches r
        LEFT JOIN reaches u ON r.rch_id_up_main = u.reach_id AND r.region = u.region
        WHERE r.rch_id_up_main IS NOT NULL
            AND r.rch_id_up_main != -9999
            AND u.reach_id IS NULL
            {where_clause}

        UNION ALL

        SELECT
            r.reach_id, r.region, r.river_name, r.x, r.y,
            'dn_main_invalid_fk' as issue_type,
            r.rch_id_dn_main as ref_reach_id
        FROM reaches r
        LEFT JOIN reaches d ON r.rch_id_dn_main = d.reach_id AND r.region = d.region
        WHERE r.rch_id_dn_main IS NOT NULL
            AND r.rch_id_dn_main != -9999
            AND d.reach_id IS NULL
            {where_clause}

        UNION ALL

        SELECT
            r.reach_id, r.region, r.river_name, r.x, r.y,
            'up_main_self_reference' as issue_type,
            r.rch_id_up_main as ref_reach_id
        FROM reaches r
        WHERE r.rch_id_up_main = r.reach_id
            {where_clause}

        UNION ALL

        SELECT
            r.reach_id, r.region, r.river_name, r.x, r.y,
            'dn_main_self_reference' as issue_type,
            r.rch_id_dn_main as ref_reach_id
        FROM reaches r
        WHERE r.rch_id_dn_main = r.reach_id
            {where_clause}

        UNION ALL

        SELECT
            r.reach_id, r.region, r.river_name, r.x, r.y,
            'up_main_not_upstream_neighbor' as issue_type,
            r.rch_id_up_main as ref_reach_id
        FROM reaches r
        WHERE r.rch_id_up_main IS NOT NULL
            AND r.rch_id_up_main != -9999
            AND EXISTS (
                SELECT 1
                FROM reaches u
                WHERE u.reach_id = r.rch_id_up_main
                    AND u.region = r.region
            )
            AND NOT EXISTS (
                SELECT 1
                FROM reach_topology rt
                WHERE rt.region = r.region
                    AND rt.reach_id = r.reach_id
                    AND rt.direction = 'up'
                    AND rt.neighbor_reach_id = r.rch_id_up_main
            )
            {where_clause}

        UNION ALL

        SELECT
            r.reach_id, r.region, r.river_name, r.x, r.y,
            'dn_main_not_downstream_neighbor' as issue_type,
            r.rch_id_dn_main as ref_reach_id
        FROM reaches r
        WHERE r.rch_id_dn_main IS NOT NULL
            AND r.rch_id_dn_main != -9999
            AND EXISTS (
                SELECT 1
                FROM reaches d
                WHERE d.reach_id = r.rch_id_dn_main
                    AND d.region = r.region
            )
            AND NOT EXISTS (
                SELECT 1
                FROM reach_topology rt
                WHERE rt.region = r.region
                    AND rt.reach_id = r.reach_id
                    AND rt.direction = 'down'
                    AND rt.neighbor_reach_id = r.rch_id_dn_main
            )
            {where_clause}
    )
    SELECT *
    FROM issues
    ORDER BY region, reach_id, issue_type
    """

    issues = conn.execute(query).fetchdf()

    total_query = f"""
    SELECT COUNT(*) FROM reaches r
    WHERE (r.rch_id_up_main IS NOT NULL AND r.rch_id_up_main != -9999)
       OR (r.rch_id_dn_main IS NOT NULL AND r.rch_id_dn_main != -9999)
    {where_clause}
    """
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="V010",
        name="main_connection_integrity",
        severity=Severity.ERROR,
        passed=len(issues) == 0,
        total_checked=total,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total if total > 0 else 0,
        details=issues,
        description="Main-connection IDs with FK/topology/self-reference violations",
    )


@register_check(
    "V012",
    Category.V17C,
    Severity.ERROR,
    "Headwater/outlet null semantics for rch_id_up_main/dn_main",
)
def check_main_connection_null_semantics(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """
    Validate expected NULL/non-NULL patterns for main-connection IDs.

    Expected:
    - n_rch_up = 0  -> rch_id_up_main IS NULL
    - n_rch_up > 0  -> rch_id_up_main IS NOT NULL
    - n_rch_down = 0 -> rch_id_dn_main IS NULL
    - n_rch_down > 0 -> rch_id_dn_main IS NOT NULL
    """
    where_clause = f"AND r.region = '{region}'" if region else ""

    try:
        conn.execute(
            "SELECT n_rch_up, n_rch_down, rch_id_up_main, rch_id_dn_main FROM reaches LIMIT 1"
        )
    except duckdb.CatalogException:
        return CheckResult(
            check_id="V012",
            name="main_connection_null_semantics",
            severity=Severity.ERROR,
            passed=True,
            total_checked=0,
            issues_found=0,
            issue_pct=0,
            details=pd.DataFrame(),
            description="Required columns not found (v17c pipeline not run)",
        )

    query = f"""
    WITH issues AS (
        SELECT
            r.reach_id, r.region, r.river_name, r.x, r.y,
            'headwater_has_up_main' as issue_type
        FROM reaches r
        WHERE r.n_rch_up = 0
            AND r.rch_id_up_main IS NOT NULL
            {where_clause}

        UNION ALL

        SELECT
            r.reach_id, r.region, r.river_name, r.x, r.y,
            'non_headwater_missing_up_main' as issue_type
        FROM reaches r
        WHERE r.n_rch_up > 0
            AND r.rch_id_up_main IS NULL
            {where_clause}

        UNION ALL

        SELECT
            r.reach_id, r.region, r.river_name, r.x, r.y,
            'outlet_has_dn_main' as issue_type
        FROM reaches r
        WHERE r.n_rch_down = 0
            AND r.rch_id_dn_main IS NOT NULL
            {where_clause}

        UNION ALL

        SELECT
            r.reach_id, r.region, r.river_name, r.x, r.y,
            'non_outlet_missing_dn_main' as issue_type
        FROM reaches r
        WHERE r.n_rch_down > 0
            AND r.rch_id_dn_main IS NULL
            {where_clause}
    )
    SELECT *
    FROM issues
    ORDER BY region, reach_id, issue_type
    """

    issues = conn.execute(query).fetchdf()

    total_query = f"""
    SELECT COUNT(*)
    FROM reaches r
    WHERE r.n_rch_up IS NOT NULL AND r.n_rch_down IS NOT NULL
    {where_clause}
    """
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="V012",
        name="main_connection_null_semantics",
        severity=Severity.ERROR,
        passed=len(issues) == 0,
        total_checked=total,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total if total > 0 else 0,
        details=issues,
        description="Headwater/outlet NULL semantics violations for main-connection IDs",
    )


@register_check(
    "V014",
    Category.V17C,
    Severity.WARNING,
    "Each (region, main_path_id) must map to one (best_headwater, best_outlet) tuple",
)
def check_main_path_id_region_consistency(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """
    Check that each regional main_path_id resolves to exactly one tuple.
    """
    try:
        conn.execute(
            "SELECT main_path_id, best_headwater, best_outlet FROM reaches LIMIT 1"
        )
    except duckdb.CatalogException:
        return CheckResult(
            check_id="V014",
            name="main_path_id_region_consistency",
            severity=Severity.WARNING,
            passed=True,
            total_checked=0,
            issues_found=0,
            issue_pct=0,
            details=pd.DataFrame(),
            description="Columns main_path_id/best_headwater/best_outlet not found (v17c pipeline not run)",
        )

    where_clause = f"WHERE r.region = '{region}'" if region else ""

    query = f"""
    SELECT
        r.region,
        r.main_path_id,
        COUNT(*) as n_reaches,
        COUNT(DISTINCT (r.best_headwater, r.best_outlet)) as n_tuples,
        LIST(DISTINCT r.best_headwater) as headwaters,
        LIST(DISTINCT r.best_outlet) as outlets
    FROM reaches r
    {where_clause}
    {"AND" if where_clause else "WHERE"} r.main_path_id IS NOT NULL
    GROUP BY r.region, r.main_path_id
    HAVING COUNT(DISTINCT (r.best_headwater, r.best_outlet)) > 1
    ORDER BY n_tuples DESC, n_reaches DESC
    LIMIT 1000
    """

    issues = conn.execute(query).fetchdf()

    issue_count_query = f"""
    SELECT COUNT(*)
    FROM (
        SELECT r.region, r.main_path_id
        FROM reaches r
        {where_clause}
        {"AND" if where_clause else "WHERE"} r.main_path_id IS NOT NULL
        GROUP BY r.region, r.main_path_id
        HAVING COUNT(DISTINCT (r.best_headwater, r.best_outlet)) > 1
    ) t
    """
    issue_count = conn.execute(issue_count_query).fetchone()[0]

    total_query = f"""
    SELECT COUNT(DISTINCT (r.region, r.main_path_id))
    FROM reaches r
    {where_clause}
    {"AND" if where_clause else "WHERE"} r.main_path_id IS NOT NULL
    """
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="V014",
        name="main_path_id_region_consistency",
        severity=Severity.WARNING,
        passed=issue_count == 0,
        total_checked=total,
        issues_found=issue_count,
        issue_pct=100 * issue_count / total if total > 0 else 0,
        details=issues,
        description="Regional main_path_id values mapping to multiple (best_headwater, best_outlet) tuples",
    )


@register_check(
    "V015",
    Category.V17C,
    Severity.WARNING,
    "Each (region, best_headwater, best_outlet) tuple must map to one main_path_id",
)
def check_tuple_to_main_path_id_uniqueness(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """
    Check inverse uniqueness: one tuple should not split across multiple IDs.
    """
    try:
        conn.execute(
            "SELECT main_path_id, best_headwater, best_outlet FROM reaches LIMIT 1"
        )
    except duckdb.CatalogException:
        return CheckResult(
            check_id="V015",
            name="tuple_to_main_path_id_uniqueness",
            severity=Severity.WARNING,
            passed=True,
            total_checked=0,
            issues_found=0,
            issue_pct=0,
            details=pd.DataFrame(),
            description="Columns main_path_id/best_headwater/best_outlet not found (v17c pipeline not run)",
        )

    where_clause = f"WHERE r.region = '{region}'" if region else ""

    query = f"""
    SELECT
        r.region,
        r.best_headwater,
        r.best_outlet,
        COUNT(*) as n_reaches,
        COUNT(DISTINCT r.main_path_id) as n_main_path_ids,
        LIST(DISTINCT r.main_path_id) as main_path_ids
    FROM reaches r
    {where_clause}
    {"AND" if where_clause else "WHERE"} r.main_path_id IS NOT NULL
        AND r.best_headwater IS NOT NULL
        AND r.best_outlet IS NOT NULL
    GROUP BY r.region, r.best_headwater, r.best_outlet
    HAVING COUNT(DISTINCT r.main_path_id) > 1
    ORDER BY n_main_path_ids DESC, n_reaches DESC
    LIMIT 1000
    """

    issues = conn.execute(query).fetchdf()

    issue_count_query = f"""
    SELECT COUNT(*)
    FROM (
        SELECT r.region, r.best_headwater, r.best_outlet
        FROM reaches r
        {where_clause}
        {"AND" if where_clause else "WHERE"} r.main_path_id IS NOT NULL
            AND r.best_headwater IS NOT NULL
            AND r.best_outlet IS NOT NULL
        GROUP BY r.region, r.best_headwater, r.best_outlet
        HAVING COUNT(DISTINCT r.main_path_id) > 1
    ) t
    """
    issue_count = conn.execute(issue_count_query).fetchone()[0]

    total_query = f"""
    SELECT COUNT(DISTINCT (r.region, r.best_headwater, r.best_outlet))
    FROM reaches r
    {where_clause}
    {"AND" if where_clause else "WHERE"} r.main_path_id IS NOT NULL
        AND r.best_headwater IS NOT NULL
        AND r.best_outlet IS NOT NULL
    """
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="V015",
        name="tuple_to_main_path_id_uniqueness",
        severity=Severity.WARNING,
        passed=issue_count == 0,
        total_checked=total,
        issues_found=issue_count,
        issue_pct=100 * issue_count / total if total > 0 else 0,
        details=issues,
        description="(best_headwater, best_outlet) tuples split across multiple main_path_id values",
    )


# pathlen_hw / pathlen_out checks (V020-V025)
# =============================================================================


@register_check(
    "V020",
    Category.V17C,
    Severity.ERROR,
    "pathlen_hw and pathlen_out must not be NULL or negative for connected reaches",
)
def check_pathlen_coverage(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """
    Check that all connected non-ghost reaches have valid pathlen_hw/pathlen_out.

    Both values should be non-NULL and >= 0. NULL indicates pipeline failure;
    negative values indicate a computation bug.
    """
    where_clause = f"AND r.region = '{region}'" if region else ""

    try:
        conn.execute("SELECT pathlen_hw, pathlen_out FROM reaches LIMIT 1")
    except duckdb.CatalogException:
        return CheckResult(
            check_id="V020",
            name="pathlen_coverage",
            severity=Severity.ERROR,
            passed=True,
            total_checked=0,
            issues_found=0,
            issue_pct=0,
            details=pd.DataFrame(),
            description="Columns pathlen_hw/pathlen_out not found (v17c pipeline not run)",
        )

    # Check if 'type' column exists (test DB may lack it)
    has_type = True
    try:
        conn.execute("SELECT type FROM reaches LIMIT 1")
    except (duckdb.CatalogException, duckdb.BinderException):
        has_type = False

    type_filter = "AND r.type NOT IN (5, 6)" if has_type else ""

    query = f"""
    SELECT
        r.reach_id, r.region, r.river_name, r.x, r.y,
        r.pathlen_hw, r.pathlen_out,
        r.n_rch_up, r.n_rch_down,
        CASE
            WHEN r.pathlen_hw IS NULL AND r.pathlen_out IS NULL THEN 'both_null'
            WHEN r.pathlen_hw IS NULL THEN 'hw_null'
            WHEN r.pathlen_out IS NULL THEN 'out_null'
            WHEN r.pathlen_hw < 0 THEN 'hw_negative'
            WHEN r.pathlen_out < 0 THEN 'out_negative'
        END as issue_type
    FROM reaches r
    WHERE (r.n_rch_up > 0 OR r.n_rch_down > 0)
        {type_filter}
        AND (r.pathlen_hw IS NULL OR r.pathlen_out IS NULL
             OR r.pathlen_hw < 0 OR r.pathlen_out < 0)
        {where_clause}
    ORDER BY r.reach_id
    """

    issues = conn.execute(query).fetchdf()

    total_query = f"""
    SELECT COUNT(*) FROM reaches r
    WHERE (n_rch_up > 0 OR n_rch_down > 0) {type_filter}
    {where_clause}
    """
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="V020",
        name="pathlen_coverage",
        severity=Severity.ERROR,
        passed=len(issues) == 0,
        total_checked=total,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total if total > 0 else 0,
        details=issues,
        description="Connected reaches with NULL or negative pathlen_hw/pathlen_out",
    )


@register_check(
    "V021",
    Category.V17C,
    Severity.WARNING,
    "pathlen_hw=0 at headwaters, pathlen_out=0 at outlets",
)
def check_pathlen_boundary_values(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """
    Check boundary conditions for pathlen_hw and pathlen_out.

    - When reach_id = best_headwater, pathlen_hw must be 0 (headwater has
      no upstream accumulation).
    - When reach_id = best_outlet, pathlen_out must be 0 (outlet has no
      downstream accumulation).
    """
    where_clause = f"AND r.region = '{region}'" if region else ""

    try:
        conn.execute(
            "SELECT pathlen_hw, pathlen_out, best_headwater, best_outlet "
            "FROM reaches LIMIT 1"
        )
    except duckdb.CatalogException:
        return CheckResult(
            check_id="V021",
            name="pathlen_boundary_values",
            severity=Severity.WARNING,
            passed=True,
            total_checked=0,
            issues_found=0,
            issue_pct=0,
            details=pd.DataFrame(),
            description="Required columns not found (v17c pipeline not run)",
        )

    query = f"""
    SELECT
        r.reach_id, r.region, r.river_name, r.x, r.y,
        r.pathlen_hw, r.pathlen_out,
        r.best_headwater, r.best_outlet,
        CASE
            WHEN r.reach_id = r.best_headwater AND r.pathlen_hw != 0
                THEN 'headwater_hw_nonzero'
            WHEN r.reach_id = r.best_outlet AND r.pathlen_out != 0
                THEN 'outlet_out_nonzero'
        END as issue_type
    FROM reaches r
    WHERE r.pathlen_hw IS NOT NULL
        AND r.best_headwater IS NOT NULL
        AND r.best_outlet IS NOT NULL
        AND (
            (r.reach_id = r.best_headwater AND r.pathlen_hw != 0)
            OR (r.reach_id = r.best_outlet AND r.pathlen_out != 0)
        )
        {where_clause}
    ORDER BY r.reach_id
    """

    issues = conn.execute(query).fetchdf()

    total_query = f"""
    SELECT COUNT(*) FROM reaches r
    WHERE pathlen_hw IS NOT NULL
        AND (reach_id = best_headwater OR reach_id = best_outlet)
        {where_clause}
    """
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="V021",
        name="pathlen_boundary_values",
        severity=Severity.WARNING,
        passed=len(issues) == 0,
        total_checked=total,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total if total > 0 else 0,
        details=issues,
        description="Headwaters with pathlen_hw != 0 or outlets with pathlen_out != 0",
    )


@register_check(
    "V022",
    Category.V17C,
    Severity.WARNING,
    "pathlen_hw step consistency along rch_id_dn_main (same best_headwater)",
    default_threshold=1.0,  # tolerance in meters
)
def check_pathlen_hw_step(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """
    Check pathlen_hw accumulation along rch_id_dn_main chain.

    For A→B via rch_id_dn_main where both share the same best_headwater:
        pathlen_hw[B] should equal pathlen_hw[A] + reach_length[B]

    Violations occur ONLY at confluences (B has n_rch_up > 1) where the
    upstream pass picked a different best predecessor than A. This is
    expected routing divergence — not a bug. ~1,551 globally (~0.7%).

    On 1:1 links (n_rch_up = 1), zero violations are expected.
    """
    tolerance = threshold if threshold is not None else 1.0
    where_clause = f"AND r1.region = '{region}'" if region else ""

    try:
        conn.execute(
            "SELECT pathlen_hw, rch_id_dn_main, best_headwater FROM reaches LIMIT 1"
        )
    except duckdb.CatalogException:
        return CheckResult(
            check_id="V022",
            name="pathlen_hw_step",
            severity=Severity.WARNING,
            passed=True,
            total_checked=0,
            issues_found=0,
            issue_pct=0,
            details=pd.DataFrame(),
            description="Required columns not found (v17c pipeline not run)",
        )

    query = f"""
    SELECT
        r1.reach_id as upstream_reach,
        r2.reach_id as downstream_reach,
        r1.region,
        r1.river_name,
        r2.x, r2.y,
        r1.pathlen_hw as up_pathlen_hw,
        r2.pathlen_hw as dn_pathlen_hw,
        r2.reach_length as dn_reach_length,
        r1.pathlen_hw + r2.reach_length as expected_dn_hw,
        r2.pathlen_hw - (r1.pathlen_hw + r2.reach_length) as diff,
        r2.n_rch_up as dn_n_rch_up,
        r1.best_headwater
    FROM reaches r1
    JOIN reaches r2
        ON r1.rch_id_dn_main = r2.reach_id
        AND r1.region = r2.region
    WHERE r1.best_headwater = r2.best_headwater
        AND r1.pathlen_hw IS NOT NULL
        AND r2.pathlen_hw IS NOT NULL
        AND ABS(r2.pathlen_hw - (r1.pathlen_hw + r2.reach_length)) > {tolerance}
        {where_clause}
    ORDER BY ABS(r2.pathlen_hw - (r1.pathlen_hw + r2.reach_length)) DESC
    LIMIT 1000
    """

    issues = conn.execute(query).fetchdf()

    # Accurate count (not capped by LIMIT)
    count_query = f"""
    SELECT
        COUNT(*) as total_violations,
        SUM(CASE WHEN r2.n_rch_up > 1 THEN 1 ELSE 0 END) as at_confluence,
        SUM(CASE WHEN r2.n_rch_up <= 1 THEN 1 ELSE 0 END) as on_1_1_link
    FROM reaches r1
    JOIN reaches r2
        ON r1.rch_id_dn_main = r2.reach_id
        AND r1.region = r2.region
    WHERE r1.best_headwater = r2.best_headwater
        AND r1.pathlen_hw IS NOT NULL
        AND r2.pathlen_hw IS NOT NULL
        AND ABS(r2.pathlen_hw - (r1.pathlen_hw + r2.reach_length)) > {tolerance}
        {where_clause}
    """
    counts = conn.execute(count_query).fetchone()
    n_violations = counts[0]
    n_confluence = int(counts[1] or 0)
    n_onetoone = int(counts[2] or 0)

    total_query = f"""
    SELECT COUNT(*) FROM reaches r1
    JOIN reaches r2
        ON r1.rch_id_dn_main = r2.reach_id
        AND r1.region = r2.region
    WHERE r1.best_headwater = r2.best_headwater
        AND r1.pathlen_hw IS NOT NULL
        AND r2.pathlen_hw IS NOT NULL
        {where_clause}
    """
    total = conn.execute(total_query).fetchone()[0]

    desc = (
        f"pathlen_hw step violations: {n_violations} total "
        f"({n_confluence} at confluences, {n_onetoone} on 1:1 links)"
    )

    return CheckResult(
        check_id="V022",
        name="pathlen_hw_step",
        severity=Severity.WARNING,
        passed=n_onetoone == 0,  # Only fail on 1:1 link violations
        total_checked=total,
        issues_found=n_violations,
        issue_pct=100 * n_violations / total if total > 0 else 0,
        details=issues,
        description=desc,
        threshold=tolerance,
    )


@register_check(
    "V023",
    Category.V17C,
    Severity.ERROR,
    "pathlen_out step consistency along rch_id_dn_main (same best_outlet)",
    default_threshold=1.0,  # tolerance in meters
)
def check_pathlen_out_step(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """
    Check pathlen_out accumulation along rch_id_dn_main chain.

    For A→B via rch_id_dn_main where both share the same best_outlet:
        pathlen_out[A] should equal pathlen_out[B] + reach_length[B]

    Unlike pathlen_hw (upstream pass), pathlen_out (downstream pass) should
    have ZERO violations because both pathlen_out and rch_id_dn_main are
    computed in the same downstream direction. Any violation is a real bug.
    """
    tolerance = threshold if threshold is not None else 1.0
    where_clause = f"AND r1.region = '{region}'" if region else ""

    try:
        conn.execute(
            "SELECT pathlen_out, rch_id_dn_main, best_outlet FROM reaches LIMIT 1"
        )
    except duckdb.CatalogException:
        return CheckResult(
            check_id="V023",
            name="pathlen_out_step",
            severity=Severity.ERROR,
            passed=True,
            total_checked=0,
            issues_found=0,
            issue_pct=0,
            details=pd.DataFrame(),
            description="Required columns not found (v17c pipeline not run)",
        )

    query = f"""
    SELECT
        r1.reach_id as upstream_reach,
        r2.reach_id as downstream_reach,
        r1.region,
        r1.river_name,
        r1.x, r1.y,
        r1.pathlen_out as up_pathlen_out,
        r2.pathlen_out as dn_pathlen_out,
        r2.reach_length as dn_reach_length,
        r2.pathlen_out + r2.reach_length as expected_up_out,
        r1.pathlen_out - (r2.pathlen_out + r2.reach_length) as diff,
        r2.n_rch_up as dn_n_rch_up,
        r1.best_outlet
    FROM reaches r1
    JOIN reaches r2
        ON r1.rch_id_dn_main = r2.reach_id
        AND r1.region = r2.region
    WHERE r1.best_outlet = r2.best_outlet
        AND r1.pathlen_out IS NOT NULL
        AND r2.pathlen_out IS NOT NULL
        AND ABS(r1.pathlen_out - (r2.pathlen_out + r2.reach_length)) > {tolerance}
        {where_clause}
    ORDER BY ABS(r1.pathlen_out - (r2.pathlen_out + r2.reach_length)) DESC
    LIMIT 1000
    """

    issues = conn.execute(query).fetchdf()

    total_query = f"""
    SELECT COUNT(*) FROM reaches r1
    JOIN reaches r2
        ON r1.rch_id_dn_main = r2.reach_id
        AND r1.region = r2.region
    WHERE r1.best_outlet = r2.best_outlet
        AND r1.pathlen_out IS NOT NULL
        AND r2.pathlen_out IS NOT NULL
        {where_clause}
    """
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="V023",
        name="pathlen_out_step",
        severity=Severity.ERROR,
        passed=len(issues) == 0,
        total_checked=total,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total if total > 0 else 0,
        details=issues,
        description=f"pathlen_out step violations (expected: 0, found: {len(issues)})",
        threshold=tolerance,
    )


@register_check(
    "V024",
    Category.V17C,
    Severity.INFO,
    "pathlen_hw + pathlen_out total path length consistency",
)
def check_pathlen_total_consistency(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """
    Check that pathlen_hw + pathlen_out is constant for same (best_headwater, best_outlet).

    On a linear path from headwater to outlet, every reach should have the
    same pathlen_hw + pathlen_out (= total path length minus headwater's
    reach_length). Variation indicates routing divergence at junctions where
    the upstream and downstream passes pick different "best" neighbors.

    This is a diagnostic check (INFO) — some divergence is inherent to the
    independent upstream/downstream pass design.
    """
    where_clause = f"AND r.region = '{region}'" if region else ""

    try:
        conn.execute(
            "SELECT pathlen_hw, pathlen_out, best_headwater, best_outlet "
            "FROM reaches LIMIT 1"
        )
    except duckdb.CatalogException:
        return CheckResult(
            check_id="V024",
            name="pathlen_total_consistency",
            severity=Severity.INFO,
            passed=True,
            total_checked=0,
            issues_found=0,
            issue_pct=0,
            details=pd.DataFrame(),
            description="Required columns not found (v17c pipeline not run)",
        )

    query = f"""
    WITH path_stats AS (
        SELECT
            r.best_headwater,
            r.best_outlet,
            r.region,
            COUNT(*) as n_reaches,
            MIN(r.pathlen_hw + r.pathlen_out) as min_total,
            MAX(r.pathlen_hw + r.pathlen_out) as max_total,
            MAX(r.pathlen_hw + r.pathlen_out)
                - MIN(r.pathlen_hw + r.pathlen_out) as spread
        FROM reaches r
        WHERE r.pathlen_hw IS NOT NULL
            AND r.best_headwater IS NOT NULL
            AND r.best_outlet IS NOT NULL
            {where_clause}
        GROUP BY r.best_headwater, r.best_outlet, r.region
        HAVING COUNT(*) > 1
    )
    SELECT
        best_headwater, best_outlet, region,
        n_reaches, min_total, max_total,
        spread,
        ROUND(100.0 * spread / NULLIF(max_total, 0), 2) as spread_pct
    FROM path_stats
    WHERE spread > 100  -- >100m divergence
    ORDER BY spread DESC
    LIMIT 500
    """

    issues = conn.execute(query).fetchdf()

    # Accurate count (not capped by LIMIT)
    count_query = f"""
    WITH path_stats AS (
        SELECT
            r.best_headwater, r.best_outlet, r.region,
            MAX(r.pathlen_hw + r.pathlen_out)
                - MIN(r.pathlen_hw + r.pathlen_out) as spread
        FROM reaches r
        WHERE r.pathlen_hw IS NOT NULL
            AND r.best_headwater IS NOT NULL
            AND r.best_outlet IS NOT NULL
            {where_clause}
        GROUP BY r.best_headwater, r.best_outlet, r.region
        HAVING COUNT(*) > 1
    )
    SELECT COUNT(*) FROM path_stats WHERE spread > 100
    """
    n_inconsistent = conn.execute(count_query).fetchone()[0]

    total_query = f"""
    SELECT COUNT(DISTINCT (best_headwater, best_outlet)) FROM reaches r
    WHERE pathlen_hw IS NOT NULL
        AND best_headwater IS NOT NULL
        AND best_outlet IS NOT NULL
        {where_clause}
    """
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="V024",
        name="pathlen_total_consistency",
        severity=Severity.INFO,
        passed=True,  # Informational
        total_checked=total,
        issues_found=n_inconsistent,
        issue_pct=100 * n_inconsistent / total if total > 0 else 0,
        details=issues,
        description=f"Paths with >100m spread in pathlen_hw + pathlen_out ({n_inconsistent} of {total} paths)",
    )


@register_check(
    "V025",
    Category.V17C,
    Severity.WARNING,
    "pathlen_hw must increase and pathlen_out must decrease downstream on ALL topology edges",
)
def check_pathlen_direction(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """
    Check directional monotonicity on all downstream topology edges.

    Along ANY downstream edge (not just rch_id_dn_main):
    - pathlen_hw should not decrease (upstream accumulation grows downstream)
    - pathlen_out should not increase (downstream accumulation shrinks downstream)

    This is weaker than V022/V023 (which check exact step amounts) but covers
    ALL edges, not just the mainstem chain.

    ~90% of violations involve side channels (main_side > 0) where routing
    divergence is expected. Only main-to-main (main_side=0 on both ends)
    violations are flagged as failures.
    """
    where_clause = f"AND r1.region = '{region}'" if region else ""

    try:
        conn.execute(
            "SELECT pathlen_hw, pathlen_out, best_headwater, best_outlet "
            "FROM reaches LIMIT 1"
        )
    except duckdb.CatalogException:
        return CheckResult(
            check_id="V025",
            name="pathlen_direction",
            severity=Severity.WARNING,
            passed=True,
            total_checked=0,
            issues_found=0,
            issue_pct=0,
            details=pd.DataFrame(),
            description="Required columns not found (v17c pipeline not run)",
        )

    # pathlen_out increases downstream = violation (should decrease)
    # pathlen_hw decreases downstream = violation (should increase)
    # Only check pairs sharing the same best_headwater/best_outlet respectively
    query = f"""
    SELECT
        r1.reach_id as upstream_reach,
        r2.reach_id as downstream_reach,
        r1.region,
        r1.river_name,
        r1.x, r1.y,
        r1.pathlen_hw as up_hw, r2.pathlen_hw as dn_hw,
        r1.pathlen_out as up_out, r2.pathlen_out as dn_out,
        r1.main_side as up_main_side,
        r2.main_side as dn_main_side,
        CASE
            WHEN r1.best_headwater = r2.best_headwater
                AND r2.pathlen_hw < r1.pathlen_hw - 1.0
                THEN 'hw_decreases_downstream'
            WHEN r1.best_outlet = r2.best_outlet
                AND r2.pathlen_out > r1.pathlen_out + 1.0
                THEN 'out_increases_downstream'
        END as issue_type,
        r2.n_rch_up as dn_n_rch_up
    FROM reaches r1
    JOIN reach_topology rt
        ON r1.reach_id = rt.reach_id AND r1.region = rt.region
    JOIN reaches r2
        ON rt.neighbor_reach_id = r2.reach_id AND rt.region = r2.region
    WHERE rt.direction = 'down'
        AND r1.pathlen_hw IS NOT NULL
        AND r2.pathlen_hw IS NOT NULL
        AND (
            (r1.best_headwater = r2.best_headwater
                AND r2.pathlen_hw < r1.pathlen_hw - 1.0)
            OR (r1.best_outlet = r2.best_outlet
                AND r2.pathlen_out > r1.pathlen_out + 1.0)
        )
        {where_clause}
    ORDER BY ABS(COALESCE(r2.pathlen_hw - r1.pathlen_hw, r2.pathlen_out - r1.pathlen_out)) DESC
    LIMIT 1000
    """

    issues = conn.execute(query).fetchdf()

    # Accurate counts (not capped by LIMIT)
    count_query = f"""
    SELECT
        COUNT(*) as total_violations,
        SUM(CASE WHEN r1.main_side = 0 AND r2.main_side = 0 THEN 1 ELSE 0 END) as main_main
    FROM reaches r1
    JOIN reach_topology rt
        ON r1.reach_id = rt.reach_id AND r1.region = rt.region
    JOIN reaches r2
        ON rt.neighbor_reach_id = r2.reach_id AND rt.region = r2.region
    WHERE rt.direction = 'down'
        AND r1.pathlen_hw IS NOT NULL
        AND r2.pathlen_hw IS NOT NULL
        AND (
            (r1.best_headwater = r2.best_headwater
                AND r2.pathlen_hw < r1.pathlen_hw - 1.0)
            OR (r1.best_outlet = r2.best_outlet
                AND r2.pathlen_out > r1.pathlen_out + 1.0)
        )
        {where_clause}
    """
    counts = conn.execute(count_query).fetchone()
    n_violations = counts[0]
    n_main_main = int(counts[1] or 0)

    total_query = f"""
    SELECT COUNT(*) FROM reaches r1
    JOIN reach_topology rt
        ON r1.reach_id = rt.reach_id AND r1.region = rt.region
    JOIN reaches r2
        ON rt.neighbor_reach_id = r2.reach_id AND rt.region = r2.region
    WHERE rt.direction = 'down'
        AND r1.pathlen_hw IS NOT NULL
        AND r2.pathlen_hw IS NOT NULL
        {where_clause}
    """
    total = conn.execute(total_query).fetchone()[0]

    n_side = n_violations - n_main_main
    desc = (
        f"pathlen direction violations: {n_violations} total "
        f"({n_side} side-channel, {n_main_main} main-to-main)"
    )

    return CheckResult(
        check_id="V025",
        name="pathlen_direction",
        severity=Severity.WARNING,
        passed=n_violations == 0,
        total_checked=total,
        issues_found=n_violations,
        issue_pct=100 * n_violations / total if total > 0 else 0,
        details=issues,
        description=desc,
    )


# subnetwork_id checks (V026-V031)
# =============================================================================

# Pfafstetter offset bands per region (from pfaf_offsets.py)
_PFAF_BANDS = {
    "AF": (1_000_001, 1_999_999),
    "EU": (2_000_001, 2_999_999),
    "AS": (3_000_001, 3_999_999),
    "OC": (5_000_001, 5_999_999),
    "SA": (6_000_001, 6_999_999),
    "NA": (7_000_001, 7_999_999),
}


def _has_column(conn: duckdb.DuckDBPyConnection, table: str, column: str) -> bool:
    """Check whether *column* exists in *table* without raising."""
    try:
        conn.execute(f"SELECT {column} FROM {table} LIMIT 0")
        return True
    except (duckdb.CatalogException, duckdb.BinderException):
        return False


def _column_missing_result(check_id: str, name: str, severity: Severity) -> CheckResult:
    return CheckResult(
        check_id=check_id,
        name=name,
        severity=severity,
        passed=True,
        total_checked=0,
        issues_found=0,
        issue_pct=0,
        details=pd.DataFrame(),
        description="Column subnetwork_id not found (v17c pipeline not run)",
    )


@register_check(
    "V026",
    Category.V17C,
    Severity.ERROR,
    "subnetwork_id must not be NULL for connected non-ghost reaches",
)
def check_subnetwork_id_coverage(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """Every connected, non-ghost reach must have a subnetwork_id assigned."""
    if not _has_column(conn, "reaches", "subnetwork_id"):
        return _column_missing_result("V026", "subnetwork_id_coverage", Severity.ERROR)

    where_clause = f"AND r.region = '{region}'" if region else ""
    has_type = _has_column(conn, "reaches", "type")
    type_filter = "AND r.type NOT IN (5, 6)" if has_type else ""

    query = f"""
    SELECT
        r.reach_id, r.region, r.river_name, r.x, r.y,
        r.n_rch_up, r.n_rch_down, r.network
    FROM reaches r
    WHERE r.subnetwork_id IS NULL
        AND (r.n_rch_up > 0 OR r.n_rch_down > 0)
        {type_filter}
        {where_clause}
    ORDER BY r.reach_id
    """
    issues = conn.execute(query).fetchdf()

    total_query = f"""
    SELECT COUNT(*) FROM reaches r
    WHERE (n_rch_up > 0 OR n_rch_down > 0) {type_filter}
    {where_clause}
    """
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="V026",
        name="subnetwork_id_coverage",
        severity=Severity.ERROR,
        passed=len(issues) == 0,
        total_checked=total,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total if total > 0 else 0,
        details=issues,
        description="Connected non-ghost reaches missing subnetwork_id",
    )


@register_check(
    "V027",
    Category.V17C,
    Severity.ERROR,
    "subnetwork_id must fall within Pfafstetter band for its region",
)
def check_subnetwork_id_pfafstetter_range(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """Verify subnetwork_id is in the correct Pfafstetter-offset band.

    NA should be 7_000_001..7_999_999, AF 1_000_001..1_999_999, etc.
    Values outside the band indicate missing or wrong offset application.
    """
    if not _has_column(conn, "reaches", "subnetwork_id"):
        return _column_missing_result(
            "V027", "subnetwork_id_pfafstetter_range", Severity.ERROR
        )

    where_clause = f"AND r.region = '{region}'" if region else ""

    # Build per-region range conditions
    range_conditions = " OR ".join(
        f"(r.region = '{rgn}' AND (r.subnetwork_id < {lo} OR r.subnetwork_id > {hi}))"
        for rgn, (lo, hi) in _PFAF_BANDS.items()
    )

    query = f"""
    SELECT
        r.reach_id, r.region, r.subnetwork_id, r.river_name, r.x, r.y
    FROM reaches r
    WHERE r.subnetwork_id IS NOT NULL
        AND ({range_conditions})
        {where_clause}
    ORDER BY r.region, r.subnetwork_id
    LIMIT 1000
    """
    issues = conn.execute(query).fetchdf()

    count_query = f"""
    SELECT COUNT(*) FROM reaches r
    WHERE r.subnetwork_id IS NOT NULL
        AND ({range_conditions})
        {where_clause}
    """
    n_issues = conn.execute(count_query).fetchone()[0]

    total_query = f"""
    SELECT COUNT(*) FROM reaches r
    WHERE subnetwork_id IS NOT NULL
    {where_clause}
    """
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="V027",
        name="subnetwork_id_pfafstetter_range",
        severity=Severity.ERROR,
        passed=n_issues == 0,
        total_checked=total,
        issues_found=n_issues,
        issue_pct=100 * n_issues / total if total > 0 else 0,
        details=issues,
        description="Reaches with subnetwork_id outside Pfafstetter band for their region",
    )


@register_check(
    "V028",
    Category.V17C,
    Severity.ERROR,
    "Topology-connected reaches must share the same subnetwork_id",
)
def check_subnetwork_id_topology_consistency(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """If A→B in reach_topology, then subnetwork_id(A) must equal subnetwork_id(B).

    This is both necessary and sufficient for verifying weakly-connected-component
    assignment: if all topology edges preserve subnetwork_id, then each component
    is correctly labeled.
    """
    if not _has_column(conn, "reaches", "subnetwork_id"):
        return _column_missing_result(
            "V028", "subnetwork_id_topology_consistency", Severity.ERROR
        )

    where_clause = f"AND r1.region = '{region}'" if region else ""

    query = f"""
    SELECT
        r1.reach_id as reach_a,
        r2.reach_id as reach_b,
        r1.region,
        rt.direction,
        r1.subnetwork_id as subnetwork_a,
        r2.subnetwork_id as subnetwork_b,
        r1.river_name, r1.x, r1.y
    FROM reaches r1
    JOIN reach_topology rt
        ON r1.reach_id = rt.reach_id AND r1.region = rt.region
    JOIN reaches r2
        ON rt.neighbor_reach_id = r2.reach_id AND rt.region = r2.region
    WHERE r1.subnetwork_id IS NOT NULL
        AND r2.subnetwork_id IS NOT NULL
        AND r1.subnetwork_id != r2.subnetwork_id
        {where_clause}
    ORDER BY r1.reach_id
    LIMIT 1000
    """
    issues = conn.execute(query).fetchdf()

    count_query = f"""
    SELECT COUNT(*) FROM reaches r1
    JOIN reach_topology rt
        ON r1.reach_id = rt.reach_id AND r1.region = rt.region
    JOIN reaches r2
        ON rt.neighbor_reach_id = r2.reach_id AND rt.region = r2.region
    WHERE r1.subnetwork_id IS NOT NULL
        AND r2.subnetwork_id IS NOT NULL
        AND r1.subnetwork_id != r2.subnetwork_id
        {where_clause}
    """
    n_issues = conn.execute(count_query).fetchone()[0]

    total_query = f"""
    SELECT COUNT(*) FROM reaches r1
    JOIN reach_topology rt
        ON r1.reach_id = rt.reach_id AND r1.region = rt.region
    JOIN reaches r2
        ON rt.neighbor_reach_id = r2.reach_id AND rt.region = r2.region
    WHERE r1.subnetwork_id IS NOT NULL
        AND r2.subnetwork_id IS NOT NULL
        {where_clause}
    """
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="V028",
        name="subnetwork_id_topology_consistency",
        severity=Severity.ERROR,
        passed=n_issues == 0,
        total_checked=total,
        issues_found=n_issues,
        issue_pct=100 * n_issues / total if total > 0 else 0,
        details=issues,
        description="Topology edges where neighbors have different subnetwork_id (WCC violation)",
    )


@register_check(
    "V029",
    Category.V17C,
    Severity.ERROR,
    "subnetwork_id must not appear in multiple regions (Pfafstetter uniqueness)",
)
def check_subnetwork_id_cross_region_uniqueness(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """Verify no subnetwork_id value is shared across regions.

    Pfafstetter offsets should guarantee disjoint ID bands. If a value
    appears in two regions, the offset was not applied or was corrupted.
    """
    if not _has_column(conn, "reaches", "subnetwork_id"):
        return _column_missing_result(
            "V029", "subnetwork_id_cross_region_uniqueness", Severity.ERROR
        )

    # Single-region queries can't find cross-region collisions,
    # but we run anyway for consistency (will always pass).
    where_clause = f"WHERE r.region = '{region}'" if region else ""

    query = f"""
    SELECT
        subnetwork_id,
        COUNT(DISTINCT region) as n_regions,
        ARRAY_AGG(DISTINCT region) as regions,
        COUNT(*) as n_reaches
    FROM reaches r
    {where_clause}
    {"AND" if where_clause else "WHERE"} subnetwork_id IS NOT NULL
    GROUP BY subnetwork_id
    HAVING COUNT(DISTINCT region) > 1
    ORDER BY n_reaches DESC
    LIMIT 500
    """
    issues = conn.execute(query).fetchdf()

    total_query = f"""
    SELECT COUNT(DISTINCT subnetwork_id) FROM reaches r
    {where_clause}
    {"AND" if where_clause else "WHERE"} subnetwork_id IS NOT NULL
    """
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="V029",
        name="subnetwork_id_cross_region_uniqueness",
        severity=Severity.ERROR,
        passed=len(issues) == 0,
        total_checked=total,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total if total > 0 else 0,
        details=issues,
        description="subnetwork_id values appearing in multiple regions (Pfafstetter collision)",
    )


@register_check(
    "V030",
    Category.V17C,
    Severity.INFO,
    "Isolated reaches (no neighbors) should form singleton subnetwork components",
)
def check_subnetwork_id_singleton_consistency(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """Reaches with n_rch_up=0 AND n_rch_down=0 should be the sole member
    of their subnetwork_id (component size = 1).
    """
    if not _has_column(conn, "reaches", "subnetwork_id"):
        return _column_missing_result(
            "V030", "subnetwork_id_singleton_consistency", Severity.INFO
        )

    where_clause = f"AND r.region = '{region}'" if region else ""

    query = f"""
    WITH isolated AS (
        SELECT reach_id, region, subnetwork_id
        FROM reaches r
        WHERE n_rch_up = 0 AND n_rch_down = 0
            AND subnetwork_id IS NOT NULL
            {where_clause}
    ),
    component_sizes AS (
        SELECT subnetwork_id, region, COUNT(*) as comp_size
        FROM reaches
        WHERE subnetwork_id IS NOT NULL
        GROUP BY subnetwork_id, region
    )
    SELECT
        i.reach_id, i.region, i.subnetwork_id,
        cs.comp_size
    FROM isolated i
    JOIN component_sizes cs
        ON i.subnetwork_id = cs.subnetwork_id AND i.region = cs.region
    WHERE cs.comp_size > 1
    ORDER BY cs.comp_size DESC
    """
    issues = conn.execute(query).fetchdf()

    total_query = f"""
    SELECT COUNT(*) FROM reaches r
    WHERE n_rch_up = 0 AND n_rch_down = 0
        AND subnetwork_id IS NOT NULL
        {where_clause}
    """
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="V030",
        name="subnetwork_id_singleton_consistency",
        severity=Severity.INFO,
        passed=True,  # Informational
        total_checked=total,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total if total > 0 else 0,
        details=issues,
        description=f"Isolated reaches sharing subnetwork_id with other reaches ({len(issues)} found)",
    )


@register_check(
    "V031",
    Category.V17C,
    Severity.INFO,
    "subnetwork_id size distribution statistics",
)
def check_subnetwork_id_distribution(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """Report component size distribution by region."""
    if not _has_column(conn, "reaches", "subnetwork_id"):
        return _column_missing_result(
            "V031", "subnetwork_id_distribution", Severity.INFO
        )

    where_clause = f"WHERE r.region = '{region}'" if region else ""

    query = f"""
    WITH comp_sizes AS (
        SELECT
            region,
            subnetwork_id,
            COUNT(*) as comp_size
        FROM reaches r
        {where_clause}
        {"AND" if where_clause else "WHERE"} subnetwork_id IS NOT NULL
        GROUP BY region, subnetwork_id
    )
    SELECT
        region,
        COUNT(*) as n_components,
        SUM(comp_size) as n_reaches,
        MIN(comp_size) as min_size,
        CAST(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY comp_size) AS INTEGER) as median_size,
        MAX(comp_size) as max_size,
        ROUND(AVG(comp_size), 1) as mean_size,
        SUM(CASE WHEN comp_size = 1 THEN 1 ELSE 0 END) as singletons
    FROM comp_sizes
    GROUP BY region
    ORDER BY region
    """
    stats = conn.execute(query).fetchdf()

    total_query = f"""
    SELECT COUNT(DISTINCT subnetwork_id) FROM reaches r
    {where_clause}
    {"AND" if where_clause else "WHERE"} subnetwork_id IS NOT NULL
    """
    total = conn.execute(total_query).fetchone()[0]

    n_components = int(stats["n_components"].sum()) if len(stats) > 0 else 0
    n_singletons = int(stats["singletons"].sum()) if len(stats) > 0 else 0

    return CheckResult(
        check_id="V031",
        name="subnetwork_id_distribution",
        severity=Severity.INFO,
        passed=True,  # Informational
        total_checked=total,
        issues_found=n_singletons,
        issue_pct=100 * n_singletons / n_components if n_components > 0 else 0,
        details=stats,
        description=f"subnetwork_id: {n_components} components, {n_singletons} singletons",
    )
