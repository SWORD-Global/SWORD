"""
SWORD Lint - Attribute Checks (A0xx)

Validates attribute values, monotonicity, and completeness.
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


def _column_exists(conn: duckdb.DuckDBPyConnection, column: str) -> bool:
    """Check if a column exists in the reaches table."""
    try:
        conn.execute(f"SELECT {column} FROM reaches LIMIT 0")
        return True
    except duckdb.BinderException:
        return False


def _skip_result(
    check_id: str, name: str, severity: Severity, column: str
) -> CheckResult:
    """Return a PASS result when a required column is missing."""
    return CheckResult(
        check_id=check_id,
        name=name,
        severity=severity,
        passed=True,
        total_checked=0,
        issues_found=0,
        issue_pct=0,
        details=pd.DataFrame(),
        description=f"Skipped: column '{column}' not found in reaches table",
    )


@register_check(
    "A002",
    Category.ATTRIBUTES,
    Severity.WARNING,
    "Slope must be non-negative and reasonable (<100 m/km)",
    default_threshold=100.0,
)
def check_slope_plausibility(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """Flag reaches with negative slopes or extremely high slopes."""
    max_slope = threshold if threshold is not None else 100.0
    where_clause = f"AND region = '{region}'" if region else ""

    query = f"""
    SELECT
        reach_id, region, river_name, x, y,
        slope, width, reach_length
    FROM reaches
    WHERE (slope < 0 OR slope > {max_slope / 1000.0})
      {where_clause}
    ORDER BY slope DESC
    """

    issues = conn.execute(query).fetchdf()

    total_query = f"""
    SELECT COUNT(*) FROM reaches WHERE 1=1 {where_clause}
    """
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="A002",
        name="slope_plausibility",
        severity=Severity.WARNING,
        passed=len(issues) == 0,
        total_checked=total,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total if total > 0 else 0,
        details=issues,
        description=f"Reaches with slope < 0 or > {max_slope} m/km",
        threshold=max_slope,
    )


@register_check(
    "A003",
    Category.ATTRIBUTES,
    Severity.ERROR,
    "Width must be positive",
)
def check_width_positive(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """Flag reaches with non-positive widths."""
    where_clause = f"AND region = '{region}'" if region else ""

    query = f"""
    SELECT
        reach_id, region, river_name, x, y,
        width, reach_length
    FROM reaches
    WHERE width <= 0
      {where_clause}
    ORDER BY reach_id
    """

    issues = conn.execute(query).fetchdf()

    total_query = f"""
    SELECT COUNT(*) FROM reaches WHERE 1=1 {where_clause}
    """
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="A003",
        name="width_positive",
        severity=Severity.ERROR,
        passed=len(issues) == 0,
        total_checked=total,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total if total > 0 else 0,
        details=issues,
        description="Reaches with width <= 0",
    )


@register_check(
    "A004",
    Category.ATTRIBUTES,
    Severity.ERROR,
    "Reach length must be positive",
)
def check_reach_length_positive(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """Flag reaches with non-positive reach lengths."""
    where_clause = f"AND region = '{region}'" if region else ""

    query = f"""
    SELECT
        reach_id, region, river_name, x, y,
        reach_length
    FROM reaches
    WHERE reach_length <= 0
      {where_clause}
    ORDER BY reach_id
    """

    issues = conn.execute(query).fetchdf()

    total_query = f"""
    SELECT COUNT(*) FROM reaches WHERE 1=1 {where_clause}
    """
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="A004",
        name="reach_length_positive",
        severity=Severity.ERROR,
        passed=len(issues) == 0,
        total_checked=total,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total if total > 0 else 0,
        details=issues,
        description="Reaches with reach_length <= 0",
    )


@register_check(
    "A005",
    Category.ATTRIBUTES,
    Severity.WARNING,
    "WSE out of plausible range (-500 to 9000 m)",
)
def check_wse_plausibility(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """Flag reaches with WSE values outside terrestrial extremes."""
    where_clause = f"AND region = '{region}'" if region else ""

    query = f"""
    SELECT
        reach_id, region, river_name, x, y,
        wse
    FROM reaches
    WHERE (wse < -500 OR wse > 9000)
      AND wse != -9999
      {where_clause}
    ORDER BY wse DESC
    """

    issues = conn.execute(query).fetchdf()

    total_query = f"""
    SELECT COUNT(*) FROM reaches WHERE wse != -9999 {where_clause}
    """
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="A005",
        name="wse_plausibility",
        severity=Severity.WARNING,
        passed=len(issues) == 0,
        total_checked=total,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total if total > 0 else 0,
        details=issues,
        description="Reaches with WSE < -500m or > 9000m",
    )


@register_check(
    "A006",
    Category.ATTRIBUTES,
    Severity.WARNING,
    "n_nodes per reach should be reasonable (1-100)",
    default_threshold=100.0,
)
def check_n_nodes_plausibility(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """Flag reaches with very few or very many nodes."""
    max_nodes = threshold if threshold is not None else 100.0
    where_clause = f"AND region = '{region}'" if region else ""

    query = f"""
    SELECT
        reach_id, region, river_name, x, y,
        n_nodes
    FROM reaches
    WHERE (n_nodes < 1 OR n_nodes > {max_nodes})
      {where_clause}
    ORDER BY n_nodes DESC
    """

    issues = conn.execute(query).fetchdf()

    total_query = f"""
    SELECT COUNT(*) FROM reaches WHERE 1=1 {where_clause}
    """
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="A006",
        name="n_nodes_plausibility",
        severity=Severity.WARNING,
        passed=len(issues) == 0,
        total_checked=total,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total if total > 0 else 0,
        details=issues,
        description=f"Reaches with n_nodes < 1 or > {max_nodes}",
        threshold=max_nodes,
    )


@register_check(
    "A007",
    Category.ATTRIBUTES,
    Severity.INFO,
    "Check for -9999 placeholder values",
)
def check_nodata_placeholders(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """Report frequency of -9999 placeholders in key numeric fields."""
    where_clause = f"AND region = '{region}'" if region else ""

    query = f"""
    SELECT
        region,
        COUNT(*) FILTER (WHERE wse = -9999) as wse_nodata,
        COUNT(*) FILTER (WHERE slope = -9999) as slope_nodata,
        COUNT(*) FILTER (WHERE width = -9999) as width_nodata,
        COUNT(*) as total
    FROM reaches
    WHERE 1=1 {where_clause}
    GROUP BY region
    """

    stats = conn.execute(query).fetchdf()

    total_nodata = (
        stats["wse_nodata"].sum()
        + stats["slope_nodata"].sum()
        + stats["width_nodata"].sum()
    )
    total_reaches = stats["total"].sum()

    return CheckResult(
        check_id="A007",
        name="nodata_placeholders",
        severity=Severity.INFO,
        passed=True,
        total_checked=total_reaches,
        issues_found=int(total_nodata),
        issue_pct=100 * total_nodata / (total_reaches * 3) if total_reaches > 0 else 0,
        details=stats,
        description="Frequency of -9999 placeholders in WSE, slope, and width",
    )


@register_check(
    "A008",
    Category.ATTRIBUTES,
    Severity.WARNING,
    "MAX(dist_out) for a region should be consistent with expected drainage length",
)
def check_max_dist_out(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """Report maximum dist_out per region."""
    where_clause = f"AND region = '{region}'" if region else ""

    query = f"""
    SELECT
        region,
        MAX(dist_out) as max_dist_out_km
    FROM reaches
    WHERE dist_out != -9999
      {where_clause}
    GROUP BY region
    ORDER BY max_dist_out_km DESC
    """

    results = conn.execute(query).fetchdf()
    results["max_dist_out_km"] = results["max_dist_out_km"] / 1000.0

    total_checked = len(results)

    return CheckResult(
        check_id="A008",
        name="max_dist_out",
        severity=Severity.INFO,
        passed=True,
        total_checked=total_checked,
        issues_found=0,
        issue_pct=0,
        details=results,
        description="Maximum hydrologic distance to outlet (km) by region",
    )


@register_check(
    "A009",
    Category.ATTRIBUTES,
    Severity.ERROR,
    "dist_out must be positive",
)
def check_dist_out_positive(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """Flag reaches with negative or zero dist_out."""
    where_clause = f"AND region = '{region}'" if region else ""

    query = f"""
    SELECT
        reach_id, region, river_name, x, y,
        dist_out
    FROM reaches
    WHERE dist_out <= 0 AND dist_out != -9999
      {where_clause}
    ORDER BY reach_id
    """

    issues = conn.execute(query).fetchdf()

    total_query = f"""
    SELECT COUNT(*) FROM reaches WHERE dist_out != -9999 {where_clause}
    """
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="A009",
        name="dist_out_positive",
        severity=Severity.ERROR,
        passed=len(issues) == 0,
        total_checked=total,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total if total > 0 else 0,
        details=issues,
        description="Reaches with dist_out <= 0",
    )


@register_check(
    "A021",
    Category.ATTRIBUTES,
    Severity.WARNING,
    "reach_id and node_id must be unique across all regions",
)
def check_global_id_uniqueness(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """Verify that reach_id and node_id are unique globally."""
    # This check is global by nature, but we can filter the report by region
    # However, for efficiency we just check for duplicates in the whole DB
    query = """
    SELECT reach_id, COUNT(*) as count, LIST(region) as regions
    FROM reaches
    GROUP BY reach_id
    HAVING count > 1
    UNION ALL
    SELECT node_id as reach_id, COUNT(*) as count, LIST(region) as regions
    FROM nodes
    GROUP BY node_id
    HAVING count > 1
    """

    issues = conn.execute(query).fetchdf()

    total_query = "SELECT (SELECT COUNT(*) FROM reaches) + (SELECT COUNT(*) FROM nodes)"
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="A021",
        name="global_id_uniqueness",
        severity=Severity.WARNING,
        passed=len(issues) == 0,
        total_checked=total,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total if total > 0 else 0,
        details=issues,
        description="Global reach_id or node_id collisions across regions",
    )


@register_check(
    "A024",
    Category.ATTRIBUTES,
    Severity.WARNING,
    "facc must be >= 0",
)
def check_facc_positive(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """Flag reaches with negative flow accumulation."""
    if not _column_exists(conn, "facc"):
        return _skip_result("A024", "facc_positive", Severity.WARNING, "facc")

    where_clause = f"AND region = '{region}'" if region else ""

    query = f"""
    SELECT
        reach_id, region, river_name, x, y,
        facc
    FROM reaches
    WHERE facc < 0
      {where_clause}
    ORDER BY reach_id
    """

    issues = conn.execute(query).fetchdf()

    total_query = f"""
    SELECT COUNT(*) FROM reaches WHERE 1=1 {where_clause}
    """
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="A024",
        name="facc_positive",
        severity=Severity.WARNING,
        passed=len(issues) == 0,
        total_checked=total,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total if total > 0 else 0,
        details=issues,
        description="Reaches with negative facc",
    )


@register_check(
    "A026",
    Category.ATTRIBUTES,
    Severity.WARNING,
    "facc should generally increase downstream",
    default_threshold=0.0,
)
def check_facc_monotonicity(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """Flag reaches where facc decreases downstream (on main path)."""
    if not _column_exists(conn, "facc"):
        return _skip_result("A026", "facc_monotonicity", Severity.WARNING, "facc")

    where_clause = f"AND r1.region = '{region}'" if region else ""

    query = f"""
    SELECT
        r1.reach_id, r1.region, r1.river_name, r1.x, r1.y,
        r1.facc as facc_up,
        r2.facc as facc_down,
        (r1.facc - r2.facc) as facc_decrease
    FROM reaches r1
    JOIN reach_topology rt ON r1.reach_id = rt.reach_id AND r1.region = rt.region
    JOIN reaches r2 ON rt.neighbor_reach_id = r2.reach_id AND rt.region = r2.region
    WHERE rt.direction = 'down'
      AND r1.facc > r2.facc
      {where_clause}
    ORDER BY facc_decrease DESC
    LIMIT 10000
    """

    issues = conn.execute(query).fetchdf()

    total_query = f"""
    SELECT COUNT(*) FROM reach_topology rt WHERE direction = 'down' {where_clause.replace("r1.", "")}
    """
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="A026",
        name="facc_monotonicity",
        severity=Severity.WARNING,
        passed=len(issues) == 0,
        total_checked=total,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total if total > 0 else 0,
        details=issues,
        description="Reaches where facc decreases downstream",
    )


@register_check(
    "A027",
    Category.ATTRIBUTES,
    Severity.WARNING,
    "reach_length must be consistent with node-to-node distance",
    default_threshold=2.0,  # factor
)
def check_reach_length_node_consistency(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """Flag reaches where reach_length differs significantly from node-to-node distance."""
    tolerance = threshold if threshold is not None else 2.0
    where_clause = f"AND r.region = '{region}'" if region else ""

    # Estimate length from node distance
    query = f"""
    WITH node_dist AS (
        SELECT
            reach_id, region,
            MAX(dist_out) - MIN(dist_out) as node_span
        FROM nodes
        WHERE dist_out != -9999
        GROUP BY reach_id, region
    )
    SELECT
        r.reach_id, r.region, r.river_name, r.x, r.y,
        r.reach_length, nd.node_span,
        ABS(r.reach_length - nd.node_span) as diff
    FROM reaches r
    JOIN node_dist nd ON r.reach_id = nd.reach_id AND r.region = nd.region
    WHERE r.reach_length > 0 AND nd.node_span > 0
      AND (r.reach_length / nd.node_span > {tolerance} OR nd.node_span / r.reach_length > {tolerance})
      {where_clause}
    ORDER BY diff DESC
    LIMIT 10000
    """

    issues = conn.execute(query).fetchdf()

    total_query = f"""
    SELECT COUNT(*) FROM reaches r WHERE 1=1 {where_clause}
    """
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="A027",
        name="reach_length_node_consistency",
        severity=Severity.WARNING,
        passed=len(issues) == 0,
        total_checked=total,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total if total > 0 else 0,
        details=issues,
        description=f"Reaches where reach_length and node-to-node distance differ by >{tolerance}x",
    )


@register_check(
    "A010",
    Category.ATTRIBUTES,
    Severity.WARNING,
    "end_reach flag should match topology",
)
def check_end_reach_consistency(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """Flag reaches where end_reach flag disagrees with actual topology.

    end_reach semantics: 0=middle, 1=headwater, 2=outlet, 3=junction.
    - headwater(1) should have n_rch_up=0
    - outlet(2) should have n_rch_down=0
    """
    where_clause = f"AND r.region = '{region}'" if region else ""

    query = f"""
    SELECT
        r.reach_id, r.region, r.river_name, r.x, r.y,
        r.end_reach, r.n_rch_up, r.n_rch_down
    FROM reaches r
    WHERE (
        (r.end_reach = 1 AND r.n_rch_up > 0)
        OR (r.end_reach = 2 AND r.n_rch_down > 0)
    )
      {where_clause}
    ORDER BY r.reach_id
    """

    issues = conn.execute(query).fetchdf()

    total_query = f"""
    SELECT COUNT(*) FROM reaches r WHERE 1=1 {where_clause}
    """
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="A010",
        name="end_reach_consistency",
        severity=Severity.WARNING,
        passed=len(issues) == 0,
        total_checked=total,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total if total > 0 else 0,
        details=issues,
        description=f"Reaches with end_reach flag mismatch: {len(issues)}",
    )


@register_check(
    "A011",
    Category.ATTRIBUTES,
    Severity.WARNING,
    "Multi-name reaches must use '; ' separator exactly",
)
def check_river_name_separator(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """Flag reaches with multi-names that use non-standard delimiters (e.g., ',', ';', ' ; ')."""
    where_clause = f"AND region = '{region}'" if region else ""

    # Look for any reach with ';' but NOT '; '
    # Also look for ',' or other common mis-delimiters
    query = f"""
    SELECT
        reach_id, region, river_name, x, y
    FROM reaches
    WHERE river_name != 'NODATA'
      AND (
          (river_name LIKE '%;%' AND river_name NOT LIKE '%; %')
          OR river_name LIKE '%,%'
          OR river_name LIKE '%/%'
          OR river_name LIKE '%|%'
      )
      {where_clause}
    ORDER BY reach_id
    """

    issues = conn.execute(query).fetchdf()

    total_query = f"""
    SELECT COUNT(*) FROM reaches WHERE river_name != 'NODATA' {where_clause}
    """
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="A011",
        name="river_name_separator",
        severity=Severity.WARNING,
        passed=len(issues) == 0,
        total_checked=total,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total if total > 0 else 0,
        details=issues,
        description="Reaches with malformed multi-name separators (must use '; ')",
    )


@register_check(
    "A012",
    Category.ATTRIBUTES,
    Severity.WARNING,
    "Multi-name reaches must be alphabetically ordered",
)
def check_river_name_order(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """Verify that multi-name concatenations are alphabetically sorted."""
    where_clause = f"AND region = '{region}'" if region else ""

    # We need to fetch the names and check in Python, or use a complex SQL sort
    # Simpler to fetch multi-name reaches and validate
    query = f"""
    SELECT
        reach_id, region, river_name, x, y
    FROM reaches
    WHERE river_name LIKE '%; %'
      {where_clause}
    """

    multi_names = conn.execute(query).fetchdf()
    issues = []

    for _, row in multi_names.iterrows():
        names = row["river_name"].split("; ")
        if names != sorted(names):
            issues.append(row)

    issues_df = pd.DataFrame(issues)

    total = len(multi_names)

    return CheckResult(
        check_id="A012",
        name="river_name_order",
        severity=Severity.WARNING,
        passed=len(issues_df) == 0,
        total_checked=total,
        issues_found=len(issues_df),
        issue_pct=100 * len(issues_df) / total if total > 0 else 0,
        details=issues_df,
        description="Multi-name reaches with incorrect alphabetical ordering",
    )


@register_check(
    "A013",
    Category.ATTRIBUTES,
    Severity.ERROR,
    "river_name must be ASCII-only for export compatibility",
)
def check_river_name_ascii(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """Flag reaches with non-ASCII characters in river_name (crucial for Shapefile/NetCDF)."""
    where_clause = f"AND region = '{region}'" if region else ""

    # DuckDB regex for non-ASCII: [^\x00-\x7F]
    query = f"""
    SELECT
        reach_id, region, river_name, x, y
    FROM reaches
    WHERE regexp_full_match(river_name, '.*[^\\x00-\\x7F].*')
      {where_clause}
    ORDER BY reach_id
    """

    issues = conn.execute(query).fetchdf()

    total_query = f"""
    SELECT COUNT(*) FROM reaches WHERE 1=1 {where_clause}
    """
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="A013",
        name="river_name_ascii",
        severity=Severity.ERROR,
        passed=len(issues) == 0,
        total_checked=total,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total if total > 0 else 0,
        details=issues,
        description="Reaches with non-ASCII characters in river_name",
    )


@register_check(
    "A014",
    Category.ATTRIBUTES,
    Severity.WARNING,
    "river_name must not have leading or trailing whitespace",
)
def check_river_name_whitespace(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """Flag reaches with leading/trailing spaces or double spaces in river_name."""
    where_clause = f"AND region = '{region}'" if region else ""

    query = f"""
    SELECT
        reach_id, region, river_name, x, y
    FROM reaches
    WHERE river_name != TRIM(river_name)
       OR river_name LIKE '%  %'
      {where_clause}
    ORDER BY reach_id
    """

    issues = conn.execute(query).fetchdf()

    total_query = f"""
    SELECT COUNT(*) FROM reaches WHERE 1=1 {where_clause}
    """
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="A014",
        name="river_name_whitespace",
        severity=Severity.WARNING,
        passed=len(issues) == 0,
        total_checked=total,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total if total > 0 else 0,
        details=issues,
        description="Reaches with leading/trailing or redundant whitespace in river_name",
    )


@register_check(
    "A030",
    Category.ATTRIBUTES,
    Severity.WARNING,
    "WSE should generally decrease downstream (water flows downhill)",
    default_threshold=10.0,  # tolerance in meters
)
def check_wse_monotonicity(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """Flag reaches where WSE increases downstream by more than threshold."""
    tolerance = threshold if threshold is not None else 10.0
    where_clause = f"AND r1.region = '{region}'" if region else ""

    query = f"""
    SELECT
        r1.reach_id, r1.region, r1.river_name, r1.x, r1.y,
        r1.wse as wse_up,
        r2.wse as wse_down,
        (r2.wse - r1.wse) as wse_increase
    FROM reaches r1
    JOIN reach_topology rt ON r1.reach_id = rt.reach_id AND r1.region = rt.region
    JOIN reaches r2 ON rt.neighbor_reach_id = r2.reach_id AND rt.region = r2.region
    WHERE rt.direction = 'down'
      AND r1.wse != -9999 AND r2.wse != -9999
      AND r2.wse > r1.wse + {tolerance}
      {where_clause}
    ORDER BY wse_increase DESC
    LIMIT 10000
    """

    issues = conn.execute(query).fetchdf()

    total_where = f"AND rt.region = '{region}'" if region else ""
    total_query = f"""
    SELECT COUNT(*) FROM reach_topology rt
    JOIN reaches r1 ON rt.reach_id = r1.reach_id AND rt.region = r1.region
    JOIN reaches r2 ON rt.neighbor_reach_id = r2.reach_id AND rt.region = r2.region
    WHERE rt.direction = 'down'
      AND r1.wse != -9999 AND r2.wse != -9999
      {total_where}
    """
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="A030",
        name="wse_monotonicity",
        severity=Severity.WARNING,
        passed=len(issues) == 0,
        total_checked=total,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total if total > 0 else 0,
        details=issues,
        description=f"Reaches where WSE increases downstream by >{tolerance}m",
        threshold=tolerance,
    )


@register_check(
    "A031",
    Category.ATTRIBUTES,
    Severity.WARNING,
    "WSE variance along reach should be reasonable",
    default_threshold=5.0,  # meters
)
def check_wse_variance(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """Flag reaches with high WSE variance among constituent nodes."""
    max_var = threshold if threshold is not None else 5.0
    where_clause = f"AND region = '{region}'" if region else ""

    query = f"""
    SELECT
        reach_id, region,
        STDDEV(wse) as wse_std,
        COUNT(*) as node_count
    FROM nodes
    WHERE wse != -9999
      {where_clause}
    GROUP BY reach_id, region
    HAVING STDDEV(wse) > {max_var}
    ORDER BY wse_std DESC
    LIMIT 10000
    """

    issues = conn.execute(query).fetchdf()

    total_query = f"""
    SELECT COUNT(DISTINCT reach_id) FROM nodes WHERE wse != -9999 {where_clause}
    """
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="A031",
        name="wse_variance",
        severity=Severity.WARNING,
        passed=len(issues) == 0,
        total_checked=total,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total if total > 0 else 0,
        details=issues,
        description=f"Reaches with WSE standard deviation >{max_var}m across nodes",
        threshold=max_var,
    )
