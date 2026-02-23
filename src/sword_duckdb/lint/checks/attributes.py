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
    default_threshold=100.0,  # m/km
)
def check_slope_reasonableness(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """
    Check that slope values are reasonable.

    Flags:
    - Negative slopes (physically impossible for flow)
    - Extremely high slopes (>100 m/km by default)
    """
    max_slope = threshold if threshold is not None else 100.0
    where_clause = f"AND r.region = '{region}'" if region else ""

    # Check for negative slopes OR unreasonably high slopes
    query = f"""
    SELECT
        r.reach_id, r.region, r.river_name, r.x, r.y,
        r.slope, r.reach_length, r.lakeflag,
        CASE
            WHEN r.slope < 0 THEN 'negative'
            WHEN r.slope > {max_slope} THEN 'too_high'
        END as issue_type
    FROM reaches r
    WHERE r.slope IS NOT NULL
        AND r.slope != -9999
        AND (r.slope < 0 OR r.slope > {max_slope})
        AND r.lakeflag = 0  -- rivers only
        {where_clause}
    ORDER BY ABS(r.slope) DESC
    """

    issues = conn.execute(query).fetchdf()

    total_query = f"""
    SELECT COUNT(*) FROM reaches r
    WHERE slope IS NOT NULL AND slope != -9999 AND lakeflag = 0
    {where_clause}
    """
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="A002",
        name="slope_reasonableness",
        severity=Severity.WARNING,
        passed=len(issues) == 0,
        total_checked=total,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total if total > 0 else 0,
        details=issues,
        description=f"Reaches with negative or excessive (>{max_slope} m/km) slope",
        threshold=max_slope,
    )


@register_check(
    "A003",
    Category.ATTRIBUTES,
    Severity.INFO,
    "Width generally increases downstream",
    default_threshold=0.3,  # 30% of upstream width
)
def check_width_trend(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """
    Check if width generally increases downstream.

    Flags reaches where downstream width is less than threshold * upstream width.
    Some decrease is normal, but dramatic decreases suggest issues.
    """
    ratio_threshold = threshold if threshold is not None else 0.3
    where_clause = f"AND r1.region = '{region}'" if region else ""

    query = f"""
    WITH reach_pairs AS (
        SELECT
            r1.reach_id,
            r1.region,
            r1.width as width_up,
            r2.width as width_down,
            r1.river_name,
            r1.x, r1.y,
            r1.lakeflag as lakeflag_up,
            r2.lakeflag as lakeflag_down
        FROM reaches r1
        JOIN reach_topology rt ON r1.reach_id = rt.reach_id AND r1.region = rt.region
        JOIN reaches r2 ON rt.neighbor_reach_id = r2.reach_id AND rt.region = r2.region
        WHERE rt.direction = 'down'
            AND r1.width > 0 AND r1.width != -9999
            AND r2.width > 0 AND r2.width != -9999
            AND r1.lakeflag = 0 AND r2.lakeflag = 0  -- rivers only
            {where_clause}
    )
    SELECT
        reach_id, region, river_name, x, y,
        width_up, width_down,
        ROUND(width_down / width_up, 3) as width_ratio
    FROM reach_pairs
    WHERE width_down < {ratio_threshold} * width_up
        AND width_up > 100  -- ignore small streams
    ORDER BY width_ratio ASC
    """

    issues = conn.execute(query).fetchdf()

    total_query = f"""
    SELECT COUNT(*) FROM reaches r1
    WHERE width > 100 AND width != -9999 AND lakeflag = 0
    {where_clause.replace("r1.", "")}
    """
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="A003",
        name="width_trend",
        severity=Severity.INFO,
        passed=len(issues) == 0,
        total_checked=total,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total if total > 0 else 0,
        details=issues,
        description=f"Reaches where downstream width < {ratio_threshold * 100:.0f}% of upstream width",
        threshold=ratio_threshold,
    )


@register_check(
    "A004",
    Category.ATTRIBUTES,
    Severity.INFO,
    "Check attribute completeness for required fields",
)
def check_attribute_completeness(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """
    Check completeness of required attributes.

    Reports percentage of null/missing values for key attributes.
    """
    where_clause = f"AND region = '{region}'" if region else ""

    required_attrs = [
        "dist_out",
        "facc",
        "wse",
        "width",
        "slope",
        "reach_length",
        "lakeflag",
        "n_rch_up",
        "n_rch_down",
    ]

    # Build completeness query
    select_parts = []
    for attr in required_attrs:
        select_parts.append(f"""
            SUM(CASE WHEN {attr} IS NULL OR {attr} = -9999 THEN 1 ELSE 0 END) as {attr}_missing,
            COUNT(*) as {attr}_total
        """)

    query = f"""
    SELECT
        {", ".join(select_parts)}
    FROM reaches
    WHERE 1=1 {where_clause}
    """

    result = conn.execute(query).fetchone()

    # Build summary DataFrame
    rows = []
    idx = 0
    for attr in required_attrs:
        missing = result[idx]
        total = result[idx + 1]
        pct_missing = 100 * missing / total if total > 0 else 0
        rows.append(
            {
                "attribute": attr,
                "missing_count": missing,
                "total_count": total,
                "pct_missing": round(pct_missing, 2),
            }
        )
        idx += 2

    details = pd.DataFrame(rows)

    # Count attributes with >5% missing as "issues"
    high_missing = details[details["pct_missing"] > 5]

    total_query = f"""
    SELECT COUNT(*) FROM reaches WHERE 1=1 {where_clause}
    """
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="A004",
        name="attribute_completeness",
        severity=Severity.INFO,
        passed=len(high_missing) == 0,
        total_checked=len(required_attrs),
        issues_found=len(high_missing),
        issue_pct=100 * len(high_missing) / len(required_attrs),
        details=details,
        description=f"Attribute completeness check ({len(high_missing)} attrs with >5% missing)",
    )


@register_check(
    "A005",
    Category.ATTRIBUTES,
    Severity.INFO,
    "trib_flag distribution (unmapped tributaries)",
)
def check_trib_flag_distribution(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """
    Report trib_flag distribution.

    trib_flag indicates UNMAPPED tributaries (rivers not in SWORD topology
    but contributing flow, detected via facc jumps from MERIT Hydro).
    - 0 = no unmapped tributary
    - 1 = unmapped tributary entering

    This is NOT about n_rch_up count - it's about external flow sources.
    """
    where_clause = f"AND r.region = '{region}'" if region else ""

    query = f"""
    SELECT
        trib_flag,
        COUNT(*) as reach_count,
        ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as pct
    FROM reaches r
    WHERE trib_flag IS NOT NULL
        {where_clause}
    GROUP BY trib_flag
    ORDER BY trib_flag
    """

    stats = conn.execute(query).fetchdf()

    total_query = f"""
    SELECT COUNT(*) FROM reaches r
    WHERE trib_flag IS NOT NULL
    {where_clause}
    """
    total = conn.execute(total_query).fetchone()[0]

    # Count reaches with unmapped tributaries
    unmapped_count = 0
    if len(stats) > 0 and 1 in stats["trib_flag"].values:
        unmapped_count = int(stats[stats["trib_flag"] == 1]["reach_count"].values[0])

    return CheckResult(
        check_id="A005",
        name="trib_flag_distribution",
        severity=Severity.INFO,
        passed=True,  # Informational
        total_checked=total,
        issues_found=unmapped_count,
        issue_pct=100 * unmapped_count / total if total > 0 else 0,
        details=stats,
        description=f"Reaches with unmapped tributaries (trib_flag=1): {unmapped_count} ({100 * unmapped_count / total:.1f}%)"
        if total > 0
        else "No data",
    )


@register_check(
    "A006",
    Category.ATTRIBUTES,
    Severity.INFO,
    "Check for extreme or outlier values in key attributes",
)
def check_attribute_outliers(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """
    Check for extreme outlier values in key attributes.

    Flags reaches with values outside typical ranges:
    - width > 50km
    - wse > 8000m (higher than any river)
    - facc > 10M km² (larger than Amazon basin)
    """
    where_clause = f"AND r.region = '{region}'" if region else ""

    query = f"""
    SELECT
        r.reach_id, r.region, r.river_name, r.x, r.y,
        r.width, r.wse, r.facc,
        CASE
            WHEN r.width > 50000 THEN 'extreme_width'
            WHEN r.wse > 8000 THEN 'extreme_wse'
            WHEN r.facc > 10000000 THEN 'extreme_facc'
        END as issue_type
    FROM reaches r
    WHERE (
            (r.width > 50000 AND r.width IS NOT NULL AND r.width != -9999)
            OR (r.wse > 8000 AND r.wse IS NOT NULL AND r.wse != -9999)
            OR (r.facc > 10000000 AND r.facc IS NOT NULL AND r.facc != -9999)
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
        check_id="A006",
        name="attribute_outliers",
        severity=Severity.INFO,
        passed=len(issues) == 0,
        total_checked=total,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total if total > 0 else 0,
        details=issues,
        description="Reaches with extreme outlier values (width>50km, wse>8000m, facc>10M km²)",
    )


@register_check(
    "A007",
    Category.ATTRIBUTES,
    Severity.WARNING,
    "Headwaters should have low facc (<10000 km²)",
    default_threshold=10000.0,  # km² - headwaters shouldn't drain huge areas
)
def check_headwater_facc(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """
    Check that headwater reaches have reasonably low flow accumulation.

    Headwaters (n_rch_up=0, end_reach=1) should have small drainage areas.
    High facc at headwaters suggests topology error (missing upstream).
    """
    max_facc = threshold if threshold is not None else 10000.0
    where_clause = f"AND r.region = '{region}'" if region else ""

    query = f"""
    SELECT
        r.reach_id, r.region, r.river_name, r.x, r.y,
        r.facc, r.n_rch_up, r.end_reach, r.width, r.type
    FROM reaches r
    WHERE r.n_rch_up = 0  -- Headwater
        AND r.facc > {max_facc}
        AND r.facc != -9999
        AND r.type NOT IN (5, 6)  -- Exclude unreliable/ghost
        {where_clause}
    ORDER BY r.facc DESC
    """

    issues = conn.execute(query).fetchdf()

    total_query = f"""
    SELECT COUNT(*) FROM reaches r
    WHERE n_rch_up = 0 AND type NOT IN (5, 6)
    {where_clause}
    """
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="A007",
        name="headwater_facc",
        severity=Severity.WARNING,
        passed=len(issues) == 0,
        total_checked=total,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total if total > 0 else 0,
        details=issues,
        description=f"Headwaters with suspiciously high facc (>{max_facc} km²) - missing upstream?",
        threshold=max_facc,
    )


@register_check(
    "A008",
    Category.ATTRIBUTES,
    Severity.WARNING,
    "Headwaters should have narrow width (<500m typically)",
    default_threshold=500.0,  # meters
)
def check_headwater_width(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """
    Check that headwater reaches have reasonable widths.

    Headwaters are typically narrow streams. Very wide headwaters
    suggest misclassification or missing upstream topology.
    """
    max_width = threshold if threshold is not None else 500.0
    where_clause = f"AND r.region = '{region}'" if region else ""

    query = f"""
    SELECT
        r.reach_id, r.region, r.river_name, r.x, r.y,
        r.width, r.facc, r.n_rch_up, r.end_reach, r.lakeflag, r.type
    FROM reaches r
    WHERE r.n_rch_up = 0  -- Headwater
        AND r.width > {max_width}
        AND r.width != -9999
        AND r.lakeflag = 0  -- Rivers only (lakes can be wide)
        AND r.type NOT IN (5, 6)  -- Exclude unreliable/ghost
        {where_clause}
    ORDER BY r.width DESC
    """

    issues = conn.execute(query).fetchdf()

    total_query = f"""
    SELECT COUNT(*) FROM reaches r
    WHERE n_rch_up = 0 AND lakeflag = 0 AND type NOT IN (5, 6)
    {where_clause}
    """
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="A008",
        name="headwater_width",
        severity=Severity.WARNING,
        passed=len(issues) == 0,
        total_checked=total,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total if total > 0 else 0,
        details=issues,
        description=f"Headwater rivers wider than {max_width}m - missing upstream topology?",
        threshold=max_width,
    )


@register_check(
    "A009",
    Category.ATTRIBUTES,
    Severity.INFO,
    "Outlets should have high facc (>1000 km² typically)",
    default_threshold=1000.0,  # km²
)
def check_outlet_facc(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """
    Check that outlet reaches have reasonable flow accumulation.

    Outlets (n_rch_down=0, end_reach=2) should drain significant areas.
    Very low facc at outlets suggests isolated/minor features.
    """
    min_facc = threshold if threshold is not None else 1000.0
    where_clause = f"AND r.region = '{region}'" if region else ""

    query = f"""
    SELECT
        r.reach_id, r.region, r.river_name, r.x, r.y,
        r.facc, r.n_rch_down, r.end_reach, r.width, r.type
    FROM reaches r
    WHERE r.n_rch_down = 0  -- Outlet
        AND r.facc < {min_facc}
        AND r.facc > 0 AND r.facc != -9999
        AND r.type NOT IN (5, 6)  -- Exclude unreliable/ghost
        {where_clause}
    ORDER BY r.facc ASC
    """

    issues = conn.execute(query).fetchdf()

    total_query = f"""
    SELECT COUNT(*) FROM reaches r
    WHERE n_rch_down = 0 AND type NOT IN (5, 6)
    {where_clause}
    """
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="A009",
        name="outlet_facc",
        severity=Severity.INFO,
        passed=True,  # Informational - small outlets are valid
        total_checked=total,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total if total > 0 else 0,
        details=issues,
        description=f"Outlets with low facc (<{min_facc} km²) - minor outlets or isolated features",
        threshold=min_facc,
    )


@register_check(
    "A021",
    Category.ATTRIBUTES,
    Severity.WARNING,
    "wse_obs_p50 should be close to wse",
    default_threshold=10.0,  # meters
)
def check_wse_obs_vs_wse(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """
    Check that SWOT-observed WSE p50 (median) is close to the reference WSE.

    Large differences suggest measurement issues or temporal variability.
    """
    if not _column_exists(conn, "wse_obs_p50"):
        return _skip_result("A021", "wse_obs_vs_wse", Severity.WARNING, "wse_obs_p50")

    max_diff = threshold if threshold is not None else 10.0
    where_clause = f"AND r.region = '{region}'" if region else ""

    query = f"""
    SELECT
        r.reach_id, r.region, r.river_name, r.x, r.y,
        r.wse, r.wse_obs_p50,
        ROUND(ABS(r.wse_obs_p50 - r.wse), 3) as wse_diff,
        r.lakeflag
    FROM reaches r
    WHERE r.wse IS NOT NULL AND r.wse != -9999
        AND r.wse_obs_p50 IS NOT NULL AND r.wse_obs_p50 != -9999
        AND ABS(r.wse_obs_p50 - r.wse) > {max_diff}
        AND r.lakeflag = 0
        {where_clause}
    ORDER BY ABS(r.wse_obs_p50 - r.wse) DESC
    """

    issues = conn.execute(query).fetchdf()

    total_query = f"""
    SELECT COUNT(*) FROM reaches r
    WHERE wse IS NOT NULL AND wse != -9999
        AND wse_obs_p50 IS NOT NULL AND wse_obs_p50 != -9999
        AND lakeflag = 0
    {where_clause}
    """
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="A021",
        name="wse_obs_vs_wse",
        severity=Severity.WARNING,
        passed=len(issues) == 0,
        total_checked=total,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total if total > 0 else 0,
        details=issues,
        description=f"Reaches where |wse_obs_p50 - wse| > {max_diff}m",
        threshold=max_diff,
    )


@register_check(
    "A024",
    Category.ATTRIBUTES,
    Severity.INFO,
    "width_obs_p50 should be reasonable vs width",
    default_threshold=3.0,  # ratio
)
def check_width_obs_vs_width(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """
    Check that SWOT-observed width p50 (median) is reasonable vs reference width.

    Flags reaches where the ratio width_obs_p50/width is very large or
    very small, suggesting measurement or classification issues.
    """
    if not _column_exists(conn, "width_obs_p50"):
        return _skip_result(
            "A024", "width_obs_vs_width", Severity.INFO, "width_obs_p50"
        )

    max_ratio = threshold if threshold is not None else 3.0
    where_clause = f"AND r.region = '{region}'" if region else ""

    query = f"""
    SELECT
        r.reach_id, r.region, r.river_name, r.x, r.y,
        r.width, r.width_obs_p50,
        ROUND(r.width_obs_p50 / r.width, 3) as width_ratio,
        r.lakeflag
    FROM reaches r
    WHERE r.width IS NOT NULL AND r.width > 0 AND r.width != -9999
        AND r.width_obs_p50 IS NOT NULL AND r.width_obs_p50 > 0
            AND r.width_obs_p50 != -9999
        AND r.lakeflag = 0
        AND (r.width_obs_p50 / r.width > {max_ratio}
             OR r.width_obs_p50 / r.width < 1.0 / {max_ratio})
        {where_clause}
    ORDER BY ABS(LN(r.width_obs_p50 / r.width)) DESC
    """

    issues = conn.execute(query).fetchdf()

    total_query = f"""
    SELECT COUNT(*) FROM reaches r
    WHERE width IS NOT NULL AND width > 0 AND width != -9999
        AND width_obs_p50 IS NOT NULL AND width_obs_p50 > 0
            AND width_obs_p50 != -9999
        AND lakeflag = 0
    {where_clause}
    """
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="A024",
        name="width_obs_vs_width",
        severity=Severity.INFO,
        passed=len(issues) == 0,
        total_checked=total,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total if total > 0 else 0,
        details=issues,
        description=f"Reaches where width_obs_p50/width ratio outside [1/{max_ratio:.0f}, {max_ratio:.0f}]",
        threshold=max_ratio,
    )


@register_check(
    "A026",
    Category.ATTRIBUTES,
    Severity.WARNING,
    "slope_obs_p50 should be non-negative",
    default_threshold=-0.01,  # m/km — slopes more negative than this are flagged
)
def check_slope_obs_nonneg(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """
    Check that SWOT-observed slope p50 (median) is non-negative.

    ~10% of observed reaches have slightly negative slope_obs_p50. This is
    expected SWOT measurement noise near zero slope — the median of the
    negative values is -0.00004 m/km (40 micrometers/km). It affects all
    regions uniformly (10-13%) and is most common on tidal, lake, and flat
    river reaches where true slope is near zero.

    The slope_obs_quality column already classifies these as 'negative' or
    'below_ref_uncertainty', and slope_obs_reliable flags them accordingly.

    Only reaches with slope_obs_p50 below the threshold (default -0.01 m/km)
    are counted as issues — these are rare (~200 globally) and may indicate
    real problems. Near-zero negatives are reported in the description but
    don't fail the check.
    """
    if not _column_exists(conn, "slope_obs_p50"):
        return _skip_result(
            "A026", "slope_obs_nonneg", Severity.WARNING, "slope_obs_p50"
        )

    cutoff = threshold if threshold is not None else -0.01
    where_clause = f"AND r.region = '{region}'" if region else ""

    query = f"""
    SELECT
        r.reach_id, r.region, r.river_name, r.x, r.y,
        r.slope_obs_p50, r.slope, r.wse, r.lakeflag,
        CASE WHEN r.slope_obs_p50 < {cutoff} THEN 'significant'
             ELSE 'measurement_noise'
        END as negative_type
    FROM reaches r
    WHERE r.slope_obs_p50 IS NOT NULL
        AND r.slope_obs_p50 != -9999
        AND r.slope_obs_p50 < 0
        {where_clause}
    ORDER BY r.slope_obs_p50 ASC
    """

    all_negatives = conn.execute(query).fetchdf()

    if len(all_negatives) > 0:
        significant = all_negatives[all_negatives["negative_type"] == "significant"]
        noise = all_negatives[all_negatives["negative_type"] == "measurement_noise"]
    else:
        significant = all_negatives
        noise = all_negatives

    total_query = f"""
    SELECT COUNT(*) FROM reaches r
    WHERE slope_obs_p50 IS NOT NULL AND slope_obs_p50 != -9999
    {where_clause}
    """
    total = conn.execute(total_query).fetchone()[0]

    n_sig = len(significant)
    n_noise = len(noise)

    desc = f"Negative slope_obs_p50: {n_sig} significant (< {cutoff} m/km)"
    if n_noise > 0:
        desc += (
            f", {n_noise} measurement noise (near-zero negatives, "
            f"expected ~10% of observed reaches from SWOT precision limits)"
        )

    return CheckResult(
        check_id="A026",
        name="slope_obs_nonneg",
        severity=Severity.WARNING,
        passed=n_sig == 0,
        total_checked=total,
        issues_found=n_sig,
        issue_pct=100 * n_sig / total if total > 0 else 0,
        details=all_negatives,
        description=desc,
        threshold=cutoff,
    )


@register_check(
    "A027",
    Category.ATTRIBUTES,
    Severity.WARNING,
    "slope_obs_p50 should be < 50 m/km",
    default_threshold=50.0,  # m/km
)
def check_slope_obs_extreme(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """
    Check that SWOT-observed slope p50 (median) is not unreasonably high.

    Slopes >50 m/km are extremely rare for rivers and likely indicate
    measurement error or reach geometry issues.
    """
    if not _column_exists(conn, "slope_obs_p50"):
        return _skip_result(
            "A027", "slope_obs_extreme", Severity.WARNING, "slope_obs_p50"
        )

    max_slope = threshold if threshold is not None else 50.0
    where_clause = f"AND r.region = '{region}'" if region else ""

    query = f"""
    SELECT
        r.reach_id, r.region, r.river_name, r.x, r.y,
        r.slope_obs_p50, r.slope, r.reach_length, r.lakeflag
    FROM reaches r
    WHERE r.slope_obs_p50 IS NOT NULL
        AND r.slope_obs_p50 != -9999
        AND r.slope_obs_p50 > {max_slope}
        AND r.lakeflag = 0
        {where_clause}
    ORDER BY r.slope_obs_p50 DESC
    """

    issues = conn.execute(query).fetchdf()

    total_query = f"""
    SELECT COUNT(*) FROM reaches r
    WHERE slope_obs_p50 IS NOT NULL AND slope_obs_p50 != -9999 AND lakeflag = 0
    {where_clause}
    """
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="A027",
        name="slope_obs_extreme",
        severity=Severity.WARNING,
        passed=len(issues) == 0,
        total_checked=total,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total if total > 0 else 0,
        details=issues,
        description=f"Reaches with slope_obs_p50 > {max_slope} m/km",
        threshold=max_slope,
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
    """
    Check that end_reach classification matches actual topology.

    end_reach values:
    - 0 = normal reach (has both up and down neighbors)
    - 1 = headwater (no upstream)
    - 2 = outlet (no downstream)
    - 3 = junction (multiple up or down)
    """
    where_clause = f"AND r.region = '{region}'" if region else ""

    query = f"""
    SELECT
        r.reach_id, r.region, r.river_name, r.x, r.y,
        r.end_reach, r.n_rch_up, r.n_rch_down,
        CASE
            WHEN r.end_reach = 1 AND r.n_rch_up > 0 THEN 'marked_headwater_but_has_upstream'
            WHEN r.end_reach = 2 AND r.n_rch_down > 0 THEN 'marked_outlet_but_has_downstream'
            WHEN r.end_reach = 0 AND r.n_rch_up = 0 THEN 'unmarked_headwater'
            WHEN r.end_reach = 0 AND r.n_rch_down = 0 THEN 'unmarked_outlet'
        END as issue_type
    FROM reaches r
    WHERE (
        (r.end_reach = 1 AND r.n_rch_up > 0) OR
        (r.end_reach = 2 AND r.n_rch_down > 0) OR
        (r.end_reach = 0 AND r.n_rch_up = 0 AND r.n_rch_down > 0) OR
        (r.end_reach = 0 AND r.n_rch_down = 0 AND r.n_rch_up > 0)
    )
        AND r.type NOT IN (5, 6)
        {where_clause}
    ORDER BY r.reach_id
    """

    issues = conn.execute(query).fetchdf()

    total_query = f"""
    SELECT COUNT(*) FROM reaches r
    WHERE type NOT IN (5, 6)
    {where_clause}
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
        description="Reaches where end_reach flag doesn't match actual topology",
    )


@register_check(
    "A030",
    Category.ATTRIBUTES,
    Severity.WARNING,
    "WSE should generally decrease downstream (water flows downhill)",
)
def check_wse_downstream_monotonicity(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """Flag reaches where wse increases downstream (counterflow)."""
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
        AND r1.wse IS NOT NULL AND r1.wse != -9999
        AND r2.wse IS NOT NULL AND r2.wse != -9999
        AND r2.wse > r1.wse
        {where_clause}
    ORDER BY wse_increase DESC
    LIMIT 10000
    """

    issues = conn.execute(query).fetchdf()

    total_query = f"""
    SELECT COUNT(*) FROM reaches r1
    JOIN reach_topology rt ON r1.reach_id = rt.reach_id AND r1.region = rt.region
    WHERE rt.direction = 'down'
        AND r1.wse IS NOT NULL AND r1.wse != -9999
        {where_clause}
    """
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="A030",
        name="wse_downstream_monotonicity",
        severity=Severity.WARNING,
        passed=len(issues) == 0,
        total_checked=total,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total if total > 0 else 0,
        details=issues,
        description="Reaches where WSE increases downstream (water should flow downhill)",
    )


@register_check(
    "A031",
    Category.ATTRIBUTES,
    Severity.WARNING,
    "Nodes with width <= 0",
)
def check_zero_node_width(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """Find nodes whose width is zero or negative.

    These nodes need their width filled (e.g. from the reach median).
    """
    where_clause = f"AND n.region = '{region}'" if region else ""

    query = f"""
    SELECT n.node_id, n.reach_id, n.region, n.width
    FROM nodes n
    WHERE n.width <= 0
        {where_clause}
    ORDER BY n.reach_id, n.node_id
    LIMIT 10000
    """
    issues = conn.execute(query).fetchdf()

    total_query = f"""
    SELECT COUNT(*) FROM nodes n WHERE 1=1 {where_clause}
    """
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="A031",
        name="zero_node_width",
        severity=Severity.WARNING,
        passed=len(issues) == 0,
        total_checked=total,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total if total > 0 else 0,
        details=issues,
        description="Nodes with width <= 0 that need filling",
    )
