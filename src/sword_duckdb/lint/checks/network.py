"""
SWORD Lint - Network Checks (Nxxx)

Validates main_side, stream_order, and path_segs consistency.
"""

from typing import Optional

import duckdb

from ..core import (
    register_check,
    Category,
    Severity,
    CheckResult,
)


@register_check(
    "N001",
    Category.NETWORK,
    Severity.ERROR,
    "main_side must be in {0, 1, 2}",
)
def check_main_side_values(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """
    Validate main_side values.

    Valid values:
    - 0: main channel (~95%)
    - 1: side channel (~3%)
    - 2: secondary outlet (~2%)
    """
    where_clause = f"AND r.region = '{region}'" if region else ""

    query = f"""
    SELECT
        r.reach_id, r.region, r.river_name, r.x, r.y,
        r.main_side, r.stream_order, r.lakeflag
    FROM reaches r
    WHERE r.main_side IS NOT NULL
        AND r.main_side NOT IN (0, 1, 2)
        {where_clause}
    ORDER BY r.reach_id
    """

    issues = conn.execute(query).fetchdf()

    total_query = f"""
    SELECT COUNT(*) FROM reaches r
    WHERE main_side IS NOT NULL
    {where_clause}
    """
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="N001",
        name="main_side_values",
        severity=Severity.ERROR,
        passed=len(issues) == 0,
        total_checked=total,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total if total > 0 else 0,
        details=issues,
        description="Reaches with invalid main_side (not in {0, 1, 2})",
    )


@register_check(
    "N002",
    Category.NETWORK,
    Severity.ERROR,
    "main_side=0 (main channel) should have valid stream_order",
)
def check_main_side_stream_order(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """
    Check that main channel reaches have a valid stream_order.

    main_side=0 means the reach is on the main channel. These should have
    a valid stream_order (not -9999). Side channels (main_side=1) and
    secondary outlets (main_side=2) are expected to lack stream_order.
    """
    where_clause = f"AND r.region = '{region}'" if region else ""

    # Check if 'type' column exists for filtering unreliable reaches
    try:
        conn.execute("SELECT type FROM reaches LIMIT 0")
        type_filter = "AND r.type NOT IN (5, 6)"
        type_filter_bare = "AND type NOT IN (5, 6)"
    except duckdb.BinderException:
        type_filter = ""
        type_filter_bare = ""

    query = f"""
    SELECT
        r.reach_id, r.region, r.river_name, r.x, r.y,
        r.main_side, r.stream_order, r.path_freq, r.width, r.lakeflag
    FROM reaches r
    WHERE r.main_side = 0
        AND (r.stream_order IS NULL OR r.stream_order = -9999)
        {type_filter}
        {where_clause}
    ORDER BY r.reach_id
    """

    issues = conn.execute(query).fetchdf()

    total_query = f"""
    SELECT COUNT(*) FROM reaches r
    WHERE main_side = 0 {type_filter_bare}
    {where_clause}
    """
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="N002",
        name="main_side_stream_order",
        severity=Severity.ERROR,
        passed=len(issues) == 0,
        total_checked=total,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total if total > 0 else 0,
        details=issues,
        description="Main channel reaches (main_side=0) with invalid stream_order (-9999)",
    )


@register_check(
    "N014",
    Category.NETWORK,
    Severity.ERROR,
    "stream_order must equal round(ln(path_freq)) + 1",
)
def check_stream_order_formula(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """
    Validate stream_order matches the log-transform formula.

    Formula: stream_order = round(ln(path_freq)) + 1
    Only checked where path_freq > 0 and path_freq != -9999.
    Excludes type 5 (unreliable) and 6 (ghost).
    """
    where_clause = f"AND r.region = '{region}'" if region else ""

    # Check if 'type' column exists
    try:
        conn.execute("SELECT type FROM reaches LIMIT 0")
        type_filter = "AND r.type NOT IN (5, 6)"
        type_filter_total = "AND type NOT IN (5, 6)"
    except duckdb.BinderException:
        type_filter = ""
        type_filter_total = ""

    query = f"""
    SELECT
        r.reach_id, r.region, r.river_name, r.x, r.y,
        r.stream_order,
        r.path_freq,
        CAST(ROUND(LN(r.path_freq)) + 1 AS INTEGER) AS expected_stream_order
    FROM reaches r
    WHERE r.path_freq > 0
        AND r.path_freq != -9999
        AND r.stream_order != -9999
        AND r.stream_order != CAST(ROUND(LN(r.path_freq)) + 1 AS INTEGER)
        {type_filter}
        {where_clause}
    ORDER BY r.reach_id
    """

    issues = conn.execute(query).fetchdf()

    total_query = f"""
    SELECT COUNT(*) FROM reaches r
    WHERE path_freq > 0
        AND path_freq != -9999
        AND stream_order != -9999
        {type_filter_total}
        {where_clause.replace("r.", "")}
    """
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="N014",
        name="stream_order_formula",
        severity=Severity.ERROR,
        passed=len(issues) == 0,
        total_checked=total,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total if total > 0 else 0,
        details=issues,
        description="Reaches where stream_order != round(ln(path_freq)) + 1",
    )


@register_check(
    "N015",
    Category.NETWORK,
    Severity.WARNING,
    "stream_order must be >= 1 when not -9999",
)
def check_stream_order_range(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """
    Validate stream_order is in valid range.

    When stream_order is not -9999 (nodata), it must be >= 1.
    No zero or negative values allowed.
    """
    where_clause = f"AND r.region = '{region}'" if region else ""

    query = f"""
    SELECT
        r.reach_id, r.region, r.river_name, r.x, r.y,
        r.stream_order, r.path_freq, r.main_side
    FROM reaches r
    WHERE r.stream_order != -9999
        AND r.stream_order < 1
        {where_clause}
    ORDER BY r.reach_id
    """

    issues = conn.execute(query).fetchdf()

    total_query = f"""
    SELECT COUNT(*) FROM reaches r
    WHERE stream_order != -9999
    {where_clause.replace("r.", "")}
    """
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="N015",
        name="stream_order_range",
        severity=Severity.WARNING,
        passed=len(issues) == 0,
        total_checked=total,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total if total > 0 else 0,
        details=issues,
        description="Reaches with stream_order < 1 (excluding -9999 nodata)",
    )


@register_check(
    "N016",
    Category.NETWORK,
    Severity.WARNING,
    "Reaches with same path_segs must form a connected component",
)
def check_path_segs_contiguity(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """
    Validate that reaches sharing a path_segs value are topologically connected.

    For a linear chain of N reaches sharing path_segs, there should be
    2*(N-1) directed topology edges (up + down) where both endpoints share
    that path_segs. Fewer edges means the segment is disconnected.
    """
    where_clause = f"AND r.region = '{region}'" if region else ""
    where_clause_r1 = f"AND r1.region = '{region}'" if region else ""

    # Count reaches per path_segs group (only groups with >1 reach)
    # Count internal directed edges per path_segs group
    # A connected chain of N reaches has 2*(N-1) directed edges
    query = f"""
    WITH seg_reach_counts AS (
        SELECT r.path_segs, COUNT(*) AS n_reaches
        FROM reaches r
        WHERE r.path_segs > 0
            AND r.path_segs != -9999
            {where_clause}
        GROUP BY r.path_segs
        HAVING COUNT(*) > 1
    ),
    internal_edges AS (
        SELECT r1.path_segs, COUNT(*) AS n_directed_edges
        FROM reaches r1
        JOIN reach_topology rt
            ON r1.reach_id = rt.reach_id AND r1.region = rt.region
        JOIN reaches r2
            ON rt.neighbor_reach_id = r2.reach_id AND rt.region = r2.region
        WHERE r1.path_segs = r2.path_segs
            AND r1.path_segs > 0
            AND r1.path_segs != -9999
            {where_clause_r1}
        GROUP BY r1.path_segs
    )
    SELECT
        sc.path_segs,
        sc.n_reaches,
        COALESCE(ie.n_directed_edges, 0) AS n_internal_edges,
        2 * (sc.n_reaches - 1) AS expected_edges
    FROM seg_reach_counts sc
    LEFT JOIN internal_edges ie ON sc.path_segs = ie.path_segs
    WHERE COALESCE(ie.n_directed_edges, 0) < 2 * (sc.n_reaches - 1)
    ORDER BY sc.n_reaches DESC
    """

    issues = conn.execute(query).fetchdf()

    total_query = f"""
    SELECT COUNT(DISTINCT path_segs) FROM reaches r
    WHERE path_segs > 0 AND path_segs != -9999
    {where_clause.replace("r.", "")}
    """
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="N016",
        name="path_segs_contiguity",
        severity=Severity.WARNING,
        passed=len(issues) == 0,
        total_checked=total,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total if total > 0 else 0,
        details=issues,
        description="path_segs groups where reaches are not fully connected via topology",
    )


@register_check(
    "N017",
    Category.NETWORK,
    Severity.INFO,
    "Junction reaches should have different path_segs than at least one neighbor",
)
def check_path_segs_junction_boundary(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """
    Check that junction reaches act as path_segs boundaries.

    Junction reaches (end_reach=3) should have a different path_segs from
    at least one of their topology neighbors, since junctions define segment
    boundaries. Informational — some junctions legitimately share path_segs
    with one branch.
    """
    where_clause = f"AND r.region = '{region}'" if region else ""

    query = f"""
    WITH junction_neighbors AS (
        SELECT
            r.reach_id,
            r.region,
            r.path_segs AS junction_path_segs,
            COUNT(DISTINCT r2.path_segs) AS n_distinct_neighbor_segs,
            COUNT(*) AS n_neighbors,
            MIN(CASE WHEN r2.path_segs != r.path_segs THEN 1 ELSE 0 END) AS has_different_seg
        FROM reaches r
        JOIN reach_topology rt
            ON r.reach_id = rt.reach_id AND r.region = rt.region
        JOIN reaches r2
            ON rt.neighbor_reach_id = r2.reach_id AND rt.region = r2.region
        WHERE r.end_reach = 3
            AND r.path_segs > 0
            AND r.path_segs != -9999
            AND r2.path_segs > 0
            AND r2.path_segs != -9999
            {where_clause}
        GROUP BY r.reach_id, r.region, r.path_segs
    )
    SELECT
        jn.reach_id, jn.region,
        r.river_name, r.x, r.y,
        jn.junction_path_segs,
        jn.n_distinct_neighbor_segs,
        jn.n_neighbors
    FROM junction_neighbors jn
    JOIN reaches r ON jn.reach_id = r.reach_id AND jn.region = r.region
    WHERE jn.n_distinct_neighbor_segs = 1
        AND jn.has_different_seg = 0
    ORDER BY jn.reach_id
    """

    issues = conn.execute(query).fetchdf()

    total_query = f"""
    SELECT COUNT(*) FROM reaches r
    WHERE end_reach = 3
        AND path_segs > 0
        AND path_segs != -9999
        {where_clause.replace("r.", "")}
    """
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="N017",
        name="path_segs_junction_boundary",
        severity=Severity.INFO,
        passed=len(issues) == 0,
        total_checked=total,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total if total > 0 else 0,
        details=issues,
        description="Junction reaches where all neighbors share the same path_segs",
    )
