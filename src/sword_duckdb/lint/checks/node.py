"""
SWORD Lint - Node Checks (N0xx)

Validates node-level data: spacing, dist_out continuity, boundary alignment,
count consistency, and index contiguity.
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
    "N003",
    Category.NETWORK,
    Severity.WARNING,
    "Adjacent nodes within a reach spaced >400m apart",
    default_threshold=400.0,
)
def check_node_spacing_gap(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """Flag adjacent nodes within a reach that are >400m apart using equirectangular distance."""
    max_spacing = threshold if threshold is not None else 400.0
    where_clause = f"AND n.region = '{region}'" if region else ""

    query = f"""
    WITH ordered_nodes AS (
        SELECT
            node_id, reach_id, region, x, y,
            LAG(node_id) OVER (PARTITION BY reach_id, region ORDER BY node_id) as prev_node_id,
            LAG(x) OVER (PARTITION BY reach_id, region ORDER BY node_id) as prev_x,
            LAG(y) OVER (PARTITION BY reach_id, region ORDER BY node_id) as prev_y
        FROM nodes n
        WHERE 1=1 {where_clause}
    )
    SELECT
        node_id, prev_node_id, reach_id, region, x, y,
        111000.0 * SQRT(
            POWER((x - prev_x) * COS(RADIANS((y + prev_y) / 2.0)), 2)
            + POWER(y - prev_y, 2)
        ) as spacing_m
    FROM ordered_nodes
    WHERE prev_node_id IS NOT NULL
        AND 111000.0 * SQRT(
            POWER((x - prev_x) * COS(RADIANS((y + prev_y) / 2.0)), 2)
            + POWER(y - prev_y, 2)
        ) > {max_spacing}
    ORDER BY spacing_m DESC
    LIMIT 10000
    """

    issues = conn.execute(query).fetchdf()

    total_query = f"""
    SELECT COUNT(*) FROM nodes n WHERE 1=1 {where_clause}
    """
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="N003",
        name="node_spacing_gap",
        severity=Severity.WARNING,
        passed=len(issues) == 0,
        total_checked=total,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total if total > 0 else 0,
        details=issues,
        description=f"Adjacent nodes spaced >{max_spacing:.0f}m apart",
        threshold=max_spacing,
    )


@register_check(
    "N004",
    Category.NETWORK,
    Severity.WARNING,
    "Node dist_out must increase along node_id order within a reach",
)
def check_node_dist_out_monotonicity(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """Check that node dist_out increases as node_id increases within each reach.

    SWORD convention: node_id increases upstream (higher node_id = higher dist_out).
    A violation means dist_out decreases where it should increase.
    """
    where_clause = f"AND n.region = '{region}'" if region else ""

    query = f"""
    WITH ordered_nodes AS (
        SELECT
            node_id, reach_id, region, dist_out,
            LAG(dist_out) OVER (PARTITION BY reach_id, region ORDER BY node_id) as prev_dist_out,
            LAG(node_id) OVER (PARTITION BY reach_id, region ORDER BY node_id) as prev_node_id
        FROM nodes n
        WHERE dist_out IS NOT NULL AND dist_out != -9999
            {where_clause}
    )
    SELECT
        node_id, prev_node_id, reach_id, region,
        prev_dist_out, dist_out,
        (prev_dist_out - dist_out) as dist_out_decrease
    FROM ordered_nodes
    WHERE prev_dist_out IS NOT NULL
        AND dist_out < prev_dist_out
    ORDER BY dist_out_decrease DESC
    LIMIT 10000
    """

    issues = conn.execute(query).fetchdf()

    total_query = f"""
    SELECT COUNT(*) FROM nodes n
    WHERE dist_out IS NOT NULL AND dist_out != -9999
    {where_clause}
    """
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="N004",
        name="node_dist_out_monotonicity",
        severity=Severity.WARNING,
        passed=len(issues) == 0,
        total_checked=total,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total if total > 0 else 0,
        details=issues,
        description="Nodes where dist_out decreases along node_id order (should increase)",
    )


@register_check(
    "N005",
    Category.NETWORK,
    Severity.WARNING,
    "Adjacent node dist_out jump >600m within a reach",
    default_threshold=600.0,
)
def check_node_dist_out_jump(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """Flag large dist_out jumps between adjacent nodes."""
    max_jump = threshold if threshold is not None else 600.0
    where_clause = f"AND n.region = '{region}'" if region else ""

    query = f"""
    WITH ordered_nodes AS (
        SELECT
            node_id, reach_id, region, dist_out, x, y,
            LAG(dist_out) OVER (PARTITION BY reach_id, region ORDER BY node_id) as prev_dist_out,
            LAG(node_id) OVER (PARTITION BY reach_id, region ORDER BY node_id) as prev_node_id
        FROM nodes n
        WHERE dist_out IS NOT NULL AND dist_out != -9999
            {where_clause}
    )
    SELECT
        node_id, prev_node_id, reach_id, region, x, y,
        prev_dist_out, dist_out,
        ABS(dist_out - prev_dist_out) as dist_out_jump
    FROM ordered_nodes
    WHERE prev_dist_out IS NOT NULL
        AND ABS(dist_out - prev_dist_out) > {max_jump}
    ORDER BY dist_out_jump DESC
    LIMIT 10000
    """

    issues = conn.execute(query).fetchdf()

    total_query = f"""
    SELECT COUNT(*) FROM nodes n
    WHERE dist_out IS NOT NULL AND dist_out != -9999
    {where_clause}
    """
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="N005",
        name="node_dist_out_jump",
        severity=Severity.WARNING,
        passed=len(issues) == 0,
        total_checked=total,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total if total > 0 else 0,
        details=issues,
        description=f"Adjacent nodes with dist_out jump >{max_jump:.0f}m",
        threshold=max_jump,
    )


@register_check(
    "N006",
    Category.NETWORK,
    Severity.WARNING,
    "Boundary node dist_out mismatch >1000m between connected reaches",
    default_threshold=1000.0,
)
def check_boundary_dist_out(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """Check dist_out continuity at reach boundaries.

    SWORD convention: node_id increases upstream (higher node_id = higher dist_out).
    For upstream reach A -> downstream reach B (direction='down'):
      A's downstream boundary = MIN(node_id) in A  (lowest dist_out in A)
      B's upstream boundary   = MAX(node_id) in B  (highest dist_out in B)
    These two should be close in dist_out.
    """
    max_diff = threshold if threshold is not None else 1000.0
    where_clause = f"AND rt.region = '{region}'" if region else ""

    query = f"""
    -- SWORD convention: node_id increases upstream (higher node_id = higher dist_out).
    -- For upstream reach A -> downstream reach B (direction='down'):
    --   A's downstream boundary = MIN(node_id) in A
    --   B's upstream boundary   = MAX(node_id) in B
    WITH up_boundary AS (
        SELECT reach_id, region,
            MIN(node_id) as boundary_node_id
        FROM nodes
        GROUP BY reach_id, region
    ),
    dn_boundary AS (
        SELECT reach_id, region,
            MAX(node_id) as boundary_node_id
        FROM nodes
        GROUP BY reach_id, region
    ),
    boundary_pairs AS (
        SELECT
            rt.reach_id as up_reach,
            rt.neighbor_reach_id as dn_reach,
            rt.region,
            n1.dist_out as up_boundary_dist_out,
            n2.dist_out as dn_boundary_dist_out,
            n1.node_id as up_boundary_node,
            n2.node_id as dn_boundary_node
        FROM reach_topology rt
        JOIN up_boundary ub ON rt.reach_id = ub.reach_id AND rt.region = ub.region
        JOIN dn_boundary db ON rt.neighbor_reach_id = db.reach_id AND rt.region = db.region
        JOIN nodes n1 ON ub.boundary_node_id = n1.node_id AND ub.region = n1.region
        JOIN nodes n2 ON db.boundary_node_id = n2.node_id AND db.region = n2.region
        WHERE rt.direction = 'down'
            AND n1.dist_out IS NOT NULL AND n1.dist_out != -9999
            AND n2.dist_out IS NOT NULL AND n2.dist_out != -9999
            {where_clause}
    )
    SELECT
        up_reach, dn_reach, region,
        up_boundary_node, dn_boundary_node,
        up_boundary_dist_out, dn_boundary_dist_out,
        ABS(up_boundary_dist_out - dn_boundary_dist_out) as boundary_gap
    FROM boundary_pairs
    WHERE ABS(up_boundary_dist_out - dn_boundary_dist_out) > {max_diff}
    ORDER BY boundary_gap DESC
    LIMIT 10000
    """

    issues = conn.execute(query).fetchdf()

    total_query = f"""
    SELECT COUNT(*) FROM reach_topology rt
    WHERE direction = 'down' {where_clause}
    """
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="N006",
        name="boundary_dist_out",
        severity=Severity.WARNING,
        passed=len(issues) == 0,
        total_checked=total,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total if total > 0 else 0,
        details=issues,
        description=f"Reach boundary node dist_out gap >{max_diff:.0f}m",
        threshold=max_diff,
    )


@register_check(
    "N007",
    Category.NETWORK,
    Severity.WARNING,
    "Boundary node geolocation >400m between connected reaches",
    default_threshold=400.0,
)
def check_boundary_node_geolocation(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """Check geographic co-location of boundary nodes at reach junctions.

    SWORD convention: node_id increases upstream (higher node_id = higher dist_out).
    For upstream reach A -> downstream reach B (direction='down'):
      A's downstream boundary = MIN(node_id) in A
      B's upstream boundary   = MAX(node_id) in B
    These two boundary nodes should be geographically close.
    """
    max_dist = threshold if threshold is not None else 400.0
    where_clause = f"AND rt.region = '{region}'" if region else ""

    query = f"""
    WITH up_boundary AS (
        SELECT reach_id, region,
            MIN(node_id) as boundary_node_id
        FROM nodes
        GROUP BY reach_id, region
    ),
    dn_boundary AS (
        SELECT reach_id, region,
            MAX(node_id) as boundary_node_id
        FROM nodes
        GROUP BY reach_id, region
    ),
    boundary_pairs AS (
        SELECT
            rt.reach_id as up_reach,
            rt.neighbor_reach_id as dn_reach,
            rt.region,
            n1.x as up_x, n1.y as up_y,
            n2.x as dn_x, n2.y as dn_y,
            n1.node_id as up_boundary_node,
            n2.node_id as dn_boundary_node
        FROM reach_topology rt
        JOIN up_boundary ub ON rt.reach_id = ub.reach_id AND rt.region = ub.region
        JOIN dn_boundary db ON rt.neighbor_reach_id = db.reach_id AND rt.region = db.region
        JOIN nodes n1 ON ub.boundary_node_id = n1.node_id AND ub.region = n1.region
        JOIN nodes n2 ON db.boundary_node_id = n2.node_id AND db.region = n2.region
        WHERE rt.direction = 'down'
            AND n1.x IS NOT NULL AND n2.x IS NOT NULL
            {where_clause}
    )
    SELECT
        up_reach, dn_reach, region,
        up_boundary_node, dn_boundary_node,
        up_x, up_y, dn_x, dn_y,
        111000.0 * SQRT(
            POWER((up_x - dn_x) * COS(RADIANS((up_y + dn_y) / 2.0)), 2)
            + POWER(up_y - dn_y, 2)
        ) as boundary_dist_m
    FROM boundary_pairs
    WHERE 111000.0 * SQRT(
            POWER((up_x - dn_x) * COS(RADIANS((up_y + dn_y) / 2.0)), 2)
            + POWER(up_y - dn_y, 2)
        ) > {max_dist}
    ORDER BY boundary_dist_m DESC
    LIMIT 10000
    """

    issues = conn.execute(query).fetchdf()

    total_query = f"""
    SELECT COUNT(*) FROM reach_topology rt
    WHERE direction = 'down' {where_clause}
    """
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="N007",
        name="boundary_node_geolocation",
        severity=Severity.WARNING,
        passed=len(issues) == 0,
        total_checked=total,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total if total > 0 else 0,
        details=issues,
        description=f"Reach boundary nodes >{max_dist:.0f}m apart geographically",
        threshold=max_dist,
    )


@register_check(
    "N008",
    Category.NETWORK,
    Severity.ERROR,
    "Actual node count must match reaches.n_nodes",
)
def check_node_count_vs_n_nodes(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """Check that actual node count per reach matches the n_nodes column."""
    where_clause = f"AND r.region = '{region}'" if region else ""

    query = f"""
    WITH actual_counts AS (
        SELECT reach_id, region, COUNT(*) as actual_count
        FROM nodes
        GROUP BY reach_id, region
    )
    SELECT
        r.reach_id, r.region, r.river_name, r.x, r.y,
        r.n_nodes as expected_count,
        COALESCE(ac.actual_count, 0) as actual_count,
        ABS(r.n_nodes - COALESCE(ac.actual_count, 0)) as count_diff
    FROM reaches r
    LEFT JOIN actual_counts ac ON r.reach_id = ac.reach_id AND r.region = ac.region
    WHERE r.n_nodes IS NOT NULL AND r.n_nodes != -9999
        AND r.n_nodes != COALESCE(ac.actual_count, 0)
        {where_clause}
    ORDER BY count_diff DESC
    LIMIT 10000
    """

    issues = conn.execute(query).fetchdf()

    total_query = f"""
    SELECT COUNT(*) FROM reaches r WHERE 1=1 {where_clause}
    """
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="N008",
        name="node_count_vs_n_nodes",
        severity=Severity.ERROR,
        passed=len(issues) == 0,
        total_checked=total,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total if total > 0 else 0,
        details=issues,
        description="Reaches where actual node count doesn't match n_nodes column",
    )


@register_check(
    "N010",
    Category.NETWORK,
    Severity.INFO,
    "Node indexes within a reach are not contiguous",
)
def check_node_index_contiguity(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """Check that node indexes (last 3 digits of node_id) are contiguous within each reach.

    SWORD uses step-10 node suffixes: 001, 011, 021, ..., 991.
    Expected count = (max_suffix - min_suffix) / 10 + 1.
    """
    where_clause = f"AND n.region = '{region}'" if region else ""

    query = f"""
    WITH node_suffixes AS (
        SELECT
            reach_id, region,
            CAST(node_id AS BIGINT) % 1000 as suffix,
            node_id
        FROM nodes n
        WHERE 1=1 {where_clause}
    ),
    reach_stats AS (
        SELECT
            reach_id, region,
            MIN(suffix) as min_suffix,
            MAX(suffix) as max_suffix,
            COUNT(*) as node_count
        FROM node_suffixes
        GROUP BY reach_id, region
    )
    SELECT
        rs.reach_id, rs.region,
        rs.min_suffix, rs.max_suffix, rs.node_count,
        CAST((rs.max_suffix - rs.min_suffix) / 10 AS INTEGER) + 1 as expected_count,
        (CAST((rs.max_suffix - rs.min_suffix) / 10 AS INTEGER) + 1) - rs.node_count as gap_count
    FROM reach_stats rs
    WHERE (CAST((rs.max_suffix - rs.min_suffix) / 10 AS INTEGER) + 1) != rs.node_count
    ORDER BY gap_count DESC
    LIMIT 10000
    """

    issues = conn.execute(query).fetchdf()

    total_query = f"""
    SELECT COUNT(DISTINCT reach_id) FROM nodes n WHERE 1=1 {where_clause}
    """
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="N010",
        name="node_index_contiguity",
        severity=Severity.INFO,
        passed=len(issues) == 0,
        total_checked=total,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total if total > 0 else 0,
        details=issues,
        description="Reaches with non-contiguous node index suffixes (gaps in step-10 numbering)",
    )


@register_check(
    "N011",
    Category.NETWORK,
    Severity.WARNING,
    "Nodes with ordering problems (zero length or length > 1000m)",
    default_threshold=1000.0,
)
def check_node_ordering_problems(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """Find nodes with zero/negative length or excessively long length.

    Both indicate ordering or derivation problems that should be fixed
    by re-deriving node positions from centerlines.
    """
    max_length = threshold or 1000.0
    where_clause = f"AND n.region = '{region}'" if region else ""

    query = f"""
    SELECT
        n.node_id,
        n.reach_id,
        n.region,
        n.node_length,
        CASE
            WHEN n.node_length <= 0 THEN 'zero_length'
            ELSE 'excessive_length'
        END AS issue_type
    FROM nodes n
    WHERE (n.node_length <= 0 OR n.node_length > {max_length})
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
        check_id="N011",
        name="node_ordering_problems",
        severity=Severity.WARNING,
        passed=len(issues) == 0,
        total_checked=total,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total if total > 0 else 0,
        details=issues,
        description="Nodes with zero/negative length or length exceeding threshold",
        threshold=max_length,
    )


@register_check(
    "N012",
    Category.NETWORK,
    Severity.WARNING,
    "Node (x,y) too far from parent reach geometry (POM Test 9a)",
    default_threshold=500.0,
)
def check_node_geolocation_vs_reach(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """Flag nodes whose (x, y) is far from their parent reach linestring.

    Uses ST_Distance (degree-based) scaled by 111 km/degree.  Requires the
    DuckDB spatial extension.
    """
    max_dist = threshold if threshold is not None else 500.0
    where_clause = f"AND n.region = '{region}'" if region else ""

    conn.execute("INSTALL spatial; LOAD spatial;")

    query = f"""
    SELECT
        n.node_id, n.reach_id, n.region, n.x, n.y,
        ROUND(ST_Distance(ST_Point(n.x, n.y), r.geom) * 111000.0, 1) as dist_m
    FROM nodes n
    JOIN reaches r ON n.reach_id = r.reach_id AND n.region = r.region
    WHERE ST_Distance(ST_Point(n.x, n.y), r.geom) * 111000.0 > {max_dist}
        {where_clause}
    ORDER BY dist_m DESC
    LIMIT 10000
    """

    issues = conn.execute(query).fetchdf()

    total_query = f"""
    SELECT COUNT(*) FROM nodes n WHERE 1=1 {where_clause}
    """
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="N012",
        name="node_geolocation_vs_reach",
        severity=Severity.WARNING,
        passed=len(issues) == 0,
        total_checked=total,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total if total > 0 else 0,
        details=issues,
        description=f"Nodes >{max_dist:.0f}m from parent reach geometry",
        threshold=max_dist,
    )


@register_check(
    "N013",
    Category.NETWORK,
    Severity.WARNING,
    "Centerline point too far from assigned node (POM Test 9d)",
    default_threshold=500.0,
)
def check_centerline_node_distance(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """Flag centerline points whose (x, y) is far from their assigned node.

    Uses equirectangular approximation — no spatial extension needed.
    """
    max_dist = threshold if threshold is not None else 500.0
    where_clause = f"AND c.region = '{region}'" if region else ""

    query = f"""
    SELECT
        c.cl_id, c.node_id, c.reach_id, c.region,
        c.x as cl_x, c.y as cl_y,
        n.x as node_x, n.y as node_y,
        ROUND(111000.0 * SQRT(
            POWER((c.x - n.x) * COS(RADIANS((c.y + n.y) / 2.0)), 2)
            + POWER(c.y - n.y, 2)
        ), 1) as dist_m
    FROM centerlines c
    JOIN nodes n ON c.node_id = n.node_id AND c.region = n.region
    WHERE 111000.0 * SQRT(
            POWER((c.x - n.x) * COS(RADIANS((c.y + n.y) / 2.0)), 2)
            + POWER(c.y - n.y, 2)
        ) > {max_dist}
        {where_clause}
    ORDER BY dist_m DESC
    LIMIT 10000
    """

    issues = conn.execute(query).fetchdf()

    total_query = f"""
    SELECT COUNT(*) FROM centerlines c WHERE 1=1 {where_clause}
    """
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="N013",
        name="centerline_node_distance",
        severity=Severity.WARNING,
        passed=len(issues) == 0,
        total_checked=total,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total if total > 0 else 0,
        details=issues,
        description=f"Centerline points >{max_dist:.0f}m from assigned node",
        threshold=max_dist,
    )
