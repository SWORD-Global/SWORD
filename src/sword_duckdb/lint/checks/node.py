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
            POWER(LEAST(ABS(x - prev_x), 360.0 - ABS(x - prev_x)) * COS(RADIANS((y + prev_y) / 2.0)), 2)
            + POWER(y - prev_y, 2)
        ) as spacing_m
    FROM ordered_nodes
    WHERE prev_node_id IS NOT NULL
        AND 111000.0 * SQRT(
            POWER(LEAST(ABS(x - prev_x), 360.0 - ABS(x - prev_x)) * COS(RADIANS((y + prev_y) / 2.0)), 2)
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

    Checks all 4 boundary node pairs (MIN/MAX node_id of each reach) and
    reports the minimum dist_out gap.  Only flags if no pair is within threshold.

    Note on violations:
    - Type A (>100km): Likely false cross-basin topological merge. High priority fix.
    - Type B (1-50km): Likely path length difference in a braided system. At a
      bifurcation, dist_out matches the longest path, creating a gap for shorter
      parallel channels.
    """
    max_diff = threshold if threshold is not None else 1000.0
    where_clause = f"AND rt.region = '{region}'" if region else ""

    query = f"""
    WITH reach_boundaries AS (
        SELECT reach_id, region,
            MIN(node_id) as min_node_id,
            MAX(node_id) as max_node_id
        FROM nodes
        GROUP BY reach_id, region
    ),
    boundary_distout AS (
        SELECT rb.reach_id, rb.region,
            n_min.dist_out as min_do, n_min.node_id as min_node,
            n_max.dist_out as max_do, n_max.node_id as max_node
        FROM reach_boundaries rb
        JOIN nodes n_min ON rb.min_node_id = n_min.node_id AND rb.region = n_min.region
        JOIN nodes n_max ON rb.max_node_id = n_max.node_id AND rb.region = n_max.region
        WHERE n_min.dist_out IS NOT NULL AND n_min.dist_out != -9999
          AND n_max.dist_out IS NOT NULL AND n_max.dist_out != -9999
    ),
    neighbor_counts AS (
        SELECT reach_id, region, COUNT(*) as n_dn_neighbors
        FROM reach_topology
        WHERE direction = 'down'
        GROUP BY reach_id, region
    ),
    all_pairs AS (
        SELECT
            rt.reach_id as up_reach,
            rt.neighbor_reach_id as dn_reach,
            rt.region,
            a.min_node as up_min_node, a.max_node as up_max_node,
            b.min_node as dn_min_node, b.max_node as dn_max_node,
            a.min_do as up_min_do, a.max_do as up_max_do,
            b.min_do as dn_min_do, b.max_do as dn_max_do,
            LEAST(
                ABS(a.min_do - b.max_do),
                ABS(a.min_do - b.min_do),
                ABS(a.max_do - b.max_do),
                ABS(a.max_do - b.min_do)
            ) as boundary_gap,
            COALESCE(nc.n_dn_neighbors, 1) as n_dn_neighbors
        FROM reach_topology rt
        JOIN boundary_distout a ON rt.reach_id = a.reach_id AND rt.region = a.region
        JOIN boundary_distout b ON rt.neighbor_reach_id = b.reach_id AND rt.region = b.region
        LEFT JOIN neighbor_counts nc ON rt.reach_id = nc.reach_id AND rt.region = nc.region
        WHERE rt.direction = 'down'
            {where_clause}
    )
    SELECT
        up_reach, dn_reach, region,
        up_min_node, up_max_node, dn_min_node, dn_max_node,
        up_min_do, up_max_do, dn_min_do, dn_max_do,
        boundary_gap,
        n_dn_neighbors
    FROM all_pairs
    WHERE boundary_gap > {max_diff}
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


def _ensure_spatial(conn: duckdb.DuckDBPyConnection) -> bool:
    """Try to load the DuckDB spatial extension. Return True if available."""
    try:
        conn.execute("LOAD spatial")
        return True
    except Exception:
        try:
            conn.execute("INSTALL spatial; LOAD spatial")
            return True
        except Exception:
            return False


@register_check(
    "N007",
    Category.NETWORK,
    Severity.WARNING,
    "Reach geometry endpoints >400m apart between connected reaches",
    default_threshold=400.0,
)
def check_boundary_node_geolocation(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """Check geographic co-location of reach geometry endpoints at junctions.

    For each downstream topology link A→B, computes the minimum distance
    across all 4 geometry endpoint combos (start/end of A vs start/end of B)
    using ST_Distance_Spheroid for geodesic accuracy.

    Previous versions used MIN/MAX(node_id) as boundary proxies, which
    produced ~92% false positives on sinuous reaches where the geographic
    endpoint falls mid-way through the node_id/dist_out ordering.
    """
    max_dist = threshold if threshold is not None else 400.0
    where_clause = f"AND rt.region = '{region}'" if region else ""

    if not _ensure_spatial(conn):
        return CheckResult(
            check_id="N007",
            name="boundary_node_geolocation",
            severity=Severity.WARNING,
            passed=True,
            total_checked=0,
            issues_found=0,
            issue_pct=0.0,
            details=None,
            description="SKIPPED – spatial extension unavailable",
            threshold=max_dist,
        )

    query = f"""
    WITH pairs AS (
        SELECT
            rt.reach_id AS up_reach,
            rt.neighbor_reach_id AS dn_reach,
            rt.region,
            ST_StartPoint(a.geom) AS a_start,
            ST_EndPoint(a.geom)   AS a_end,
            ST_StartPoint(b.geom) AS b_start,
            ST_EndPoint(b.geom)   AS b_end
        FROM reach_topology rt
        JOIN reaches a ON a.reach_id = rt.reach_id AND a.region = rt.region
        JOIN reaches b ON b.reach_id = rt.neighbor_reach_id AND b.region = rt.region
        WHERE rt.direction = 'down'
            AND a.geom IS NOT NULL AND b.geom IS NOT NULL
            {where_clause}
    ),
    dists AS (
        SELECT
            up_reach, dn_reach, region,
            LEAST(
                ST_Distance_Spheroid(ST_FlipCoordinates(a_start), ST_FlipCoordinates(b_start)),
                ST_Distance_Spheroid(ST_FlipCoordinates(a_start), ST_FlipCoordinates(b_end)),
                ST_Distance_Spheroid(ST_FlipCoordinates(a_end),   ST_FlipCoordinates(b_start)),
                ST_Distance_Spheroid(ST_FlipCoordinates(a_end),   ST_FlipCoordinates(b_end))
            ) AS boundary_dist_m
        FROM pairs
    )
    SELECT
        up_reach, dn_reach, region,
        ROUND(boundary_dist_m, 1) AS boundary_dist_m
    FROM dists
    WHERE boundary_dist_m > {max_dist}
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
        description=f"Reach geometry endpoints >{max_dist:.0f}m apart",
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
            POWER(LEAST(ABS(c.x - n.x), 360.0 - ABS(c.x - n.x)) * COS(RADIANS((c.y + n.y) / 2.0)), 2)
            + POWER(c.y - n.y, 2)
        ), 1) as dist_m
    FROM centerlines c
    JOIN nodes n ON c.node_id = n.node_id AND c.region = n.region
    WHERE 111000.0 * SQRT(
            POWER(LEAST(ABS(c.x - n.x), 360.0 - ABS(c.x - n.x)) * COS(RADIANS((c.y + n.y) / 2.0)), 2)
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
