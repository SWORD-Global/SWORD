"""
SWORD Lint - Topology Checks (T0xx)

Validates topology consistency and flow direction properties.
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
    "T001",
    Category.TOPOLOGY,
    Severity.ERROR,
    "dist_out must decrease downstream (min of all downstream neighbors)",
    default_threshold=100.0,  # tolerance in meters
)
def check_dist_out_monotonicity(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """
    Check that dist_out (distance to outlet) decreases downstream.

    For reaches with multiple downstream neighbors (bifurcations), at least
    one path should lead to a nearer outlet. We check the MINIMUM downstream
    dist_out, which handles multi-outlet networks correctly.

    Violations indicate topology errors or incorrect flow direction.
    """
    tolerance = threshold if threshold is not None else 100.0
    where_clause = f"AND r1.region = '{region}'" if region else ""

    # Check MINIMUM downstream dist_out - at bifurcations, one path may go
    # to a more distant outlet, which is expected network behavior
    query = f"""
    WITH min_downstream AS (
        SELECT
            r1.reach_id,
            r1.region,
            r1.dist_out as dist_out_up,
            MIN(r2.dist_out) as min_dist_out_down,
            r1.river_name,
            r1.x, r1.y,
            r1.n_rch_down
        FROM reaches r1
        JOIN reach_topology rt ON r1.reach_id = rt.reach_id AND r1.region = rt.region
        JOIN reaches r2 ON rt.neighbor_reach_id = r2.reach_id AND rt.region = r2.region
        WHERE rt.direction = 'down'
            AND r1.dist_out > 0 AND r1.dist_out != -9999
            AND r2.dist_out > 0 AND r2.dist_out != -9999
            {where_clause}
        GROUP BY r1.reach_id, r1.region, r1.dist_out, r1.river_name, r1.x, r1.y, r1.n_rch_down
    )
    SELECT
        reach_id, region, river_name, x, y,
        dist_out_up, min_dist_out_down,
        (min_dist_out_down - dist_out_up) as dist_out_increase,
        n_rch_down
    FROM min_downstream
    WHERE min_dist_out_down > dist_out_up + {tolerance}
    ORDER BY dist_out_increase DESC
    """

    issues = conn.execute(query).fetchdf()

    total_query = f"""
    SELECT COUNT(*) FROM reaches r1
    WHERE dist_out > 0 AND dist_out != -9999
    {where_clause.replace("r1.", "")}
    """
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="T001",
        name="dist_out_monotonicity",
        severity=Severity.ERROR,
        passed=len(issues) == 0,
        total_checked=total,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total if total > 0 else 0,
        details=issues,
        description="Reaches where min(downstream dist_out) increases (topology error)",
        threshold=tolerance,
    )


@register_check(
    "T002",
    Category.TOPOLOGY,
    Severity.WARNING,
    "path_freq should increase toward outlets",
)
def check_path_freq_monotonicity(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """
    Check that path_freq increases toward outlets.

    At confluences, downstream path_freq should be >= max(upstream path_freqs).

    Side-channel and secondary-outlet reaches (main_side=1 or 2) that rejoin
    the main channel at a confluence are expected to carry high path_freq from
    the bifurcation point where they branched off. When they merge back, the
    main channel's path_freq at the rejoin point is often lower — this is
    structurally correct, not a data error. These edges are tagged as
    'side_rejoin' in the details and excluded from the issue count.
    """
    where_clause = f"AND r1.region = '{region}'" if region else ""

    query = f"""
    WITH downstream_freqs AS (
        SELECT
            r1.reach_id,
            r1.region,
            r1.path_freq as pf_up,
            r2.path_freq as pf_down,
            r1.river_name,
            r1.x, r1.y,
            r1.main_side as up_main_side,
            r2.main_side as dn_main_side
        FROM reaches r1
        JOIN reach_topology rt ON r1.reach_id = rt.reach_id AND r1.region = rt.region
        JOIN reaches r2 ON rt.neighbor_reach_id = r2.reach_id AND rt.region = r2.region
        WHERE rt.direction = 'down'
            AND r1.path_freq > 0 AND r1.path_freq != -9999
            AND r2.path_freq > 0 AND r2.path_freq != -9999
            {where_clause}
    )
    SELECT
        reach_id, region, river_name, x, y,
        pf_up, pf_down,
        (pf_up - pf_down) as pf_decrease,
        up_main_side, dn_main_side,
        -- Side/secondary-outlet reaches rejoining main channel: expected drop
        CASE WHEN up_main_side IN (1, 2) THEN 'side_rejoin'
             ELSE 'unexpected'
        END as violation_type
    FROM downstream_freqs
    WHERE pf_down < pf_up
    ORDER BY
        CASE WHEN up_main_side IN (1, 2) THEN 1 ELSE 0 END,
        pf_decrease DESC
    """

    all_violations = conn.execute(query).fetchdf()

    # Split: true issues vs expected side-channel rejoin drops
    if len(all_violations) > 0:
        unexpected = all_violations[all_violations["violation_type"] == "unexpected"]
        side_rejoin = all_violations[all_violations["violation_type"] == "side_rejoin"]
    else:
        unexpected = all_violations
        side_rejoin = all_violations

    total_query = f"""
    SELECT COUNT(*) FROM reaches r1
    WHERE path_freq > 0 AND path_freq != -9999
    {where_clause.replace("r1.", "")}
    """
    total = conn.execute(total_query).fetchone()[0]

    n_unexpected = len(unexpected)
    n_side_rejoin = len(side_rejoin)

    desc = f"path_freq decreases downstream: {n_unexpected} unexpected"
    if n_side_rejoin > 0:
        desc += (
            f", {n_side_rejoin} expected (side/secondary-outlet rejoin — "
            f"main_side IN (1,2) carrying high path_freq from bifurcation)"
        )

    return CheckResult(
        check_id="T002",
        name="path_freq_monotonicity",
        severity=Severity.WARNING,
        passed=n_unexpected == 0,
        total_checked=total,
        issues_found=n_unexpected,
        issue_pct=100 * n_unexpected / total if total > 0 else 0,
        details=all_violations,
        description=desc,
    )


@register_check(
    "T003",
    Category.TOPOLOGY,
    Severity.WARNING,
    "facc should increase downstream",
)
def check_facc_monotonicity(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """
    Check that flow accumulation (facc) increases downstream.

    Downstream reaches should have >= facc values.
    """
    where_clause = f"AND r1.region = '{region}'" if region else ""

    query = f"""
    WITH reach_pairs AS (
        SELECT
            r1.reach_id,
            r1.region,
            r1.facc as facc_up,
            r2.facc as facc_down,
            r1.river_name,
            r1.x, r1.y,
            r1.lakeflag,
            r1.n_rch_down
        FROM reaches r1
        JOIN reach_topology rt ON r1.reach_id = rt.reach_id AND r1.region = rt.region
        JOIN reaches r2 ON rt.neighbor_reach_id = r2.reach_id AND rt.region = r2.region
        WHERE rt.direction = 'down'
            AND r1.facc > 0 AND r1.facc != -9999
            AND r2.facc > 0 AND r2.facc != -9999
            {where_clause}
    )
    SELECT
        reach_id, region, river_name, x, y,
        facc_up, facc_down,
        (facc_up - facc_down) as facc_decrease
    FROM reach_pairs
    WHERE facc_down < facc_up * 0.95  -- 5% tolerance
        AND n_rch_down < 2  -- exclude bifurcation edges (expected facc drop)
    ORDER BY facc_decrease DESC
    """

    issues = conn.execute(query).fetchdf()

    total_query = f"""
    SELECT COUNT(*) FROM reaches r1
    WHERE facc > 0 AND facc != -9999
    {where_clause.replace("r1.", "")}
    """
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="T003",
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
    "T004",
    Category.TOPOLOGY,
    Severity.WARNING,
    "Orphan reaches with no neighbors",
)
def check_orphan_reaches(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """
    Find reaches with no upstream AND no downstream neighbors.

    These are disconnected from the network (unless single-reach networks).
    """
    where_clause = f"AND r.region = '{region}'" if region else ""

    query = f"""
    SELECT
        r.reach_id, r.region, r.river_name, r.x, r.y,
        r.n_rch_up, r.n_rch_down, r.network, r.reach_length, r.width
    FROM reaches r
    WHERE r.n_rch_up = 0 AND r.n_rch_down = 0
        {where_clause}
    ORDER BY r.reach_length DESC
    """

    issues = conn.execute(query).fetchdf()

    total_query = f"""
    SELECT COUNT(*) FROM reaches r WHERE 1=1 {where_clause}
    """
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="T004",
        name="orphan_reaches",
        severity=Severity.WARNING,
        passed=len(issues) == 0,
        total_checked=total,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total if total > 0 else 0,
        details=issues,
        description="Reaches with no upstream or downstream neighbors (disconnected)",
    )


@register_check(
    "T005",
    Category.TOPOLOGY,
    Severity.ERROR,
    "n_rch_up/down must match actual topology counts",
)
def check_neighbor_count_consistency(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """
    Check that n_rch_up and n_rch_down match actual topology table counts.

    Mismatches indicate stale or corrupted topology data.
    """
    where_clause = f"AND r.region = '{region}'" if region else ""
    where_clause_rt = f"AND rt.region = '{region}'" if region else ""

    query = f"""
    WITH actual_counts AS (
        SELECT
            rt.reach_id,
            rt.region,
            SUM(CASE WHEN rt.direction = 'up' THEN 1 ELSE 0 END) as actual_up,
            SUM(CASE WHEN rt.direction = 'down' THEN 1 ELSE 0 END) as actual_down
        FROM reach_topology rt
        WHERE 1=1 {where_clause_rt}
        GROUP BY rt.reach_id, rt.region
    )
    SELECT
        r.reach_id, r.region, r.river_name, r.x, r.y,
        r.n_rch_up, r.n_rch_down,
        COALESCE(ac.actual_up, 0) as actual_up,
        COALESCE(ac.actual_down, 0) as actual_down,
        ABS(r.n_rch_up - COALESCE(ac.actual_up, 0)) as up_diff,
        ABS(r.n_rch_down - COALESCE(ac.actual_down, 0)) as down_diff
    FROM reaches r
    LEFT JOIN actual_counts ac ON r.reach_id = ac.reach_id AND r.region = ac.region
    WHERE (r.n_rch_up != COALESCE(ac.actual_up, 0)
           OR r.n_rch_down != COALESCE(ac.actual_down, 0))
        {where_clause}
    ORDER BY (ABS(r.n_rch_up - COALESCE(ac.actual_up, 0)) +
              ABS(r.n_rch_down - COALESCE(ac.actual_down, 0))) DESC
    """

    issues = conn.execute(query).fetchdf()

    total_query = f"""
    SELECT COUNT(*) FROM reaches r WHERE 1=1 {where_clause}
    """
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="T005",
        name="neighbor_count_consistency",
        severity=Severity.ERROR,
        passed=len(issues) == 0,
        total_checked=total,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total if total > 0 else 0,
        details=issues,
        description="Reaches where n_rch_up/n_rch_down doesn't match topology table",
    )


@register_check(
    "T006",
    Category.TOPOLOGY,
    Severity.INFO,
    "Connected component analysis",
)
def check_connected_components(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """
    Analyze network connectivity and report component statistics.

    Uses the 'network' field to identify distinct connected components.
    Reports single-reach networks and very small networks.
    """
    where_clause = f"AND r.region = '{region}'" if region else ""

    # Get network size distribution
    query = f"""
    WITH network_sizes AS (
        SELECT
            network,
            region,
            COUNT(*) as reach_count,
            MIN(reach_id) as sample_reach_id
        FROM reaches r
        WHERE network IS NOT NULL AND network > 0
            {where_clause}
        GROUP BY network, region
    )
    SELECT
        network, region, reach_count, sample_reach_id
    FROM network_sizes
    WHERE reach_count = 1  -- Single-reach networks (most suspicious)
    ORDER BY network
    """

    issues = conn.execute(query).fetchdf()

    # Get total network count and reach count
    stats_query = f"""
    SELECT
        COUNT(DISTINCT network) as total_networks,
        COUNT(*) as total_reaches
    FROM reaches r
    WHERE network IS NOT NULL AND network > 0
        {where_clause}
    """
    stats = conn.execute(stats_query).fetchone()
    total_networks = stats[0] if stats else 0

    return CheckResult(
        check_id="T006",
        name="connected_components",
        severity=Severity.INFO,
        passed=True,  # Informational, always "passes"
        total_checked=total_networks,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total_networks if total_networks > 0 else 0,
        details=issues,
        description=f"Single-reach networks ({len(issues)} of {total_networks} total networks)",
    )


@register_check(
    "T007",
    Category.TOPOLOGY,
    Severity.WARNING,
    "Topology reciprocity (A→B implies B→A)",
)
def check_topology_reciprocity(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """
    Check that topology relationships are reciprocal.

    If reach A has downstream neighbor B, then B should have upstream neighbor A.
    """
    where_clause = f"AND rt1.region = '{region}'" if region else ""

    query = f"""
    WITH forward_edges AS (
        SELECT
            rt1.reach_id as from_reach,
            rt1.neighbor_reach_id as to_reach,
            rt1.region,
            rt1.direction
        FROM reach_topology rt1
        WHERE 1=1 {where_clause}
    ),
    expected_reverse AS (
        SELECT
            to_reach as reach_id,
            from_reach as expected_neighbor,
            region,
            CASE WHEN direction = 'down' THEN 'up' ELSE 'down' END as expected_direction
        FROM forward_edges
    ),
    missing_reverse AS (
        SELECT
            er.reach_id,
            er.expected_neighbor,
            er.region,
            er.expected_direction
        FROM expected_reverse er
        LEFT JOIN reach_topology rt2
            ON er.reach_id = rt2.reach_id
            AND er.expected_neighbor = rt2.neighbor_reach_id
            AND er.region = rt2.region
            AND er.expected_direction = rt2.direction
        WHERE rt2.reach_id IS NULL
    )
    SELECT
        mr.reach_id, mr.region, mr.expected_neighbor, mr.expected_direction,
        r.river_name, r.x, r.y
    FROM missing_reverse mr
    JOIN reaches r ON mr.reach_id = r.reach_id AND mr.region = r.region
    ORDER BY mr.reach_id
    """

    issues = conn.execute(query).fetchdf()

    total_query = f"""
    SELECT COUNT(*) FROM reach_topology rt1 WHERE 1=1 {where_clause}
    """
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="T007",
        name="topology_reciprocity",
        severity=Severity.WARNING,
        passed=len(issues) == 0,
        total_checked=total,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total if total > 0 else 0,
        details=issues,
        description="Missing reciprocal topology edges (A→B without B←A)",
    )


@register_check(
    "T008",
    Category.TOPOLOGY,
    Severity.ERROR,
    "dist_out must be non-negative (except fill value -9999)",
)
def check_dist_out_negative(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """
    Check that dist_out values are non-negative.

    Negative values (other than -9999 fill) indicate data corruption.
    """
    where_clause = f"AND r.region = '{region}'" if region else ""

    query = f"""
    SELECT
        r.reach_id, r.region, r.river_name, r.x, r.y,
        r.dist_out, r.n_rch_up, r.n_rch_down
    FROM reaches r
    WHERE r.dist_out < 0 AND r.dist_out != -9999
        {where_clause}
    ORDER BY r.dist_out ASC
    """

    issues = conn.execute(query).fetchdf()

    total_query = f"""
    SELECT COUNT(*) FROM reaches r
    WHERE dist_out IS NOT NULL AND dist_out != -9999
    {where_clause}
    """
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="T008",
        name="dist_out_negative",
        severity=Severity.ERROR,
        passed=len(issues) == 0,
        total_checked=total,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total if total > 0 else 0,
        details=issues,
        description="Reaches with negative dist_out (data corruption)",
    )


@register_check(
    "T009",
    Category.TOPOLOGY,
    Severity.ERROR,
    "dist_out=0 only valid for outlets (end_reach=2)",
)
def check_dist_out_zero_at_nonoutlet(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """
    Check that dist_out=0 only occurs at outlet reaches.

    Outlets are marked with end_reach=2 or n_rch_down=0.
    """
    where_clause = f"AND r.region = '{region}'" if region else ""

    query = f"""
    SELECT
        r.reach_id, r.region, r.river_name, r.x, r.y,
        r.dist_out, r.end_reach, r.n_rch_down
    FROM reaches r
    WHERE r.dist_out = 0
        AND r.end_reach != 2  -- Not marked as outlet
        AND r.n_rch_down > 0  -- Has downstream neighbors (not actually outlet)
        {where_clause}
    ORDER BY r.reach_id
    """

    issues = conn.execute(query).fetchdf()

    total_query = f"""
    SELECT COUNT(*) FROM reaches r
    WHERE dist_out = 0
    {where_clause}
    """
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="T009",
        name="dist_out_zero_at_nonoutlet",
        severity=Severity.ERROR,
        passed=len(issues) == 0,
        total_checked=total,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total if total > 0 else 0,
        details=issues,
        description="Reaches with dist_out=0 but not outlets (topology error)",
    )


@register_check(
    "T010",
    Category.TOPOLOGY,
    Severity.ERROR,
    "Headwaters must have path_freq >= 1",
)
def check_headwater_path_freq(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """
    Check that headwater reaches (n_rch_up=0) have path_freq >= 1.

    Headwaters should be visited at least once in any outlet-to-headwater traversal.
    """
    where_clause = f"AND r.region = '{region}'" if region else ""

    query = f"""
    SELECT
        r.reach_id, r.region, r.river_name, r.x, r.y,
        r.path_freq, r.n_rch_up, r.n_rch_down, r.type
    FROM reaches r
    WHERE r.n_rch_up = 0  -- Headwater
        AND (r.path_freq IS NULL OR r.path_freq < 1 OR r.path_freq = -9999)
        AND r.type NOT IN (5, 6)  -- Exclude unreliable/ghost
        {where_clause}
    ORDER BY r.reach_id
    """

    issues = conn.execute(query).fetchdf()

    total_query = f"""
    SELECT COUNT(*) FROM reaches r
    WHERE n_rch_up = 0 AND type NOT IN (5, 6)
    {where_clause}
    """
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="T010",
        name="headwater_path_freq",
        severity=Severity.ERROR,
        passed=len(issues) == 0,
        total_checked=total,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total if total > 0 else 0,
        details=issues,
        description="Headwater reaches with path_freq < 1 (traversal error)",
    )


@register_check(
    "T011",
    Category.TOPOLOGY,
    Severity.WARNING,
    "Zero path_freq only valid for disconnected reaches",
)
def check_path_freq_zero(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """
    Check that path_freq=0 only occurs for disconnected reaches.

    Connected reaches (n_rch_up > 0 OR n_rch_down > 0) should have path_freq > 0.
    """
    where_clause = f"AND r.region = '{region}'" if region else ""

    query = f"""
    SELECT
        r.reach_id, r.region, r.river_name, r.x, r.y,
        r.path_freq, r.n_rch_up, r.n_rch_down, r.type
    FROM reaches r
    WHERE r.path_freq = 0
        AND (r.n_rch_up > 0 OR r.n_rch_down > 0)  -- Connected
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
        check_id="T011",
        name="path_freq_zero",
        severity=Severity.WARNING,
        passed=len(issues) == 0,
        total_checked=total,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total if total > 0 else 0,
        details=issues,
        description="Connected reaches with path_freq=0 (traversal bug)",
    )


@register_check(
    "T012",
    Category.TOPOLOGY,
    Severity.ERROR,
    "Topology referential integrity (all neighbor_reach_ids exist in reaches)",
)
def check_topology_referential_integrity(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """
    Check that all neighbor_reach_id values in reach_topology exist in reaches.

    Derived from DrainageAreaFix/ ``ChecksPriorToAddingJunction()`` which
    verifies all junction members exist in the reach file before adding a
    junction. A dangling reference breaks both the CVXPY integrator and our
    v3 conservation pipeline.

    T005 checks counts match, T007 checks reciprocity, but neither checks
    that referenced reach_ids actually exist in the reaches table.
    """
    where_clause = f"AND rt.region = '{region}'" if region else ""

    query = f"""
    SELECT
        rt.reach_id as source_reach_id,
        rt.neighbor_reach_id as dangling_reach_id,
        rt.region,
        rt.direction,
        rt.neighbor_rank
    FROM reach_topology rt
    LEFT JOIN reaches r ON rt.neighbor_reach_id = r.reach_id
        AND rt.region = r.region
    WHERE r.reach_id IS NULL
        {where_clause}
    ORDER BY rt.reach_id
    """

    issues = conn.execute(query).fetchdf()

    total_query = f"""
    SELECT COUNT(*) FROM reach_topology rt WHERE 1=1 {where_clause}
    """
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="T012",
        name="topology_referential_integrity",
        severity=Severity.ERROR,
        passed=len(issues) == 0,
        total_checked=total,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total if total > 0 else 0,
        details=issues,
        description="Topology edges referencing non-existent reach_ids (dangling references)",
    )


@register_check(
    "T013",
    Category.TOPOLOGY,
    Severity.ERROR,
    "Reach must not reference itself as neighbor",
)
def check_self_referential_topology(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """Check that no reach lists itself as a neighbor in reach_topology."""
    where_clause = f"AND rt.region = '{region}'" if region else ""

    query = f"""
    SELECT
        rt.reach_id, rt.region, rt.direction, rt.neighbor_rank,
        r.river_name, r.x, r.y
    FROM reach_topology rt
    JOIN reaches r ON rt.reach_id = r.reach_id AND rt.region = r.region
    WHERE rt.reach_id = rt.neighbor_reach_id
        {where_clause}
    ORDER BY rt.reach_id
    """

    issues = conn.execute(query).fetchdf()

    total_query = f"""
    SELECT COUNT(*) FROM reach_topology rt WHERE 1=1 {where_clause}
    """
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="T013",
        name="self_referential_topology",
        severity=Severity.ERROR,
        passed=len(issues) == 0,
        total_checked=total,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total if total > 0 else 0,
        details=issues,
        description="Topology edges where reach references itself as neighbor",
    )


@register_check(
    "T014",
    Category.TOPOLOGY,
    Severity.ERROR,
    "Same pair must not appear in both up and down directions",
)
def check_bidirectional_topology(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """Check that no reach pair has edges in both directions (A upstream of B AND A downstream of B)."""
    where_clause = f"AND rt1.region = '{region}'" if region else ""

    query = f"""
    SELECT
        rt1.reach_id, rt1.neighbor_reach_id, rt1.region,
        r.river_name, r.x, r.y
    FROM reach_topology rt1
    JOIN reach_topology rt2
        ON rt1.reach_id = rt2.reach_id
        AND rt1.neighbor_reach_id = rt2.neighbor_reach_id
        AND rt1.region = rt2.region
    JOIN reaches r ON rt1.reach_id = r.reach_id AND rt1.region = r.region
    WHERE rt1.direction = 'up' AND rt2.direction = 'down'
        {where_clause}
    ORDER BY rt1.reach_id
    """

    issues = conn.execute(query).fetchdf()

    total_query = f"""
    SELECT COUNT(DISTINCT reach_id) FROM reach_topology rt1 WHERE 1=1 {where_clause}
    """
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="T014",
        name="bidirectional_topology",
        severity=Severity.ERROR,
        passed=len(issues) == 0,
        total_checked=total,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total if total > 0 else 0,
        details=issues,
        description="Reach pairs that appear as both upstream and downstream neighbors",
    )


@register_check(
    "T015",
    Category.TOPOLOGY,
    Severity.INFO,
    "Shortcut edges bypassing intermediate reach (A→B→C and A→C)",
)
def check_topology_shortcut(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """Find shortcut edges: A→C downstream where A→B→C also exists."""
    where_clause = f"AND rt1.region = '{region}'" if region else ""

    query = f"""
    SELECT
        rt1.reach_id as reach_a,
        rt2.reach_id as reach_b,
        rt3.neighbor_reach_id as reach_c,
        rt1.region,
        r.river_name, r.x, r.y
    FROM reach_topology rt1
    JOIN reach_topology rt2
        ON rt1.neighbor_reach_id = rt2.reach_id
        AND rt1.region = rt2.region
    JOIN reach_topology rt3
        ON rt1.reach_id = rt3.reach_id
        AND rt2.neighbor_reach_id = rt3.neighbor_reach_id
        AND rt1.region = rt3.region
    JOIN reaches r ON rt1.reach_id = r.reach_id AND rt1.region = r.region
    WHERE rt1.direction = 'down'
        AND rt2.direction = 'down'
        AND rt3.direction = 'down'
        AND rt1.neighbor_reach_id != rt3.neighbor_reach_id
        {where_clause}
    ORDER BY rt1.reach_id
    LIMIT 10000
    """

    issues = conn.execute(query).fetchdf()

    total_query = f"""
    SELECT COUNT(DISTINCT reach_id) FROM reach_topology rt1
    WHERE direction = 'down' {where_clause}
    """
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="T015",
        name="topology_shortcut",
        severity=Severity.INFO,
        passed=len(issues) == 0,
        total_checked=total,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total if total > 0 else 0,
        details=issues,
        description="Shortcut edges where A→C exists alongside A→B→C",
    )


@register_check(
    "T017",
    Category.TOPOLOGY,
    Severity.WARNING,
    "dist_out jump >30 km between connected reaches",
    default_threshold=30000.0,
)
def check_dist_out_jump(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """Flag downstream pairs where dist_out changes by more than threshold."""
    max_jump = threshold if threshold is not None else 30000.0
    where_clause = f"AND r1.region = '{region}'" if region else ""

    query = f"""
    SELECT
        r1.reach_id, r1.region, r1.river_name, r1.x, r1.y,
        r1.dist_out as dist_out_up,
        r2.dist_out as dist_out_down,
        ABS(r1.dist_out - r2.dist_out) as dist_out_jump
    FROM reaches r1
    JOIN reach_topology rt ON r1.reach_id = rt.reach_id AND r1.region = rt.region
    JOIN reaches r2 ON rt.neighbor_reach_id = r2.reach_id AND rt.region = r2.region
    WHERE rt.direction = 'down'
        AND r1.dist_out != -9999 AND r2.dist_out != -9999
        AND r1.dist_out > 0 AND r2.dist_out > 0
        AND ABS(r1.dist_out - r2.dist_out) > {max_jump}
        {where_clause}
    ORDER BY dist_out_jump DESC
    LIMIT 10000
    """

    issues = conn.execute(query).fetchdf()

    total_query = f"""
    SELECT COUNT(*) FROM reaches r1
    JOIN reach_topology rt ON r1.reach_id = rt.reach_id AND r1.region = rt.region
    JOIN reaches r2 ON rt.neighbor_reach_id = r2.reach_id AND rt.region = r2.region
    WHERE rt.direction = 'down'
        AND r1.dist_out != -9999 AND r2.dist_out != -9999
        AND r1.dist_out > 0 AND r2.dist_out > 0
        {where_clause}
    """
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="T017",
        name="dist_out_jump",
        severity=Severity.WARNING,
        passed=len(issues) == 0,
        total_checked=total,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total if total > 0 else 0,
        details=issues,
        description=f"Connected reaches with dist_out jump >{max_jump / 1000:.0f} km",
        threshold=max_jump,
    )


@register_check(
    "T018",
    Category.TOPOLOGY,
    Severity.ERROR,
    "Reach IDs must be 11 digits ending in 1/3/4/5/6; node IDs must be 14 digits",
)
def check_id_format(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """Validate reach_id and node_id format conventions."""
    where_reach = f"AND r.region = '{region}'" if region else ""
    where_node = f"AND n.region = '{region}'" if region else ""

    query = f"""
    SELECT reach_id as entity_id, region, 'reach' as issue_type,
        CASE
            WHEN LENGTH(CAST(reach_id AS VARCHAR)) != 11 THEN 'wrong_length'
            WHEN CAST(reach_id AS VARCHAR)[-1:] NOT IN ('1','3','4','5','6') THEN 'bad_suffix'
        END as reason
    FROM reaches r
    WHERE (
        LENGTH(CAST(reach_id AS VARCHAR)) != 11
        OR CAST(reach_id AS VARCHAR)[-1:] NOT IN ('1','3','4','5','6')
    )
        {where_reach}
    UNION ALL
    SELECT node_id as entity_id, region, 'node' as issue_type,
        CASE
            WHEN LENGTH(CAST(node_id AS VARCHAR)) != 14 THEN 'wrong_length'
        END as reason
    FROM nodes n
    WHERE LENGTH(CAST(node_id AS VARCHAR)) != 14
        {where_node}
    ORDER BY issue_type, entity_id
    LIMIT 10000
    """

    issues = conn.execute(query).fetchdf()

    total_query = f"""
    SELECT
        (SELECT COUNT(*) FROM reaches r WHERE 1=1 {where_reach})
        + (SELECT COUNT(*) FROM nodes n WHERE 1=1 {where_node})
    """
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="T018",
        name="id_format",
        severity=Severity.ERROR,
        passed=len(issues) == 0,
        total_checked=total,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total if total > 0 else 0,
        details=issues,
        description="IDs with invalid format (reach: 11 digits ending 1/3/4/5/6; node: 14 digits)",
    )


@register_check(
    "T019",
    Category.TOPOLOGY,
    Severity.INFO,
    "Reaches with river_name = 'NODATA' (unnamed)",
)
def check_river_name_nodata(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """Report coverage of unnamed reaches (river_name = 'NODATA')."""
    where_clause = f"AND r.region = '{region}'" if region else ""

    query = f"""
    SELECT
        r.region, COUNT(*) as nodata_count
    FROM reaches r
    WHERE r.river_name = 'NODATA'
        {where_clause}
    GROUP BY r.region
    ORDER BY nodata_count DESC
    """

    issues = conn.execute(query).fetchdf()

    total_query = f"""
    SELECT COUNT(*) FROM reaches r WHERE 1=1 {where_clause}
    """
    total = conn.execute(total_query).fetchone()[0]

    nodata_total = int(issues["nodata_count"].sum()) if len(issues) > 0 else 0

    return CheckResult(
        check_id="T019",
        name="river_name_nodata",
        severity=Severity.INFO,
        passed=True,  # Informational
        total_checked=total,
        issues_found=nodata_total,
        issue_pct=100 * nodata_total / total if total > 0 else 0,
        details=issues,
        description=f"Reaches with river_name='NODATA': {nodata_total} ({100 * nodata_total / total:.1f}%)"
        if total > 0
        else "No reaches found",
    )


@register_check(
    "T020",
    Category.TOPOLOGY,
    Severity.INFO,
    "Reach river_name disagrees with all neighbors' consensus",
)
def check_river_name_consensus(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """Flag reaches whose river_name differs from ALL their neighbors (consensus mismatch)."""
    where_clause = f"AND r.region = '{region}'" if region else ""

    nn_where = f"AND rt.region = '{region}'" if region else ""

    query = f"""
    WITH neighbor_names AS (
        SELECT
            rt.reach_id, rt.region,
            r2.river_name as neighbor_name
        FROM reach_topology rt
        JOIN reaches r2 ON rt.neighbor_reach_id = r2.reach_id AND rt.region = r2.region
        WHERE r2.river_name != 'NODATA'
            {nn_where}
    ),
    consensus AS (
        SELECT
            reach_id, region,
            MODE(neighbor_name) as consensus_name,
            COUNT(DISTINCT neighbor_name) as distinct_names,
            COUNT(*) as total_neighbors
        FROM neighbor_names
        GROUP BY reach_id, region
    )
    SELECT
        r.reach_id, r.region, r.river_name, r.x, r.y,
        c.consensus_name, c.distinct_names, c.total_neighbors
    FROM reaches r
    JOIN consensus c ON r.reach_id = c.reach_id AND r.region = c.region
    WHERE r.river_name != 'NODATA'
        AND c.distinct_names = 1
        AND r.river_name != c.consensus_name
        {where_clause}
    ORDER BY c.total_neighbors DESC
    LIMIT 10000
    """

    issues = conn.execute(query).fetchdf()

    total_query = f"""
    SELECT COUNT(*) FROM reaches r
    WHERE river_name != 'NODATA'
    {where_clause}
    """
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="T020",
        name="river_name_consensus",
        severity=Severity.INFO,
        passed=len(issues) == 0,
        total_checked=total,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total if total > 0 else 0,
        details=issues,
        description="Reaches where river_name disagrees with all neighbors' consensus",
    )


@register_check(
    "T021",
    Category.TOPOLOGY,
    Severity.WARNING,
    "dist_out increases on ANY downstream edge (bifurcation bug)",
    default_threshold=100.0,
)
def check_dist_out_bifurcation_monotonicity(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """
    Check per-edge dist_out monotonicity at bifurcations.

    T001 checks MIN(downstream dist_out) — true topology errors where no
    branch decreases. This check finds bifurcations where at least one
    downstream branch has HIGHER dist_out (the other branch decreases
    correctly). These are caused by UNC's dist_out algorithm settling for
    a partially-computed downstream value instead of waiting for MAX.
    """
    tolerance = threshold if threshold is not None else 100.0
    where_clause = f"AND r1.region = '{region}'" if region else ""

    query = f"""
    SELECT
        r1.reach_id, r1.region, r1.river_name, r1.x, r1.y,
        r1.dist_out as dist_out_up,
        r2.dist_out as dist_out_down,
        r2.reach_id as downstream_reach_id,
        (r2.dist_out - r1.dist_out) as dist_out_increase,
        r1.n_rch_down
    FROM reaches r1
    JOIN reach_topology rt ON r1.reach_id = rt.reach_id AND r1.region = rt.region
    JOIN reaches r2 ON rt.neighbor_reach_id = r2.reach_id AND rt.region = r2.region
    WHERE rt.direction = 'down'
        AND r1.dist_out > 0 AND r1.dist_out != -9999
        AND r2.dist_out > 0 AND r2.dist_out != -9999
        AND r2.dist_out > r1.dist_out + {tolerance}
        {where_clause}
    ORDER BY dist_out_increase DESC
    """

    issues = conn.execute(query).fetchdf()

    total_query = f"""
    SELECT COUNT(*) FROM reaches r1
    JOIN reach_topology rt ON r1.reach_id = rt.reach_id AND r1.region = rt.region
    JOIN reaches r2 ON rt.neighbor_reach_id = r2.reach_id AND rt.region = r2.region
    WHERE rt.direction = 'down'
        AND r1.dist_out > 0 AND r1.dist_out != -9999
        AND r2.dist_out > 0 AND r2.dist_out != -9999
        {where_clause}
    """
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="T021",
        name="dist_out_bifurcation_monotonicity",
        severity=Severity.WARNING,
        passed=len(issues) == 0,
        total_checked=total,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total if total > 0 else 0,
        details=issues,
        description="Downstream edges where dist_out increases (bifurcation bug)",
        threshold=tolerance,
    )
