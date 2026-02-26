"""
SWORD Lint - Classification Checks (C0xx)

Validates lake/river classification and type assignments.
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
    "C001",
    Category.CLASSIFICATION,
    Severity.WARNING,
    "River reaches between lake reaches (lake sandwich)",
)
def check_lake_sandwich(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """
    Find river reaches sandwiched between lake reaches.

    A "lake sandwich" is a river reach (lakeflag=0) that has:
    - At least one upstream lake neighbor (lakeflag=1)
    - At least one downstream lake neighbor (lakeflag=1)

    These may be misclassified lake sections.
    """
    where_clause = f"AND r.region = '{region}'" if region else ""

    query = f"""
    WITH river_reaches AS (
        SELECT reach_id, region, x, y, reach_length, width, river_name
        FROM reaches
        WHERE lakeflag = 0 {where_clause.replace("r.", "")}
    ),
    has_lake_upstream AS (
        SELECT DISTINCT rt.reach_id, rt.region
        FROM reach_topology rt
        JOIN reaches r ON rt.neighbor_reach_id = r.reach_id AND rt.region = r.region
        WHERE rt.direction = 'up' AND r.lakeflag = 1
    ),
    has_lake_downstream AS (
        SELECT DISTINCT rt.reach_id, rt.region
        FROM reach_topology rt
        JOIN reaches r ON rt.neighbor_reach_id = r.reach_id AND rt.region = r.region
        WHERE rt.direction = 'down' AND r.lakeflag = 1
    )
    SELECT
        rr.reach_id, rr.region, rr.river_name, rr.x, rr.y,
        rr.reach_length, rr.width
    FROM river_reaches rr
    JOIN has_lake_upstream hu ON rr.reach_id = hu.reach_id AND rr.region = hu.region
    JOIN has_lake_downstream hd ON rr.reach_id = hd.reach_id AND rr.region = hd.region
    ORDER BY rr.reach_length DESC
    """

    issues = conn.execute(query).fetchdf()

    total_query = f"""
    SELECT COUNT(*) FROM reaches r WHERE lakeflag = 0 {where_clause}
    """
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="C001",
        name="lake_sandwich",
        severity=Severity.WARNING,
        passed=len(issues) == 0,
        total_checked=total,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total if total > 0 else 0,
        details=issues,
        description="River reaches sandwiched between lake reaches (potential misclassification)",
    )


@register_check(
    "C002",
    Category.CLASSIFICATION,
    Severity.INFO,
    "Check lakeflag distribution",
)
def check_lakeflag_distribution(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """
    Report lakeflag distribution.

    lakeflag values:
    - 0: river
    - 1: lake
    - 2: canal
    - 3: tidal
    """
    where_clause = f"AND region = '{region}'" if region else ""

    query = f"""
    SELECT
        lakeflag,
        CASE lakeflag
            WHEN 0 THEN 'river'
            WHEN 1 THEN 'lake'
            WHEN 2 THEN 'canal'
            WHEN 3 THEN 'tidal'
            ELSE 'unknown'
        END as type_name,
        COUNT(*) as count,
        ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as pct
    FROM reaches
    WHERE lakeflag IS NOT NULL
    {where_clause}
    GROUP BY lakeflag
    ORDER BY lakeflag
    """

    details = conn.execute(query).fetchdf()

    total_query = f"""
    SELECT COUNT(*) FROM reaches WHERE lakeflag IS NOT NULL {where_clause}
    """
    total = conn.execute(total_query).fetchone()[0]

    # Flag unknown lakeflag values
    unknown_count = 0
    if len(details) > 0:
        unknown = details[~details["lakeflag"].isin([0, 1, 2, 3])]
        unknown_count = unknown["count"].sum() if len(unknown) > 0 else 0

    return CheckResult(
        check_id="C002",
        name="lakeflag_distribution",
        severity=Severity.INFO,
        passed=unknown_count == 0,
        total_checked=total,
        issues_found=unknown_count,
        issue_pct=100 * unknown_count / total if total > 0 else 0,
        details=details,
        description=f"Lakeflag distribution ({unknown_count} unknown values)",
    )


@register_check(
    "C003",
    Category.CLASSIFICATION,
    Severity.INFO,
    "Check type field distribution",
)
def check_type_distribution(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """
    Report reach type distribution.

    Type values (from SWORD PDD v17b):
    - 1: river
    - 2: (unused — no reaches have this value)
    - 3: lake_on_river (NOT tidal — tidal is lakeflag=3)
    - 4: dam/artificial
    - 5: unreliable topology
    - 6: ghost reach
    """
    where_clause = f"AND region = '{region}'" if region else ""

    # Check if 'type' column exists
    try:
        query = f"""
        SELECT
            type,
            CASE type
                WHEN 1 THEN 'river'
                WHEN 2 THEN 'lake'
                WHEN 3 THEN 'lake_on_river'
                WHEN 4 THEN 'artificial'
                WHEN 5 THEN 'unassigned'
                WHEN 6 THEN 'unreliable'
                ELSE 'unknown'
            END as type_name,
            COUNT(*) as count,
            ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as pct
        FROM reaches
        WHERE type IS NOT NULL
        {where_clause}
        GROUP BY type
        ORDER BY type
        """

        details = conn.execute(query).fetchdf()

        total_query = f"""
        SELECT COUNT(*) FROM reaches WHERE type IS NOT NULL {where_clause}
        """
        total = conn.execute(total_query).fetchone()[0]

        # Flag type=6 (unreliable) reaches
        unreliable_count = 0
        if len(details) > 0:
            unreliable = details[details["type"] == 6]
            unreliable_count = unreliable["count"].sum() if len(unreliable) > 0 else 0

        return CheckResult(
            check_id="C003",
            name="type_distribution",
            severity=Severity.INFO,
            passed=True,  # Informational
            total_checked=total,
            issues_found=unreliable_count,
            issue_pct=100 * unreliable_count / total if total > 0 else 0,
            details=details,
            description=f"Type distribution ({unreliable_count} unreliable reaches)",
        )

    except (duckdb.CatalogException, duckdb.BinderException):
        # 'type' column doesn't exist
        return CheckResult(
            check_id="C003",
            name="type_distribution",
            severity=Severity.INFO,
            passed=True,
            total_checked=0,
            issues_found=0,
            issue_pct=0,
            details=pd.DataFrame(),
            description="Type column not present in database",
        )


@register_check(
    "C004",
    Category.CLASSIFICATION,
    Severity.INFO,  # Changed to INFO - needs investigation which is authoritative
    "Lakeflag/type cross-tabulation",
)
def check_lakeflag_type_consistency(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """
    Report lakeflag vs type cross-tabulation for investigation.

    SWORD type values (from reach_id last digit):
    - 1 = river
    - 3 = lake on river
    - 4 = dam or waterfall
    - 5 = unreliable topology
    - 6 = ghost reach

    SWORD lakeflag values:
    - 0 = river
    - 1 = lake
    - 2 = canal
    - 3 = tidal

    Key mismatches to investigate:
    - lakeflag=0 (river) but type=3 (lake_on_river): river section through lake?
    - lakeflag=1 (lake) but type=1 (river): misclassification?

    NOTE: Unclear which field is more authoritative - needs investigation.
    """
    where_clause = f"AND region = '{region}'" if region else ""

    try:
        # Get cross-tabulation
        query = f"""
        SELECT
            lakeflag, type, COUNT(*) as count
        FROM reaches
        WHERE lakeflag IS NOT NULL AND type IS NOT NULL
            {where_clause}
        GROUP BY lakeflag, type
        ORDER BY lakeflag, type
        """

        stats = conn.execute(query).fetchdf()

        # Count potential mismatches (for reporting)
        # Main expected: lakeflag=0/type=1 (river/river), lakeflag=1/type=3 (lake/lake_on_river)
        # type=3 is "lake_on_river" (NOT tidal). Tidal is lakeflag=3.
        mismatch_query = f"""
        SELECT COUNT(*) FROM reaches
        WHERE lakeflag IS NOT NULL AND type IS NOT NULL
            AND NOT (
                    (lakeflag = 0 AND type IN (1, 3, 4)) -- river: river, lake_on_river, dam
                    OR (lakeflag = 1 AND type IN (3, 4)) -- lake: lake_on_river or dam
                    OR (lakeflag = 2 AND type IN (1, 4)) -- canal: river or artificial
                    OR (lakeflag = 3 AND type IN (3))    -- tidal: lake_on_river (tidal estuary)
                    OR type IN (5, 6)                    -- unreliable/ghost
                )
            {where_clause}
        """
        mismatch_count = conn.execute(mismatch_query).fetchone()[0]

        total_query = f"""
        SELECT COUNT(*) FROM reaches
        WHERE lakeflag IS NOT NULL AND type IS NOT NULL
        {where_clause}
        """
        total = conn.execute(total_query).fetchone()[0]

        return CheckResult(
            check_id="C004",
            name="lakeflag_type_consistency",
            severity=Severity.INFO,
            passed=True,  # Informational - needs investigation
            total_checked=total,
            issues_found=mismatch_count,
            issue_pct=100 * mismatch_count / total if total > 0 else 0,
            details=stats,
            description=f"Potential lakeflag/type mismatches: {mismatch_count} ({100 * mismatch_count / total:.1f}%) - needs investigation",
        )

    except (duckdb.CatalogException, duckdb.BinderException):
        # 'type' column doesn't exist
        return CheckResult(
            check_id="C004",
            name="lakeflag_type_consistency",
            severity=Severity.INFO,
            passed=True,
            total_checked=0,
            issues_found=0,
            issue_pct=0,
            details=pd.DataFrame(),
            description="Type column not present in database (cannot check consistency)",
        )


@register_check(
    "O001",
<<<<<<< HEAD
    Category.OBSTRUCTION,
=======
    Category.CLASSIFICATION,
>>>>>>> ad53e4b (feat: add DL-GROD ingestion and obstruction lint checks (#127))
    Severity.ERROR,
    "obstr_type must be in {0, 1, 2, 3, 4, 5}",
)
def check_obstr_type_values(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """Validate obstr_type values.

<<<<<<< HEAD
    Valid values (v17c encoding — different from v17b):
=======
    Valid values:
>>>>>>> ad53e4b (feat: add DL-GROD ingestion and obstruction lint checks (#127))
    - 0: no obstruction
    - 1: dam (GROD/DL-GROD)
    - 2: low-head dam (DL-GROD)
    - 3: lock (GROD/DL-GROD)
    - 4: waterfall (HydroFALLS)
    - 5: partial dam (DL-GROD, He et al. 2025)
<<<<<<< HEAD

    Note: v17b used 2=lock, 3=low-perm — numeric range overlaps so this
    check passes both encodings, but semantics differ. Only run against v17c.
=======
>>>>>>> ad53e4b (feat: add DL-GROD ingestion and obstruction lint checks (#127))
    """
    where_clause = f"AND region = '{region}'" if region else ""

    query = f"""
    SELECT reach_id, region, x, y, obstr_type
    FROM reaches
    WHERE obstr_type IS NOT NULL
      AND obstr_type NOT IN (0, 1, 2, 3, 4, 5)
    {where_clause}
    ORDER BY reach_id
    """
    issues = conn.execute(query).fetchdf()

    total = conn.execute(
        f"SELECT COUNT(*) FROM reaches WHERE obstr_type IS NOT NULL {where_clause}"
    ).fetchone()[0]

    return CheckResult(
        check_id="O001",
        name="obstr_type_values",
        severity=Severity.ERROR,
        passed=len(issues) == 0,
        total_checked=total,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total if total > 0 else 0,
        details=issues,
        description=f"Reaches with invalid obstr_type (not in 0-5): {len(issues)}",
    )


@register_check(
    "O002",
<<<<<<< HEAD
    Category.OBSTRUCTION,
=======
    Category.CLASSIFICATION,
>>>>>>> ad53e4b (feat: add DL-GROD ingestion and obstruction lint checks (#127))
    Severity.WARNING,
    "grod_id/dl_grod_id non-zero only for obstr_type 1, 2, 3, 5",
)
def check_grod_id_consistency(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """Validate grod_id and dl_grod_id are only set for dam/lock types.

    grod_id and dl_grod_id should be non-zero only when obstr_type in {1, 2, 3, 5}.
    obstr_type=4 (waterfall) uses hfalls_id instead.
    """
    where_clause = f"AND region = '{region}'" if region else ""

    # Check if dl_grod_id column exists
    cols = {
        r[0]
        for r in conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name='reaches'"
        ).fetchall()
    }
    dl_grod_clause = (
        "OR (dl_grod_id IS NOT NULL AND dl_grod_id != 0 AND obstr_type NOT IN (1, 2, 3, 5))"
        if "dl_grod_id" in cols
        else ""
    )

    query = f"""
    SELECT reach_id, region, x, y, obstr_type, grod_id,
           {"dl_grod_id," if "dl_grod_id" in cols else ""}
           hfalls_id
    FROM reaches
    WHERE (
        (grod_id IS NOT NULL AND grod_id != 0 AND obstr_type NOT IN (1, 2, 3, 5))
        {dl_grod_clause}
    )
    {where_clause}
    ORDER BY reach_id
    """
    issues = conn.execute(query).fetchdf()

<<<<<<< HEAD
    # Denominator: reaches with any non-zero obstruction ID (matching issue query filters)
    dl_grod_filter = (
        "OR (dl_grod_id IS NOT NULL AND dl_grod_id != 0)"
        if "dl_grod_id" in cols
        else ""
    )
    total = conn.execute(
        f"SELECT COUNT(*) FROM reaches WHERE ((grod_id IS NOT NULL AND grod_id != 0) {dl_grod_filter}) {where_clause}"
=======
    total = conn.execute(
        f"SELECT COUNT(*) FROM reaches WHERE grod_id IS NOT NULL {where_clause}"
>>>>>>> ad53e4b (feat: add DL-GROD ingestion and obstruction lint checks (#127))
    ).fetchone()[0]

    return CheckResult(
        check_id="O002",
        name="grod_id_consistency",
        severity=Severity.WARNING,
        passed=len(issues) == 0,
        total_checked=total,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total if total > 0 else 0,
        details=issues,
        description=f"Reaches with grod/dl_grod_id set for non-dam/lock type: {len(issues)}",
    )


@register_check(
    "O003",
<<<<<<< HEAD
    Category.OBSTRUCTION,
=======
    Category.CLASSIFICATION,
>>>>>>> ad53e4b (feat: add DL-GROD ingestion and obstruction lint checks (#127))
    Severity.WARNING,
    "hfalls_id non-zero only for obstr_type 4 (waterfall)",
)
def check_hfalls_id_consistency(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """Validate hfalls_id is only set for waterfall reaches (obstr_type=4)."""
    where_clause = f"AND region = '{region}'" if region else ""

    query = f"""
    SELECT reach_id, region, x, y, obstr_type, hfalls_id
    FROM reaches
    WHERE hfalls_id IS NOT NULL
      AND hfalls_id != 0
<<<<<<< HEAD
      AND (obstr_type IS NULL OR obstr_type != 4)
=======
      AND obstr_type != 4
>>>>>>> ad53e4b (feat: add DL-GROD ingestion and obstruction lint checks (#127))
    {where_clause}
    ORDER BY reach_id
    """
    issues = conn.execute(query).fetchdf()

    total = conn.execute(
        f"SELECT COUNT(*) FROM reaches WHERE hfalls_id IS NOT NULL AND hfalls_id != 0 {where_clause}"
    ).fetchone()[0]

    return CheckResult(
        check_id="O003",
        name="hfalls_id_consistency",
        severity=Severity.WARNING,
        passed=len(issues) == 0,
        total_checked=total,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total if total > 0 else 0,
        details=issues,
        description=f"Reaches with hfalls_id set for non-waterfall type: {len(issues)}",
    )


@register_check(
    "C005",
    Category.CLASSIFICATION,
    Severity.WARNING,
    "Centerline centroid far from parent reach centroid",
    default_threshold=5000.0,
)
def check_centerline_reach_distance(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """Flag centerlines whose centroid is far from their parent reach centroid."""
    max_dist = threshold if threshold is not None else 5000.0
    where_clause = f"AND cl.region = '{region}'" if region else ""

    # Check if centerlines table exists
    try:
        conn.execute("SELECT cl_id FROM centerlines LIMIT 0")
    except (duckdb.CatalogException, duckdb.BinderException):
        return CheckResult(
            check_id="C005",
            name="centerline_reach_distance",
            severity=Severity.WARNING,
            passed=True,
            total_checked=0,
            issues_found=0,
            issue_pct=0,
            details=pd.DataFrame(),
            description="Skipped: centerlines table not found",
        )

    query = f"""
    WITH cl_centroids AS (
        SELECT
            reach_id, region,
            AVG(x) as cl_x, AVG(y) as cl_y,
            COUNT(*) as cl_count
        FROM centerlines cl
        WHERE 1=1 {where_clause}
        GROUP BY reach_id, region
    )
    SELECT
        r.reach_id, r.region, r.river_name, r.x, r.y,
        cc.cl_x, cc.cl_y, cc.cl_count,
        111000.0 * SQRT(
            POWER((r.x - cc.cl_x) * COS(RADIANS((r.y + cc.cl_y) / 2.0)), 2)
            + POWER(r.y - cc.cl_y, 2)
        ) as dist_m
    FROM reaches r
    JOIN cl_centroids cc ON r.reach_id = cc.reach_id AND r.region = cc.region
    WHERE 111000.0 * SQRT(
            POWER((r.x - cc.cl_x) * COS(RADIANS((r.y + cc.cl_y) / 2.0)), 2)
            + POWER(r.y - cc.cl_y, 2)
        ) > {max_dist}
    ORDER BY dist_m DESC
    LIMIT 10000
    """

    issues = conn.execute(query).fetchdf()

    total_query = f"""
    SELECT COUNT(DISTINCT reach_id)
    FROM centerlines cl
    WHERE 1=1 {where_clause}
    """
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="C005",
        name="centerline_reach_distance",
        severity=Severity.WARNING,
        passed=len(issues) == 0,
        total_checked=total,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total if total > 0 else 0,
        details=issues,
        description=f"Centerlines with centroid >{max_dist / 1000:.0f}km from parent reach",
        threshold=max_dist,
    )


@register_check(
    "C006",
    Category.CLASSIFICATION,
    Severity.WARNING,
    "Centerline centroid closer to adjacent node than its assigned node",
)
def check_centerline_node_assignment(
    conn: duckdb.DuckDBPyConnection,
    region: Optional[str] = None,
    threshold: Optional[float] = None,
) -> CheckResult:
    """Flag nodes where the centroid of assigned centerlines is closer to an adjacent node.

    For each node, compute the centroid of its centerlines, then compare the
    equirectangular distance to the own node vs the previous/next node (by
    node_id within the same reach). If the centroid is closer to a neighbor,
    the centerline assignment may be wrong.
    """
    where_clause = f"AND cl.region = '{region}'" if region else ""
    node_where = f"AND n.region = '{region}'" if region else ""

    # Check if centerlines table exists
    try:
        conn.execute("SELECT cl_id FROM centerlines LIMIT 0")
    except (duckdb.CatalogException, duckdb.BinderException):
        return CheckResult(
            check_id="C006",
            name="centerline_node_assignment",
            severity=Severity.WARNING,
            passed=True,
            total_checked=0,
            issues_found=0,
            issue_pct=0,
            details=pd.DataFrame(),
            description="Skipped: centerlines table not found",
        )

    query = f"""
    WITH cl_centroids AS (
        SELECT
            node_id, reach_id, region,
            AVG(x) as cx, AVG(y) as cy,
            COUNT(*) as cl_count
        FROM centerlines cl
        WHERE 1=1 {where_clause}
        GROUP BY node_id, reach_id, region
    ),
    node_with_neighbors AS (
        SELECT
            n.node_id, n.reach_id, n.region, n.x as nx, n.y as ny,
            LAG(n.node_id) OVER w as prev_node_id,
            LAG(n.x) OVER w as prev_x,
            LAG(n.y) OVER w as prev_y,
            LEAD(n.node_id) OVER w as next_node_id,
            LEAD(n.x) OVER w as next_x,
            LEAD(n.y) OVER w as next_y
        FROM nodes n
        WHERE 1=1 {node_where}
        WINDOW w AS (PARTITION BY n.reach_id, n.region ORDER BY n.node_id)
    ),
    distances AS (
        SELECT
            cc.node_id, cc.reach_id, cc.region, cc.cl_count,
            cc.cx, cc.cy, nw.nx, nw.ny,
            111000.0 * SQRT(
                POWER((cc.cx - nw.nx) * COS(RADIANS((cc.cy + nw.ny) / 2.0)), 2)
                + POWER(cc.cy - nw.ny, 2)
            ) as dist_own,
            CASE WHEN nw.prev_x IS NOT NULL THEN
                111000.0 * SQRT(
                    POWER((cc.cx - nw.prev_x) * COS(RADIANS((cc.cy + nw.prev_y) / 2.0)), 2)
                    + POWER(cc.cy - nw.prev_y, 2)
                )
            END as dist_prev,
            CASE WHEN nw.next_x IS NOT NULL THEN
                111000.0 * SQRT(
                    POWER((cc.cx - nw.next_x) * COS(RADIANS((cc.cy + nw.next_y) / 2.0)), 2)
                    + POWER(cc.cy - nw.next_y, 2)
                )
            END as dist_next,
            nw.prev_node_id, nw.next_node_id
        FROM cl_centroids cc
        JOIN node_with_neighbors nw
            ON cc.node_id = nw.node_id
            AND cc.reach_id = nw.reach_id
            AND cc.region = nw.region
    )
    SELECT
        node_id, reach_id, region, cl_count,
        ROUND(dist_own, 1) as dist_own_m,
        CASE
            WHEN dist_prev IS NOT NULL AND dist_prev < dist_own
                AND (dist_next IS NULL OR dist_prev <= dist_next)
                THEN prev_node_id
            WHEN dist_next IS NOT NULL AND dist_next < dist_own
                THEN next_node_id
        END as closer_node_id,
        ROUND(LEAST(
            COALESCE(dist_prev, 1e9),
            COALESCE(dist_next, 1e9)
        ), 1) as dist_closer_m
    FROM distances
    WHERE (dist_prev IS NOT NULL AND dist_prev < dist_own)
       OR (dist_next IS NOT NULL AND dist_next < dist_own)
    ORDER BY (dist_own - LEAST(
        COALESCE(dist_prev, 1e9),
        COALESCE(dist_next, 1e9)
    )) DESC
    LIMIT 10000
    """

    issues = conn.execute(query).fetchdf()

    total_query = f"""
    SELECT COUNT(DISTINCT node_id)
    FROM centerlines cl
    WHERE 1=1 {where_clause}
    """
    total = conn.execute(total_query).fetchone()[0]

    return CheckResult(
        check_id="C006",
        name="centerline_node_assignment",
        severity=Severity.WARNING,
        passed=len(issues) == 0,
        total_checked=total,
        issues_found=len(issues),
        issue_pct=100 * len(issues) / total if total > 0 else 0,
        details=issues,
        description="Nodes where centerline centroid is closer to an adjacent node",
    )
