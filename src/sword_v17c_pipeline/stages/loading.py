"""Data loading stage for v17c pipeline."""

import duckdb
import pandas as pd

from ._logging import log


def load_topology(conn: duckdb.DuckDBPyConnection, region: str) -> pd.DataFrame:
    """Load reach_topology from DuckDB."""
    log(f"Loading topology for {region}...")
    df = conn.execute(
        """
        SELECT reach_id, direction, neighbor_rank, neighbor_reach_id
        FROM reach_topology
        WHERE region = ?
    """,
        [region.upper()],
    ).fetchdf()
    log(f"Loaded {len(df):,} topology rows")
    return df


def load_reaches(conn: duckdb.DuckDBPyConnection, region: str) -> pd.DataFrame:
    """Load reaches with attributes."""
    log(f"Loading reaches for {region}...")

    # Get available columns (handles older DBs without v17c columns)
    cols_result = conn.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name = 'reaches'"
    ).fetchall()
    available_cols = {row[0].lower() for row in cols_result}

    # Core columns (required)
    core_cols = [
        "reach_id",
        "region",
        "reach_length",
        "width",
        "slope",
        "facc",
        "n_rch_up",
        "n_rch_down",
        "dist_out",
        "path_freq",
        "stream_order",
        "lakeflag",
        "trib_flag",
    ]

    # Optional columns (v17c additions)
    optional_cols = [
        "wse",
        "wse_obs_p50",
        "width_obs_p50",
        "n_obs",
        "main_side",
        "type",
        "end_reach",
        "path_order",
        "path_segs",
    ]

    # Build column list
    select_cols = [c for c in core_cols if c.lower() in available_cols]
    select_cols += [c for c in optional_cols if c.lower() in available_cols]

    df = conn.execute(
        f"""
        SELECT {", ".join(select_cols)}
        FROM reaches
        WHERE region = ?
    """,
        [region.upper()],
    ).fetchdf()
    log(f"Loaded {len(df):,} reaches")
    return df


def recompute_facc_flow_corrected(db_path: str, v17b_path: str, region: str) -> int:
    """
    Recompute facc for reaches whose flow direction differs from v17b.

    Flow correction flipped topology direction for some reaches but did not
    update facc to match, causing F006 (junction conservation) and T003
    (monotonicity) violations.

    Uses topological propagation (Kahn's algorithm) in upstream-first order:
      new_facc = sum(upstream_facc) + max(0, old_facc - max(upstream_facc))

    Opens its own DuckDB connection — caller must not hold a write lock.

    Returns
    -------
    int
        Number of reaches whose facc changed by > 1 km².
    """
    import duckdb as _duckdb

    region_upper = region.upper()
    con = _duckdb.connect(db_path)
    con.execute("INSTALL spatial; LOAD spatial;")

    # Attach v17b to identify flow-corrected reaches
    safe_v17b = v17b_path.replace("'", "''")
    try:
        con.execute(f"ATTACH '{safe_v17b}' AS v17b_fc (READ_ONLY)")
    except Exception:
        pass  # already attached

    flipped_df = con.execute(f"""
        SELECT DISTINCT c.reach_id
        FROM reach_topology c
        JOIN v17b_fc.reach_topology b
            ON c.reach_id = b.reach_id
            AND c.region = b.region
            AND c.neighbor_reach_id = b.neighbor_reach_id
        WHERE c.direction != b.direction
          AND c.region = '{region_upper}'
    """).fetchdf()

    try:
        con.execute("DETACH v17b_fc")
    except Exception:
        pass

    if flipped_df.empty:
        log(f"  No flow-corrected reaches for {region}")
        con.close()
        return 0

    flipped_set = set(flipped_df["reach_id"].tolist())
    ids_sql = ",".join(str(r) for r in flipped_set)
    log(f"  {len(flipped_set)} flow-corrected reaches in {region}")

    reach_info = (
        con.execute(f"""
        SELECT reach_id, facc FROM reaches
        WHERE reach_id IN ({ids_sql}) AND region = '{region_upper}'
    """)
        .fetchdf()
        .set_index("reach_id")
    )

    topo = con.execute(f"""
        SELECT reach_id, neighbor_reach_id
        FROM reach_topology
        WHERE reach_id IN ({ids_sql}) AND region = '{region_upper}' AND direction = 'up'
    """).fetchdf()

    upstream: dict[int, list[int]] = {}
    for _, row in topo.iterrows():
        upstream.setdefault(int(row["reach_id"]), []).append(
            int(row["neighbor_reach_id"])
        )

    external_neighbors = {
        n for ns in upstream.values() for n in ns if n not in flipped_set
    }
    neighbor_facc: dict[int, float] = {}
    if external_neighbors:
        ext_sql = ",".join(str(r) for r in external_neighbors)
        neighbor_facc = (
            con.execute(f"""
            SELECT reach_id, facc FROM reaches WHERE reach_id IN ({ext_sql})
        """)
            .fetchdf()
            .set_index("reach_id")["facc"]
            .to_dict()
        )

    # Kahn's topological sort (upstream-first within flipped set)
    in_degree = {r: 0 for r in flipped_set}
    downstream_in_set: dict[int, list[int]] = {}
    for reach_id, up_neighbors in upstream.items():
        for n in up_neighbors:
            if n in flipped_set:
                in_degree[reach_id] += 1
                downstream_in_set.setdefault(n, []).append(reach_id)

    queue = [r for r, deg in in_degree.items() if deg == 0]
    order: list[int] = []
    remaining = dict(in_degree)
    while queue:
        r = queue.pop(0)
        order.append(r)
        for dn in downstream_in_set.get(r, []):
            remaining[dn] -= 1
            if remaining[dn] == 0:
                queue.append(dn)

    if len(order) != len(flipped_set):
        log(
            f"  WARNING: cycle in flow-corrected subgraph ({region}); falling back to facc sort"
        )
        order = sorted(
            flipped_set,
            key=lambda r: (
                float(reach_info.loc[r, "facc"]) if r in reach_info.index else 0.0
            ),
        )

    # Propagate
    corrected: dict[int, float] = {}
    for reach_id in order:
        if reach_id not in reach_info.index:
            continue
        old_facc = float(reach_info.loc[reach_id, "facc"])
        up_neighbors = upstream.get(reach_id, [])
        if not up_neighbors:
            corrected[reach_id] = old_facc
            continue
        up_faccs = [
            corrected.get(n, neighbor_facc.get(n))
            for n in up_neighbors
            if corrected.get(n, neighbor_facc.get(n)) is not None
        ]
        if not up_faccs:
            corrected[reach_id] = old_facc
            continue
        sum_up = sum(up_faccs)
        incremental = max(0.0, old_facc - max(up_faccs))
        corrected[reach_id] = sum_up + incremental

    changes = [
        (rid, nf)
        for rid, nf in corrected.items()
        if rid in reach_info.index
        and abs(nf - float(reach_info.loc[rid, "facc"])) > 1.0
    ]

    if not changes:
        log(f"  No significant facc changes needed in {region}")
        con.close()
        return 0

    log(f"  {len(changes)} facc values will change by > 1 km² in {region}")
    updates = pd.DataFrame(changes, columns=["reach_id", "new_facc"])

    rtree = con.execute(
        "SELECT index_name, table_name, sql FROM duckdb_indexes() WHERE sql LIKE '%RTREE%'"
    ).fetchall()
    for idx_name, _, _ in rtree:
        con.execute(f'DROP INDEX "{idx_name}"')
    try:
        con.register("facc_fc_updates", updates)
        con.execute(f"""
            UPDATE reaches
            SET facc = fu.new_facc
            FROM facc_fc_updates fu
            WHERE reaches.reach_id = fu.reach_id AND reaches.region = '{region_upper}'
        """)
    finally:
        for _, _, sql in rtree:
            con.execute(sql)

    con.close()
    log(f"  Updated {len(changes)} flow-corrected facc values in {region}")
    return len(changes)


def run_facc_corrections(db_path: str, v17b_path: str, region: str) -> int:
    """
    Detect and correct facc anomalies using the biphase denoise pipeline.

    This opens its own DuckDB connection internally, so the caller must
    close any existing write connection before calling this function.

    Parameters
    ----------
    db_path : str
        Path to sword_v17c.duckdb.
    v17b_path : str
        Path to sword_v17b.duckdb (read-only baseline).
    region : str
        Region code (e.g. 'NA').

    Returns
    -------
    int
        Number of corrections applied.
    """
    from sword_duckdb.facc_detection.correct_facc_denoise import correct_facc_denoise

    log(f"Running biphase facc denoise for {region}...")
    corrections_df = correct_facc_denoise(
        db_path=db_path,
        v17b_path=v17b_path,
        region=region,
        dry_run=False,
    )
    n = len(corrections_df)
    log(f"Facc corrections applied: {n:,}")
    return n
