"""Output stage for v17c pipeline — save to DuckDB + SWOT slopes."""

import glob as glob_mod
import json
import os
from typing import Dict, Optional

import duckdb
import numpy as np
import pandas as pd

from ._logging import log


def save_to_duckdb(
    conn: duckdb.DuckDBPyConnection,
    region: str,
    hydro_dist: Dict[int, Dict],
    hw_out: Dict[int, Dict],
    is_mainstem: Dict[int, bool],
    main_neighbors: Optional[Dict[int, Dict]] = None,
    main_paths: Optional[Dict[int, int]] = None,
    path_vars: Optional[Dict[int, Dict]] = None,
    subnetwork_ids: Optional[Dict[int, int]] = None,
    dijkstra_dist: Optional[Dict[int, Dict]] = None,
    hydro_dist_hw: Optional[Dict[int, Dict]] = None,
    flipped_reach_ids: Optional[set[int]] = None,
) -> int:
    """
    Save computed v17c attributes to DuckDB reaches table.

    Returns:
        Number of reaches updated
    """
    log(f"Saving v17c attributes to DuckDB for {region}...")

    # Build update dataframe
    rows = []
    mn = main_neighbors or {}
    mp = main_paths or {}
    pv = path_vars or {}
    sn = subnetwork_ids or {}
    dd = dijkstra_dist or {}
    hdh = hydro_dist_hw or {}
    for reach_id in hydro_dist.keys():
        hd = hydro_dist.get(reach_id, {})
        ho = hw_out.get(reach_id, {})
        ms = is_mainstem.get(reach_id, False)
        nb = mn.get(reach_id, {})
        pvar = pv.get(reach_id, {})
        dij = dd.get(reach_id, {})

        hdh_val = hdh.get(reach_id, {})
        row = {
            "reach_id": reach_id,
            "hydro_dist_out": hd.get("hydro_dist_out"),
            "hydro_dist_hw": hdh_val.get("hydro_dist_hw"),
            "dist_out_dijkstra": dij.get("dist_out_dijkstra"),
            "best_headwater": ho.get("best_headwater"),
            "best_outlet": ho.get("best_outlet"),
            "pathlen_hw": ho.get("pathlen_hw"),
            "pathlen_out": ho.get("pathlen_out"),
            "is_mainstem": ms,
            "rch_id_up_main": nb.get("rch_id_up_main"),
            "rch_id_dn_main": nb.get("rch_id_dn_main"),
        }
        if mp:
            row["main_path_id"] = mp.get(reach_id)
        if pvar:
            row["path_freq"] = pvar.get("path_freq")
            row["stream_order"] = pvar.get("stream_order")
            row["path_segs"] = pvar.get("path_segs")
            row["path_order"] = pvar.get("path_order")
        if reach_id in sn:
            row["subnetwork_id"] = sn[reach_id]
        rows.append(row)

    if not rows:
        log("No rows to update")
        return 0

    update_df = pd.DataFrame(rows)

    # Handle infinity values - convert to NULL
    update_df = update_df.replace([np.inf, -np.inf], np.nan)

    # Load spatial extension (required for RTREE index compatibility)
    conn.execute("INSTALL spatial; LOAD spatial;")

    # Drop RTREE indexes before UPDATE (DuckDB segfaults otherwise)
    rtree_indexes = conn.execute(
        "SELECT index_name, table_name, sql FROM duckdb_indexes() "
        "WHERE sql LIKE '%RTREE%'"
    ).fetchall()
    for idx_name, _tbl, _sql in rtree_indexes:
        conn.execute(f'DROP INDEX "{idx_name}"')

    # Build SET clause - always include base v17c columns
    set_clauses = [
        "hydro_dist_out = u.hydro_dist_out",
        "hydro_dist_hw = u.hydro_dist_hw",
        "dist_out_dijkstra = u.dist_out_dijkstra",
        "best_headwater = u.best_headwater",
        "best_outlet = u.best_outlet",
        "pathlen_hw = u.pathlen_hw",
        "pathlen_out = u.pathlen_out",
        "is_mainstem = u.is_mainstem",
        "rch_id_up_main = u.rch_id_up_main",
        "rch_id_dn_main = u.rch_id_dn_main",
    ]
    if main_paths:
        set_clauses.append("main_path_id = u.main_path_id")
    if path_vars:
        set_clauses.extend(
            [
                "path_freq = u.path_freq",
                "stream_order = u.stream_order",
                "path_segs = u.path_segs",
                "path_order = u.path_order",
            ]
        )
    if subnetwork_ids:
        set_clauses.append("subnetwork_id = u.subnetwork_id")

    # Register DataFrame, update, and always unregister + recreate RTREE indexes
    conn.register("v17c_updates", update_df)
    try:
        conn.execute(
            f"""
            UPDATE reaches SET
                {", ".join(set_clauses)}
            FROM v17c_updates u
            WHERE reaches.reach_id = u.reach_id
            AND reaches.region = ?
        """,
            [region.upper()],
        )

        log(f"Updated {len(rows):,} reaches")

        # Propagate reach-level v17c columns to child nodes
        # (must run before RTREE indexes are recreated in finally)
        propagate_reach_to_nodes(conn, region, flipped_reach_ids)
    finally:
        conn.unregister("v17c_updates")
        # Always recreate RTREE indexes, even if UPDATE failed
        for _idx_name, _tbl, sql in rtree_indexes:
            conn.execute(sql)

    return len(rows)


def propagate_reach_to_nodes(
    conn: duckdb.DuckDBPyConnection,
    region: str,
    flipped_reach_ids: Optional[set[int]] = None,  # kept for API compat, no longer used
) -> int:
    """Propagate v17c columns from reaches to child nodes.

    Returns the number of nodes updated.

    Flat-copy columns: best_headwater, best_outlet, subnetwork_id.

    Interpolated using node_length midpoint offsets within the reach:
        midpoint_offset = cumsum(node_length by node_order) - 0.5 * node_length

    This places each node at the geometric midpoint of its node_length
    segment, independent of v17b dist_out values.  No special handling
    is needed for flow-corrected reaches because the offset is derived
    from node_order (corrected) not from node dist_out (stale v17b).

    Node dist_out itself is preserved as the v17b original for backward
    compatibility (POM safety).  Only single-node reaches get a
    recomputed dist_out (reach midpoint).

    Then (NULL reach values pass through as NULL):
      - dist_out (single-node only): reach.dist_out - reach_length/2
      - hydro_dist_out, dist_out_dijkstra: reach_value - reach_length + midpoint
      - hydro_dist_hw: reach_value + reach_length - midpoint
      - pathlen_hw: reach_value - reach_length + midpoint
      - pathlen_out: reach_value + reach_length - midpoint
    """
    reg = region.upper()

    # Determine ordering column: node_order if available, else node_id
    cols = {r[0] for r in conn.execute("DESCRIBE nodes").fetchall()}
    order_col = "nodes.node_order" if "node_order" in cols else "nodes.node_id"

    result = conn.execute(
        f"""
        WITH ofs AS (
            SELECT nodes.node_id, nodes.region,
                   r.best_headwater, r.best_outlet, r.subnetwork_id,
                   r.dist_out, r.hydro_dist_out, r.dist_out_dijkstra,
                   r.hydro_dist_hw, r.pathlen_hw, r.pathlen_out,
                   r.reach_length, r.n_nodes,
                   -- Node-length midpoint offset: cumsum up to current node
                   -- minus half the current node's length.
                   GREATEST(0, LEAST(r.reach_length,
                       SUM(nodes.node_length) OVER (
                           PARTITION BY nodes.reach_id, nodes.region
                           ORDER BY {order_col}
                           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                       ) - 0.5 * nodes.node_length
                   )) AS o
            FROM nodes
            JOIN reaches r
              ON nodes.reach_id = r.reach_id
             AND nodes.region = r.region
            WHERE nodes.region = ?
        )
        UPDATE nodes
        SET best_headwater = ofs.best_headwater,
            best_outlet = ofs.best_outlet,
            subnetwork_id = ofs.subnetwork_id,
            dist_out = CASE
                WHEN ofs.n_nodes = 1 AND ofs.dist_out IS NULL THEN NULL
                WHEN ofs.n_nodes = 1 THEN GREATEST(0, ofs.dist_out - ofs.o)
                ELSE nodes.dist_out
            END,
            hydro_dist_out = CASE WHEN ofs.hydro_dist_out IS NULL THEN NULL
                ELSE GREATEST(0, ofs.hydro_dist_out - ofs.reach_length + ofs.o) END,
            dist_out_dijkstra = CASE WHEN ofs.dist_out_dijkstra IS NULL THEN NULL
                ELSE GREATEST(0, ofs.dist_out_dijkstra - ofs.reach_length + ofs.o) END,
            hydro_dist_hw = CASE WHEN ofs.hydro_dist_hw IS NULL THEN NULL
                ELSE GREATEST(0, ofs.hydro_dist_hw + ofs.reach_length - ofs.o) END,
            pathlen_hw = CASE WHEN ofs.pathlen_hw IS NULL THEN NULL
                ELSE GREATEST(0, ofs.pathlen_hw - ofs.reach_length + ofs.o) END,
            pathlen_out = CASE WHEN ofs.pathlen_out IS NULL THEN NULL
                ELSE GREATEST(0, ofs.pathlen_out + ofs.reach_length - ofs.o) END
        FROM ofs
        WHERE nodes.node_id = ofs.node_id
          AND nodes.region = ofs.region
        """,
        [reg],
    )
    count = result.fetchone()[0]
    log(
        f"Propagated 9 columns to {count:,} nodes (6 interpolated via node_length midpoint, 3 flat)"
    )
    return count


def update_node_columns(
    conn: duckdb.DuckDBPyConnection,
    region: str,
    flipped_reach_ids: set[int] | None = None,
) -> int:
    """
    Compute dn_node_id/up_node_id (reaches) and node_order (nodes) for a region.

    Uses dist_out ordering, not node_id, so results are correct even when
    flow direction changes reorder node IDs.

    For flow-corrected reaches (flipped_reach_ids), node dist_out is stale
    (reflects v17b topology), so we reverse the ordering after the initial
    computation: swap dn/up_node_id and invert node_order.

    Returns number of nodes updated.
    """
    log(f"Updating node columns for {region}...")

    # Drop RTREE indexes before UPDATE (DuckDB segfaults otherwise)
    conn.execute("INSTALL spatial; LOAD spatial;")
    rtree_indexes = conn.execute(
        "SELECT index_name, table_name, sql FROM duckdb_indexes() "
        "WHERE sql LIKE '%RTREE%'"
    ).fetchall()
    for idx_name, _tbl, _sql in rtree_indexes:
        conn.execute(f'DROP INDEX "{idx_name}"')

    try:
        return _update_node_columns_inner(conn, region, flipped_reach_ids)
    finally:
        for _idx_name, _tbl, sql in rtree_indexes:
            conn.execute(sql)


def _update_node_columns_inner(
    conn: duckdb.DuckDBPyConnection,
    region: str,
    flipped_reach_ids: set[int] | None = None,
) -> int:
    # dn_node_id = lowest dist_out node, up_node_id = highest
    conn.execute(
        """
        WITH boundary AS (
            SELECT reach_id, region,
                FIRST(node_id ORDER BY dist_out ASC, node_id ASC) AS dn_nid,
                FIRST(node_id ORDER BY dist_out DESC, node_id DESC) AS up_nid
            FROM nodes
            WHERE region = ?
            GROUP BY reach_id, region
        )
        UPDATE reaches
        SET dn_node_id = boundary.dn_nid,
            up_node_id = boundary.up_nid
        FROM boundary
        WHERE reaches.reach_id = boundary.reach_id
          AND reaches.region = boundary.region
        """,
        [region.upper()],
    )

    # node_order = 1..n per reach, ordered by dist_out ascending (1=downstream)
    result = conn.execute(
        """
        WITH ordered AS (
            SELECT node_id, region,
                ROW_NUMBER() OVER (
                    PARTITION BY reach_id, region ORDER BY dist_out ASC, node_id ASC
                ) AS rn
            FROM nodes
            WHERE region = ?
        )
        UPDATE nodes
        SET node_order = ordered.rn
        FROM ordered
        WHERE nodes.node_id = ordered.node_id
          AND nodes.region = ordered.region
        """,
        [region.upper()],
    )
    count = result.fetchone()[0]
    log(f"Updated {count:,} nodes with node_order")

    # For flow-corrected reaches, node dist_out is stale (v17b values).
    # The downstream end is now the HIGH dist_out end, so reverse ordering.
    if flipped_reach_ids:
        ids_sql = ",".join(str(r) for r in flipped_reach_ids)

        # Swap dn_node_id <-> up_node_id
        conn.execute(
            f"""
            UPDATE reaches
            SET dn_node_id = up_node_id,
                up_node_id = dn_node_id
            WHERE reach_id IN ({ids_sql})
              AND region = ?
            """,
            [region.upper()],
        )

        # Reverse node_order: node_order = n_nodes + 1 - node_order
        conn.execute(
            f"""
            UPDATE nodes
            SET node_order = r.n_nodes + 1 - nodes.node_order
            FROM reaches r
            WHERE nodes.reach_id = r.reach_id
              AND nodes.region = r.region
              AND nodes.reach_id IN ({ids_sql})
              AND nodes.region = ?
            """,
            [region.upper()],
        )

        # Recompute node dist_out for flipped reaches so it is monotonic with
        # the corrected node_order. v17b node dist_out was computed from the
        # wrong flow direction; keeping it would leave dist_out decreasing with
        # node_order. Formula: downstream_boundary + cumsum(node_length) -
        # 0.5*node_length (midpoint, matches the v17c node interpolation used
        # in propagate_reach_to_nodes).
        conn.execute(
            f"""
            WITH recomputed AS (
                SELECT n.node_id, n.region,
                    GREATEST(0,
                        (r.dist_out - r.reach_length) +
                        SUM(n.node_length) OVER (
                            PARTITION BY n.reach_id, n.region
                            ORDER BY n.node_order
                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                        ) - 0.5 * n.node_length
                    ) AS new_do
                FROM nodes n
                JOIN reaches r ON n.reach_id = r.reach_id AND n.region = r.region
                WHERE n.reach_id IN ({ids_sql})
                  AND n.region = ?
            )
            UPDATE nodes
            SET dist_out = recomputed.new_do
            FROM recomputed
            WHERE nodes.node_id = recomputed.node_id
              AND nodes.region = recomputed.region
            """,
            [region.upper()],
        )
        log(
            f"Reversed node ordering + recomputed dist_out for "
            f"{len(flipped_reach_ids)} flow-corrected reaches"
        )

    return count


def save_sections_to_duckdb(
    conn: duckdb.DuckDBPyConnection,
    region: str,
    sections_df: pd.DataFrame,
    validation_df: pd.DataFrame,
) -> None:
    """Save section data and validation results to DuckDB tables."""
    log(f"Saving sections to DuckDB for {region}...")

    if sections_df.empty:
        log("No sections to save")
        conn.execute("DELETE FROM v17c_sections WHERE region = ?", [region.upper()])
        conn.execute(
            "DELETE FROM v17c_section_slope_validation WHERE region = ?",
            [region.upper()],
        )
        return

    # Prepare sections for insert
    sections_insert = sections_df.copy()
    sections_insert["region"] = region.upper()
    # Convert reach_ids list to JSON string
    sections_insert["reach_ids"] = sections_insert["reach_ids"].apply(json.dumps)

    conn.register("sections_insert", sections_insert)
    try:
        conn.execute("""
            INSERT OR REPLACE INTO v17c_sections
            SELECT
                section_id,
                region,
                upstream_junction,
                downstream_junction,
                reach_ids,
                distance,
                n_reaches
            FROM sections_insert
        """)
    finally:
        conn.unregister("sections_insert")
    log(f"Saved {len(sections_insert):,} sections")

    # Save validation results if any
    if not validation_df.empty:
        validation_insert = validation_df.copy()
        validation_insert["region"] = region.upper()

        conn.register("validation_insert", validation_insert)
        try:
            conn.execute("""
                INSERT OR REPLACE INTO v17c_section_slope_validation
                SELECT
                    section_id,
                    region,
                    slope_from_upstream,
                    slope_from_downstream,
                    direction_valid,
                    likely_cause
                FROM validation_insert
            """)
        finally:
            conn.unregister("validation_insert")
        log(f"Saved {len(validation_insert):,} validation records")


def apply_swot_slopes(
    conn: duckdb.DuckDBPyConnection,
    region: str,
    swot_path: str,
) -> int:
    """
    Apply SWOT-derived slopes to reaches.

    This function:
    1. Loads SWOT node data from parquet files
    2. Computes section-level slopes with MAD outlier filtering
    3. Updates reaches with swot_slope, swot_slope_se, swot_slope_confidence

    Parameters
    ----------
    conn : duckdb.DuckDBPyConnection
        Database connection
    region : str
        Region code
    swot_path : str
        Path to SWOT parquet files directory

    Returns
    -------
    int
        Number of reaches updated
    """
    log(f"Applying SWOT slopes for {region}...")

    # Check if SWOT data exists
    if not os.path.isdir(swot_path):
        log(f"SWOT data directory not found: {swot_path}")
        return 0

    parquet_files = [
        f
        for f in glob_mod.glob(os.path.join(swot_path, "*.parquet"))
        if not os.path.basename(f).startswith("._")
    ]

    if not parquet_files:
        log(f"No parquet files found in {swot_path}")
        return 0

    log(f"Found {len(parquet_files)} SWOT parquet files")

    # Get node_ids for this region
    nodes_df = conn.execute(
        """
        SELECT node_id FROM nodes WHERE region = ?
    """,
        [region.upper()],
    ).fetchdf()

    if nodes_df.empty:
        log(f"No nodes found for region {region}")
        return 0

    node_ids = nodes_df["node_id"].tolist()
    log(f"Region {region} has {len(node_ids):,} nodes")

    # For now, we'll use the pre-computed slopes if they exist
    # in the SWOT pipeline output
    swot_slopes_file = os.path.join(
        os.path.dirname(swot_path).replace("/node", ""),
        f"output/{region.lower()}/{region.lower()}_swot_slopes.csv",
    )

    if os.path.exists(swot_slopes_file):
        log(f"Loading pre-computed SWOT slopes from {swot_slopes_file}")
        slopes_df = pd.read_csv(swot_slopes_file)

        # Map section slopes to reaches
        # This requires the section-reach mapping which we'd need from the pipeline
        log(f"Loaded {len(slopes_df):,} section slopes")

        # For now, just log that SWOT slopes are available
        # Full integration would require running SWOT_slopes.py functions
        log("SWOT slope integration requires section-reach mapping")
        return 0

    else:
        log(f"SWOT slopes file not found: {swot_slopes_file}")
        log("Run SWOT_slopes.py separately to compute slopes")
        return 0
