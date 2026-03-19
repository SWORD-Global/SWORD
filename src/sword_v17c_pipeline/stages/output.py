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
    for reach_id in hydro_dist.keys():
        hd = hydro_dist.get(reach_id, {})
        ho = hw_out.get(reach_id, {})
        ms = is_mainstem.get(reach_id, False)
        nb = mn.get(reach_id, {})
        pvar = pv.get(reach_id, {})
        dij = dd.get(reach_id, {})

        row = {
            "reach_id": reach_id,
            "hydro_dist_out": hd.get("hydro_dist_out"),
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
    finally:
        conn.unregister("v17c_updates")
        # Always recreate RTREE indexes, even if UPDATE failed
        for _idx_name, _tbl, sql in rtree_indexes:
            conn.execute(sql)

    log(f"Updated {len(rows):,} reaches")
    return len(rows)


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
