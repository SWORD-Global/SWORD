#!/usr/bin/env python3
"""
v17c Pipeline - Simplified SWORD v17c attribute computation.

This script:
1. Builds a reach-level directed graph from v17b topology
2. Computes v17c attributes (hydro_dist_out, best_headwater, is_mainstem, etc.)
3. Optionally applies SWOT-derived slopes with quality filtering
4. Writes results directly to sword_v17c.duckdb via SWORDWorkflow

NO MILP optimization - uses original v17b topology for flow direction.

Usage:
    # Process single region
    python -m src.sword_v17c_pipeline.v17c_pipeline --db data/duckdb/sword_v17c.duckdb --region NA

    # Process all regions
    python -m src.sword_v17c_pipeline.v17c_pipeline --db data/duckdb/sword_v17c.duckdb --all

    # Skip SWOT slope integration (faster)
    python -m src.sword_v17c_pipeline.v17c_pipeline --db data/duckdb/sword_v17c.duckdb --all --skip-swot
"""

import argparse
import os
import sys
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import duckdb
import networkx as nx
import numpy as np
import pandas as pd

from .gates import GateFailure, gate_post_save, gate_source_data

# ---------------------------------------------------------------------------
# Re-exports from stage modules (backwards compatibility for tests/callers)
# ---------------------------------------------------------------------------
from .stages.loading import (
    load_topology,
    load_reaches,
    run_facc_corrections,
)
from .stages.graph import (
    get_effective_width,
    build_reach_graph,
    identify_junctions,
    build_section_graph,
)
from .stages.path_variables import compute_path_variables
from .stages.distances import compute_hydro_distances, compute_best_headwater_outlet
from .stages.mainstem import compute_mainstem, compute_main_neighbors
from .stages.output import save_to_duckdb, save_sections_to_duckdb, apply_swot_slopes
from .stages._logging import log

__all__ = [
    "REGIONS",
    "RegionResult",
    "log",
    "get_effective_width",
    "load_topology",
    "load_reaches",
    "run_facc_corrections",
    "build_reach_graph",
    "identify_junctions",
    "build_section_graph",
    "compute_path_variables",
    "compute_hydro_distances",
    "compute_best_headwater_outlet",
    "compute_mainstem",
    "compute_main_neighbors",
    "save_to_duckdb",
    "save_sections_to_duckdb",
    "apply_swot_slopes",
    "compute_junction_slopes",
    "create_v17c_tables",
    "process_region",
    "run_pipeline",
]

# Regions in processing order (largest to smallest for progress feedback)
REGIONS = ["NA", "SA", "EU", "AF", "AS", "OC"]


@dataclass
class RegionResult:
    """Structured result from processing a single region."""

    region: str
    ok: bool
    reaches_processed: int
    reaches_updated: int
    failed_gate: Optional[str] = None
    failed_checks: List[str] = field(default_factory=list)
    stats: Dict = field(default_factory=dict)


# =============================================================================
# SWOT Slope Validation (Junction-Level) — stays in orchestrator
# =============================================================================


def compute_junction_slopes(
    G: nx.DiGraph, sections_df: pd.DataFrame, reaches_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Compute slopes at junction endpoints for each section.

    For each section, we compute:
    - slope_from_upstream: slope measured from upstream junction (should be NEGATIVE)
    - slope_from_downstream: slope measured from downstream junction (should be POSITIVE)
    """
    log("Computing junction slopes from SWOT data...")

    # Create reach -> WSE mapping
    wse_map = {}
    for _, row in reaches_df.iterrows():
        rid = int(row["reach_id"])
        wse = row.get("wse_obs_mean")
        if pd.notna(wse):
            wse_map[rid] = wse

    results = []

    for _, section in sections_df.iterrows():
        section_id = section["section_id"]
        upstream_j = section["upstream_junction"]
        downstream_j = section["downstream_junction"]
        reach_ids = section["reach_ids"]
        total_distance = section["distance"]

        if len(reach_ids) < 2:
            continue

        # Get WSE values for reaches in this section
        wse_data = []
        cumulative_dist = 0

        for rid in reach_ids:
            wse = wse_map.get(rid)
            reach_len = G.nodes[rid].get("reach_length", 0) if rid in G.nodes else 0

            if wse is not None:
                wse_data.append(
                    {
                        "reach_id": rid,
                        "wse": wse,
                        "dist_from_upstream": cumulative_dist,
                        "dist_from_downstream": total_distance - cumulative_dist,
                    }
                )
            cumulative_dist += reach_len

        if len(wse_data) < 2:
            continue

        wse_df = pd.DataFrame(wse_data)

        # Compute slope from upstream junction
        try:
            slope_upstream = np.polyfit(wse_df["dist_from_upstream"], wse_df["wse"], 1)[
                0
            ]
        except Exception:
            slope_upstream = np.nan

        # Compute slope from downstream junction
        try:
            slope_downstream = np.polyfit(
                wse_df["dist_from_downstream"], wse_df["wse"], 1
            )[0]
        except Exception:
            slope_downstream = np.nan

        # Determine if slopes match expected signs
        upstream_correct = slope_upstream < 0 if pd.notna(slope_upstream) else None
        downstream_correct = (
            slope_downstream > 0 if pd.notna(slope_downstream) else None
        )

        direction_valid = (
            upstream_correct and downstream_correct
            if (upstream_correct is not None and downstream_correct is not None)
            else None
        )

        # Determine likely cause if invalid
        likely_cause = None
        if direction_valid is False:
            lakeflag = (
                G.nodes[upstream_j].get("lakeflag", 0) if upstream_j in G.nodes else 0
            )
            if lakeflag > 0:
                likely_cause = "lake_section"
            elif pd.notna(slope_upstream) and abs(slope_upstream) > 0.05:
                likely_cause = "extreme_slope_data_error"
            elif pd.notna(slope_downstream) and abs(slope_downstream) > 0.05:
                likely_cause = "extreme_slope_data_error"
            else:
                likely_cause = "potential_topology_error"

        results.append(
            {
                "section_id": section_id,
                "upstream_junction": upstream_j,
                "downstream_junction": downstream_j,
                "n_reaches": len(reach_ids),
                "n_reaches_with_wse": len(wse_df),
                "distance": total_distance,
                "slope_from_upstream": slope_upstream,
                "slope_from_downstream": slope_downstream,
                "direction_valid": direction_valid,
                "likely_cause": likely_cause,
            }
        )

    junction_slopes_df = pd.DataFrame(results)

    # Summary
    if not junction_slopes_df.empty:
        n_total = len(junction_slopes_df)
        n_valid = junction_slopes_df["direction_valid"].sum()
        n_invalid = (junction_slopes_df["direction_valid"] == False).sum()  # noqa: E712 — pandas bool comparison

        log("Junction slope validation:")
        log(f"  Total sections with SWOT data: {n_total:,}")
        log(f"  Direction valid: {n_valid:,} ({100 * n_valid / n_total:.1f}%)")
        log(f"  Direction INVALID: {n_invalid:,} ({100 * n_invalid / n_total:.1f}%)")

    return junction_slopes_df


# =============================================================================
# DuckDB Output
# =============================================================================


def process_region(
    db_path: str,
    region: str,
    user_id: str = "v17c_pipeline",
    skip_swot: bool = False,
    swot_path: Optional[str] = None,
    skip_facc: bool = False,
    v17b_path: str = "data/duckdb/sword_v17b.duckdb",
    skip_path_vars: bool = False,
    skip_flow_correction: bool = True,
    skip_gates: bool = False,
) -> RegionResult:
    """
    Process a single region through the v17c pipeline.

    Parameters
    ----------
    db_path : str
        Path to sword_v17c.duckdb
    region : str
        Region code (NA, SA, EU, AF, AS, OC)
    user_id : str
        User ID for provenance
    skip_swot : bool
        Skip SWOT slope integration
    swot_path : str, optional
        Path to SWOT data directory
    skip_facc : bool
        Skip facc anomaly correction (default: run corrections)
    v17b_path : str
        Path to sword_v17b.duckdb (read-only baseline for facc denoise)
    skip_flow_correction : bool
        Skip flow direction correction (default: True — disabled until
        scoring reliability is improved, see issue #70)
    skip_gates : bool
        Skip lint validation gates (default: False)

    Returns
    -------
    RegionResult
        Structured result with ok, stats, and gate failure info
    """
    # Import SWORDWorkflow for provenance
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from sword_duckdb import SWORDWorkflow
    from sword_duckdb.schema import normalize_region

    region = normalize_region(region)
    log(f"\n{'=' * 60}")
    log(f"Processing region: {region}")
    log(f"{'=' * 60}")

    # Run facc corrections BEFORE opening the long-lived workflow.
    # correct_facc_denoise opens its own DuckDB write connection, so we
    # must not hold one simultaneously.
    n_facc_corrections = 0
    if not skip_facc:
        n_facc_corrections = run_facc_corrections(db_path, v17b_path, region)

    # Initialize workflow (after facc, so no write-lock conflict)
    workflow = SWORDWorkflow(user_id=user_id)
    sword = workflow.load(db_path, region, reason=f"v17c pipeline for {region}")
    conn = sword.db.conn

    try:
        return _process_region_inner(
            workflow=workflow,
            conn=conn,
            db_path=db_path,
            region=region,
            n_facc_corrections=n_facc_corrections,
            skip_swot=skip_swot,
            swot_path=swot_path,
            skip_path_vars=skip_path_vars,
            skip_flow_correction=skip_flow_correction,
            skip_gates=skip_gates,
        )
    finally:
        workflow.close()


def _process_region_inner(
    *,
    workflow,
    conn: duckdb.DuckDBPyConnection,
    db_path: str,
    region: str,
    n_facc_corrections: int,
    skip_swot: bool,
    swot_path: Optional[str],
    skip_path_vars: bool,
    skip_flow_correction: bool,
    skip_gates: bool,
) -> RegionResult:
    """Inner implementation of process_region (called inside try/finally)."""
    from sword_duckdb.schema import add_v17c_columns

    # Ensure v17c columns exist
    add_v17c_columns(conn)

    # Create v17c tables if they don't exist
    create_v17c_tables(conn)

    # Load data from database
    topology_df = load_topology(conn, region)
    reaches_df = load_reaches(conn, region)

    # Gate: validate source data before graph build
    if not skip_gates:
        conn.execute("CHECKPOINT")
        try:
            gate_source_data(db_path, region, conn=conn)
        except GateFailure as e:
            return RegionResult(
                region=region,
                ok=False,
                reaches_processed=len(reaches_df),
                reaches_updated=0,
                failed_gate=e.label,
                failed_checks=e.failed_checks,
            )

    # Build reach-level graph (uses corrected facc from DB)
    G = build_reach_graph(topology_df, reaches_df)

    # Validate DAG
    if not nx.is_directed_acyclic_graph(G):
        log("WARNING: Graph contains cycles!")
        # Find cycles for debugging
        try:
            cycle = nx.find_cycle(G)
            log(f"Example cycle: {cycle[:5]}...")
        except nx.NetworkXNoCycle:
            pass

    # Identify junctions and build section graph
    junctions = identify_junctions(G)
    R, sections_df = build_section_graph(G, junctions)

    # Compute path variables (path_freq, stream_order, path_segs, path_order)
    path_vars = None
    if not skip_path_vars:
        path_vars = compute_path_variables(G, sections_df)

    # Compute junction-level validation (uses WSE data)
    has_wse = (
        reaches_df["wse_obs_mean"].notna().any()
        if "wse_obs_mean" in reaches_df.columns
        else False
    )
    validation_df = pd.DataFrame()

    if has_wse:
        validation_df = compute_junction_slopes(G, sections_df, reaches_df)

    # Flow direction correction (auto-flip high-confidence wrong-direction sections)
    flow_correction_stats = {}
    if has_wse and not skip_flow_correction and not validation_df.empty:
        from .flow_direction import correct_flow_directions

        def _rebuild(c, r):
            tdf = load_topology(c, r)
            rdf = load_reaches(c, r)
            g = build_reach_graph(tdf, rdf)
            j = identify_junctions(g)
            _, sdf = build_section_graph(g, j)
            vdf = compute_junction_slopes(g, sdf, rdf)
            return g, sdf, vdf

        flow_correction_stats = correct_flow_directions(
            conn,
            region,
            G,
            sections_df,
            validation_df,
            reaches_df,
            rebuild_fn=_rebuild,
        )

        # If any flips happened, rebuild graph/sections for downstream computations
        if flow_correction_stats.get("n_flipped", 0) > 0:
            topology_df = load_topology(conn, region)
            G = build_reach_graph(topology_df, reaches_df)
            junctions = identify_junctions(G)
            R, sections_df = build_section_graph(G, junctions)
            validation_df = compute_junction_slopes(G, sections_df, reaches_df)

    # Load existing best_headwater/best_outlet for change tracking
    old_hw_out = {}
    try:
        old_df = conn.execute(
            """
            SELECT reach_id, best_headwater, best_outlet
            FROM reaches
            WHERE region = ? AND best_headwater IS NOT NULL
        """,
            [region.upper()],
        ).fetchdf()
        for _, row in old_df.iterrows():
            old_hw_out[int(row["reach_id"])] = {
                "best_headwater": row["best_headwater"],
                "best_outlet": row["best_outlet"],
            }
        log(f"Loaded {len(old_hw_out):,} existing best_headwater/outlet assignments")
    except Exception:
        log("No existing best_headwater/outlet assignments (first run)")

    # Compute new attributes
    hydro_dist = compute_hydro_distances(G)
    hw_out = compute_best_headwater_outlet(G)
    is_mainstem = compute_mainstem(G, hw_out)
    main_neighbors = compute_main_neighbors(G)

    # Log changes in best_headwater/best_outlet assignments
    if old_hw_out:
        n_hw_changed = 0
        n_out_changed = 0
        for rid, new_vals in hw_out.items():
            if rid in old_hw_out:
                old_vals = old_hw_out[rid]
                if new_vals["best_headwater"] != old_vals["best_headwater"]:
                    n_hw_changed += 1
                if new_vals["best_outlet"] != old_vals["best_outlet"]:
                    n_out_changed += 1
        log(
            f"Routing changes: {n_hw_changed:,} best_headwater, {n_out_changed:,} best_outlet"
        )

    # Save to DuckDB with provenance
    with workflow.transaction(f"v17c attributes for {region}"):
        n_updated = save_to_duckdb(
            conn,
            region,
            hydro_dist,
            hw_out,
            is_mainstem,
            main_neighbors,
            path_vars=path_vars,
        )
        save_sections_to_duckdb(conn, region, sections_df, validation_df)

    # Apply SWOT slopes if requested
    n_swot_updated = 0
    if not skip_swot and swot_path:
        with workflow.transaction(f"SWOT slopes for {region}"):
            n_swot_updated = apply_swot_slopes(conn, region, swot_path)

    # Gate: validate post-save output integrity
    if not skip_gates:
        conn.execute("CHECKPOINT")
        try:
            gate_post_save(db_path, region, conn=conn)
        except GateFailure as e:
            return RegionResult(
                region=region,
                ok=False,
                reaches_processed=len(reaches_df),
                reaches_updated=n_updated,
                failed_gate=e.label,
                failed_checks=e.failed_checks,
                stats={
                    "facc_corrections": n_facc_corrections,
                    "sections": len(sections_df),
                },
            )

    # Summary statistics
    stats = {
        "facc_corrections": n_facc_corrections,
        "sections": len(sections_df),
        "junctions": len(junctions),
        "mainstem_reaches": sum(is_mainstem.values()),
        "swot_updated": n_swot_updated,
    }
    if path_vars:
        n_valid_pf = sum(1 for v in path_vars.values() if v.get("path_freq", -9999) > 0)
        stats["path_freq_valid"] = n_valid_pf
        stats["path_freq_excluded"] = len(path_vars) - n_valid_pf

    if not validation_df.empty:
        stats["validation_valid"] = int(validation_df["direction_valid"].sum())
        stats["validation_invalid"] = int(
            (validation_df["direction_valid"] == False).sum()  # noqa: E712 — pandas bool comparison
        )

    if flow_correction_stats:
        stats["flow_flipped"] = flow_correction_stats.get("n_flipped", 0)
        stats["flow_manual_review"] = flow_correction_stats.get("n_manual_review", 0)

    log(f"\nRegion {region} complete: {n_updated:,} reaches updated")
    return RegionResult(
        region=region,
        ok=True,
        reaches_processed=len(reaches_df),
        reaches_updated=n_updated,
        stats=stats,
    )


def create_v17c_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """Create v17c-specific tables if they don't exist."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS v17c_sections (
            section_id INTEGER,
            region VARCHAR(2),
            upstream_junction BIGINT,
            downstream_junction BIGINT,
            reach_ids VARCHAR,
            distance DOUBLE,
            n_reaches INTEGER,
            PRIMARY KEY (section_id, region)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS v17c_section_slope_validation (
            section_id INTEGER,
            region VARCHAR(2),
            slope_from_upstream DOUBLE,
            slope_from_downstream DOUBLE,
            direction_valid BOOLEAN,
            likely_cause VARCHAR,
            PRIMARY KEY (section_id, region)
        )
    """)

    from .flow_direction import create_flow_corrections_table

    create_flow_corrections_table(conn)


def run_pipeline(
    db_path: str,
    regions: List[str],
    user_id: str = "v17c_pipeline",
    skip_swot: bool = False,
    swot_path: Optional[str] = None,
    skip_facc: bool = False,
    v17b_path: str = "data/duckdb/sword_v17b.duckdb",
    skip_path_vars: bool = False,
    skip_flow_correction: bool = True,
    skip_gates: bool = False,
) -> List[RegionResult]:
    """
    Run the v17c pipeline for multiple regions.

    Parameters
    ----------
    db_path : str
        Path to sword_v17c.duckdb
    regions : list
        List of region codes to process
    user_id : str
        User ID for provenance tracking
    skip_swot : bool
        Skip SWOT slope integration
    swot_path : str, optional
        Path to SWOT data directory
    skip_facc : bool
        Skip facc anomaly correction
    v17b_path : str
        Path to sword_v17b.duckdb (read-only baseline for facc denoise)
    skip_gates : bool
        Skip lint validation gates (default: False)

    Returns
    -------
    list[RegionResult]
        Structured results per region
    """
    log(f"v17c Pipeline - Processing {len(regions)} regions")
    log(f"Database: {db_path}")
    log(f"Skip SWOT: {skip_swot}")
    log(f"Skip FACC: {skip_facc}")
    log(f"Skip path vars: {skip_path_vars}")
    log(f"Skip flow correction: {skip_flow_correction}")
    log(f"Skip gates: {skip_gates}")
    if swot_path:
        log(f"SWOT path: {swot_path}")

    all_results: List[RegionResult] = []

    for region in regions:
        try:
            result = process_region(
                db_path=db_path,
                region=region,
                user_id=user_id,
                skip_swot=skip_swot,
                swot_path=swot_path,
                skip_facc=skip_facc,
                v17b_path=v17b_path,
                skip_path_vars=skip_path_vars,
                skip_flow_correction=skip_flow_correction,
                skip_gates=skip_gates,
            )
            all_results.append(result)
        except Exception as e:
            log(f"ERROR processing {region}: {e}")
            import traceback

            traceback.print_exc()
            all_results.append(
                RegionResult(
                    region=region,
                    ok=False,
                    reaches_processed=0,
                    reaches_updated=0,
                    stats={"error": str(e)},
                )
            )

    # Print summary
    log("\n" + "=" * 60)
    log("PIPELINE SUMMARY")
    log("=" * 60)

    total_updated = 0
    for r in all_results:
        if not r.ok:
            err = r.stats.get("error", r.failed_gate or "unknown")
            log(f"{r.region}: FAILED - {err}")
        else:
            s = r.stats
            facc_str = (
                f", {s['facc_corrections']:,} facc fixes"
                if s.get("facc_corrections")
                else ""
            )
            pf_str = (
                f", {s['path_freq_valid']:,} valid pf"
                if s.get("path_freq_valid")
                else ""
            )
            log(
                f"{r.region}: {r.reaches_updated:,} reaches, "
                f"{s['sections']:,} sections, "
                f"{s['mainstem_reaches']:,} mainstem{facc_str}{pf_str}"
            )
            total_updated += r.reaches_updated

    log(f"\nTotal reaches updated: {total_updated:,}")

    return all_results


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="v17c Pipeline - Compute and save v17c attributes to DuckDB"
    )
    parser.add_argument("--db", required=True, help="Path to sword_v17c.duckdb")
    parser.add_argument(
        "--region", help="Single region to process (NA, SA, EU, AF, AS, OC)"
    )
    parser.add_argument("--all", action="store_true", help="Process all regions")
    parser.add_argument(
        "--skip-swot", action="store_true", help="Skip SWOT slope integration"
    )
    parser.add_argument(
        "--swot-path",
        default="/Volumes/SWORD_DATA/data/swot/RiverSP_D_parq/node",
        help="Path to SWOT parquet files",
    )
    parser.add_argument(
        "--user-id", default="v17c_pipeline", help="User ID for provenance tracking"
    )
    parser.add_argument(
        "--skip-facc",
        action="store_true",
        help="Skip facc anomaly correction (default: run corrections before v17c computation)",
    )
    parser.add_argument(
        "--v17b",
        default="data/duckdb/sword_v17b.duckdb",
        help="Path to sword_v17b.duckdb (read-only baseline for facc denoise)",
    )
    parser.add_argument(
        "--skip-path-vars",
        action="store_true",
        help="Skip path variable recomputation (path_freq, stream_order, path_segs, path_order)",
    )
    parser.add_argument(
        "--skip-flow-correction",
        action="store_true",
        help="Skip flow direction correction",
    )
    parser.add_argument(
        "--enable-flow-correction",
        action="store_true",
        default=True,
        help="Enable flow direction correction (Default: True)",
    )
    parser.add_argument(
        "--rollback-flow-corrections",
        metavar="RUN_ID",
        help="Rollback flow corrections for the given run_id, then exit",
    )
    parser.add_argument(
        "--skip-gates",
        action="store_true",
        help="Skip lint validation gates between pipeline stages",
    )

    args = parser.parse_args()

    # Determine regions to process
    if args.all:
        regions = REGIONS
    elif args.region:
        regions = [args.region.upper()]
    else:
        parser.error("Either --region or --all must be specified")

    # Validate database exists
    if not os.path.exists(args.db):
        print(f"ERROR: Database not found: {args.db}")
        sys.exit(1)

    # Handle rollback mode
    if args.rollback_flow_corrections:
        from .flow_direction import rollback_flow_corrections

        conn = duckdb.connect(args.db)
        for region in regions:
            rollback_flow_corrections(conn, region, args.rollback_flow_corrections)
        conn.close()
        sys.exit(0)

    # Run pipeline
    stats = run_pipeline(
        db_path=args.db,
        regions=regions,
        user_id=args.user_id,
        skip_swot=args.skip_swot,
        swot_path=args.swot_path,
        skip_facc=args.skip_facc,
        v17b_path=args.v17b,
        skip_path_vars=args.skip_path_vars,
        skip_flow_correction=not args.enable_flow_correction,
        skip_gates=args.skip_gates,
    )

    # Exit with error if any region failed
    if any(not r.ok for r in stats):
        sys.exit(1)


if __name__ == "__main__":
    main()
