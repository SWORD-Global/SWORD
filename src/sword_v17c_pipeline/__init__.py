"""
SWORD v17c Pipeline
===================

This module contains the simplified pipeline for generating SWORD v17c from v17b.

Pipeline Steps:
1. Load v17b topology from DuckDB
2. Build reach-level directed graph
3. Compute v17c attributes (hydro_dist_out, best_headwater, is_mainstem, etc.)
4. Optionally integrate SWOT-derived slopes
5. Write results to sword_v17c.duckdb with provenance

Note: The original MILP-based phi optimization files have been archived in _archived/.
The v17c pipeline now uses the original v17b topology instead.

New Columns Added by Pipeline:
- dist_out_dijkstra: Dijkstra shortest-path distance to any outlet (m)
- hydro_dist_out: Mainstem distance to best_outlet via rch_id_dn_main chain (m)
- best_headwater: Chosen headwater reach ID upstream
- best_outlet: Chosen outlet reach ID downstream
- pathlen_hw: Path length to headwater
- pathlen_out: Path length to outlet
- is_mainstem_edge: Boolean flag for mainstem edges
- swot_slope: SWOT-derived slope (m/km)
- swot_slope_se: Standard error of SWOT slope
- swot_slope_confidence: Confidence flag (R=reliable, U=unreliable)

Usage:
    # Process all regions
    python -m src.sword_v17c_pipeline.v17c_pipeline --db data/duckdb/sword_v17c.duckdb --all

    # Process single region
    python -m src.sword_v17c_pipeline.v17c_pipeline --db data/duckdb/sword_v17c.duckdb --region NA

    # Skip SWOT integration
    python -m src.sword_v17c_pipeline.v17c_pipeline --db data/duckdb/sword_v17c.duckdb --all --skip-swot

Or using shell script:
    cd src/sword_v17c_pipeline
    ./run_pipeline.sh  # All regions
    REGION=NA ./run_pipeline.sh  # Single region
"""

__version__ = "2.0.0"

from .v17c_pipeline import (
    REGIONS,
    RegionResult,
    apply_swot_slopes,
    build_reach_graph,
    build_section_graph,
    compute_best_headwater_outlet,
    compute_dijkstra_distances,
    compute_hydro_distances,  # noqa: F401 — alias for compute_dijkstra_distances
    compute_junction_slopes,
    compute_mainstem,
    compute_mainstem_distances,
    compute_main_neighbors,
    compute_path_variables,
    create_v17c_tables,
    get_effective_width,
    identify_junctions,
    load_reaches,
    load_topology,
    log,
    process_region,
    run_facc_corrections,
    run_pipeline,
    save_sections_to_duckdb,
    save_to_duckdb,
)
from .gates import GateFailure, GateResult, gate_source_data, gate_post_save

__all__ = [
    "GateFailure",
    "GateResult",
    "REGIONS",
    "RegionResult",
    "apply_swot_slopes",
    "build_reach_graph",
    "build_section_graph",
    "compute_best_headwater_outlet",
    "compute_dijkstra_distances",
    "compute_hydro_distances",
    "compute_junction_slopes",
    "compute_mainstem",
    "compute_mainstem_distances",
    "compute_main_neighbors",
    "compute_path_variables",
    "create_v17c_tables",
    "gate_post_save",
    "gate_source_data",
    "get_effective_width",
    "identify_junctions",
    "load_reaches",
    "load_topology",
    "log",
    "process_region",
    "run_facc_corrections",
    "run_pipeline",
    "save_sections_to_duckdb",
    "save_to_duckdb",
]
