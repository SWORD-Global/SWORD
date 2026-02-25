# -*- coding: utf-8 -*-
"""
SWORD Reconstruction Engine
===========================

This module provides the capability to reconstruct SWORD attributes from their
original source datasets. It enables:

1. **Attribute Reconstruction**: Recalculate derived attributes (wse, facc, slope, etc.)
   from source data (MERIT Hydro, GRWL, etc.)

2. **Source Lineage**: Track which source datasets contributed to each attribute

3. **Recipe System**: Store and replay complex multi-step reconstruction pipelines

Key Source Datasets:
    - GRWL: River centerlines and width measurements (Allen & Pavelsky, 2018)
    - MERIT Hydro: Elevation (wse) and flow accumulation (facc) (Yamazaki et al., 2019)
    - HydroBASINS: Basin boundaries and drainage areas (Lehner & Grill, 2013)
    - GRanD: Dam locations and reservoir data (Lehner et al., 2011)
    - GROD: Global River Obstruction Database (Yang et al., 2021)

Example Usage:
    from sword_duckdb import SWORDWorkflow
    from sword_duckdb.reconstruction import ReconstructionEngine

    workflow = SWORDWorkflow()
    sword = workflow.load('sword_v17b.duckdb', 'NA')

    # Initialize reconstruction engine
    engine = ReconstructionEngine(
        sword,
        provenance=workflow.provenance,
    )

    # Reconstruct dist_out from topology
    engine.reconstruct('reach.dist_out', reach_ids=[123, 456])

    # Reconstruct wse from MERIT Hydro
    engine.reconstruct('reach.wse', reach_ids=[123, 456])

    # Validate reconstruction
    report = engine.validate('reach.wse', tolerance=0.01)
"""

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Union

import numpy as np

if TYPE_CHECKING:
    from .sword_class import SWORD
    from .provenance import ProvenanceLogger

logger = logging.getLogger(__name__)


class SourceDataset(Enum):
    """Known source datasets for SWORD reconstruction."""

    GRWL = "GRWL"  # Global River Widths from Landsat
    MERIT_HYDRO = "MERIT_HYDRO"  # Multi-Error-Removed Improved-Terrain Hydro
    HYDROBASINS = "HYDROBASINS"  # HydroBASINS watersheds
    HYDROLAKES = "HYDROLAKES"  # HydroLAKES lake extents
    HYDROFALLS = "HYDROFALLS"  # HydroFALLS waterfall database
    GRAND = "GRAND"  # Global Reservoir and Dam database
    GROD = "GROD"  # Global River Obstruction Database
    SWOT_TRACKS = "SWOT_TRACKS"  # SWOT satellite orbit tracks
    ICE_FLAGS = "ICE_FLAGS"  # Ice flag dataset (external CSV)
    RIVER_NAMES = "RIVER_NAMES"  # River names database
    MAX_WIDTH_RASTER = "MAX_WIDTH_RASTER"  # Max width raster (post-processing)
    COMPUTED = "COMPUTED"  # Computed from other SWORD attributes
    MANUAL = "MANUAL"  # Manual edits
    INHERITED = "INHERITED"  # Inherited from parent entity (reach → node)


class DerivationMethod(Enum):
    """Methods used to derive attributes from source data."""

    DIRECT = "direct"  # Direct value from source
    INTERPOLATED = "interpolated"  # Interpolated between source points
    AGGREGATED = "aggregated"  # Aggregated from multiple source values
    MEAN = "mean"  # Mean of source values
    MEDIAN = "median"  # Median of source values
    MODE = "mode"  # Most frequent value (for categorical)
    MAX = "max"  # Maximum of source values
    MIN = "min"  # Minimum of source values
    VARIANCE = "variance"  # Variance of source values
    COUNT = "count"  # Count of entities
    SUM = "sum"  # Sum of values
    LINEAR_REGRESSION = "linear_regression"  # Linear regression fit
    LOG_TRANSFORM = "log_transform"  # Logarithmic transformation
    SPATIAL_JOIN = "spatial_join"  # Spatial intersection/join
    SPATIAL_PROXIMITY = "spatial_proximity"  # cKDTree-based proximity search
    GRAPH_TRAVERSAL = "graph_traversal"  # Network graph traversal
    PATH_ACCUMULATION = "path_accumulation"  # Cumulative along paths
    COMPUTED = "computed"  # Computed from other attributes
    INHERITED = "inherited"  # Inherited from parent entity


@dataclass
class AttributeSpec:
    """Specification for how an attribute is derived."""

    name: str  # Full attribute name (e.g., "reach.wse")
    source: SourceDataset  # Primary source dataset
    method: DerivationMethod  # How it's derived
    source_columns: List[str]  # Columns in source needed
    dependencies: List[str]  # Other SWORD attributes needed
    description: str  # Human-readable description

    @property
    def entity_type(self) -> str:
        """Extract entity type from name (reach, node, centerline)."""
        return self.name.split(".")[0]

    @property
    def attribute_name(self) -> str:
        """Extract attribute name without entity prefix."""
        return self.name.split(".")[1]


# =============================================================================
# ATTRIBUTE SOURCE MAPPINGS
# =============================================================================
# This defines how EVERY SWORD attribute was originally constructed.
# Based on analysis of: Reach_Definition_Tools_v11.py, Merge_Tools_v06.py,
# Attach_Fill_Variables.py, Add_Trib_Flag.py, path_variables_nc.py, stream_order.py

ATTRIBUTE_SOURCES: Dict[str, AttributeSpec] = {
    # =========================================================================
    # REACH ATTRIBUTES
    # =========================================================================
    # --- Basic Geometry ---
    "reach.x": AttributeSpec(
        name="reach.x",
        source=SourceDataset.GRWL,
        method=DerivationMethod.MEAN,
        source_columns=["lon"],
        dependencies=["centerline.x"],
        description="Reach centroid longitude: np.mean(lon[reach_centerlines])",
    ),
    "reach.y": AttributeSpec(
        name="reach.y",
        source=SourceDataset.GRWL,
        method=DerivationMethod.MEAN,
        source_columns=["lat"],
        dependencies=["centerline.y"],
        description="Reach centroid latitude: np.mean(lat[reach_centerlines])",
    ),
    "reach.x_min": AttributeSpec(
        name="reach.x_min",
        source=SourceDataset.GRWL,
        method=DerivationMethod.MIN,
        source_columns=["lon"],
        dependencies=["centerline.x"],
        description="Reach bounding box minimum longitude",
    ),
    "reach.x_max": AttributeSpec(
        name="reach.x_max",
        source=SourceDataset.GRWL,
        method=DerivationMethod.MAX,
        source_columns=["lon"],
        dependencies=["centerline.x"],
        description="Reach bounding box maximum longitude",
    ),
    "reach.y_min": AttributeSpec(
        name="reach.y_min",
        source=SourceDataset.GRWL,
        method=DerivationMethod.MIN,
        source_columns=["lat"],
        dependencies=["centerline.y"],
        description="Reach bounding box minimum latitude",
    ),
    "reach.y_max": AttributeSpec(
        name="reach.y_max",
        source=SourceDataset.GRWL,
        method=DerivationMethod.MAX,
        source_columns=["lat"],
        dependencies=["centerline.y"],
        description="Reach bounding box maximum latitude",
    ),
    "reach.reach_length": AttributeSpec(
        name="reach.reach_length",
        source=SourceDataset.COMPUTED,
        method=DerivationMethod.SUM,
        source_columns=[],
        dependencies=["centerline.x", "centerline.y"],
        description="Reach length: sum of Euclidean distances between consecutive centerline points",
    ),
    "reach.n_nodes": AttributeSpec(
        name="reach.n_nodes",
        source=SourceDataset.COMPUTED,
        method=DerivationMethod.COUNT,
        source_columns=[],
        dependencies=["node.reach_id"],
        description="Number of nodes in reach: len(unique(node_id[reach]))",
    ),
    # --- Water Surface Elevation ---
    "reach.wse": AttributeSpec(
        name="reach.wse",
        source=SourceDataset.MERIT_HYDRO,
        method=DerivationMethod.MEDIAN,
        source_columns=["elevation"],
        dependencies=["centerline.x", "centerline.y"],
        description="Water surface elevation: np.median(elevation[reach_centerlines])",
    ),
    "reach.wse_var": AttributeSpec(
        name="reach.wse_var",
        source=SourceDataset.MERIT_HYDRO,
        method=DerivationMethod.VARIANCE,
        source_columns=["elevation"],
        dependencies=["centerline.x", "centerline.y"],
        description="WSE variance: np.var(elevation[reach_centerlines])",
    ),
    "reach.slope": AttributeSpec(
        name="reach.slope",
        source=SourceDataset.COMPUTED,
        method=DerivationMethod.LINEAR_REGRESSION,
        source_columns=[],
        dependencies=["node.wse", "node.dist_out"],
        description="Water surface slope: np.linalg.lstsq(dist/1000, elevation) in m/km",
    ),
    # --- Width ---
    "reach.width": AttributeSpec(
        name="reach.width",
        source=SourceDataset.GRWL,
        method=DerivationMethod.MEDIAN,
        source_columns=["width"],
        dependencies=["centerline.width"],
        description="River width: np.median(width[reach_centerlines])",
    ),
    "reach.width_var": AttributeSpec(
        name="reach.width_var",
        source=SourceDataset.GRWL,
        method=DerivationMethod.VARIANCE,
        source_columns=["width"],
        dependencies=["centerline.width"],
        description="Width variance: np.var(width[reach_centerlines])",
    ),
    "reach.max_width": AttributeSpec(
        name="reach.max_width",
        source=SourceDataset.MAX_WIDTH_RASTER,
        method=DerivationMethod.SPATIAL_JOIN,
        source_columns=["max_width"],
        dependencies=["reach.geom"],
        description="Maximum width from post-processing spatial join with max width raster",
    ),
    # --- Flow & Hydrology ---
    "reach.facc": AttributeSpec(
        name="reach.facc",
        source=SourceDataset.MERIT_HYDRO,
        method=DerivationMethod.MAX,
        source_columns=["facc"],
        dependencies=["centerline.x", "centerline.y"],
        description="Flow accumulation (km²): np.max(facc[reach_centerlines]) - downstream has highest",
    ),
    "reach.dist_out": AttributeSpec(
        name="reach.dist_out",
        source=SourceDataset.COMPUTED,
        method=DerivationMethod.PATH_ACCUMULATION,
        source_columns=[],
        dependencies=["reach.reach_length", "reach_topology"],
        description="Distance from outlet (m): graph traversal from outlet upstream, accumulating reach lengths",
    ),
    "reach.lakeflag": AttributeSpec(
        name="reach.lakeflag",
        source=SourceDataset.GRWL,
        method=DerivationMethod.MODE,
        source_columns=["lake_flag"],
        dependencies=["centerline.lakeflag"],
        description="Lake flag mode: 0=river, 1=lake, 2=canal, 3=tidal",
    ),
    # --- Obstructions ---
    "reach.obstr_type": AttributeSpec(
        name="reach.obstr_type",
        source=SourceDataset.GROD,
        method=DerivationMethod.MAX,
        source_columns=["obstruction_type"],
        dependencies=["centerline.grod"],
        description="Obstruction type: np.max(GROD[reach]), values >5 reset to 0. 0=none, 1=dam, 2=low-head dam, 3=lock, 4=waterfall, 5=partial dam",
    ),
    "reach.grod_id": AttributeSpec(
        name="reach.grod_id",
        source=SourceDataset.GROD,
        method=DerivationMethod.SPATIAL_JOIN,
        source_columns=["grod_fid"],
        dependencies=["reach.obstr_type"],
        description="GROD database ID at max obstruction point",
    ),
    "reach.hfalls_id": AttributeSpec(
        name="reach.hfalls_id",
        source=SourceDataset.HYDROFALLS,
        method=DerivationMethod.SPATIAL_JOIN,
        source_columns=["hfalls_fid"],
        dependencies=["reach.obstr_type"],
        description="HydroFALLS ID (only if obstr_type == 4)",
    ),
    # --- Channel Information ---
    "reach.n_chan_max": AttributeSpec(
        name="reach.n_chan_max",
        source=SourceDataset.GRWL,
        method=DerivationMethod.MAX,
        source_columns=["nchan"],
        dependencies=["centerline.nchan"],
        description="Maximum number of channels: np.max(nchan[reach_centerlines])",
    ),
    "reach.n_chan_mod": AttributeSpec(
        name="reach.n_chan_mod",
        source=SourceDataset.GRWL,
        method=DerivationMethod.MODE,
        source_columns=["nchan"],
        dependencies=["centerline.nchan"],
        description="Mode of channel count: most frequent nchan value",
    ),
    # --- Topology ---
    "reach.n_rch_up": AttributeSpec(
        name="reach.n_rch_up",
        source=SourceDataset.COMPUTED,
        method=DerivationMethod.COUNT,
        source_columns=[],
        dependencies=["reach_topology"],
        description="Count of upstream reaches after flow filtering",
    ),
    "reach.n_rch_down": AttributeSpec(
        name="reach.n_rch_down",
        source=SourceDataset.COMPUTED,
        method=DerivationMethod.COUNT,
        source_columns=[],
        dependencies=["reach_topology"],
        description="Count of downstream reaches after flow filtering",
    ),
    # --- SWOT Observations ---
    "reach.swot_obs": AttributeSpec(
        name="reach.swot_obs",
        source=SourceDataset.SWOT_TRACKS,
        method=DerivationMethod.MAX,
        source_columns=["num_observations"],
        dependencies=["reach.geom"],
        description="Max SWOT observations: np.max(num_obs[reach]) in 21-day cycle",
    ),
    "reach.iceflag": AttributeSpec(
        name="reach.iceflag",
        source=SourceDataset.ICE_FLAGS,
        method=DerivationMethod.SPATIAL_JOIN,
        source_columns=["ice_flag"],
        dependencies=["reach.reach_id"],
        description="Ice flag: 366-day array per reach from external CSV spatial join",
    ),
    "reach.low_slope_flag": AttributeSpec(
        name="reach.low_slope_flag",
        source=SourceDataset.COMPUTED,
        method=DerivationMethod.COMPUTED,
        source_columns=[],
        dependencies=["reach.slope"],
        description="Low slope flag: 1 if slope too low for discharge estimation",
    ),
    "reach.sinuosity": AttributeSpec(
        name="reach.sinuosity",
        source=SourceDataset.COMPUTED,
        method=DerivationMethod.COMPUTED,
        source_columns=[],
        dependencies=["centerline.x", "centerline.y"],
        description="Reach sinuosity: arc_length / straight_line_distance from centerline geometry",
    ),
    "reach.coastal_flag": AttributeSpec(
        name="reach.coastal_flag",
        source=SourceDataset.COMPUTED,
        method=DerivationMethod.COMPUTED,
        source_columns=[],
        dependencies=["node.lakeflag"],
        description="Coastal/tidal flag: 1 if >25% of nodes have lakeflag >= 3 (tidal)",
    ),
    # --- Metadata ---
    "reach.river_name": AttributeSpec(
        name="reach.river_name",
        source=SourceDataset.RIVER_NAMES,
        method=DerivationMethod.SPATIAL_JOIN,
        source_columns=["name"],
        dependencies=["reach.geom"],
        description="River name(s): spatial join with names shapefile, semicolon-separated",
    ),
    "reach.edit_flag": AttributeSpec(
        name="reach.edit_flag",
        source=SourceDataset.MANUAL,
        method=DerivationMethod.DIRECT,
        source_columns=[],
        dependencies=[],
        description="Edit flag: comma-separated codes tracking post-processing modifications",
    ),
    "reach.trib_flag": AttributeSpec(
        name="reach.trib_flag",
        source=SourceDataset.MERIT_HYDRO,
        method=DerivationMethod.SPATIAL_PROXIMITY,
        source_columns=["stream_order"],
        dependencies=["reach.geom"],
        description="STUB: External tributary from MHV (NOT n_rch_up>1). Requires MHV data files.",
    ),
    # --- Network Analysis (Path Variables) ---
    "reach.path_freq": AttributeSpec(
        name="reach.path_freq",
        source=SourceDataset.COMPUTED,
        method=DerivationMethod.GRAPH_TRAVERSAL,
        source_columns=[],
        dependencies=["reach_topology"],
        description="Path frequency: flow traversal count from outlet to headwater",
    ),
    "reach.path_order": AttributeSpec(
        name="reach.path_order",
        source=SourceDataset.COMPUTED,
        method=DerivationMethod.GRAPH_TRAVERSAL,
        source_columns=[],
        dependencies=["reach_topology", "reach.path_freq"],
        description="Path order: 1=longest to N=shortest during pathway construction",
    ),
    "reach.path_segs": AttributeSpec(
        name="reach.path_segs",
        source=SourceDataset.COMPUTED,
        method=DerivationMethod.COMPUTED,
        source_columns=[],
        dependencies=["reach.path_order", "reach.path_freq"],
        description="Path segments: unique segment IDs between junctions",
    ),
    "reach.stream_order": AttributeSpec(
        name="reach.stream_order",
        source=SourceDataset.COMPUTED,
        method=DerivationMethod.LOG_TRANSFORM,
        source_columns=[],
        dependencies=["reach.path_freq"],
        description="Stream order: round(log(path_freq)) + 1 where path_freq > 0",
    ),
    "reach.main_side": AttributeSpec(
        name="reach.main_side",
        source=SourceDataset.COMPUTED,
        method=DerivationMethod.GRAPH_TRAVERSAL,
        source_columns=[],
        dependencies=["reach_topology", "reach.path_freq"],
        description="Main/side channel: 0=main, 1=side, 2=secondary outlet",
    ),
    "reach.end_reach": AttributeSpec(
        name="reach.end_reach",
        source=SourceDataset.COMPUTED,
        method=DerivationMethod.GRAPH_TRAVERSAL,
        source_columns=[],
        dependencies=["reach_topology"],
        description="End reach type: 0=main, 1=headwater, 2=outlet, 3=junction",
    ),
    "reach.network": AttributeSpec(
        name="reach.network",
        source=SourceDataset.COMPUTED,
        method=DerivationMethod.GRAPH_TRAVERSAL,
        source_columns=[],
        dependencies=["reach_topology"],
        description="Connected network ID: groups of hydrologically connected reaches",
    ),
    # =========================================================================
    # NODE ATTRIBUTES
    # =========================================================================
    # --- Basic Geometry ---
    "node.x": AttributeSpec(
        name="node.x",
        source=SourceDataset.GRWL,
        method=DerivationMethod.MEAN,
        source_columns=["lon"],
        dependencies=["centerline.x", "centerline.node_id"],
        description="Node longitude: np.mean(lon[node_centerlines])",
    ),
    "node.y": AttributeSpec(
        name="node.y",
        source=SourceDataset.GRWL,
        method=DerivationMethod.MEAN,
        source_columns=["lat"],
        dependencies=["centerline.y", "centerline.node_id"],
        description="Node latitude: np.mean(lat[node_centerlines])",
    ),
    "node.node_length": AttributeSpec(
        name="node.node_length",
        source=SourceDataset.COMPUTED,
        method=DerivationMethod.DIRECT,
        source_columns=[],
        dependencies=["centerline.node_dist"],
        description="Node length (~200m): unique(node_dist[node_centerlines])[0]",
    ),
    # --- Water Surface Elevation ---
    "node.wse": AttributeSpec(
        name="node.wse",
        source=SourceDataset.MERIT_HYDRO,
        method=DerivationMethod.MEDIAN,
        source_columns=["elevation"],
        dependencies=["centerline.x", "centerline.y", "centerline.node_id"],
        description="Node WSE: np.median(elevation[node_centerlines])",
    ),
    "node.wse_var": AttributeSpec(
        name="node.wse_var",
        source=SourceDataset.MERIT_HYDRO,
        method=DerivationMethod.VARIANCE,
        source_columns=["elevation"],
        dependencies=["centerline.x", "centerline.y", "centerline.node_id"],
        description="Node WSE variance: np.var(elevation[node_centerlines])",
    ),
    # --- Width ---
    "node.width": AttributeSpec(
        name="node.width",
        source=SourceDataset.GRWL,
        method=DerivationMethod.MEDIAN,
        source_columns=["width"],
        dependencies=["centerline.width", "centerline.node_id"],
        description="Node width: np.median(width[node_centerlines])",
    ),
    "node.width_var": AttributeSpec(
        name="node.width_var",
        source=SourceDataset.GRWL,
        method=DerivationMethod.VARIANCE,
        source_columns=["width"],
        dependencies=["centerline.width", "centerline.node_id"],
        description="Node width variance: np.var(width[node_centerlines])",
    ),
    "node.max_width": AttributeSpec(
        name="node.max_width",
        source=SourceDataset.MAX_WIDTH_RASTER,
        method=DerivationMethod.SPATIAL_JOIN,
        source_columns=["max_width"],
        dependencies=["node.geom"],
        description="Node max width: spatial join, ratio-adjusted for multichannel",
    ),
    # --- Flow & Hydrology ---
    "node.facc": AttributeSpec(
        name="node.facc",
        source=SourceDataset.MERIT_HYDRO,
        method=DerivationMethod.MAX,
        source_columns=["facc"],
        dependencies=["centerline.x", "centerline.y", "centerline.node_id"],
        description="Node flow accumulation (km²): np.max(facc[node_centerlines])",
    ),
    "node.dist_out": AttributeSpec(
        name="node.dist_out",
        source=SourceDataset.COMPUTED,
        method=DerivationMethod.INTERPOLATED,
        source_columns=[],
        dependencies=["reach.dist_out", "node.reach_id"],
        description="Node distance from outlet: interpolated from reach dist_out by position",
    ),
    "node.lakeflag": AttributeSpec(
        name="node.lakeflag",
        source=SourceDataset.GRWL,
        method=DerivationMethod.MODE,
        source_columns=["lake_flag"],
        dependencies=["centerline.lakeflag", "centerline.node_id"],
        description="Node lake flag mode: 0=river, 1=lake, 2=canal, 3=tidal",
    ),
    # --- Obstructions ---
    "node.obstr_type": AttributeSpec(
        name="node.obstr_type",
        source=SourceDataset.GROD,
        method=DerivationMethod.MAX,
        source_columns=["obstruction_type"],
        dependencies=["centerline.grod", "centerline.node_id"],
        description="Node obstruction type: np.max(GROD[node_centerlines])",
    ),
    "node.grod_id": AttributeSpec(
        name="node.grod_id",
        source=SourceDataset.GROD,
        method=DerivationMethod.SPATIAL_JOIN,
        source_columns=["grod_fid"],
        dependencies=["node.obstr_type"],
        description="Node GROD database ID",
    ),
    "node.hfalls_id": AttributeSpec(
        name="node.hfalls_id",
        source=SourceDataset.HYDROFALLS,
        method=DerivationMethod.SPATIAL_JOIN,
        source_columns=["hfalls_fid"],
        dependencies=["node.obstr_type"],
        description="Node HydroFALLS ID (only if obstr_type == 4)",
    ),
    # --- Channel Information ---
    "node.n_chan_max": AttributeSpec(
        name="node.n_chan_max",
        source=SourceDataset.GRWL,
        method=DerivationMethod.MAX,
        source_columns=["nchan"],
        dependencies=["centerline.nchan", "centerline.node_id"],
        description="Node max channel count: np.max(nchan[node_centerlines])",
    ),
    "node.n_chan_mod": AttributeSpec(
        name="node.n_chan_mod",
        source=SourceDataset.GRWL,
        method=DerivationMethod.MODE,
        source_columns=["nchan"],
        dependencies=["centerline.nchan", "centerline.node_id"],
        description="Node channel count mode",
    ),
    # --- SWOT Search Parameters ---
    "node.wth_coef": AttributeSpec(
        name="node.wth_coef",
        source=SourceDataset.COMPUTED,
        method=DerivationMethod.DIRECT,
        source_columns=[],
        dependencies=[],
        description="Width coefficient for SWOT search window: default 0.5",
    ),
    "node.ext_dist_coef": AttributeSpec(
        name="node.ext_dist_coef",
        source=SourceDataset.COMPUTED,
        method=DerivationMethod.COMPUTED,
        source_columns=[],
        dependencies=["node.max_width", "node.width"],
        description="Max search window coefficient: default 5, adjusted by max_wth/width ratio",
    ),
    # --- Morphology ---
    "node.meander_length": AttributeSpec(
        name="node.meander_length",
        source=SourceDataset.COMPUTED,
        method=DerivationMethod.COMPUTED,
        source_columns=[],
        dependencies=["centerline.x", "centerline.y", "centerline.node_id"],
        description="Meander length (m) computed from centerline geometry",
    ),
    "node.sinuosity": AttributeSpec(
        name="node.sinuosity",
        source=SourceDataset.COMPUTED,
        method=DerivationMethod.COMPUTED,
        source_columns=[],
        dependencies=["node.node_length", "node.meander_length"],
        description="Sinuosity ratio: actual_length / straight_line_distance",
    ),
    # --- Metadata ---
    "node.river_name": AttributeSpec(
        name="node.river_name",
        source=SourceDataset.RIVER_NAMES,
        method=DerivationMethod.SPATIAL_JOIN,
        source_columns=["name"],
        dependencies=["node.geom"],
        description="Node river name: spatial join with names shapefile",
    ),
    "node.manual_add": AttributeSpec(
        name="node.manual_add",
        source=SourceDataset.COMPUTED,
        method=DerivationMethod.COMPUTED,
        source_columns=[],
        dependencies=["node.width"],
        description="Manual add flag: 1 if width == 1 (indicating manual addition)",
    ),
    "node.edit_flag": AttributeSpec(
        name="node.edit_flag",
        source=SourceDataset.MANUAL,
        method=DerivationMethod.DIRECT,
        source_columns=[],
        dependencies=[],
        description="Edit flag: comma-separated update codes",
    ),
    "node.trib_flag": AttributeSpec(
        name="node.trib_flag",
        source=SourceDataset.MERIT_HYDRO,
        method=DerivationMethod.SPATIAL_PROXIMITY,
        source_columns=["stream_order"],
        dependencies=["node.geom"],
        description="STUB: External tributary from MHV (NOT n_rch_up>1). Requires MHV data files.",
    ),
    # --- Network Analysis (inherited from reach) ---
    "node.path_freq": AttributeSpec(
        name="node.path_freq",
        source=SourceDataset.INHERITED,
        method=DerivationMethod.INHERITED,
        source_columns=[],
        dependencies=["reach.path_freq", "node.reach_id"],
        description="Path frequency: inherited from parent reach",
    ),
    "node.path_order": AttributeSpec(
        name="node.path_order",
        source=SourceDataset.INHERITED,
        method=DerivationMethod.INHERITED,
        source_columns=[],
        dependencies=["reach.path_order", "node.reach_id"],
        description="Path order: inherited from parent reach",
    ),
    "node.path_segs": AttributeSpec(
        name="node.path_segs",
        source=SourceDataset.INHERITED,
        method=DerivationMethod.INHERITED,
        source_columns=[],
        dependencies=["reach.path_segs", "node.reach_id"],
        description="Path segments: inherited from parent reach",
    ),
    "node.stream_order": AttributeSpec(
        name="node.stream_order",
        source=SourceDataset.INHERITED,
        method=DerivationMethod.INHERITED,
        source_columns=[],
        dependencies=["reach.stream_order", "node.reach_id"],
        description="Stream order: inherited from parent reach",
    ),
    "node.main_side": AttributeSpec(
        name="node.main_side",
        source=SourceDataset.INHERITED,
        method=DerivationMethod.INHERITED,
        source_columns=[],
        dependencies=["reach.main_side", "node.reach_id"],
        description="Main/side channel: inherited from parent reach",
    ),
    "node.end_reach": AttributeSpec(
        name="node.end_reach",
        source=SourceDataset.INHERITED,
        method=DerivationMethod.INHERITED,
        source_columns=[],
        dependencies=["reach.end_reach", "node.reach_id"],
        description="End reach type: inherited from parent reach",
    ),
    "node.network": AttributeSpec(
        name="node.network",
        source=SourceDataset.INHERITED,
        method=DerivationMethod.INHERITED,
        source_columns=[],
        dependencies=["reach.network", "node.reach_id"],
        description="Network ID: inherited from parent reach",
    ),
    # =========================================================================
    # CENTERLINE ATTRIBUTES
    # =========================================================================
    "centerline.x": AttributeSpec(
        name="centerline.x",
        source=SourceDataset.GRWL,
        method=DerivationMethod.DIRECT,
        source_columns=["lon"],
        dependencies=[],
        description="Centerline longitude from GRWL river centerlines",
    ),
    "centerline.y": AttributeSpec(
        name="centerline.y",
        source=SourceDataset.GRWL,
        method=DerivationMethod.DIRECT,
        source_columns=["lat"],
        dependencies=[],
        description="Centerline latitude from GRWL river centerlines",
    ),
    "centerline.reach_id": AttributeSpec(
        name="centerline.reach_id",
        source=SourceDataset.COMPUTED,
        method=DerivationMethod.COMPUTED,
        source_columns=[],
        dependencies=["centerline.x", "centerline.y"],
        description="Parent reach ID assigned during reach definition",
    ),
    "centerline.node_id": AttributeSpec(
        name="centerline.node_id",
        source=SourceDataset.COMPUTED,
        method=DerivationMethod.COMPUTED,
        source_columns=[],
        dependencies=["centerline.x", "centerline.y"],
        description="Parent node ID assigned during node definition (~200m intervals)",
    ),
}


class ReconstructionEngine:
    """
    Engine for reconstructing SWORD attributes from source datasets.

    This class provides methods to:
    1. Reconstruct individual attributes from their source data
    2. Track lineage of reconstructed values
    3. Validate reconstructed values against existing data
    4. Execute reconstruction recipes

    Parameters
    ----------
    sword : SWORD
        The SWORD database instance
    provenance : ProvenanceLogger, optional
        Provenance logger for tracking reconstruction operations
    source_data_dir : str or Path, optional
        Directory containing source datasets (MERIT, GRWL, etc.)

    Attributes
    ----------
    attribute_sources : dict
        Mapping of attribute names to their source specifications
    """

    def __init__(
        self,
        sword: "SWORD",
        provenance: Optional["ProvenanceLogger"] = None,
        source_data_dir: Optional[Union[str, Path]] = None,
    ):
        self._sword = sword
        self._provenance = provenance
        self._source_data_dir = Path(source_data_dir) if source_data_dir else None
        self._conn = sword._db.connect()  # Use private _db attribute
        self._region = sword.region

        # Reconstruction function registry
        self._reconstructors: Dict[str, Callable] = {
            # Reach - hydrology/elevation
            "reach.dist_out": self._reconstruct_reach_dist_out,
            "reach.wse": self._reconstruct_reach_wse,
            "reach.wse_var": self._reconstruct_reach_wse_var,
            "reach.slope": self._reconstruct_reach_slope,
            "reach.facc": self._reconstruct_reach_facc,
            "reach.reach_length": self._reconstruct_reach_length,
            # Reach - width
            "reach.width": self._reconstruct_reach_width,
            "reach.width_var": self._reconstruct_reach_width_var,
            # Reach - geometry
            "reach.x": self._reconstruct_reach_x,
            "reach.y": self._reconstruct_reach_y,
            "reach.x_min": self._reconstruct_reach_x_min,
            "reach.x_max": self._reconstruct_reach_x_max,
            "reach.y_min": self._reconstruct_reach_y_min,
            "reach.y_max": self._reconstruct_reach_y_max,
            # Reach - counts
            "reach.n_nodes": self._reconstruct_reach_n_nodes,
            "reach.n_rch_up": self._reconstruct_reach_n_rch_up,
            "reach.n_rch_down": self._reconstruct_reach_n_rch_down,
            # Reach - categorical (mode)
            "reach.lakeflag": self._reconstruct_reach_lakeflag,
            "reach.n_chan_max": self._reconstruct_reach_n_chan_max,
            "reach.n_chan_mod": self._reconstruct_reach_n_chan_mod,
            # Reach - network analysis
            "reach.stream_order": self._reconstruct_reach_stream_order,
            "reach.path_freq": self._reconstruct_reach_path_freq,
            "reach.end_reach": self._reconstruct_reach_end_reach,
            "reach.network": self._reconstruct_reach_network,
            # Node - basic
            "node.wse": self._reconstruct_node_wse,
            "node.facc": self._reconstruct_node_facc,
            "node.dist_out": self._reconstruct_node_dist_out,
            "node.x": self._reconstruct_node_x,
            "node.y": self._reconstruct_node_y,
            "node.node_length": self._reconstruct_node_node_length,
            # Node - sinuosity and meander
            "node.sinuosity": self._reconstruct_node_sinuosity,
            "node.meander_length": self._reconstruct_node_meander_length,
            # Node - inherited from reach
            "node.stream_order": self._reconstruct_node_inherited("stream_order"),
            "node.path_freq": self._reconstruct_node_inherited("path_freq"),
            "node.path_order": self._reconstruct_node_inherited("path_order"),
            "node.path_segs": self._reconstruct_node_inherited("path_segs"),
            "node.main_side": self._reconstruct_node_inherited("main_side"),
            "node.end_reach": self._reconstruct_node_inherited("end_reach"),
            "node.network": self._reconstruct_node_inherited("network"),
            # Node - width and coefficients
            "node.wth_coef": self._reconstruct_node_wth_coef,
            "node.ext_dist_coef": self._reconstruct_node_ext_dist_coef,
            "node.max_width": self._reconstruct_node_max_width,
            "node.trib_flag": self._reconstruct_node_trib_flag,
            "node.obstr_type": self._reconstruct_node_obstr_type,
            # Node - centerline aggregation
            "node.lakeflag": self._reconstruct_node_lakeflag,
            "node.n_chan_max": self._reconstruct_node_n_chan_max,
            "node.n_chan_mod": self._reconstruct_node_n_chan_mod,
            "node.width": self._reconstruct_node_width,
            "node.width_var": self._reconstruct_node_width_var,
            "node.wse_var": self._reconstruct_node_wse_var,
            # Reach - additional attributes
            "reach.max_width": self._reconstruct_reach_max_width,
            "reach.sinuosity": self._reconstruct_reach_sinuosity,
            "reach.coastal_flag": self._reconstruct_reach_coastal_flag,
            "reach.low_slope_flag": self._reconstruct_reach_low_slope_flag,
            "reach.swot_obs": self._reconstruct_reach_swot_obs,
            "reach.main_side": self._reconstruct_reach_main_side,
            "reach.obstr_type": self._reconstruct_reach_obstr_type,
            "reach.path_order": self._reconstruct_reach_path_order,
            "reach.path_segs": self._reconstruct_reach_path_segs,
            "reach.trib_flag": self._reconstruct_reach_trib_flag,
            # Stub reconstructors (require external data)
            "node.grod_id": self._reconstruct_node_grod_id,
            "node.hfalls_id": self._reconstruct_node_hfalls_id,
            "node.river_name": self._reconstruct_node_river_name,
            "reach.grod_id": self._reconstruct_reach_grod_id,
            "reach.hfalls_id": self._reconstruct_reach_hfalls_id,
            "reach.river_name": self._reconstruct_reach_river_name,
            "reach.iceflag": self._reconstruct_reach_iceflag,
            # Non-reconstructable (manual edits) - preserve values
            "node.edit_flag": self._reconstruct_node_edit_flag,
            "node.manual_add": self._reconstruct_node_manual_add,
            "reach.edit_flag": self._reconstruct_reach_edit_flag,
            # Centerline source data stubs
            "centerline.x": self._reconstruct_centerline_x,
            "centerline.y": self._reconstruct_centerline_y,
            "centerline.reach_id": self._reconstruct_centerline_reach_id,
            "centerline.node_id": self._reconstruct_centerline_node_id,
        }

    @property
    def attribute_sources(self) -> Dict[str, AttributeSpec]:
        """Get the attribute-to-source mappings."""
        return ATTRIBUTE_SOURCES

    def get_source_info(self, attribute: str) -> Optional[AttributeSpec]:
        """
        Get source information for an attribute.

        Parameters
        ----------
        attribute : str
            Full attribute name (e.g., "reach.wse")

        Returns
        -------
        AttributeSpec or None
            Source specification, or None if unknown
        """
        return ATTRIBUTE_SOURCES.get(attribute)

    def list_reconstructable_attributes(self) -> List[str]:
        """
        List all attributes that can be reconstructed.

        Returns
        -------
        list of str
            Attribute names that have reconstruction functions
        """
        return list(self._reconstructors.keys())

    def can_reconstruct(self, attribute: str) -> bool:
        """
        Check if an attribute can be reconstructed.

        Parameters
        ----------
        attribute : str
            Full attribute name

        Returns
        -------
        bool
            True if reconstruction is available
        """
        return attribute in self._reconstructors

    # =========================================================================
    # MAIN RECONSTRUCTION API
    # =========================================================================

    def reconstruct(
        self,
        attribute: str,
        entity_ids: Optional[List[int]] = None,
        force: bool = False,
        reason: str = None,
    ) -> Dict[str, Any]:
        """
        Reconstruct an attribute from source data.

        Parameters
        ----------
        attribute : str
            Full attribute name (e.g., "reach.wse", "node.facc")
        entity_ids : list of int, optional
            Specific entities to reconstruct. If None, reconstructs all.
        force : bool, optional
            If True, reconstruct even if values exist
        reason : str, optional
            Reason for reconstruction (logged to provenance)

        Returns
        -------
        dict
            Reconstruction results including:
            - 'reconstructed': number of values reconstructed
            - 'entity_ids': list of affected entity IDs
            - 'attribute': the attribute name

        Raises
        ------
        ValueError
            If attribute is not reconstructable

        Example
        -------
        >>> result = engine.reconstruct('reach.dist_out', reach_ids=[123, 456])
        >>> print(f"Reconstructed {result['reconstructed']} values")
        """
        if not self.can_reconstruct(attribute):
            available = self.list_reconstructable_attributes()
            raise ValueError(
                f"Cannot reconstruct '{attribute}'. Available: {available}"
            )

        logger.info(f"Reconstructing {attribute} for region {self._region}")

        # Get the reconstruction function
        reconstructor = self._reconstructors[attribute]

        # Execute with provenance tracking
        if self._provenance:
            spec = self.get_source_info(attribute)
            with self._provenance.operation(
                "RECONSTRUCT",
                table_name=spec.entity_type + "s" if spec else None,
                entity_ids=entity_ids,
                region=self._region,
                reason=reason or f"Reconstruct {attribute}",
                details={
                    "attribute": attribute,
                    "source": spec.source.value if spec else "UNKNOWN",
                    "method": spec.method.value if spec else "UNKNOWN",
                },
                affected_columns=[attribute.split(".")[1]],
            ) as op_id:
                result = reconstructor(entity_ids, force)
                result["operation_id"] = op_id
        else:
            result = reconstructor(entity_ids, force)

        logger.info(f"Reconstructed {result['reconstructed']} {attribute} values")
        return result

    def reconstruct_from_centerlines(
        self,
        attributes: Optional[List[str]] = None,
        reach_ids: Optional[List[int]] = None,
        reason: str = None,
    ) -> Dict[str, Any]:
        """
        Reconstruct derived attributes from centerline geometry.

        This reconstructs attributes that are computed from the centerline
        points (wse, slope, length, facc).

        Parameters
        ----------
        attributes : list of str, optional
            Attributes to reconstruct. Defaults to all centerline-derived.
        reach_ids : list of int, optional
            Specific reaches. If None, all reaches.
        reason : str, optional
            Reason for reconstruction

        Returns
        -------
        dict
            Results for each attribute reconstructed
        """
        # Default attributes derived from centerlines
        centerline_derived = [
            "reach.wse",
            "reach.slope",
            "reach.reach_length",
            "reach.facc",
        ]

        if attributes is None:
            attributes = centerline_derived

        results = {}
        for attr in attributes:
            if self.can_reconstruct(attr):
                try:
                    results[attr] = self.reconstruct(
                        attr, entity_ids=reach_ids, reason=reason
                    )
                except Exception as e:
                    logger.error(f"Failed to reconstruct {attr}: {e}")
                    results[attr] = {"error": str(e)}

        return results

    def validate(
        self,
        attribute: str,
        entity_ids: Optional[List[int]] = None,
        tolerance: float = 0.01,
    ) -> Dict[str, Any]:
        """
        Validate reconstruction against existing values.

        Compares reconstructed values to current values to verify
        reconstruction accuracy.

        Parameters
        ----------
        attribute : str
            Attribute to validate
        entity_ids : list of int, optional
            Specific entities to validate
        tolerance : float, optional
            Relative tolerance for comparison

        Returns
        -------
        dict
            Validation report including:
            - 'passed': bool - whether validation passed
            - 'total': number of values compared
            - 'within_tolerance': number within tolerance
            - 'max_difference': maximum relative difference
            - 'failures': list of entity IDs that failed
        """
        if not self.can_reconstruct(attribute):
            raise ValueError(f"Cannot validate '{attribute}' - no reconstructor")

        spec = self.get_source_info(attribute)
        entity_type = spec.entity_type if spec else attribute.split(".")[0]
        attr_name = attribute.split(".")[1]

        # Get current values
        view = self._get_view(entity_type)
        current_values = getattr(view, attr_name)[:]
        entity_ids_all = view.id[:]

        if entity_ids is not None:
            mask = np.isin(entity_ids_all, entity_ids)
            current_values = current_values[mask]
            entity_ids_all = entity_ids_all[mask]

        # Reconstruct without saving
        reconstructor = self._reconstructors[attribute]
        result = reconstructor(
            list(entity_ids_all) if entity_ids else None, force=True, dry_run=True
        )

        if "values" not in result:
            return {
                "passed": False,
                "error": "Reconstructor did not return values for validation",
            }

        reconstructed_values = result["values"]

        # Compare
        # Handle NaN values
        valid_mask = ~(np.isnan(current_values) | np.isnan(reconstructed_values))

        if not np.any(valid_mask):
            return {
                "passed": True,
                "total": len(current_values),
                "within_tolerance": 0,
                "note": "All values are NaN",
            }

        current_valid = current_values[valid_mask]
        reconstructed_valid = reconstructed_values[valid_mask]

        # Relative difference (avoid division by zero)
        with np.errstate(divide="ignore", invalid="ignore"):
            rel_diff = np.abs(current_valid - reconstructed_valid) / np.maximum(
                np.abs(current_valid), 1e-10
            )

        within_tol = rel_diff <= tolerance
        failures = entity_ids_all[valid_mask][~within_tol]

        return {
            "passed": np.all(within_tol),
            "total": len(current_valid),
            "within_tolerance": int(np.sum(within_tol)),
            "max_difference": float(np.max(rel_diff)) if len(rel_diff) > 0 else 0,
            "mean_difference": float(np.mean(rel_diff)) if len(rel_diff) > 0 else 0,
            "failures": list(failures[:100]),  # Limit to first 100
            "tolerance": tolerance,
        }

    # =========================================================================
    # RECIPE SYSTEM
    # =========================================================================

    def register_recipe(
        self,
        name: str,
        target_attributes: List[str],
        required_sources: List[str],
        script_path: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        description: str = None,
    ) -> int:
        """
        Register a reconstruction recipe in the database.

        Parameters
        ----------
        name : str
            Unique recipe name
        target_attributes : list of str
            Attributes this recipe produces
        required_sources : list of str
            Source datasets needed
        script_path : str, optional
            Path to reconstruction script
        parameters : dict, optional
            Default parameters
        description : str, optional
            Recipe description

        Returns
        -------
        int
            Recipe ID
        """
        # Compute script hash if path provided
        script_hash = None
        if script_path and Path(script_path).exists():
            with open(script_path, "rb") as f:
                script_hash = hashlib.sha256(f.read()).hexdigest()

        # Get next ID
        result = self._conn.execute(
            "SELECT COALESCE(MAX(recipe_id), 0) + 1 FROM sword_reconstruction_recipes"
        ).fetchone()
        recipe_id = result[0]

        # Convert lists to array literals
        target_sql = f"[{', '.join(repr(a) for a in target_attributes)}]"
        sources_sql = f"[{', '.join(repr(s) for s in required_sources)}]"

        self._conn.execute(
            f"""
            INSERT INTO sword_reconstruction_recipes (
                recipe_id, name, description,
                target_attributes, required_sources,
                script_path, script_hash, parameters
            ) VALUES (
                ?, ?, ?,
                {target_sql}, {sources_sql},
                ?, ?, ?
            )
        """,
            [
                recipe_id,
                name,
                description,
                script_path,
                script_hash,
                json.dumps(parameters) if parameters else None,
            ],
        )

        logger.info(f"Registered recipe '{name}' (ID: {recipe_id})")
        return recipe_id

    def get_recipe(self, name: str) -> Optional[Dict[str, Any]]:
        """
        Get a recipe by name.

        Parameters
        ----------
        name : str
            Recipe name

        Returns
        -------
        dict or None
            Recipe details
        """
        result = self._conn.execute(
            """
            SELECT recipe_id, name, description, target_attributes,
                   required_sources, script_path, script_hash, parameters
            FROM sword_reconstruction_recipes
            WHERE name = ?
        """,
            [name],
        ).fetchone()

        if not result:
            return None

        return {
            "recipe_id": result[0],
            "name": result[1],
            "description": result[2],
            "target_attributes": result[3],
            "required_sources": result[4],
            "script_path": result[5],
            "script_hash": result[6],
            "parameters": json.loads(result[7]) if result[7] else None,
        }

    def list_recipes(self) -> List[Dict[str, Any]]:
        """
        List all registered recipes.

        Returns
        -------
        list of dict
            All recipes
        """
        results = self._conn.execute("""
            SELECT recipe_id, name, description, target_attributes
            FROM sword_reconstruction_recipes
            ORDER BY name
        """).fetchall()

        return [
            {
                "recipe_id": r[0],
                "name": r[1],
                "description": r[2],
                "target_attributes": r[3],
            }
            for r in results
        ]

    # =========================================================================
    # RECONSTRUCTION IMPLEMENTATIONS
    # =========================================================================

    def _get_view(self, entity_type: str) -> Any:
        """Get the view for an entity type."""
        view_map = {
            "reach": self._sword.reaches,
            "node": self._sword.nodes,
            "centerline": self._sword.centerlines,
        }
        return view_map[entity_type]

    def _reconstruct_reach_dist_out(
        self,
        reach_ids: Optional[List[int]] = None,
        force: bool = False,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """
        Reconstruct reach dist_out using graph traversal from outlets upstream.

        Algorithm:
        1. Find outlet reaches (no downstream neighbors or dist_out = 0)
        2. BFS/DFS upstream from outlets
        3. dist_out = parent_dist_out + parent_length
        """
        logger.info("Reconstructing reach.dist_out via graph traversal")

        # Get topology
        topology_df = self._conn.execute(
            """
            SELECT reach_id, direction, neighbor_reach_id
            FROM reach_topology
            WHERE region = ?
        """,
            [self._region],
        ).fetchdf()

        # Get reach lengths
        reaches_df = self._conn.execute(
            """
            SELECT reach_id, reach_length, dist_out
            FROM reaches
            WHERE region = ?
        """,
            [self._region],
        ).fetchdf()

        # Build upstream adjacency (for each reach, who is upstream of it)
        upstream_map = {}  # reach_id -> list of upstream reach_ids
        downstream_map = {}  # reach_id -> list of downstream reach_ids

        for _, row in topology_df.iterrows():
            reach = row["reach_id"]
            neighbor = row["neighbor_reach_id"]
            direction = row["direction"]

            if direction == "up":
                if reach not in upstream_map:
                    upstream_map[reach] = []
                upstream_map[reach].append(neighbor)
            else:  # down
                if reach not in downstream_map:
                    downstream_map[reach] = []
                downstream_map[reach].append(neighbor)

        # Find outlets (reaches with no downstream neighbors)
        all_reaches = set(reaches_df["reach_id"])
        outlets = all_reaches - set(downstream_map.keys())

        # Also consider reaches already marked as outlets
        outlet_mask = reaches_df["dist_out"] == 0
        outlets.update(reaches_df[outlet_mask]["reach_id"].tolist())

        logger.info(f"Found {len(outlets)} outlet reaches")

        # BFS from outlets upstream
        reach_lengths = dict(zip(reaches_df["reach_id"], reaches_df["reach_length"]))
        new_dist_out = {}

        # Initialize outlets with dist_out = 0
        queue = [(outlet_id, 0.0) for outlet_id in outlets]
        visited = set()

        while queue:
            reach_id, dist = queue.pop(0)

            if reach_id in visited:
                continue

            visited.add(reach_id)
            new_dist_out[reach_id] = dist

            # Add upstream reaches to queue
            for upstream_id in upstream_map.get(reach_id, []):
                if upstream_id not in visited:
                    # Distance to upstream = my dist_out + my length
                    upstream_dist = dist + (reach_lengths.get(reach_id, 0) or 0)
                    queue.append((upstream_id, upstream_dist))

        # Filter to requested reach_ids
        if reach_ids is not None:
            new_dist_out = {k: v for k, v in new_dist_out.items() if k in reach_ids}

        result_ids = list(new_dist_out.keys())
        result_values = np.array([new_dist_out[rid] for rid in result_ids])

        if dry_run:
            return {
                "reconstructed": len(result_ids),
                "entity_ids": result_ids,
                "attribute": "reach.dist_out",
                "values": result_values,
            }

        # Update the database
        for reach_id, dist in new_dist_out.items():
            self._conn.execute(
                """
                UPDATE reaches SET dist_out = ? WHERE reach_id = ? AND region = ?
            """,
                [dist, reach_id, self._region],
            )

        # Update in-memory array
        if len(result_ids) > 0:
            reach_view = self._sword.reaches
            for reach_id, dist in new_dist_out.items():
                idx = np.where(reach_view.id == reach_id)[0]
                if len(idx) > 0:
                    reach_view.dist_out._data[idx[0]] = dist

        return {
            "reconstructed": len(result_ids),
            "entity_ids": result_ids,
            "attribute": "reach.dist_out",
        }

    def _reconstruct_reach_wse(
        self,
        reach_ids: Optional[List[int]] = None,
        force: bool = False,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """
        Reconstruct reach wse as median of node elevations.

        Algorithm:
        1. For each reach, get all nodes
        2. Take median of node wse values
        """
        logger.info("Reconstructing reach.wse from node elevations")

        # Query to compute median wse for each reach
        where_clause = ""
        params = [self._region]

        if reach_ids is not None:
            placeholders = ", ".join(["?"] * len(reach_ids))
            where_clause = f"AND n.reach_id IN ({placeholders})"
            params.extend(reach_ids)

        result_df = self._conn.execute(
            f"""
            SELECT
                n.reach_id,
                MEDIAN(n.wse) as median_wse
            FROM nodes n
            WHERE n.region = ? {where_clause}
            GROUP BY n.reach_id
            HAVING COUNT(*) > 0
        """,
            params,
        ).fetchdf()

        result_ids = result_df["reach_id"].tolist()
        result_values = result_df["median_wse"].values

        if dry_run:
            return {
                "reconstructed": len(result_ids),
                "entity_ids": result_ids,
                "attribute": "reach.wse",
                "values": result_values,
            }

        # Update database
        for reach_id, wse in zip(result_ids, result_values):
            if wse is not None and not np.isnan(wse):
                self._conn.execute(
                    """
                    UPDATE reaches SET wse = ? WHERE reach_id = ? AND region = ?
                """,
                    [float(wse), reach_id, self._region],
                )

        # Update in-memory array
        reach_view = self._sword.reaches
        for reach_id, wse in zip(result_ids, result_values):
            if wse is not None and not np.isnan(wse):
                idx = np.where(reach_view.id == reach_id)[0]
                if len(idx) > 0:
                    reach_view.wse._data[idx[0]] = wse

        return {
            "reconstructed": len(result_ids),
            "entity_ids": result_ids,
            "attribute": "reach.wse",
        }

    def _reconstruct_reach_slope(
        self,
        reach_ids: Optional[List[int]] = None,
        force: bool = False,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """
        Reconstruct reach slope using linear regression of wse vs distance.

        Algorithm:
        1. For each reach, get centerlines with (dist_along_reach, wse)
        2. Fit linear regression: wse = a * dist + b
        3. slope = a (converted to m/km)
        """
        logger.info("Reconstructing reach.slope via linear regression")

        # This requires centerline-level data with distances
        # For now, use node-level approximation
        where_clause = ""
        params = [self._region]

        if reach_ids is not None:
            placeholders = ", ".join(["?"] * len(reach_ids))
            where_clause = f"AND reach_id IN ({placeholders})"
            params.extend(reach_ids)

        # Get nodes for each reach, ordered by dist_out
        nodes_df = self._conn.execute(
            f"""
            SELECT reach_id, node_id, wse, dist_out
            FROM nodes
            WHERE region = ? {where_clause}
            ORDER BY reach_id, dist_out DESC
        """,
            params,
        ).fetchdf()

        # Compute slope for each reach
        result_ids = []
        result_values = []

        for reach_id in nodes_df["reach_id"].unique():
            reach_nodes = nodes_df[nodes_df["reach_id"] == reach_id]

            if len(reach_nodes) < 2:
                continue

            dist = reach_nodes["dist_out"].values
            wse = reach_nodes["wse"].values

            # Filter out NaN values
            valid = ~(np.isnan(dist) | np.isnan(wse))
            if np.sum(valid) < 2:
                continue

            dist_valid = dist[valid]
            wse_valid = wse[valid]

            # Linear regression: wse = slope * dist + intercept
            # Use distance in km for slope in m/km
            dist_km = dist_valid / 1000.0

            try:
                # lstsq: solve Ax = b where A = [[dist, 1]], x = [[slope], [intercept]], b = wse
                A = np.column_stack([dist_km, np.ones_like(dist_km)])
                result, _, _, _ = np.linalg.lstsq(A, wse_valid, rcond=None)
                slope = result[0]  # m/km (negative for downstream flow)

                result_ids.append(reach_id)
                result_values.append(abs(slope))  # Store absolute value
            except Exception:
                continue

        result_values = np.array(result_values)

        if dry_run:
            return {
                "reconstructed": len(result_ids),
                "entity_ids": result_ids,
                "attribute": "reach.slope",
                "values": result_values,
            }

        # Update database
        for reach_id, slope in zip(result_ids, result_values):
            self._conn.execute(
                """
                UPDATE reaches SET slope = ? WHERE reach_id = ? AND region = ?
            """,
                [float(slope), reach_id, self._region],
            )

        # Update in-memory array
        reach_view = self._sword.reaches
        for reach_id, slope in zip(result_ids, result_values):
            idx = np.where(reach_view.id == reach_id)[0]
            if len(idx) > 0:
                reach_view.slope._data[idx[0]] = slope

        return {
            "reconstructed": len(result_ids),
            "entity_ids": result_ids,
            "attribute": "reach.slope",
        }

    def _reconstruct_reach_facc(
        self,
        reach_ids: Optional[List[int]] = None,
        force: bool = False,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """
        Reconstruct reach facc as maximum of node facc values.
        """
        logger.info("Reconstructing reach.facc from node values (max)")

        where_clause = ""
        params = [self._region]

        if reach_ids is not None:
            placeholders = ", ".join(["?"] * len(reach_ids))
            where_clause = f"AND n.reach_id IN ({placeholders})"
            params.extend(reach_ids)

        result_df = self._conn.execute(
            f"""
            SELECT
                n.reach_id,
                MAX(n.facc) as max_facc
            FROM nodes n
            WHERE n.region = ? {where_clause}
            GROUP BY n.reach_id
        """,
            params,
        ).fetchdf()

        result_ids = result_df["reach_id"].tolist()
        result_values = result_df["max_facc"].values

        if dry_run:
            return {
                "reconstructed": len(result_ids),
                "entity_ids": result_ids,
                "attribute": "reach.facc",
                "values": result_values,
            }

        # Update database
        for reach_id, facc in zip(result_ids, result_values):
            if facc is not None and not np.isnan(facc):
                self._conn.execute(
                    """
                    UPDATE reaches SET facc = ? WHERE reach_id = ? AND region = ?
                """,
                    [float(facc), reach_id, self._region],
                )

        # Update in-memory array
        reach_view = self._sword.reaches
        for reach_id, facc in zip(result_ids, result_values):
            if facc is not None and not np.isnan(facc):
                idx = np.where(reach_view.id == reach_id)[0]
                if len(idx) > 0:
                    reach_view.facc._data[idx[0]] = facc

        return {
            "reconstructed": len(result_ids),
            "entity_ids": result_ids,
            "attribute": "reach.facc",
        }

    def _reconstruct_reach_length(
        self,
        reach_ids: Optional[List[int]] = None,
        force: bool = False,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """
        Reconstruct reach length from centerline geometry.

        Algorithm:
        1. For each reach, get all centerline points in order
        2. Sum Euclidean distances between consecutive points
        """
        logger.info("Reconstructing reach.reach_length from centerline geometry")

        where_clause = ""
        params = [self._region]

        if reach_ids is not None:
            placeholders = ", ".join(["?"] * len(reach_ids))
            where_clause = f"AND reach_id IN ({placeholders})"
            params.extend(reach_ids)

        # Get centerlines ordered by cl_id within each reach
        cl_df = self._conn.execute(
            f"""
            SELECT reach_id, cl_id, x, y
            FROM centerlines
            WHERE region = ? {where_clause}
            ORDER BY reach_id, cl_id
        """,
            params,
        ).fetchdf()

        result_ids = []
        result_values = []

        for reach_id in cl_df["reach_id"].unique():
            reach_cl = cl_df[cl_df["reach_id"] == reach_id].sort_values("cl_id")

            if len(reach_cl) < 2:
                continue

            x = reach_cl["x"].values
            y = reach_cl["y"].values

            # Calculate distances (approximate using Euclidean at small scales)
            # For more accuracy, would need to project to meters
            dx = np.diff(x)
            dy = np.diff(y)

            # Convert degrees to approximate meters (at equator, 1 deg ≈ 111 km)
            # This is a simplification - proper projection would be better
            lat_mid = np.mean(y)
            meters_per_deg_lon = 111320 * np.cos(np.radians(lat_mid))
            meters_per_deg_lat = 110540

            dx_m = dx * meters_per_deg_lon
            dy_m = dy * meters_per_deg_lat

            distances = np.sqrt(dx_m**2 + dy_m**2)
            total_length = np.sum(distances)

            result_ids.append(reach_id)
            result_values.append(total_length)

        result_values = np.array(result_values)

        if dry_run:
            return {
                "reconstructed": len(result_ids),
                "entity_ids": result_ids,
                "attribute": "reach.reach_length",
                "values": result_values,
            }

        # Update database
        for reach_id, length in zip(result_ids, result_values):
            self._conn.execute(
                """
                UPDATE reaches SET reach_length = ? WHERE reach_id = ? AND region = ?
            """,
                [float(length), reach_id, self._region],
            )

        # Update in-memory array
        reach_view = self._sword.reaches
        for reach_id, length in zip(result_ids, result_values):
            idx = np.where(reach_view.id == reach_id)[0]
            if len(idx) > 0:
                reach_view.reach_length._data[idx[0]] = length

        return {
            "reconstructed": len(result_ids),
            "entity_ids": result_ids,
            "attribute": "reach.reach_length",
        }

    def _reconstruct_node_wse(
        self,
        node_ids: Optional[List[int]] = None,
        force: bool = False,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """
        Reconstruct node wse from centerline elevations.

        For now, this is a placeholder that uses existing node wse.
        Full reconstruction would require the original MERIT Hydro data.
        """
        logger.info("Node wse reconstruction requires MERIT Hydro source data")

        # Without source data, we can only return existing values
        return {
            "reconstructed": 0,
            "entity_ids": [],
            "attribute": "node.wse",
            "note": "Requires MERIT Hydro source data",
        }

    def _reconstruct_node_facc(
        self,
        node_ids: Optional[List[int]] = None,
        force: bool = False,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """
        Reconstruct node facc from MERIT Hydro.

        Placeholder - requires source data.
        """
        logger.info("Node facc reconstruction requires MERIT Hydro source data")

        return {
            "reconstructed": 0,
            "entity_ids": [],
            "attribute": "node.facc",
            "note": "Requires MERIT Hydro source data",
        }

    def _reconstruct_node_dist_out(
        self,
        node_ids: Optional[List[int]] = None,
        force: bool = False,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """
        Reconstruct node dist_out from parent reach dist_out.
        """
        logger.info("Reconstructing node.dist_out from reach values")

        where_clause = ""
        params = [self._region]

        if node_ids is not None:
            placeholders = ", ".join(["?"] * len(node_ids))
            where_clause = f"AND n.node_id IN ({placeholders})"
            params.extend(node_ids)

        # Get reach dist_out for each node
        result_df = self._conn.execute(
            f"""
            SELECT
                n.node_id,
                r.dist_out as reach_dist_out
            FROM nodes n
            JOIN reaches r ON n.reach_id = r.reach_id AND n.region = r.region
            WHERE n.region = ? {where_clause}
        """,
            params,
        ).fetchdf()

        result_ids = result_df["node_id"].tolist()
        result_values = result_df["reach_dist_out"].values

        if dry_run:
            return {
                "reconstructed": len(result_ids),
                "entity_ids": result_ids,
                "attribute": "node.dist_out",
                "values": result_values,
            }

        # Update database
        for node_id, dist in zip(result_ids, result_values):
            if dist is not None and not np.isnan(dist):
                self._conn.execute(
                    """
                    UPDATE nodes SET dist_out = ? WHERE node_id = ? AND region = ?
                """,
                    [float(dist), node_id, self._region],
                )

        # Update in-memory array
        node_view = self._sword.nodes
        for node_id, dist in zip(result_ids, result_values):
            if dist is not None and not np.isnan(dist):
                idx = np.where(node_view.id == node_id)[0]
                if len(idx) > 0:
                    node_view.dist_out._data[idx[0]] = dist

        return {
            "reconstructed": len(result_ids),
            "entity_ids": result_ids,
            "attribute": "node.dist_out",
        }

    # =========================================================================
    # ADDITIONAL REACH RECONSTRUCTORS
    # =========================================================================

    def _reconstruct_reach_wse_var(
        self,
        reach_ids: Optional[List[int]] = None,
        force: bool = False,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """Reconstruct reach wse_var as variance of node elevations."""
        logger.info("Reconstructing reach.wse_var from node elevations")

        where_clause = ""
        params = [self._region]
        if reach_ids is not None:
            placeholders = ", ".join(["?"] * len(reach_ids))
            where_clause = f"AND n.reach_id IN ({placeholders})"
            params.extend(reach_ids)

        result_df = self._conn.execute(
            f"""
            SELECT n.reach_id, VARIANCE(n.wse) as wse_var
            FROM nodes n
            WHERE n.region = ? {where_clause}
            GROUP BY n.reach_id
        """,
            params,
        ).fetchdf()

        return self._update_reach_attribute("wse_var", result_df, dry_run)

    def _reconstruct_reach_width(
        self,
        reach_ids: Optional[List[int]] = None,
        force: bool = False,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """Reconstruct reach width as median of node widths."""
        logger.info("Reconstructing reach.width from node widths (median)")

        where_clause = ""
        params = [self._region]
        if reach_ids is not None:
            placeholders = ", ".join(["?"] * len(reach_ids))
            where_clause = f"AND n.reach_id IN ({placeholders})"
            params.extend(reach_ids)

        result_df = self._conn.execute(
            f"""
            SELECT n.reach_id, MEDIAN(n.width) as width
            FROM nodes n
            WHERE n.region = ? {where_clause}
            GROUP BY n.reach_id
        """,
            params,
        ).fetchdf()

        return self._update_reach_attribute("width", result_df, dry_run)

    def _reconstruct_reach_width_var(
        self,
        reach_ids: Optional[List[int]] = None,
        force: bool = False,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """Reconstruct reach width_var as variance of node widths."""
        logger.info("Reconstructing reach.width_var from node widths")

        where_clause = ""
        params = [self._region]
        if reach_ids is not None:
            placeholders = ", ".join(["?"] * len(reach_ids))
            where_clause = f"AND n.reach_id IN ({placeholders})"
            params.extend(reach_ids)

        result_df = self._conn.execute(
            f"""
            SELECT n.reach_id, VARIANCE(n.width) as width_var
            FROM nodes n
            WHERE n.region = ? {where_clause}
            GROUP BY n.reach_id
        """,
            params,
        ).fetchdf()

        return self._update_reach_attribute("width_var", result_df, dry_run)

    def _reconstruct_reach_x(
        self,
        reach_ids: Optional[List[int]] = None,
        force: bool = False,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """Reconstruct reach x (centroid longitude) as mean of centerline x."""
        logger.info("Reconstructing reach.x from centerline coordinates (mean)")

        where_clause = ""
        params = [self._region]
        if reach_ids is not None:
            placeholders = ", ".join(["?"] * len(reach_ids))
            where_clause = f"AND c.reach_id IN ({placeholders})"
            params.extend(reach_ids)

        result_df = self._conn.execute(
            f"""
            SELECT c.reach_id, AVG(c.x) as x
            FROM centerlines c
            WHERE c.region = ? {where_clause}
            GROUP BY c.reach_id
        """,
            params,
        ).fetchdf()

        return self._update_reach_attribute("x", result_df, dry_run)

    def _reconstruct_reach_y(
        self,
        reach_ids: Optional[List[int]] = None,
        force: bool = False,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """Reconstruct reach y (centroid latitude) as mean of centerline y."""
        logger.info("Reconstructing reach.y from centerline coordinates (mean)")

        where_clause = ""
        params = [self._region]
        if reach_ids is not None:
            placeholders = ", ".join(["?"] * len(reach_ids))
            where_clause = f"AND c.reach_id IN ({placeholders})"
            params.extend(reach_ids)

        result_df = self._conn.execute(
            f"""
            SELECT c.reach_id, AVG(c.y) as y
            FROM centerlines c
            WHERE c.region = ? {where_clause}
            GROUP BY c.reach_id
        """,
            params,
        ).fetchdf()

        return self._update_reach_attribute("y", result_df, dry_run)

    def _reconstruct_reach_x_min(
        self,
        reach_ids: Optional[List[int]] = None,
        force: bool = False,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """Reconstruct reach x_min (bounding box) as min of centerline x."""
        logger.info("Reconstructing reach.x_min from centerline coordinates")

        where_clause = ""
        params = [self._region]
        if reach_ids is not None:
            placeholders = ", ".join(["?"] * len(reach_ids))
            where_clause = f"AND c.reach_id IN ({placeholders})"
            params.extend(reach_ids)

        result_df = self._conn.execute(
            f"""
            SELECT c.reach_id, MIN(c.x) as x_min
            FROM centerlines c
            WHERE c.region = ? {where_clause}
            GROUP BY c.reach_id
        """,
            params,
        ).fetchdf()

        return self._update_reach_attribute("x_min", result_df, dry_run)

    def _reconstruct_reach_x_max(
        self,
        reach_ids: Optional[List[int]] = None,
        force: bool = False,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """Reconstruct reach x_max (bounding box) as max of centerline x."""
        logger.info("Reconstructing reach.x_max from centerline coordinates")

        where_clause = ""
        params = [self._region]
        if reach_ids is not None:
            placeholders = ", ".join(["?"] * len(reach_ids))
            where_clause = f"AND c.reach_id IN ({placeholders})"
            params.extend(reach_ids)

        result_df = self._conn.execute(
            f"""
            SELECT c.reach_id, MAX(c.x) as x_max
            FROM centerlines c
            WHERE c.region = ? {where_clause}
            GROUP BY c.reach_id
        """,
            params,
        ).fetchdf()

        return self._update_reach_attribute("x_max", result_df, dry_run)

    def _reconstruct_reach_y_min(
        self,
        reach_ids: Optional[List[int]] = None,
        force: bool = False,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """Reconstruct reach y_min (bounding box) as min of centerline y."""
        logger.info("Reconstructing reach.y_min from centerline coordinates")

        where_clause = ""
        params = [self._region]
        if reach_ids is not None:
            placeholders = ", ".join(["?"] * len(reach_ids))
            where_clause = f"AND c.reach_id IN ({placeholders})"
            params.extend(reach_ids)

        result_df = self._conn.execute(
            f"""
            SELECT c.reach_id, MIN(c.y) as y_min
            FROM centerlines c
            WHERE c.region = ? {where_clause}
            GROUP BY c.reach_id
        """,
            params,
        ).fetchdf()

        return self._update_reach_attribute("y_min", result_df, dry_run)

    def _reconstruct_reach_y_max(
        self,
        reach_ids: Optional[List[int]] = None,
        force: bool = False,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """Reconstruct reach y_max (bounding box) as max of centerline y."""
        logger.info("Reconstructing reach.y_max from centerline coordinates")

        where_clause = ""
        params = [self._region]
        if reach_ids is not None:
            placeholders = ", ".join(["?"] * len(reach_ids))
            where_clause = f"AND c.reach_id IN ({placeholders})"
            params.extend(reach_ids)

        result_df = self._conn.execute(
            f"""
            SELECT c.reach_id, MAX(c.y) as y_max
            FROM centerlines c
            WHERE c.region = ? {where_clause}
            GROUP BY c.reach_id
        """,
            params,
        ).fetchdf()

        return self._update_reach_attribute("y_max", result_df, dry_run)

    def _reconstruct_reach_n_nodes(
        self,
        reach_ids: Optional[List[int]] = None,
        force: bool = False,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """Reconstruct reach n_nodes as count of nodes per reach."""
        logger.info("Reconstructing reach.n_nodes from node count")

        where_clause = ""
        params = [self._region]
        if reach_ids is not None:
            placeholders = ", ".join(["?"] * len(reach_ids))
            where_clause = f"AND n.reach_id IN ({placeholders})"
            params.extend(reach_ids)

        result_df = self._conn.execute(
            f"""
            SELECT n.reach_id, COUNT(DISTINCT n.node_id) as n_nodes
            FROM nodes n
            WHERE n.region = ? {where_clause}
            GROUP BY n.reach_id
        """,
            params,
        ).fetchdf()

        return self._update_reach_attribute("n_nodes", result_df, dry_run)

    def _reconstruct_reach_n_rch_up(
        self,
        reach_ids: Optional[List[int]] = None,
        force: bool = False,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """Reconstruct reach n_rch_up from topology."""
        logger.info("Reconstructing reach.n_rch_up from topology")

        where_clause = ""
        params = [self._region]
        if reach_ids is not None:
            placeholders = ", ".join(["?"] * len(reach_ids))
            where_clause = f"AND t.reach_id IN ({placeholders})"
            params.extend(reach_ids)

        result_df = self._conn.execute(
            f"""
            SELECT t.reach_id, COUNT(*) as n_rch_up
            FROM reach_topology t
            WHERE t.region = ? AND t.direction = 'up' {where_clause}
            GROUP BY t.reach_id
        """,
            params,
        ).fetchdf()

        return self._update_reach_attribute("n_rch_up", result_df, dry_run)

    def _reconstruct_reach_n_rch_down(
        self,
        reach_ids: Optional[List[int]] = None,
        force: bool = False,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """Reconstruct reach n_rch_down from topology."""
        logger.info("Reconstructing reach.n_rch_down from topology")

        where_clause = ""
        params = [self._region]
        if reach_ids is not None:
            placeholders = ", ".join(["?"] * len(reach_ids))
            where_clause = f"AND t.reach_id IN ({placeholders})"
            params.extend(reach_ids)

        result_df = self._conn.execute(
            f"""
            SELECT t.reach_id, COUNT(*) as n_rch_down
            FROM reach_topology t
            WHERE t.region = ? AND t.direction = 'down' {where_clause}
            GROUP BY t.reach_id
        """,
            params,
        ).fetchdf()

        return self._update_reach_attribute("n_rch_down", result_df, dry_run)

    def _reconstruct_reach_lakeflag(
        self,
        reach_ids: Optional[List[int]] = None,
        force: bool = False,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """Reconstruct reach lakeflag as mode of node lakeflag values."""
        logger.info("Reconstructing reach.lakeflag from node values (mode)")

        where_clause = ""
        params = [self._region]
        if reach_ids is not None:
            placeholders = ", ".join(["?"] * len(reach_ids))
            where_clause = f"AND n.reach_id IN ({placeholders})"
            params.extend(reach_ids)

        # Mode = most frequent value
        result_df = self._conn.execute(
            f"""
            SELECT reach_id, lakeflag
            FROM (
                SELECT n.reach_id, n.lakeflag, COUNT(*) as cnt,
                       ROW_NUMBER() OVER (PARTITION BY n.reach_id ORDER BY COUNT(*) DESC) as rn
                FROM nodes n
                WHERE n.region = ? {where_clause}
                GROUP BY n.reach_id, n.lakeflag
            )
            WHERE rn = 1
        """,
            params,
        ).fetchdf()

        return self._update_reach_attribute("lakeflag", result_df, dry_run)

    def _reconstruct_reach_n_chan_max(
        self,
        reach_ids: Optional[List[int]] = None,
        force: bool = False,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """Reconstruct reach n_chan_max as max of node n_chan_max."""
        logger.info("Reconstructing reach.n_chan_max from node values (max)")

        where_clause = ""
        params = [self._region]
        if reach_ids is not None:
            placeholders = ", ".join(["?"] * len(reach_ids))
            where_clause = f"AND n.reach_id IN ({placeholders})"
            params.extend(reach_ids)

        result_df = self._conn.execute(
            f"""
            SELECT n.reach_id, MAX(n.n_chan_max) as n_chan_max
            FROM nodes n
            WHERE n.region = ? {where_clause}
            GROUP BY n.reach_id
        """,
            params,
        ).fetchdf()

        return self._update_reach_attribute("n_chan_max", result_df, dry_run)

    def _reconstruct_reach_n_chan_mod(
        self,
        reach_ids: Optional[List[int]] = None,
        force: bool = False,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """Reconstruct reach n_chan_mod as mode of node n_chan_mod."""
        logger.info("Reconstructing reach.n_chan_mod from node values (mode)")

        where_clause = ""
        params = [self._region]
        if reach_ids is not None:
            placeholders = ", ".join(["?"] * len(reach_ids))
            where_clause = f"AND n.reach_id IN ({placeholders})"
            params.extend(reach_ids)

        result_df = self._conn.execute(
            f"""
            SELECT reach_id, n_chan_mod
            FROM (
                SELECT n.reach_id, n.n_chan_mod, COUNT(*) as cnt,
                       ROW_NUMBER() OVER (PARTITION BY n.reach_id ORDER BY COUNT(*) DESC) as rn
                FROM nodes n
                WHERE n.region = ? {where_clause}
                GROUP BY n.reach_id, n.n_chan_mod
            )
            WHERE rn = 1
        """,
            params,
        ).fetchdf()

        return self._update_reach_attribute("n_chan_mod", result_df, dry_run)

    # =========================================================================
    # NETWORK ANALYSIS RECONSTRUCTORS
    # =========================================================================

    def _reconstruct_reach_stream_order(
        self,
        reach_ids: Optional[List[int]] = None,
        force: bool = False,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """
        Reconstruct stream_order from path_freq using: round(log(path_freq)) + 1.

        Stream order is a logarithmic transformation of path frequency.
        """
        logger.info("Reconstructing reach.stream_order from path_freq (log transform)")

        where_clause = ""
        params = [self._region]
        if reach_ids is not None:
            placeholders = ", ".join(["?"] * len(reach_ids))
            where_clause = f"AND reach_id IN ({placeholders})"
            params.extend(reach_ids)

        # Get path_freq values
        result_df = self._conn.execute(
            f"""
            SELECT reach_id, path_freq
            FROM reaches
            WHERE region = ? AND path_freq > 0 {where_clause}
        """,
            params,
        ).fetchdf()

        if len(result_df) == 0:
            return {
                "reconstructed": 0,
                "entity_ids": [],
                "attribute": "reach.stream_order",
            }

        # Compute stream_order = round(log(path_freq)) + 1
        result_df["stream_order"] = np.round(np.log(result_df["path_freq"])) + 1
        result_df["stream_order"] = result_df["stream_order"].astype(int)

        return self._update_reach_attribute("stream_order", result_df, dry_run)

    def _reconstruct_reach_path_freq(
        self,
        reach_ids: Optional[List[int]] = None,
        force: bool = False,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """
        Reconstruct path_freq using graph traversal from outlets.

        Algorithm:
        1. Find all outlets (n_rch_down = 0)
        2. BFS from outlets upstream, incrementing path_freq for each visit
        3. Each reach's path_freq = number of downstream paths that pass through it
        """
        logger.info("Reconstructing reach.path_freq via graph traversal")

        # Get topology
        topology_df = self._conn.execute(
            """
            SELECT reach_id, direction, neighbor_reach_id
            FROM reach_topology
            WHERE region = ?
        """,
            [self._region],
        ).fetchdf()

        # Build adjacency maps
        upstream_map = {}  # reach_id -> list of upstream reach_ids
        downstream_map = {}  # reach_id -> list of downstream reach_ids

        for _, row in topology_df.iterrows():
            reach = row["reach_id"]
            neighbor = row["neighbor_reach_id"]
            direction = row["direction"]

            if direction == "up":
                if reach not in upstream_map:
                    upstream_map[reach] = []
                upstream_map[reach].append(neighbor)
            else:
                if reach not in downstream_map:
                    downstream_map[reach] = []
                downstream_map[reach].append(neighbor)

        # Get all reaches
        reaches_df = self._conn.execute(
            """
            SELECT reach_id FROM reaches WHERE region = ?
        """,
            [self._region],
        ).fetchdf()
        all_reaches = set(reaches_df["reach_id"])

        # Find outlets (no downstream neighbors)
        outlets = all_reaches - set(downstream_map.keys())

        # Initialize path_freq to 0
        path_freq = {rid: 0 for rid in all_reaches}

        # BFS from each outlet, counting visits
        for outlet_id in outlets:
            visited = set()
            queue = [outlet_id]

            while queue:
                reach_id = queue.pop(0)
                if reach_id in visited:
                    continue
                visited.add(reach_id)
                path_freq[reach_id] += 1

                # Add upstream reaches
                for upstream_id in upstream_map.get(reach_id, []):
                    if upstream_id not in visited:
                        queue.append(upstream_id)

        # Repair connected reaches still at path_freq=0 (side channels, topology gaps)
        needs_repair = {
            rid
            for rid in all_reaches
            if path_freq[rid] == 0 and (rid in upstream_map or rid in downstream_map)
        }
        for _ in range(10):
            if not needs_repair:
                break
            repaired = set()
            for rid in needs_repair:
                # Try downstream neighbor's path_freq first
                dn_pf = [
                    path_freq[n]
                    for n in downstream_map.get(rid, [])
                    if path_freq[n] > 0
                ]
                if dn_pf:
                    path_freq[rid] = max(dn_pf)
                    repaired.add(rid)
                    continue
                # Try summing upstream neighbors
                up_neighbors = upstream_map.get(rid, [])
                if up_neighbors and all(path_freq[n] > 0 for n in up_neighbors):
                    path_freq[rid] = sum(path_freq[n] for n in up_neighbors)
                    repaired.add(rid)
            needs_repair -= repaired
            if not repaired:
                break
        if needs_repair:
            logger.warning(
                f"path_freq: {len(needs_repair)} reaches still at 0 after repair"
            )

        # Filter to requested reach_ids
        if reach_ids is not None:
            path_freq = {k: v for k, v in path_freq.items() if k in reach_ids}

        result_df = self._conn.execute(
            f"""
            SELECT reach_id FROM reaches WHERE region = ?
            {"AND reach_id IN (" + ",".join(["?"] * len(reach_ids)) + ")" if reach_ids else ""}
        """,
            [self._region] + (list(reach_ids) if reach_ids else []),
        ).fetchdf()

        result_df["path_freq"] = result_df["reach_id"].map(path_freq)

        return self._update_reach_attribute("path_freq", result_df, dry_run)

    def _reconstruct_reach_end_reach(
        self,
        reach_ids: Optional[List[int]] = None,
        force: bool = False,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """
        Reconstruct end_reach type from topology.

        Values: 0=main, 1=headwater, 2=outlet, 3=junction
        """
        logger.info("Reconstructing reach.end_reach from topology")

        # Get topology counts
        topology_df = self._conn.execute(
            """
            SELECT
                r.reach_id,
                COALESCE(up.n_up, 0) as n_up,
                COALESCE(down.n_down, 0) as n_down
            FROM reaches r
            LEFT JOIN (
                SELECT reach_id, COUNT(*) as n_up
                FROM reach_topology
                WHERE region = ? AND direction = 'up'
                GROUP BY reach_id
            ) up ON r.reach_id = up.reach_id
            LEFT JOIN (
                SELECT reach_id, COUNT(*) as n_down
                FROM reach_topology
                WHERE region = ? AND direction = 'down'
                GROUP BY reach_id
            ) down ON r.reach_id = down.reach_id
            WHERE r.region = ?
        """,
            [self._region, self._region, self._region],
        ).fetchdf()

        # Classify: headwater (no upstream), outlet (no downstream),
        # junction (confluence OR bifurcation), main (else)
        def classify_reach(row):
            n_up = row["n_up"]
            n_down = row["n_down"]
            if n_up == 0:
                return 1  # headwater
            elif n_down == 0:
                return 2  # outlet
            elif n_up > 1 or n_down > 1:
                return 3  # junction (confluence OR bifurcation)
            else:
                return 0  # main

        topology_df["end_reach"] = topology_df.apply(classify_reach, axis=1)

        if reach_ids is not None:
            topology_df = topology_df[topology_df["reach_id"].isin(reach_ids)]

        return self._update_reach_attribute("end_reach", topology_df, dry_run)

    def _reconstruct_reach_network(
        self,
        reach_ids: Optional[List[int]] = None,
        force: bool = False,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """
        Reconstruct network ID using connected components.

        Each connected subgraph gets a unique network ID.
        """
        logger.info("Reconstructing reach.network via connected components")

        # Get topology
        topology_df = self._conn.execute(
            """
            SELECT reach_id, neighbor_reach_id
            FROM reach_topology
            WHERE region = ?
        """,
            [self._region],
        ).fetchdf()

        # Get all reaches
        reaches_df = self._conn.execute(
            """
            SELECT reach_id FROM reaches WHERE region = ?
        """,
            [self._region],
        ).fetchdf()
        all_reaches = set(reaches_df["reach_id"])

        # Build adjacency (undirected)
        adjacency = {rid: set() for rid in all_reaches}
        for _, row in topology_df.iterrows():
            reach = row["reach_id"]
            neighbor = row["neighbor_reach_id"]
            if reach in adjacency and neighbor in all_reaches:
                adjacency[reach].add(neighbor)
                adjacency[neighbor].add(reach)

        # Find connected components using BFS
        network_ids = {}
        visited = set()
        current_network = 1

        for start_reach in all_reaches:
            if start_reach in visited:
                continue

            # BFS from this reach
            queue = [start_reach]
            component = []

            while queue:
                reach_id = queue.pop(0)
                if reach_id in visited:
                    continue
                visited.add(reach_id)
                component.append(reach_id)

                for neighbor in adjacency.get(reach_id, []):
                    if neighbor not in visited:
                        queue.append(neighbor)

            # Assign network ID to all reaches in component
            for rid in component:
                network_ids[rid] = current_network

            current_network += 1

        # Build result dataframe
        result_df = reaches_df.copy()
        result_df["network"] = result_df["reach_id"].map(network_ids)

        if reach_ids is not None:
            result_df = result_df[result_df["reach_id"].isin(reach_ids)]

        return self._update_reach_attribute("network", result_df, dry_run)

    # =========================================================================
    # SINUOSITY AND MEANDER RECONSTRUCTORS
    # =========================================================================

    def _reconstruct_node_sinuosity(
        self,
        node_ids: Optional[List[int]] = None,
        force: bool = False,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """
        Reconstruct node sinuosity from centerline geometry.

        Sinuosity = arc_length / straight_line_distance

        Based on Leopold and Wolman (1960):
        - Meander wavelength ≈ 10 * river width
        - Sinuosity evaluated over half-wavelength distance

        Algorithm:
        1. For each node, get centerline points
        2. Calculate cumulative distance along centerline
        3. Calculate straight-line distance between node endpoints
        4. sinuosity = cumulative_distance / straight_line_distance
        """
        logger.info("Reconstructing node.sinuosity from centerline geometry")

        where_clause = ""
        params = [self._region]
        if node_ids is not None:
            placeholders = ", ".join(["?"] * len(node_ids))
            where_clause = f"AND c.node_id IN ({placeholders})"
            params.extend(node_ids)

        # Get centerline points per node, ordered
        cl_df = self._conn.execute(
            f"""
            SELECT c.node_id, c.x, c.y, c.cl_id
            FROM centerlines c
            WHERE c.region = ? {where_clause}
            ORDER BY c.node_id, c.cl_id
        """,
            params,
        ).fetchdf()

        if len(cl_df) == 0:
            return {"reconstructed": 0, "entity_ids": [], "attribute": "node.sinuosity"}

        # Calculate sinuosity per node
        sinuosity_results = []
        for node_id, group in cl_df.groupby("node_id"):
            x = group["x"].values
            y = group["y"].values

            if len(x) < 2:
                sinuosity_results.append({"node_id": node_id, "sinuosity": 1.0})
                continue

            # Calculate arc length (cumulative distance along centerline)
            dx = np.diff(x)
            dy = np.diff(y)
            # Convert to approximate meters (rough, assuming ~111km per degree)
            segment_lengths = np.sqrt(
                (dx * 111000) ** 2 + (dy * 111000 * np.cos(np.radians(np.mean(y)))) ** 2
            )
            arc_length = np.sum(segment_lengths)

            # Calculate straight-line distance between endpoints
            straight_line = np.sqrt(
                ((x[-1] - x[0]) * 111000) ** 2
                + ((y[-1] - y[0]) * 111000 * np.cos(np.radians(np.mean(y)))) ** 2
            )

            # Sinuosity = arc / straight (minimum 1.0)
            if straight_line > 0:
                sinuosity = max(1.0, arc_length / straight_line)
            else:
                sinuosity = 1.0

            sinuosity_results.append({"node_id": node_id, "sinuosity": sinuosity})

        import pandas as pd

        result_df = pd.DataFrame(sinuosity_results)

        return self._update_node_attribute("sinuosity", result_df, dry_run)

    def _reconstruct_node_meander_length(
        self,
        node_ids: Optional[List[int]] = None,
        force: bool = False,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """
        Reconstruct node meander_length from width.

        Based on Leopold and Wolman (1960) and Soar and Thorne (2001):
        meander_length ≈ 10 * width
        """
        logger.info("Reconstructing node.meander_length from width")

        where_clause = ""
        params = [self._region]
        if node_ids is not None:
            placeholders = ", ".join(["?"] * len(node_ids))
            where_clause = f"AND node_id IN ({placeholders})"
            params.extend(node_ids)

        # meander_length = 10 * width
        result_df = self._conn.execute(
            f"""
            SELECT node_id, width * 10 as meander_length
            FROM nodes
            WHERE region = ? AND width > 0 {where_clause}
        """,
            params,
        ).fetchdf()

        return self._update_node_attribute("meander_length", result_df, dry_run)

    def _reconstruct_node_node_length(
        self,
        node_ids: Optional[List[int]] = None,
        force: bool = False,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """
        Reconstruct node_length as the arc length of centerline points within the node.
        """
        logger.info("Reconstructing node.node_length from centerline geometry")

        where_clause = ""
        params = [self._region]
        if node_ids is not None:
            placeholders = ", ".join(["?"] * len(node_ids))
            where_clause = f"AND c.node_id IN ({placeholders})"
            params.extend(node_ids)

        # Get centerline points per node
        cl_df = self._conn.execute(
            f"""
            SELECT c.node_id, c.x, c.y, c.cl_id
            FROM centerlines c
            WHERE c.region = ? {where_clause}
            ORDER BY c.node_id, c.cl_id
        """,
            params,
        ).fetchdf()

        if len(cl_df) == 0:
            return {
                "reconstructed": 0,
                "entity_ids": [],
                "attribute": "node.node_length",
            }

        # Calculate length per node
        length_results = []
        for node_id, group in cl_df.groupby("node_id"):
            x = group["x"].values
            y = group["y"].values

            if len(x) < 2:
                length_results.append({"node_id": node_id, "node_length": 0.0})
                continue

            # Calculate arc length in meters
            dx = np.diff(x)
            dy = np.diff(y)
            segment_lengths = np.sqrt(
                (dx * 111000) ** 2 + (dy * 111000 * np.cos(np.radians(np.mean(y)))) ** 2
            )
            arc_length = np.sum(segment_lengths)

            length_results.append({"node_id": node_id, "node_length": arc_length})

        import pandas as pd

        result_df = pd.DataFrame(length_results)

        return self._update_node_attribute("node_length", result_df, dry_run)

    # =========================================================================
    # ADDITIONAL NODE RECONSTRUCTORS
    # =========================================================================

    def _reconstruct_node_x(
        self,
        node_ids: Optional[List[int]] = None,
        force: bool = False,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """Reconstruct node x (longitude) as mean of centerline x."""
        logger.info("Reconstructing node.x from centerline coordinates")

        where_clause = ""
        params = [self._region]
        if node_ids is not None:
            placeholders = ", ".join(["?"] * len(node_ids))
            where_clause = f"AND c.node_id IN ({placeholders})"
            params.extend(node_ids)

        result_df = self._conn.execute(
            f"""
            SELECT c.node_id, AVG(c.x) as x
            FROM centerlines c
            WHERE c.region = ? {where_clause}
            GROUP BY c.node_id
        """,
            params,
        ).fetchdf()

        return self._update_node_attribute("x", result_df, dry_run)

    def _reconstruct_node_y(
        self,
        node_ids: Optional[List[int]] = None,
        force: bool = False,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """Reconstruct node y (latitude) as mean of centerline y."""
        logger.info("Reconstructing node.y from centerline coordinates")

        where_clause = ""
        params = [self._region]
        if node_ids is not None:
            placeholders = ", ".join(["?"] * len(node_ids))
            where_clause = f"AND c.node_id IN ({placeholders})"
            params.extend(node_ids)

        result_df = self._conn.execute(
            f"""
            SELECT c.node_id, AVG(c.y) as y
            FROM centerlines c
            WHERE c.region = ? {where_clause}
            GROUP BY c.node_id
        """,
            params,
        ).fetchdf()

        return self._update_node_attribute("y", result_df, dry_run)

    def _reconstruct_node_inherited(self, attr_name: str) -> Callable:
        """
        Factory function that returns a reconstructor for inherited node attributes.

        These attributes are copied directly from the parent reach.
        """

        def reconstructor(
            node_ids: Optional[List[int]] = None,
            force: bool = False,
            dry_run: bool = False,
        ) -> Dict[str, Any]:
            _ = force  # unused but part of interface
            logger.info(f"Reconstructing node.{attr_name} from parent reach")

            where_clause = ""
            params: List[Any] = [self._region]
            if node_ids is not None:
                placeholders = ", ".join(["?"] * len(node_ids))
                where_clause = f"AND n.node_id IN ({placeholders})"
                params.extend(node_ids)

            result_df = self._conn.execute(
                f"""
                SELECT n.node_id, r.{attr_name} as {attr_name}
                FROM nodes n
                JOIN reaches r ON n.reach_id = r.reach_id AND n.region = r.region
                WHERE n.region = ? {where_clause}
            """,
                params,
            ).fetchdf()

            return self._update_node_attribute(attr_name, result_df, dry_run)

        return reconstructor

    # =========================================================================
    # HELPER METHODS
    # =========================================================================

    def _update_reach_attribute(
        self,
        attr_name: str,
        result_df,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """Helper to update a reach attribute from a result dataframe."""
        result_ids = result_df["reach_id"].tolist()
        result_values = result_df[attr_name].values

        if dry_run:
            return {
                "reconstructed": len(result_ids),
                "entity_ids": result_ids,
                "attribute": f"reach.{attr_name}",
                "values": result_values,
            }

        # Update database
        for reach_id, value in zip(result_ids, result_values):
            if value is not None and not (isinstance(value, float) and np.isnan(value)):
                self._conn.execute(
                    f"""
                    UPDATE reaches SET {attr_name} = ? WHERE reach_id = ? AND region = ?
                """,
                    [
                        float(value)
                        if isinstance(value, (int, float, np.integer, np.floating))
                        else value,
                        reach_id,
                        self._region,
                    ],
                )

        # Update in-memory array
        reach_view = self._sword.reaches
        if hasattr(reach_view, attr_name):
            attr_array = getattr(reach_view, attr_name)
            for reach_id, value in zip(result_ids, result_values):
                if value is not None and not (
                    isinstance(value, float) and np.isnan(value)
                ):
                    idx = np.where(reach_view.id == reach_id)[0]
                    if len(idx) > 0:
                        attr_array._data[idx[0]] = value

        return {
            "reconstructed": len(result_ids),
            "entity_ids": result_ids,
            "attribute": f"reach.{attr_name}",
        }

    def _update_node_attribute(
        self,
        attr_name: str,
        result_df,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """Helper to update a node attribute from a result dataframe."""
        result_ids = result_df["node_id"].tolist()
        result_values = result_df[attr_name].values

        if dry_run:
            return {
                "reconstructed": len(result_ids),
                "entity_ids": result_ids,
                "attribute": f"node.{attr_name}",
                "values": result_values,
            }

        # Update database
        for node_id, value in zip(result_ids, result_values):
            if value is not None and not (isinstance(value, float) and np.isnan(value)):
                self._conn.execute(
                    f"""
                    UPDATE nodes SET {attr_name} = ? WHERE node_id = ? AND region = ?
                """,
                    [
                        float(value)
                        if isinstance(value, (int, float, np.integer, np.floating))
                        else value,
                        node_id,
                        self._region,
                    ],
                )

        # Update in-memory array
        node_view = self._sword.nodes
        if hasattr(node_view, attr_name):
            attr_array = getattr(node_view, attr_name)
            for node_id, value in zip(result_ids, result_values):
                if value is not None and not (
                    isinstance(value, float) and np.isnan(value)
                ):
                    idx = np.where(node_view.id == node_id)[0]
                    if len(idx) > 0:
                        attr_array._data[idx[0]] = value

        return {
            "reconstructed": len(result_ids),
            "entity_ids": result_ids,
            "attribute": f"node.{attr_name}",
        }

    # =========================================================================
    # ADDITIONAL RECONSTRUCTORS (Width, Ext Dist, Sinuosity, etc.)
    # =========================================================================

    def _reconstruct_node_wth_coef(
        self,
        node_ids: Optional[List[int]] = None,
        force: bool = False,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """
        Reconstruct node wth_coef (width coefficient).

        Based on Attach_Fill_Variables.py:
        wth_coef = max_width / width
        If max_width not available or width <= 0, default to 1.0
        """
        logger.info("Reconstructing node.wth_coef from max_width/width ratio")

        where_clause = ""
        params = [self._region]
        if node_ids is not None:
            placeholders = ", ".join(["?"] * len(node_ids))
            where_clause = f"AND node_id IN ({placeholders})"
            params.extend(node_ids)

        # wth_coef = max_width / width, with safeguards
        result_df = self._conn.execute(
            f"""
            SELECT
                node_id,
                CASE
                    WHEN width > 0 AND max_width > 0 THEN max_width / width
                    ELSE 1.0
                END as wth_coef
            FROM nodes
            WHERE region = ? {where_clause}
        """,
            params,
        ).fetchdf()

        return self._update_node_attribute("wth_coef", result_df, dry_run)

    def _reconstruct_node_ext_dist_coef(
        self,
        node_ids: Optional[List[int]] = None,
        force: bool = False,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """
        Reconstruct node ext_dist_coef (extreme distance coefficient).

        Based on Identify_Lake_Nodes.py and Attach_Fill_Variables.py:
        - Default: 20 (no lake proximity, standard search distance)
        - If near lake:
            - Single channel (n_chan_max = 1): 1
            - Multi-channel (n_chan_max > 1): 2
        - If lakeflag >= 1 (in lake/reservoir): 5

        Note: Full algorithm requires external GRWL mask intersection with lakes.
        This simplified version uses lakeflag as a proxy.
        """
        logger.info("Reconstructing node.ext_dist_coef from lakeflag and channel count")

        where_clause = ""
        params = [self._region]
        if node_ids is not None:
            placeholders = ", ".join(["?"] * len(node_ids))
            where_clause = f"AND node_id IN ({placeholders})"
            params.extend(node_ids)

        # Simplified logic based on lakeflag
        result_df = self._conn.execute(
            f"""
            SELECT
                node_id,
                CASE
                    WHEN lakeflag >= 1 THEN 5  -- In lake/reservoir
                    WHEN n_chan_max > 1 THEN 2  -- Multi-channel (proxy for near lake)
                    ELSE 20  -- Default for rivers
                END as ext_dist_coef
            FROM nodes
            WHERE region = ? {where_clause}
        """,
            params,
        ).fetchdf()

        return self._update_node_attribute("ext_dist_coef", result_df, dry_run)

    def _reconstruct_node_max_width(
        self,
        node_ids: Optional[List[int]] = None,
        force: bool = False,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """
        Reconstruct node max_width.

        Full algorithm (from Attach_Fill_Variables.py attach_max_wth):
        - For multi-channel reaches: comes from external max_width raster
        - For single-channel: max_width = width

        This simplified version uses width as max_width when external data unavailable.
        """
        logger.info("Reconstructing node.max_width (simplified: from width)")

        where_clause = ""
        params = [self._region]
        if node_ids is not None:
            placeholders = ", ".join(["?"] * len(node_ids))
            where_clause = f"AND node_id IN ({placeholders})"
            params.extend(node_ids)

        # Simplified: max_width = width for single channel
        # Multi-channel would need external raster
        result_df = self._conn.execute(
            f"""
            SELECT
                node_id,
                CASE
                    WHEN n_chan_max > 1 AND max_width > 0 THEN max_width  -- Keep existing if set
                    ELSE width  -- Default to width
                END as max_width
            FROM nodes
            WHERE region = ? AND width > 0 {where_clause}
        """,
            params,
        ).fetchdf()

        return self._update_node_attribute("max_width", result_df, dry_run)

    def _reconstruct_node_trib_flag(
        self,
        node_ids: Optional[List[int]] = None,
        force: bool = False,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """Reconstruct node trib_flag from MHV spatial join."""
        mhv_dir = self._source_data_dir / "MHV_SWORD" if self._source_data_dir else None
        if mhv_dir is None or not mhv_dir.exists():
            logger.warning("node.trib_flag: MHV data not found. Preserving values.")
            return {
                "status": "skipped",
                "reason": "MHV not found",
                "updated": 0,
                "dry_run": dry_run,
            }

        import geopandas as gpd
        from scipy.spatial import cKDTree

        logger.info("Reconstructing node.trib_flag from MHV spatial join")

        where_clause = ""
        params = [self._region]
        if node_ids is not None:
            placeholders = ", ".join(["?"] * len(node_ids))
            where_clause = f"AND node_id IN ({placeholders})"
            params.extend(node_ids)

        nodes_df = self._conn.execute(
            f"SELECT node_id, x, y, reach_id FROM nodes WHERE region = ? {where_clause}",
            params,
        ).fetchdf()
        if nodes_df.empty:
            return {"status": "ok", "updated": 0, "dry_run": dry_run}

        nodes_df["basin"] = nodes_df["node_id"].astype(str).str[:2].astype(int)
        needed_basins = nodes_df["basin"].unique()

        mhv_files = sorted(mhv_dir.glob("*pts*"))
        if not mhv_files:
            mhv_files = sorted(mhv_dir.glob("*.gpkg")) + sorted(mhv_dir.glob("*.shp"))

        trib_flag = np.zeros(len(nodes_df), dtype=np.int32)

        for mhv_file in mhv_files:
            fname = mhv_file.stem
            basin_candidates = [
                int(s) for s in fname.split("_") if s.isdigit() and len(s) <= 2
            ]
            if not basin_candidates:
                continue
            file_basin = basin_candidates[-1]
            if file_basin not in needed_basins:
                continue

            try:
                mhv = gpd.read_file(mhv_file)
            except Exception as e:
                logger.warning(f"Failed to read {mhv_file}: {e}")
                continue

            if "sword_flag" in mhv.columns and "strmorder" in mhv.columns:
                mhv = mhv[(mhv["sword_flag"] == 0) & (mhv["strmorder"] >= 3)]
            if mhv.empty or "x" not in mhv.columns or "y" not in mhv.columns:
                continue

            kdt = cKDTree(np.column_stack([mhv["x"].values, mhv["y"].values]))
            basin_mask = nodes_df["basin"] == file_basin
            if not basin_mask.any():
                continue
            node_pts = np.column_stack(
                [
                    nodes_df.loc[basin_mask, "x"].values,
                    nodes_df.loc[basin_mask, "y"].values,
                ]
            )
            distances, _ = kdt.query(node_pts, k=1)
            trib_flag[basin_mask.values] = np.where(
                distances <= 0.003, 1, trib_flag[basin_mask.values]
            )

        nodes_df["trib_flag"] = trib_flag
        return self._update_node_attribute(
            "trib_flag", nodes_df[["node_id", "trib_flag"]], dry_run
        )

    def _reconstruct_node_obstr_type(
        self,
        node_ids: Optional[List[int]] = None,
        force: bool = False,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """Reconstruct node obstr_type from GROD spatial join (0=none, 1=dam, 2=low-head dam, 3=lock, 4=waterfall, 5=partial dam)."""
        where_clause = ""
        params = [self._region]
        if node_ids is not None:
            placeholders = ", ".join(["?"] * len(node_ids))
            where_clause = f"AND node_id IN ({placeholders})"
            params.extend(node_ids)

        nodes_df = self._conn.execute(
            f"SELECT node_id, x, y FROM nodes WHERE region = ? {where_clause}", params
        ).fetchdf()
        if nodes_df.empty:
            return {"status": "ok", "updated": 0, "dry_run": dry_run}

        _, obstr_types = self._load_grod_data(nodes_df)
        if obstr_types is None:
            logger.info(
                "Reconstructing node.obstr_type from parent reach (no GROD data)"
            )
            where_node = where_clause.replace("node_id", "n.node_id")
            result_df = self._conn.execute(
                f"""
                SELECT n.node_id,
                       CASE WHEN r.obstr_type > 5 THEN 0
                            ELSE COALESCE(r.obstr_type, 0) END as obstr_type
                FROM nodes n
                JOIN reaches r ON n.reach_id = r.reach_id AND n.region = r.region
                WHERE n.region = ? {where_node}
            """,
                [self._region] + (list(node_ids) if node_ids else []),
            ).fetchdf()
            return self._update_node_attribute("obstr_type", result_df, dry_run)

        logger.info("Reconstructing node.obstr_type from GROD spatial join")
        nodes_df["obstr_type"] = obstr_types
        return self._update_node_attribute(
            "obstr_type", nodes_df[["node_id", "obstr_type"]], dry_run
        )

    def _reconstruct_reach_max_width(
        self,
        reach_ids: Optional[List[int]] = None,
        force: bool = False,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """
        Reconstruct reach max_width as max of node max_widths.

        Based on Attach_Fill_Variables.py:
        reach.max_width = np.max(node.max_width[reach_nodes])
        """
        logger.info("Reconstructing reach.max_width from node max_widths")

        where_clause = ""
        params = [self._region]
        if reach_ids is not None:
            placeholders = ", ".join(["?"] * len(reach_ids))
            where_clause = f"AND n.reach_id IN ({placeholders})"
            params.extend(reach_ids)

        result_df = self._conn.execute(
            f"""
            SELECT
                n.reach_id,
                MAX(n.max_width) as max_width
            FROM nodes n
            WHERE n.region = ? AND n.max_width > 0 {where_clause}
            GROUP BY n.reach_id
        """,
            params,
        ).fetchdf()

        return self._update_reach_attribute("max_width", result_df, dry_run)

    def _reconstruct_reach_sinuosity(
        self,
        reach_ids: Optional[List[int]] = None,
        force: bool = False,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """
        Reconstruct reach sinuosity from centerline geometry.

        Based on CalculateSinuositySWORD.m and SinuosityMinAreaVarMinReach.m:
        sinuosity = arc_length / straight_line_distance

        For reach-level: average of node sinuosities or direct calculation from
        centerline endpoints.
        """
        logger.info("Reconstructing reach.sinuosity from centerline geometry")

        where_clause = ""
        params = [self._region]
        if reach_ids is not None:
            placeholders = ", ".join(["?"] * len(reach_ids))
            where_clause = f"AND c.reach_id IN ({placeholders})"
            params.extend(reach_ids)

        # Get reach centerlines
        cl_df = self._conn.execute(
            f"""
            SELECT c.reach_id, c.x, c.y, c.cl_id
            FROM centerlines c
            WHERE c.region = ? {where_clause}
            ORDER BY c.reach_id, c.cl_id
        """,
            params,
        ).fetchdf()

        if len(cl_df) == 0:
            return {
                "reconstructed": 0,
                "entity_ids": [],
                "attribute": "reach.sinuosity",
            }

        sinuosity_results = []
        for reach_id, group in cl_df.groupby("reach_id"):
            x = group["x"].values
            y = group["y"].values

            if len(x) < 2:
                sinuosity_results.append({"reach_id": reach_id, "sinuosity": 1.0})
                continue

            # Arc length (sum of segment lengths)
            dx = np.diff(x)
            dy = np.diff(y)
            mean_lat = np.mean(y)
            segment_lengths = np.sqrt(
                (dx * 111000) ** 2 + (dy * 111000 * np.cos(np.radians(mean_lat))) ** 2
            )
            arc_length = np.sum(segment_lengths)

            # Straight-line distance (first to last)
            straight_dx = x[-1] - x[0]
            straight_dy = y[-1] - y[0]
            straight_dist = np.sqrt(
                (straight_dx * 111000) ** 2
                + (straight_dy * 111000 * np.cos(np.radians(mean_lat))) ** 2
            )

            if straight_dist > 0:
                sinuosity = arc_length / straight_dist
            else:
                sinuosity = 1.0

            # Clamp to reasonable values
            sinuosity = max(1.0, min(sinuosity, 10.0))
            sinuosity_results.append({"reach_id": reach_id, "sinuosity": sinuosity})

        import pandas as pd

        result_df = pd.DataFrame(sinuosity_results)

        return self._update_reach_attribute("sinuosity", result_df, dry_run)

    def _reconstruct_reach_coastal_flag(
        self,
        reach_ids: Optional[List[int]] = None,
        force: bool = False,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """
        Reconstruct reach coastal_flag from node lakeflag distribution.

        Based on analysis of SWORD processing:
        coastal_flag = 1 if >25% of nodes have lakeflag >= 3 (tidal/coastal)

        lakeflag values:
        - 0: river
        - 1: lake
        - 2: canal
        - 3: tidal (coastal)
        """
        logger.info("Reconstructing reach.coastal_flag from node lakeflag distribution")

        where_clause = ""
        params = [self._region]
        if reach_ids is not None:
            placeholders = ", ".join(["?"] * len(reach_ids))
            where_clause = f"AND n.reach_id IN ({placeholders})"
            params.extend(reach_ids)

        # Calculate fraction of nodes with lakeflag >= 3
        result_df = self._conn.execute(
            f"""
            SELECT
                n.reach_id,
                CASE
                    WHEN COUNT(*) > 0 AND
                         SUM(CASE WHEN n.lakeflag >= 3 THEN 1 ELSE 0 END)::FLOAT / COUNT(*) > 0.25
                    THEN 1
                    ELSE 0
                END as coastal_flag
            FROM nodes n
            WHERE n.region = ? {where_clause}
            GROUP BY n.reach_id
        """,
            params,
        ).fetchdf()

        return self._update_reach_attribute("coastal_flag", result_df, dry_run)

    def _reconstruct_reach_low_slope_flag(
        self,
        reach_ids: Optional[List[int]] = None,
        force: bool = False,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """
        Reconstruct reach low_slope_flag from slope values.

        Based on SWORD processing:
        low_slope_flag = 1 if slope is too low for reliable discharge estimation.

        Typical threshold: slope < 0.00001 m/m (0.01 m/km)
        """
        logger.info("Reconstructing reach.low_slope_flag from slope values")

        where_clause = ""
        params = [self._region]
        if reach_ids is not None:
            placeholders = ", ".join(["?"] * len(reach_ids))
            where_clause = f"AND reach_id IN ({placeholders})"
            params.extend(reach_ids)

        # Threshold: 0.01 m/km (assuming slope stored in m/km)
        result_df = self._conn.execute(
            f"""
            SELECT
                reach_id,
                CASE
                    WHEN slope IS NULL OR slope < 0.01 THEN 1
                    ELSE 0
                END as low_slope_flag
            FROM reaches
            WHERE region = ? {where_clause}
        """,
            params,
        ).fetchdf()

        return self._update_reach_attribute("low_slope_flag", result_df, dry_run)

    def _reconstruct_reach_swot_obs(
        self,
        reach_ids: Optional[List[int]] = None,
        force: bool = False,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """
        Reconstruct reach swot_obs (SWOT observation count).

        Note: Full implementation requires external SWOT orbit data.
        This is a stub that returns 0 (unknown).
        """
        logger.info(
            "Reconstructing reach.swot_obs (stub - requires external SWOT data)"
        )

        where_clause = ""
        params = [self._region]
        if reach_ids is not None:
            placeholders = ", ".join(["?"] * len(reach_ids))
            where_clause = f"AND reach_id IN ({placeholders})"
            params.extend(reach_ids)

        # Stub: return current value or 0
        result_df = self._conn.execute(
            f"""
            SELECT
                reach_id,
                COALESCE(swot_obs, 0) as swot_obs
            FROM reaches
            WHERE region = ? {where_clause}
        """,
            params,
        ).fetchdf()

        return self._update_reach_attribute("swot_obs", result_df, dry_run)

    def _reconstruct_node_facc(
        self,
        node_ids: Optional[List[int]] = None,
        force: bool = False,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """
        Reconstruct node facc (flow accumulation).

        facc is interpolated from MERIT Hydro along the centerline.
        Full implementation requires external MERIT data.

        This simplified version inherits from reach.
        """
        logger.info("Reconstructing node.facc from parent reach")

        where_clause = ""
        params = [self._region]
        if node_ids is not None:
            placeholders = ", ".join(["?"] * len(node_ids))
            where_clause = f"AND n.node_id IN ({placeholders})"
            params.extend(node_ids)

        result_df = self._conn.execute(
            f"""
            SELECT n.node_id, r.facc as facc
            FROM nodes n
            JOIN reaches r ON n.reach_id = r.reach_id AND n.region = r.region
            WHERE n.region = ? {where_clause}
        """,
            params,
        ).fetchdf()

        return self._update_node_attribute("facc", result_df, dry_run)

    def _reconstruct_node_wse(
        self,
        node_ids: Optional[List[int]] = None,
        force: bool = False,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """
        Reconstruct node wse from centerline elevation interpolation.

        Full implementation requires MERIT Hydro DEM.
        This calculates from centerline mean for now.
        """
        logger.info("Reconstructing node.wse from centerlines")

        where_clause = ""
        params = [self._region]
        if node_ids is not None:
            placeholders = ", ".join(["?"] * len(node_ids))
            where_clause = f"AND c.node_id IN ({placeholders})"
            params.extend(node_ids)

        # Use existing wse if available, otherwise inherit from reach
        result_df = self._conn.execute(
            f"""
            SELECT
                n.node_id,
                COALESCE(n.wse, r.wse) as wse
            FROM nodes n
            JOIN reaches r ON n.reach_id = r.reach_id AND n.region = r.region
            WHERE n.region = ?
            {where_clause.replace("c.node_id", "n.node_id")}
        """,
            params,
        ).fetchdf()

        return self._update_node_attribute("wse", result_df, dry_run)

    def _reconstruct_node_dist_out(
        self,
        node_ids: Optional[List[int]] = None,
        force: bool = False,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """
        Reconstruct node dist_out from reach dist_out and node position.

        Algorithm:
        - Get reach dist_out
        - Interpolate within reach based on node position along centerline
        """
        logger.info("Reconstructing node.dist_out from reach and position")

        where_clause = ""
        params = [self._region]
        if node_ids is not None:
            placeholders = ", ".join(["?"] * len(node_ids))
            where_clause = f"AND n.node_id IN ({placeholders})"
            params.extend(node_ids)

        # Simplified: inherit from reach
        # Full implementation would interpolate based on position
        result_df = self._conn.execute(
            f"""
            SELECT n.node_id, r.dist_out as dist_out
            FROM nodes n
            JOIN reaches r ON n.reach_id = r.reach_id AND n.region = r.region
            WHERE n.region = ? {where_clause}
        """,
            params,
        ).fetchdf()

        return self._update_node_attribute("dist_out", result_df, dry_run)

    # =========================================================================
    # ADDITIONAL NODE RECONSTRUCTORS (from centerline aggregation)
    # =========================================================================

    def _reconstruct_node_lakeflag(
        self,
        node_ids: Optional[List[int]] = None,
        force: bool = False,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """
        Reconstruct node lakeflag as mode of centerline lakeflags.

        lakeflag values: 0=river, 1=lake, 2=canal, 3=tidal
        """
        logger.info("Reconstructing node.lakeflag from centerline mode")

        where_clause = ""
        params = [self._region]
        if node_ids is not None:
            placeholders = ", ".join(["?"] * len(node_ids))
            where_clause = f"AND c.node_id IN ({placeholders})"
            params.extend(node_ids)

        # Mode of lakeflag per node
        result_df = self._conn.execute(
            f"""
            SELECT
                c.node_id,
                MODE(c.lakeflag) as lakeflag
            FROM centerlines c
            WHERE c.region = ? {where_clause}
            GROUP BY c.node_id
        """,
            params,
        ).fetchdf()

        return self._update_node_attribute("lakeflag", result_df, dry_run)

    def _reconstruct_node_n_chan_max(
        self,
        node_ids: Optional[List[int]] = None,
        force: bool = False,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """
        STUB: n_chan_max is a native node attribute from GRWL (via NetCDF).
        It does NOT exist on centerlines — cannot be reconstructed from other data.
        Reach-level n_chan_max is reconstructed from nodes (see _reconstruct_reach_n_chan_max).
        """
        logger.warning(
            "node.n_chan_max is a source-level attribute (GRWL/NetCDF) — "
            "cannot be reconstructed. Preserving existing values."
        )
        return {"status": "stub", "reason": "source-level attribute, not derivable"}

    def _reconstruct_node_n_chan_mod(
        self,
        node_ids: Optional[List[int]] = None,
        force: bool = False,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """
        STUB: n_chan_mod is a native node attribute from GRWL (via NetCDF).
        It does NOT exist on centerlines — cannot be reconstructed from other data.
        Reach-level n_chan_mod is reconstructed from nodes (see _reconstruct_reach_n_chan_mod).
        """
        logger.warning(
            "node.n_chan_mod is a source-level attribute (GRWL/NetCDF) — "
            "cannot be reconstructed. Preserving existing values."
        )
        return {"status": "stub", "reason": "source-level attribute, not derivable"}

    def _reconstruct_node_width(
        self,
        node_ids: Optional[List[int]] = None,
        force: bool = False,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """
        Reconstruct node width as median of centerline widths.
        """
        logger.info("Reconstructing node.width from centerline median")

        where_clause = ""
        params = [self._region]
        if node_ids is not None:
            placeholders = ", ".join(["?"] * len(node_ids))
            where_clause = f"AND c.node_id IN ({placeholders})"
            params.extend(node_ids)

        result_df = self._conn.execute(
            f"""
            SELECT
                c.node_id,
                MEDIAN(c.width) as width
            FROM centerlines c
            WHERE c.region = ? AND c.width > 0 {where_clause}
            GROUP BY c.node_id
        """,
            params,
        ).fetchdf()

        return self._update_node_attribute("width", result_df, dry_run)

    def _reconstruct_node_width_var(
        self,
        node_ids: Optional[List[int]] = None,
        force: bool = False,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """
        Reconstruct node width_var as variance of centerline widths.
        """
        logger.info("Reconstructing node.width_var from centerline variance")

        where_clause = ""
        params = [self._region]
        if node_ids is not None:
            placeholders = ", ".join(["?"] * len(node_ids))
            where_clause = f"AND c.node_id IN ({placeholders})"
            params.extend(node_ids)

        result_df = self._conn.execute(
            f"""
            SELECT
                c.node_id,
                VAR_SAMP(c.width) as width_var
            FROM centerlines c
            WHERE c.region = ? AND c.width > 0 {where_clause}
            GROUP BY c.node_id
            HAVING COUNT(*) > 1
        """,
            params,
        ).fetchdf()

        return self._update_node_attribute("width_var", result_df, dry_run)

    def _reconstruct_node_wse_var(
        self,
        node_ids: Optional[List[int]] = None,
        force: bool = False,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """
        Reconstruct node wse_var as variance of centerline wse values.
        """
        logger.info("Reconstructing node.wse_var from centerline variance")

        where_clause = ""
        params = [self._region]
        if node_ids is not None:
            placeholders = ", ".join(["?"] * len(node_ids))
            where_clause = f"AND c.node_id IN ({placeholders})"
            params.extend(node_ids)

        result_df = self._conn.execute(
            f"""
            SELECT
                c.node_id,
                VAR_SAMP(c.wse) as wse_var
            FROM centerlines c
            WHERE c.region = ? AND c.wse IS NOT NULL {where_clause}
            GROUP BY c.node_id
            HAVING COUNT(*) > 1
        """,
            params,
        ).fetchdf()

        return self._update_node_attribute("wse_var", result_df, dry_run)

    # =========================================================================
    # ADDITIONAL REACH RECONSTRUCTORS
    # =========================================================================

    def _reconstruct_reach_main_side(
        self,
        reach_ids: Optional[List[int]] = None,
        force: bool = False,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """
        Reconstruct main_side from topology and path_freq.

        0 = Main channel (highest path_freq at bifurcations)
        1 = Side channel (lower path_freq at bifurcations)
        2 = Secondary outlet (outlet that isn't primary of its network)
        """
        logger.info("Reconstructing reach.main_side from topology + path_freq")

        topology_df = self._conn.execute(
            "SELECT reach_id, direction, neighbor_reach_id "
            "FROM reach_topology WHERE region = ?",
            [self._region],
        ).fetchdf()

        reaches_df = self._conn.execute(
            "SELECT reach_id, path_freq, facc, network FROM reaches WHERE region = ?",
            [self._region],
        ).fetchdf()

        pf_map = dict(zip(reaches_df["reach_id"], reaches_df["path_freq"]))
        facc_map = dict(zip(reaches_df["reach_id"], reaches_df["facc"]))
        network_map = dict(zip(reaches_df["reach_id"], reaches_df["network"]))
        all_reach_ids = set(reaches_df["reach_id"])

        downstream_map: Dict[int, list] = {}
        for _, row in topology_df.iterrows():
            if row["direction"] == "down":
                downstream_map.setdefault(row["reach_id"], []).append(
                    row["neighbor_reach_id"]
                )

        outlets = all_reach_ids - set(downstream_map.keys())

        # Primary outlet per network = highest facc
        primary_outlets: Dict[Any, int] = {}
        for oid in outlets:
            net = network_map.get(oid, -1)
            if net not in primary_outlets or facc_map.get(oid, 0) > facc_map.get(
                primary_outlets[net], 0
            ):
                primary_outlets[net] = oid

        main_side = {rid: 0 for rid in all_reach_ids}

        # Secondary outlets
        for oid in outlets:
            net = network_map.get(oid, -1)
            if primary_outlets.get(net) != oid:
                main_side[oid] = 2

        # Side channels: at bifurcations, lower path_freq downstream = side
        for rid, dn_neighbors in downstream_map.items():
            if len(dn_neighbors) > 1:
                sorted_dn = sorted(
                    dn_neighbors,
                    key=lambda n: pf_map.get(n, 0),
                    reverse=True,
                )
                for side_reach in sorted_dn[1:]:
                    main_side[side_reach] = 1

        if reach_ids is not None:
            main_side = {k: v for k, v in main_side.items() if k in reach_ids}

        result_df = self._conn.execute(
            "SELECT reach_id FROM reaches WHERE region = ?"
            + (
                " AND reach_id IN (" + ",".join(["?"] * len(reach_ids)) + ")"
                if reach_ids
                else ""
            ),
            [self._region] + (list(reach_ids) if reach_ids else []),
        ).fetchdf()
        result_df["main_side"] = result_df["reach_id"].map(main_side)

        return self._update_reach_attribute("main_side", result_df, dry_run)

    def _reconstruct_reach_obstr_type(
        self,
        reach_ids: Optional[List[int]] = None,
        force: bool = False,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """
        Reconstruct reach obstr_type as max of node obstr_types.

        obstr_type: 0=none, 1=dam, 2=low-head dam, 3=lock, 4=waterfall, 5=partial dam
        Values >5 are reset to 0.
        """
        logger.info("Reconstructing reach.obstr_type from node max")

        where_clause = ""
        params = [self._region]
        if reach_ids is not None:
            placeholders = ", ".join(["?"] * len(reach_ids))
            where_clause = f"AND n.reach_id IN ({placeholders})"
            params.extend(reach_ids)

        result_df = self._conn.execute(
            f"""
            SELECT
                n.reach_id,
                CASE
                    WHEN MAX(n.obstr_type) > 5 THEN 0
                    ELSE COALESCE(MAX(n.obstr_type), 0)
                END as obstr_type
            FROM nodes n
            WHERE n.region = ? {where_clause}
            GROUP BY n.reach_id
        """,
            params,
        ).fetchdf()

        return self._update_reach_attribute("obstr_type", result_df, dry_run)

    def _reconstruct_reach_path_order(
        self,
        reach_ids: Optional[List[int]] = None,
        force: bool = False,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """
        Reconstruct reach path_order (order within path from outlet).

        path_order: position within path from outlet (1) upstream.
        Legacy (path_variables_nc.py) derived from shortest-path traces.
        This approximation partitions by path_freq and orders by dist_out,
        which is equivalent for non-branching segments.
        """
        logger.info("Reconstructing reach.path_order from topology")

        where_clause = ""
        params = [self._region]
        if reach_ids is not None:
            placeholders = ", ".join(["?"] * len(reach_ids))
            where_clause = f"AND reach_id IN ({placeholders})"
            params.extend(reach_ids)

        # path_order is based on dist_out - lower dist_out = lower path_order
        # Order reaches by dist_out within each path
        result_df = self._conn.execute(
            f"""
            WITH ranked AS (
                SELECT
                    reach_id,
                    path_freq,
                    dist_out,
                    ROW_NUMBER() OVER (
                        PARTITION BY path_freq
                        ORDER BY dist_out ASC
                    ) as path_order
                FROM reaches
                WHERE region = ? {where_clause}
            )
            SELECT reach_id, path_order
            FROM ranked
        """,
            params,
        ).fetchdf()

        return self._update_reach_attribute("path_order", result_df, dry_run)

    def _reconstruct_reach_path_segs(
        self,
        reach_ids: Optional[List[int]] = None,
        force: bool = False,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """
        Reconstruct path_segs: unique segment ID per (path_order, path_freq) combo.

        Approximation of legacy stream_order.py. Assigns a sequential ID to each
        unique (path_order, path_freq) combination, ordered by path_order then path_freq.
        Reaches with invalid path_freq get path_segs=-9999.
        """
        logger.info("Reconstructing reach.path_segs from (path_order, path_freq)")

        where_clause = ""
        params = [self._region, self._region]
        if reach_ids is not None:
            placeholders = ", ".join(["?"] * len(reach_ids))
            where_clause = f"AND r.reach_id IN ({placeholders})"
            params.extend(reach_ids)

        result_df = self._conn.execute(
            f"""
            WITH combos AS (
                SELECT DISTINCT path_order, path_freq
                FROM reaches
                WHERE region = ? AND path_freq > 0
                ORDER BY path_order, path_freq
            ),
            numbered AS (
                SELECT path_order, path_freq,
                       ROW_NUMBER() OVER (ORDER BY path_order, path_freq) as path_segs
                FROM combos
            )
            SELECT r.reach_id,
                   COALESCE(n.path_segs, -9999) as path_segs
            FROM reaches r
            LEFT JOIN numbered n
                ON r.path_order = n.path_order AND r.path_freq = n.path_freq
            WHERE r.region = ? {where_clause}
        """,
            params,
        ).fetchdf()

        return self._update_reach_attribute("path_segs", result_df, dry_run)

    def _reconstruct_reach_trib_flag(
        self,
        reach_ids: Optional[List[int]] = None,
        force: bool = False,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """Reconstruct reach trib_flag as MAX of node trib_flags."""
        logger.info("Reconstructing reach.trib_flag from node max")
        where_clause = ""
        params = [self._region]
        if reach_ids is not None:
            placeholders = ", ".join(["?"] * len(reach_ids))
            where_clause = f"AND n.reach_id IN ({placeholders})"
            params.extend(reach_ids)
        result_df = self._conn.execute(
            f"SELECT n.reach_id, MAX(n.trib_flag) as trib_flag FROM nodes n WHERE n.region = ? {where_clause} GROUP BY n.reach_id",
            params,
        ).fetchdf()
        return self._update_reach_attribute("trib_flag", result_df, dry_run)

    # =========================================================================
    # STUB RECONSTRUCTORS (require external data)
    # =========================================================================
    # These reconstructors are stubs that document the requirement for external
    # data sources. They preserve existing values or return defaults.

    def _load_grod_data(self, nodes_df):
        """Load GROD data and spatial-join to nodes. Returns (grod_ids, obstr_types) arrays."""
        grod_dir = self._source_data_dir / "GROD" if self._source_data_dir else None
        if grod_dir is None or not grod_dir.exists():
            return None, None

        import geopandas as gpd
        from scipy.spatial import cKDTree

        grod_files = sorted(grod_dir.glob("*.gpkg")) + sorted(grod_dir.glob("*.shp"))
        if not grod_files:
            return None, None

        all_grod = []
        for f in grod_files:
            try:
                all_grod.append(gpd.read_file(f))
            except Exception as e:
                logger.warning(f"Failed to read {f}: {e}")
        if not all_grod:
            return None, None

        import pandas as pd

        grod = pd.concat(all_grod, ignore_index=True)

        # Normalize column names
        col_map = {}
        for c in grod.columns:
            if c.lower() == "grod_id":
                col_map[c] = "grod_id"
            elif c.lower() in ("type", "obstr_type", "obstruction_type"):
                col_map[c] = "obstr_type"
        grod = grod.rename(columns=col_map)

        if "geometry" in grod.columns:
            grod["_x"] = grod.geometry.centroid.x
            grod["_y"] = grod.geometry.centroid.y
        elif "x" in grod.columns and "y" in grod.columns:
            grod["_x"] = grod["x"]
            grod["_y"] = grod["y"]
        else:
            return None, None

        grod = grod.dropna(subset=["_x", "_y"])
        if grod.empty:
            return None, None

        kdt = cKDTree(np.column_stack([grod["_x"].values, grod["_y"].values]))
        node_pts = np.column_stack([nodes_df["x"].values, nodes_df["y"].values])
        distances, indices = kdt.query(node_pts, k=1)

        grod_ids = np.zeros(len(nodes_df), dtype=np.int64)
        obstr_types = np.zeros(len(nodes_df), dtype=np.int32)

        matched = distances <= 0.005
        if matched.any():
            if "grod_id" in grod.columns:
                grod_ids[matched] = grod["grod_id"].values[indices[matched]]
            if "obstr_type" in grod.columns:
                raw_types = grod["obstr_type"].values[indices[matched]].astype(np.int32)
                raw_types[raw_types > 4] = 0
                obstr_types[matched] = raw_types

        return grod_ids, obstr_types

    def _reconstruct_node_grod_id(
        self,
        node_ids: Optional[List[int]] = None,
        force: bool = False,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """Reconstruct node grod_id from GROD spatial join."""
        where_clause = ""
        params = [self._region]
        if node_ids is not None:
            placeholders = ", ".join(["?"] * len(node_ids))
            where_clause = f"AND node_id IN ({placeholders})"
            params.extend(node_ids)

        nodes_df = self._conn.execute(
            f"SELECT node_id, x, y FROM nodes WHERE region = ? {where_clause}", params
        ).fetchdf()
        if nodes_df.empty:
            return {"status": "ok", "updated": 0, "dry_run": dry_run}

        grod_ids, _ = self._load_grod_data(nodes_df)
        if grod_ids is None:
            logger.warning("node.grod_id: GROD data not found. Preserving values.")
            result_df = self._conn.execute(
                f"SELECT node_id, COALESCE(grod_id, 0) as grod_id FROM nodes WHERE region = ? {where_clause}",
                params,
            ).fetchdf()
            return self._update_node_attribute("grod_id", result_df, dry_run)

        logger.info("Reconstructing node.grod_id from GROD spatial join")
        nodes_df["grod_id"] = grod_ids
        return self._update_node_attribute(
            "grod_id", nodes_df[["node_id", "grod_id"]], dry_run
        )

    def _reconstruct_node_hfalls_id(
        self,
        node_ids: Optional[List[int]] = None,
        force: bool = False,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """Reconstruct node hfalls_id from HydroFALLS spatial join."""
        hf_dir = self._source_data_dir / "HydroFALLS" if self._source_data_dir else None
        if hf_dir is None or not hf_dir.exists():
            logger.warning(
                "node.hfalls_id: HydroFALLS data not found. Preserving values."
            )
            where_clause = ""
            params = [self._region]
            if node_ids is not None:
                placeholders = ", ".join(["?"] * len(node_ids))
                where_clause = f"AND node_id IN ({placeholders})"
                params.extend(node_ids)
            result_df = self._conn.execute(
                f"SELECT node_id, COALESCE(hfalls_id, 0) as hfalls_id FROM nodes WHERE region = ? {where_clause}",
                params,
            ).fetchdf()
            return self._update_node_attribute("hfalls_id", result_df, dry_run)

        import geopandas as gpd
        from scipy.spatial import cKDTree

        logger.info("Reconstructing node.hfalls_id from HydroFALLS spatial join")

        where_clause = ""
        params = [self._region]
        if node_ids is not None:
            placeholders = ", ".join(["?"] * len(node_ids))
            where_clause = f"AND node_id IN ({placeholders})"
            params.extend(node_ids)

        nodes_df = self._conn.execute(
            f"SELECT node_id, x, y FROM nodes WHERE region = ? {where_clause}", params
        ).fetchdf()
        if nodes_df.empty:
            return {"status": "ok", "updated": 0, "dry_run": dry_run}

        hf_files = sorted(hf_dir.glob("*.gpkg")) + sorted(hf_dir.glob("*.shp"))
        if not hf_files:
            nodes_df["hfalls_id"] = 0
            return self._update_node_attribute(
                "hfalls_id", nodes_df[["node_id", "hfalls_id"]], dry_run
            )

        import pandas as pd

        all_hf = []
        for f in hf_files:
            try:
                all_hf.append(gpd.read_file(f))
            except Exception as e:
                logger.warning(f"Failed to read {f}: {e}")
        if not all_hf:
            nodes_df["hfalls_id"] = 0
            return self._update_node_attribute(
                "hfalls_id", nodes_df[["node_id", "hfalls_id"]], dry_run
            )

        hf = pd.concat(all_hf, ignore_index=True)

        # Normalize column name
        id_col = None
        for c in hf.columns:
            if c.lower() == "hfalls_id":
                id_col = c
                break
        if id_col is None:
            nodes_df["hfalls_id"] = 0
            return self._update_node_attribute(
                "hfalls_id", nodes_df[["node_id", "hfalls_id"]], dry_run
            )

        if "geometry" in hf.columns:
            hf["_x"] = hf.geometry.centroid.x
            hf["_y"] = hf.geometry.centroid.y
        elif "x" in hf.columns and "y" in hf.columns:
            hf["_x"] = hf["x"]
            hf["_y"] = hf["y"]
        else:
            nodes_df["hfalls_id"] = 0
            return self._update_node_attribute(
                "hfalls_id", nodes_df[["node_id", "hfalls_id"]], dry_run
            )

        hf = hf.dropna(subset=["_x", "_y"])
        if hf.empty:
            nodes_df["hfalls_id"] = 0
            return self._update_node_attribute(
                "hfalls_id", nodes_df[["node_id", "hfalls_id"]], dry_run
            )

        kdt = cKDTree(np.column_stack([hf["_x"].values, hf["_y"].values]))
        node_pts = np.column_stack([nodes_df["x"].values, nodes_df["y"].values])
        distances, indices = kdt.query(node_pts, k=1)

        hfalls_ids = np.zeros(len(nodes_df), dtype=np.int64)
        matched = distances <= 0.005
        if matched.any():
            hfalls_ids[matched] = hf[id_col].values[indices[matched]]

        nodes_df["hfalls_id"] = hfalls_ids
        return self._update_node_attribute(
            "hfalls_id", nodes_df[["node_id", "hfalls_id"]], dry_run
        )

    def _reconstruct_node_river_name(
        self,
        node_ids: Optional[List[int]] = None,
        force: bool = False,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """Reconstruct node river_name inherited from parent reach."""
        logger.info("Reconstructing node.river_name from parent reach")
        where_clause = ""
        params = [self._region]
        if node_ids is not None:
            placeholders = ", ".join(["?"] * len(node_ids))
            where_clause = f"AND n.node_id IN ({placeholders})"
            params.extend(node_ids)
        result_df = self._conn.execute(
            f"""
            SELECT n.node_id, COALESCE(r.river_name, 'NODATA') as river_name
            FROM nodes n
            JOIN reaches r ON n.reach_id = r.reach_id AND n.region = r.region
            WHERE n.region = ? {where_clause}
        """,
            params,
        ).fetchdf()
        return self._update_node_attribute("river_name", result_df, dry_run)

    def _reconstruct_reach_grod_id(
        self,
        reach_ids: Optional[List[int]] = None,
        force: bool = False,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """Reconstruct reach grod_id as MAX of node grod_ids."""
        logger.info("Reconstructing reach.grod_id from node max")
        where_clause = ""
        params = [self._region]
        if reach_ids is not None:
            placeholders = ", ".join(["?"] * len(reach_ids))
            where_clause = f"AND n.reach_id IN ({placeholders})"
            params.extend(reach_ids)
        result_df = self._conn.execute(
            f"SELECT n.reach_id, MAX(n.grod_id) as grod_id FROM nodes n WHERE n.region = ? {where_clause} GROUP BY n.reach_id",
            params,
        ).fetchdf()
        return self._update_reach_attribute("grod_id", result_df, dry_run)

    def _reconstruct_reach_hfalls_id(
        self,
        reach_ids: Optional[List[int]] = None,
        force: bool = False,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """Reconstruct reach hfalls_id as MAX of node hfalls_ids."""
        logger.info("Reconstructing reach.hfalls_id from node max")
        where_clause = ""
        params = [self._region]
        if reach_ids is not None:
            placeholders = ", ".join(["?"] * len(reach_ids))
            where_clause = f"AND n.reach_id IN ({placeholders})"
            params.extend(reach_ids)
        result_df = self._conn.execute(
            f"SELECT n.reach_id, MAX(n.hfalls_id) as hfalls_id FROM nodes n WHERE n.region = ? {where_clause} GROUP BY n.reach_id",
            params,
        ).fetchdf()
        return self._update_reach_attribute("hfalls_id", result_df, dry_run)

    def _reconstruct_reach_river_name(
        self,
        reach_ids: Optional[List[int]] = None,
        force: bool = False,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """Reconstruct reach river_name from external names data or default NODATA."""
        names_dir = (
            self._source_data_dir / "river_names" if self._source_data_dir else None
        )

        where_clause = ""
        params = [self._region]
        if reach_ids is not None:
            placeholders = ", ".join(["?"] * len(reach_ids))
            where_clause = f"AND reach_id IN ({placeholders})"
            params.extend(reach_ids)

        reaches_df = self._conn.execute(
            f"SELECT reach_id, x, y FROM reaches WHERE region = ? {where_clause}",
            params,
        ).fetchdf()
        if reaches_df.empty:
            return {"status": "ok", "updated": 0, "dry_run": dry_run}

        # Default all to NODATA
        reaches_df["river_name"] = "NODATA"

        if names_dir is not None and names_dir.exists():
            import geopandas as gpd
            from scipy.spatial import cKDTree

            name_files = sorted(names_dir.glob("*.gpkg")) + sorted(
                names_dir.glob("*.shp")
            )
            if name_files:
                import pandas as pd

                all_names = []
                for f in name_files:
                    try:
                        all_names.append(gpd.read_file(f))
                    except Exception as e:
                        logger.warning(f"Failed to read {f}: {e}")
                if all_names:
                    names = pd.concat(all_names, ignore_index=True)
                    # Find the name column
                    name_col = None
                    for c in names.columns:
                        if c.lower() in ("river_name", "name", "river_nam"):
                            name_col = c
                            break
                    if name_col and "geometry" in names.columns:
                        names["_x"] = names.geometry.centroid.x
                        names["_y"] = names.geometry.centroid.y
                        names = names.dropna(subset=["_x", "_y", name_col])
                        if not names.empty:
                            logger.info(
                                "Reconstructing reach.river_name from names spatial join"
                            )
                            kdt = cKDTree(
                                np.column_stack(
                                    [names["_x"].values, names["_y"].values]
                                )
                            )
                            reach_pts = np.column_stack(
                                [reaches_df["x"].values, reaches_df["y"].values]
                            )
                            distances, indices = kdt.query(reach_pts, k=1)
                            matched = distances <= 0.01
                            if matched.any():
                                reaches_df.loc[matched, "river_name"] = names[
                                    name_col
                                ].values[indices[matched]]

        return self._update_reach_attribute(
            "river_name", reaches_df[["reach_id", "river_name"]], dry_run
        )

    def _reconstruct_reach_iceflag(
        self,
        reach_ids: Optional[List[int]] = None,
        force: bool = False,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """Reconstruct iceflag from reach_ice_flags table (max of daily values)."""
        logger.info("Reconstructing reach.iceflag from reach_ice_flags")

        count = self._conn.execute(
            "SELECT COUNT(*) FROM reach_ice_flags WHERE reach_id IN "
            "(SELECT reach_id FROM reaches WHERE region = ?)",
            [self._region],
        ).fetchone()[0]

        if count == 0 and self._source_data_dir:
            ice_dir = self._source_data_dir / "ice_flags"
            if ice_dir and ice_dir.exists():
                import pandas as pd

                for csv_file in ice_dir.glob("*.csv"):
                    df = pd.read_csv(csv_file)
                    if "reach_id" not in df.columns:
                        continue
                    day_cols = [c for c in df.columns if c != "reach_id"]
                    if day_cols:
                        melted = df.melt(
                            id_vars=["reach_id"],
                            value_vars=day_cols,
                            var_name="jd",
                            value_name="iceflag",
                        )
                        melted["julian_day"] = (
                            melted["jd"].str.extract(r"(\d+)")[0].astype(int)
                        )
                        melted = melted.dropna(
                            subset=["reach_id", "julian_day", "iceflag"]
                        )
                        if melted.empty:
                            continue
                        self._conn.executemany(
                            "INSERT INTO reach_ice_flags VALUES (?, ?, ?) ON CONFLICT DO NOTHING",
                            melted[
                                ["reach_id", "julian_day", "iceflag"]
                            ].values.tolist(),
                        )
                        logger.info(f"Loaded {len(melted)} ice records from {csv_file}")

        where_clause = ""
        params = [self._region]
        if reach_ids:
            placeholders = ", ".join(["?"] * len(reach_ids))
            where_clause = f"AND r.reach_id IN ({placeholders})"
            params.extend(reach_ids)

        result_df = self._conn.execute(
            f"""
            SELECT r.reach_id, COALESCE(MAX(i.iceflag), -9999) as iceflag
            FROM reaches r LEFT JOIN reach_ice_flags i ON r.reach_id = i.reach_id
            WHERE r.region = ? {where_clause} GROUP BY r.reach_id
        """,
            params,
        ).fetchdf()

        return self._update_reach_attribute("iceflag", result_df, dry_run)

    # =========================================================================
    # NON-RECONSTRUCTABLE ATTRIBUTES (manual edits)
    # =========================================================================
    # These attributes track manual edits and cannot be reconstructed from
    # source data. They are registered for completeness but log warnings.

    def _reconstruct_node_edit_flag(
        self,
        node_ids: Optional[List[int]] = None,
        force: bool = False,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """
        Node edit_flag tracks manual edits - NOT RECONSTRUCTABLE.

        This preserves existing values. Edit flags should only be set
        through manual edit operations.
        """
        logger.warning(
            "node.edit_flag tracks manual edits - cannot reconstruct, preserving values"
        )

        where_clause = ""
        params = [self._region]
        if node_ids is not None:
            placeholders = ", ".join(["?"] * len(node_ids))
            where_clause = f"AND node_id IN ({placeholders})"
            params.extend(node_ids)

        result_df = self._conn.execute(
            f"""
            SELECT node_id, COALESCE(edit_flag, '') as edit_flag
            FROM nodes
            WHERE region = ? {where_clause}
        """,
            params,
        ).fetchdf()

        return self._update_node_attribute("edit_flag", result_df, dry_run)

    def _reconstruct_node_manual_add(
        self,
        node_ids: Optional[List[int]] = None,
        force: bool = False,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """
        Node manual_add flag - NOT RECONSTRUCTABLE.

        Indicates manually added nodes (width == 1 is a proxy).
        This preserves existing values.
        """
        logger.warning(
            "node.manual_add tracks manual additions - cannot reconstruct, preserving values"
        )

        where_clause = ""
        params = [self._region]
        if node_ids is not None:
            placeholders = ", ".join(["?"] * len(node_ids))
            where_clause = f"AND node_id IN ({placeholders})"
            params.extend(node_ids)

        result_df = self._conn.execute(
            f"""
            SELECT node_id, COALESCE(manual_add, 0) as manual_add
            FROM nodes
            WHERE region = ? {where_clause}
        """,
            params,
        ).fetchdf()

        return self._update_node_attribute("manual_add", result_df, dry_run)

    def _reconstruct_reach_edit_flag(
        self,
        reach_ids: Optional[List[int]] = None,
        force: bool = False,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """
        Reach edit_flag tracks manual edits - NOT RECONSTRUCTABLE.

        This preserves existing values. Edit flags should only be set
        through manual edit operations.
        """
        logger.warning(
            "reach.edit_flag tracks manual edits - cannot reconstruct, preserving values"
        )

        where_clause = ""
        params = [self._region]
        if reach_ids is not None:
            placeholders = ", ".join(["?"] * len(reach_ids))
            where_clause = f"AND reach_id IN ({placeholders})"
            params.extend(reach_ids)

        result_df = self._conn.execute(
            f"""
            SELECT reach_id, COALESCE(edit_flag, '') as edit_flag
            FROM reaches
            WHERE region = ? {where_clause}
        """,
            params,
        ).fetchdf()

        return self._update_reach_attribute("edit_flag", result_df, dry_run)

    # =========================================================================
    # CENTERLINE RECONSTRUCTORS (source data stubs)
    # =========================================================================
    # Centerline attributes are the raw source data from GRWL. These stubs
    # document that they cannot be reconstructed without re-processing GRWL.

    def _reconstruct_centerline_x(
        self,
        cl_ids: Optional[List[int]] = None,
        force: bool = False,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """
        Centerline x (longitude) - SOURCE DATA from GRWL.

        REQUIRES: Re-processing of GRWL river centerlines
        This is a stub - centerline coordinates are source data.
        """
        logger.warning("centerline.x is source data from GRWL - cannot reconstruct")
        return {
            "reconstructed": 0,
            "entity_ids": [],
            "attribute": "centerline.x",
            "note": "Source data from GRWL - requires re-processing",
        }

    def _reconstruct_centerline_y(
        self,
        cl_ids: Optional[List[int]] = None,
        force: bool = False,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """
        Centerline y (latitude) - SOURCE DATA from GRWL.

        REQUIRES: Re-processing of GRWL river centerlines
        This is a stub - centerline coordinates are source data.
        """
        logger.warning("centerline.y is source data from GRWL - cannot reconstruct")
        return {
            "reconstructed": 0,
            "entity_ids": [],
            "attribute": "centerline.y",
            "note": "Source data from GRWL - requires re-processing",
        }

    def _reconstruct_centerline_reach_id(
        self,
        cl_ids: Optional[List[int]] = None,
        force: bool = False,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """
        Centerline reach_id assignment.

        REQUIRES: Re-running reach definition algorithm
        This is a stub - reach assignment is done during SWORD construction.
        """
        logger.warning("centerline.reach_id requires re-running reach definition")
        return {
            "reconstructed": 0,
            "entity_ids": [],
            "attribute": "centerline.reach_id",
            "note": "Requires re-running reach definition algorithm",
        }

    def _reconstruct_centerline_node_id(
        self,
        cl_ids: Optional[List[int]] = None,
        force: bool = False,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """
        Centerline node_id assignment.

        REQUIRES: Re-running node definition algorithm (~200m spacing)
        This is a stub - node assignment is done during SWORD construction.
        """
        logger.warning("centerline.node_id requires re-running node definition")
        return {
            "reconstructed": 0,
            "entity_ids": [],
            "attribute": "centerline.node_id",
            "note": "Requires re-running node definition algorithm",
        }
