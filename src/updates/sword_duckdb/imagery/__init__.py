# -*- coding: utf-8 -*-
"""
SWORD Imagery Pipeline
======================

Satellite imagery inspection for SWORD hydrography updates.

This module provides tools to:
1. Search for Sentinel-2 imagery via STAC API (Element 84 Earth Search)
2. Access OPERA DSWx pre-computed water masks (NASA/JPL)
3. Stream COG tiles efficiently (windowed reads)
4. Compute NDWI water index
5. Cache imagery locally
6. Track imagery metadata in DuckDB

Example Usage:
    from sword_duckdb.imagery import ImageryPipeline, OPERADSWxClient

    # Using OPERA DSWx water masks (recommended)
    dswx = OPERADSWxClient()
    composite = dswx.get_composite(bbox=(-98.3, 35.0, -98.2, 35.1),
                                    start_date="2024-01-01",
                                    end_date="2024-12-31")
    print(f"Water fraction: {composite.stats['any_water_fraction']:.2%}")

    # Or using Sentinel-2 NDWI
    pipeline = ImageryPipeline(sword)
    result = pipeline.get_imagery_for_reach(reach_id=12345678901)
    print(f"Water fraction: {result.stats['water_fraction']:.2%}")
"""

from .config import ImageryConfig, DEFAULT_CONFIG, S2_BANDS, apply_gdal_env
from .exceptions import (
    ImageryError,
    STACSearchError,
    NoImageryFoundError,
    COGReadError,
    CacheError,
    NDWIComputationError,
    BandMismatchError,
)
from .stac_client import SentinelSTACClient
from .cog_reader import COGReader
from .ndwi import NDWIComputer, NDWIResult, otsu_threshold, compute_mndwi
from .cache import ImageryCache
from .schema import (
    create_imagery_tables,
    get_imagery_schema_sql,
    insert_acquisition,
    insert_reach_imagery,
    get_reach_imagery_history,
)
from .pipeline import ImageryPipeline, ImageryResult
from .opera_dswx import OPERADSWxClient, DSWxResult, DSWxComposite
from .centerline import (
    CenterlineResult,
    CenterlineComparison,
    extract_centerline,
    compare_centerlines,
    create_cost_field,
    find_endpoints_from_mask,
)
from .river_priors import (
    RiverPriorClient,
    RiverPriorResult,
    CorridorMask,
)
from .fusion import (
    FusionEngine,
    FusionResult,
    ReachFusionStats,
    fuse_reach,
    UPLAND,
    STABLE,
    NEW,
    ABANDONED,
    CLASS_NAMES,
)
from .water_mask import (
    WaterMaskResult,
    extract_water_mask,
    adaptive_thresholds,
    connected_to_core,
    morphological_reconstruction_fill,
    extract_river_network,
    keep_main_component,
)
from .centerline_solver import (
    CenterlinePath,
    build_channel_likelihood,
    build_cost_surface,
    extract_centerline as extract_centerline_astar,
    path_to_mask,
)
from .fusion_centerline import (
    FusionCenterlineExtractor,
    FusionCenterlineResult,
    FusionWaterMask,
    WaterMaskClassification,
    extract_fusion_centerline,
    classify_sword_accuracy,
    filter_elongated_features,
)
from .sword_updater import (
    SWORDUpdater,
    SWORDUpdateResult,
)
from .water_ensemble import (
    WaterEnsemble,
    EnsembleResult,
    CompositeResult,
    WaterIndexComputer,
    ML4FloodsInference,
    DeepWaterMapInference,
    OSMWaterSource,
    ESRILandCoverSource,
    CloudFilter,
    SceneCompositor,
    SCL_BAD_PIXELS,
    remove_small_blobs,
    extract_channel_mask,
    create_corridor_from_geometry,
)

# MLX-based semantic segmentation (optional - requires mlx)
try:
    from .mlx_segmentation import (
        TiramisuMLX,
        TiramisuSegmenter,
        TiramisuResult,
        segment_with_tiramisu,
        BACKGROUND,
        RIVER,
        LAKE,
        BAR,
        CLASS_NAMES as TIRAMISU_CLASS_NAMES,
        MLX_AVAILABLE,
    )
except ImportError:
    MLX_AVAILABLE = False
    TiramisuMLX = None
    TiramisuSegmenter = None
    TiramisuResult = None
    segment_with_tiramisu = None
    BACKGROUND = RIVER = LAKE = BAR = None
    TIRAMISU_CLASS_NAMES = None

__all__ = [
    # High-level pipeline
    "ImageryPipeline",
    "ImageryResult",
    # Configuration
    "ImageryConfig",
    "DEFAULT_CONFIG",
    "S2_BANDS",
    "apply_gdal_env",
    # STAC client
    "SentinelSTACClient",
    # COG reader
    "COGReader",
    # NDWI
    "NDWIComputer",
    "NDWIResult",
    "otsu_threshold",
    "compute_mndwi",
    # Cache
    "ImageryCache",
    # Schema
    "create_imagery_tables",
    "get_imagery_schema_sql",
    "insert_acquisition",
    "insert_reach_imagery",
    "get_reach_imagery_history",
    # OPERA DSWx
    "OPERADSWxClient",
    "DSWxResult",
    "DSWxComposite",
    # Centerline extraction
    "CenterlineResult",
    "CenterlineComparison",
    "extract_centerline",
    "compare_centerlines",
    "create_cost_field",
    "find_endpoints_from_mask",
    # River Priors
    "RiverPriorClient",
    "RiverPriorResult",
    "CorridorMask",
    # Fusion
    "FusionEngine",
    "FusionResult",
    "ReachFusionStats",
    "fuse_reach",
    "UPLAND",
    "STABLE",
    "NEW",
    "ABANDONED",
    "CLASS_NAMES",
    # Water Mask
    "WaterMaskResult",
    "extract_water_mask",
    "adaptive_thresholds",
    "connected_to_core",
    "morphological_reconstruction_fill",
    "extract_river_network",
    "keep_main_component",
    # Centerline Solver (A* optimal path)
    "CenterlinePath",
    "build_channel_likelihood",
    "build_cost_surface",
    "extract_centerline_astar",
    "path_to_mask",
    # Fusion Centerline (OPERA + S2)
    "FusionCenterlineExtractor",
    "FusionCenterlineResult",
    "FusionWaterMask",
    "WaterMaskClassification",
    "extract_fusion_centerline",
    "classify_sword_accuracy",
    "filter_elongated_features",
    # SWORD Updater (RivGraph + DT snap)
    "SWORDUpdater",
    "SWORDUpdateResult",
    # Water Ensemble (multi-method voting)
    "WaterEnsemble",
    "EnsembleResult",
    "CompositeResult",
    "WaterIndexComputer",
    "ML4FloodsInference",
    "DeepWaterMapInference",
    "OSMWaterSource",
    "ESRILandCoverSource",
    "CloudFilter",
    "SceneCompositor",
    "SCL_BAD_PIXELS",
    "remove_small_blobs",
    "extract_channel_mask",
    "create_corridor_from_geometry",
    # MLX Tiramisu Semantic Segmentation (optional)
    "TiramisuMLX",
    "TiramisuSegmenter",
    "TiramisuResult",
    "segment_with_tiramisu",
    "BACKGROUND",
    "RIVER",
    "LAKE",
    "BAR",
    "TIRAMISU_CLASS_NAMES",
    "MLX_AVAILABLE",
    # Exceptions
    "ImageryError",
    "STACSearchError",
    "NoImageryFoundError",
    "COGReadError",
    "CacheError",
    "NDWIComputationError",
    "BandMismatchError",
]
