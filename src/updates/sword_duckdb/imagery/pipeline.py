# -*- coding: utf-8 -*-
"""
Imagery Pipeline Orchestrator
=============================

High-level API for imagery inspection of SWORD reaches.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List, Dict, Any, Tuple, TYPE_CHECKING
import logging

import numpy as np
from pystac import Item
from affine import Affine
from rasterio.crs import CRS

from .config import ImageryConfig, DEFAULT_CONFIG
from .stac_client import SentinelSTACClient
from .cog_reader import COGReader
from .ndwi import NDWIComputer, NDWIResult
from .cache import ImageryCache
from .exceptions import ImageryError, NoImageryFoundError
from . import schema as imagery_schema

if TYPE_CHECKING:
    from ..sword_class import SWORD
    import duckdb

logger = logging.getLogger(__name__)


@dataclass
class ImageryResult:
    """Result of imagery processing for a reach."""

    reach_id: int
    region: str
    item: Item
    bbox: Tuple[float, float, float, float]
    ndwi: np.ndarray
    water_mask: np.ndarray
    transform: Affine
    crs: CRS
    threshold: float
    stats: Dict[str, Any]
    acquisition_id: Optional[int] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


class ImageryPipeline:
    """
    High-level pipeline for imagery inspection of SWORD reaches.

    Orchestrates STAC search, COG streaming, NDWI computation, and caching.
    """

    def __init__(
        self,
        sword: "SWORD",
        config: ImageryConfig = None,
        db_conn: "duckdb.DuckDBPyConnection" = None,
    ):
        """
        Initialize imagery pipeline.

        Args:
            sword: SWORD instance with loaded data
            config: Imagery configuration
            db_conn: DuckDB connection for metadata tracking
        """
        self.sword = sword
        self.config = config or DEFAULT_CONFIG
        self.db_conn = db_conn

        # Initialize components
        self.stac_client = SentinelSTACClient(self.config)
        self.cog_reader = COGReader(self.config)
        self.ndwi_computer = NDWIComputer(self.config)
        self.cache = ImageryCache(self.config) if self.config.cache_enabled else None

        # Ensure imagery tables exist
        if self.db_conn is not None:
            imagery_schema.create_imagery_tables(self.db_conn)

    def get_imagery_for_reach(
        self,
        reach_id: int,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        max_cloud_cover: Optional[float] = None,
        use_otsu: bool = False,
        force_refresh: bool = False,
    ) -> ImageryResult:
        """
        Get imagery and compute NDWI for a single reach.

        Args:
            reach_id: SWORD reach ID
            start_date: Start date for imagery search
            end_date: End date for imagery search
            max_cloud_cover: Maximum cloud cover percentage
            use_otsu: Use Otsu thresholding for water classification
            force_refresh: Force refresh even if cached

        Returns:
            ImageryResult with NDWI and water mask
        """
        region = self.sword.region

        # Get reach bbox
        bbox = self._get_reach_bbox(reach_id)
        logger.info(f"Processing reach {reach_id}, bbox: {bbox}")

        # Search for imagery
        items = self.stac_client.search_by_bbox(
            bbox=bbox,
            start_date=start_date,
            end_date=end_date,
            max_cloud_cover=max_cloud_cover,
        )

        if len(items) == 0:
            raise NoImageryFoundError(
                f"No imagery found for reach {reach_id}",
                bbox=bbox,
                date_range=(start_date, end_date),
                cloud_cover=max_cloud_cover,
            )

        # Select best item
        item = self.stac_client.get_best_item(items)

        # Check cache
        cache_key_prefix = f"{item.id}_{reach_id}"
        if self.cache and not force_refresh:
            cached_ndwi = self.cache.get(item.id, bbox, "ndwi")
            if cached_ndwi is not None:
                logger.info(f"Using cached NDWI for reach {reach_id}")
                ndwi_data, transform, crs = cached_ndwi
                # Still need to compute water mask
                water_mask, threshold = self.ndwi_computer.classify_water(
                    ndwi_data, use_otsu=use_otsu
                )
                stats = self._compute_stats(ndwi_data, water_mask, threshold)
                return ImageryResult(
                    reach_id=reach_id,
                    region=region,
                    item=item,
                    bbox=bbox,
                    ndwi=ndwi_data,
                    water_mask=water_mask,
                    transform=transform,
                    crs=crs,
                    threshold=threshold,
                    stats=stats,
                    metadata=self.cog_reader.get_item_metadata(item),
                )

        # Read bands
        green, nir, transform, crs = self.cog_reader.read_bands_for_ndwi(item, bbox)

        # Compute NDWI
        result = self.ndwi_computer.compute_with_mask(green, nir, use_otsu=use_otsu)

        # Cache NDWI
        if self.cache:
            self.cache.put(item.id, bbox, "ndwi", result.ndwi, transform, crs)

        # Log to database
        acquisition_id = None
        if self.db_conn is not None:
            acquisition_id = self._log_acquisition(item, bbox, region, [reach_id])
            self._log_reach_imagery(reach_id, region, acquisition_id, result)

        return ImageryResult(
            reach_id=reach_id,
            region=region,
            item=item,
            bbox=bbox,
            ndwi=result.ndwi,
            water_mask=result.water_mask,
            transform=transform,
            crs=crs,
            threshold=result.threshold,
            stats=result.stats,
            acquisition_id=acquisition_id,
            metadata=self.cog_reader.get_item_metadata(item),
        )

    def get_imagery_for_reaches(
        self,
        reach_ids: List[int],
        **kwargs,
    ) -> Dict[int, ImageryResult]:
        """
        Get imagery for multiple reaches.

        Args:
            reach_ids: List of reach IDs
            **kwargs: Additional arguments for get_imagery_for_reach

        Returns:
            Dictionary mapping reach_id to ImageryResult
        """
        results = {}
        for reach_id in reach_ids:
            try:
                results[reach_id] = self.get_imagery_for_reach(reach_id, **kwargs)
            except ImageryError as e:
                logger.warning(f"Failed to get imagery for reach {reach_id}: {e}")
                continue
        return results

    def _get_reach_bbox(
        self,
        reach_id: int,
    ) -> Tuple[float, float, float, float]:
        """Get bounding box for a reach with buffer."""
        # Find reach index (ReachesView uses 'id' not 'reach_id')
        reach_idx = (self.sword.reaches.id == reach_id).argmax()
        if self.sword.reaches.id[reach_idx] != reach_id:
            raise ImageryError(f"Reach {reach_id} not found")

        # Get bounds
        x_min = float(self.sword.reaches.x_min[reach_idx])
        x_max = float(self.sword.reaches.x_max[reach_idx])
        y_min = float(self.sword.reaches.y_min[reach_idx])
        y_max = float(self.sword.reaches.y_max[reach_idx])

        # Apply buffer
        buffer = self.config.buffer_m
        lat_center = (y_min + y_max) / 2
        buffer_deg = buffer / 111320
        import math

        buffer_deg_lon = buffer_deg / max(0.1, abs(math.cos(math.radians(lat_center))))

        return (
            x_min - buffer_deg_lon,
            y_min - buffer_deg,
            x_max + buffer_deg_lon,
            y_max + buffer_deg,
        )

    def _compute_stats(
        self,
        ndwi: np.ndarray,
        water_mask: np.ndarray,
        threshold: float,
    ) -> Dict[str, Any]:
        """Compute statistics for cached NDWI."""
        valid_mask = ndwi != self.config.ndwi_nodata
        valid_ndwi = ndwi[valid_mask]

        return {
            "ndwi_mean": float(np.mean(valid_ndwi)) if len(valid_ndwi) > 0 else None,
            "ndwi_std": float(np.std(valid_ndwi)) if len(valid_ndwi) > 0 else None,
            "ndwi_min": float(np.min(valid_ndwi)) if len(valid_ndwi) > 0 else None,
            "ndwi_max": float(np.max(valid_ndwi)) if len(valid_ndwi) > 0 else None,
            "water_pixel_count": int(np.sum(water_mask)),
            "total_valid_pixels": int(np.sum(valid_mask)),
            "water_fraction": float(np.sum(water_mask) / max(1, np.sum(valid_mask))),
            "threshold": threshold,
        }

    def _log_acquisition(
        self,
        item: Item,
        bbox: Tuple[float, float, float, float],
        region: str,
        reach_ids: List[int],
    ) -> int:
        """Log acquisition to database."""
        xmin, ymin, xmax, ymax = bbox
        bbox_wkt = f"POLYGON(({xmin} {ymin}, {xmax} {ymin}, {xmax} {ymax}, {xmin} {ymax}, {xmin} {ymin}))"

        props = item.properties
        metadata = self.cog_reader.get_item_metadata(item)

        return imagery_schema.insert_acquisition(
            self.db_conn,
            stac_item_id=item.id,
            bbox_wkt=bbox_wkt,
            acquired_at=props.get("datetime"),
            cloud_cover=props.get("eo:cloud_cover"),
            region=region,
            reach_ids=reach_ids,
            metadata=metadata,
        )

    def _log_reach_imagery(
        self,
        reach_id: int,
        region: str,
        acquisition_id: int,
        result: NDWIResult,
    ) -> None:
        """Log reach imagery statistics to database."""
        imagery_schema.insert_reach_imagery(
            self.db_conn,
            reach_id=reach_id,
            region=region,
            acquisition_id=acquisition_id,
            ndwi_mean=result.stats.get("ndwi_mean"),
            ndwi_std=result.stats.get("ndwi_std"),
            water_fraction=result.stats.get("water_fraction"),
            water_pixel_count=result.stats.get("water_pixel_count"),
            total_valid_pixels=result.stats.get("total_valid_pixels"),
            threshold_used=result.threshold,
        )

    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        if self.cache:
            return self.cache.get_stats()
        return {"cache_enabled": False}

    def clear_cache(self) -> None:
        """Clear the imagery cache."""
        if self.cache:
            self.cache.clear()
