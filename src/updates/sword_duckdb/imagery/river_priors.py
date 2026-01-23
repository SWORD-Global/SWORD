# -*- coding: utf-8 -*-
"""
River Prior Data Sources
========================

Query river location priors from OSM and SWORD.

- OSM Overpass API: Global waterway geometries (crowd-sourced, often current)
- SWORD: Database centerlines we're validating

Fusion approach: Compare OSM priors against OPERA observations to detect
where rivers actually are vs where OSM/SWORD says they should be.
"""

from dataclasses import dataclass, field
from typing import Tuple, Optional, List, Dict, Any, Union
import logging
import time

import numpy as np
import requests
import geopandas as gpd
from shapely.geometry import LineString
from shapely.ops import unary_union
from affine import Affine
from rasterio.features import rasterize
from rasterio.crs import CRS

from .exceptions import ImageryError

logger = logging.getLogger(__name__)

# API Endpoints
OVERPASS_URL = "https://overpass-api.de/api/interpreter"

# OSM waterway types to query
OSM_WATERWAY_TYPES = ["river", "stream", "canal"]


@dataclass
class RiverPriorResult:
    """Result from river prior query."""

    geometry: gpd.GeoDataFrame
    source: str  # "osm" or "sword"
    bbox: Tuple[float, float, float, float]
    feature_count: int
    stats: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CorridorMask:
    """Rasterized river corridor mask."""

    mask: np.ndarray  # Binary mask (True = within corridor)
    transform: Affine
    crs: CRS
    buffer_m: float
    source: str
    stats: Dict[str, Any] = field(default_factory=dict)


class RiverPriorClient:
    """
    Client for fetching river location priors from OSM and SWORD.

    Usage:
        client = RiverPriorClient()

        # Get OSM waterways and create corridor mask
        corridor = client.get_osm_corridor_mask(
            bbox, transform, shape, target_crs, buffer_m=200
        )

        # Or get SWORD centerline as corridor
        corridor = client.get_sword_corridor_mask(
            sword, reach_id, transform, shape, target_crs
        )
    """

    def __init__(
        self,
        timeout: int = 60,
        retries: int = 3,
        retry_delay: float = 2.0,
    ):
        """
        Initialize river prior client.

        Args:
            timeout: HTTP request timeout in seconds
            retries: Number of retry attempts
            retry_delay: Delay between retries in seconds
        """
        self.timeout = timeout
        self.retries = retries
        self.retry_delay = retry_delay

    def get_osm_waterways(
        self,
        bbox: Tuple[float, float, float, float],
        waterway_types: Optional[List[str]] = None,
    ) -> RiverPriorResult:
        """
        Query OSM Overpass API for waterways.

        Args:
            bbox: (xmin, ymin, xmax, ymax) in WGS84
            waterway_types: List of OSM waterway types to query

        Returns:
            RiverPriorResult with waterway geometries
        """
        if waterway_types is None:
            waterway_types = OSM_WATERWAY_TYPES

        xmin, ymin, xmax, ymax = bbox

        # Build Overpass QL query
        type_filters = "".join(
            [f'way["waterway"="{wt}"]({ymin},{xmin},{ymax},{xmax});' for wt in waterway_types]
        )

        query = f"""
        [out:json][timeout:{self.timeout}];
        (
          {type_filters}
        );
        out geom;
        """

        # Execute query with retries
        for attempt in range(self.retries):
            try:
                response = requests.post(
                    OVERPASS_URL,
                    data={"data": query},
                    timeout=self.timeout,
                )
                response.raise_for_status()
                data = response.json()
                break
            except (requests.RequestException, ValueError) as e:
                if attempt < self.retries - 1:
                    logger.warning(f"Overpass query failed (attempt {attempt + 1}): {e}")
                    time.sleep(self.retry_delay * (attempt + 1))
                else:
                    raise ImageryError(f"Overpass API failed after {self.retries} attempts: {e}")

        # Parse response to GeoDataFrame
        features = []
        for element in data.get("elements", []):
            if element.get("type") == "way" and "geometry" in element:
                coords = [(p["lon"], p["lat"]) for p in element["geometry"]]
                if len(coords) >= 2:
                    features.append(
                        {
                            "geometry": LineString(coords),
                            "osm_id": element.get("id"),
                            "waterway": element.get("tags", {}).get("waterway"),
                            "name": element.get("tags", {}).get("name"),
                        }
                    )

        if features:
            gdf = gpd.GeoDataFrame(features, crs="EPSG:4326")
        else:
            gdf = gpd.GeoDataFrame(columns=["geometry", "osm_id", "waterway", "name"], crs="EPSG:4326")

        logger.info(f"OSM returned {len(features)} waterway features for bbox {bbox}")

        return RiverPriorResult(
            geometry=gdf,
            source="osm",
            bbox=bbox,
            feature_count=len(features),
            stats={"waterway_types": waterway_types},
        )

    def get_sword_centerline(
        self,
        sword,
        reach_id: int,
    ) -> RiverPriorResult:
        """
        Get SWORD centerline as prior geometry.

        Args:
            sword: SWORD object
            reach_id: SWORD reach ID

        Returns:
            RiverPriorResult with centerline geometry
        """
        # Get centerline points for the reach
        result = sword.db.execute(
            """
            SELECT x, y
            FROM centerlines
            WHERE reach_id = ?
            ORDER BY cl_id
            """,
            [reach_id],
        ).fetchall()

        if not result:
            raise ImageryError(f"No centerline found for reach {reach_id}")

        coords = [(x, y) for x, y in result]
        if len(coords) < 2:
            raise ImageryError(f"Centerline for reach {reach_id} has fewer than 2 points")

        centerline = LineString(coords)

        # Get reach bounds for bbox
        bounds = sword.db.execute(
            """
            SELECT x_min, y_min, x_max, y_max, width
            FROM reaches
            WHERE reach_id = ?
            """,
            [reach_id],
        ).fetchone()

        if not bounds:
            raise ImageryError(f"No reach info for reach {reach_id}")

        x_min, y_min, x_max, y_max, width = bounds

        gdf = gpd.GeoDataFrame(
            [{"geometry": centerline, "reach_id": reach_id, "width": width}],
            crs="EPSG:4326",
        )

        bbox = (x_min, y_min, x_max, y_max)

        logger.info(f"SWORD centerline for reach {reach_id}: {len(coords)} points, width={width:.1f}m")

        return RiverPriorResult(
            geometry=gdf,
            source="sword",
            bbox=bbox,
            feature_count=1,
            stats={"reach_id": reach_id, "width": width, "n_points": len(coords)},
        )

    def get_osm_corridor_mask(
        self,
        bbox: Tuple[float, float, float, float],
        transform: Affine,
        shape: Tuple[int, int],
        target_crs: Union[CRS, str] = "EPSG:4326",
        buffer_m: float = 200.0,
        waterway_types: Optional[List[str]] = None,
    ) -> CorridorMask:
        """
        Create corridor mask from OSM waterways.

        Args:
            bbox: (xmin, ymin, xmax, ymax) in WGS84
            transform: Affine transform for output raster
            shape: (height, width) of output raster
            target_crs: Target CRS for output (should match OPERA data)
            buffer_m: Buffer width in meters
            waterway_types: OSM waterway types to include

        Returns:
            CorridorMask with binary mask
        """
        # Get OSM waterways
        prior = self.get_osm_waterways(bbox, waterway_types)

        if prior.feature_count == 0:
            logger.warning(f"No OSM waterways found for bbox {bbox}")
            return CorridorMask(
                mask=np.zeros(shape, dtype=bool),
                transform=transform,
                crs=CRS.from_string(str(target_crs)) if isinstance(target_crs, str) else target_crs,
                buffer_m=buffer_m,
                source="osm",
                stats={"feature_count": 0},
            )

        # Get UTM CRS for buffering
        centroid = prior.geometry.unary_union.centroid
        utm_zone = int((centroid.x + 180) / 6) + 1
        utm_crs = f"EPSG:326{utm_zone:02d}" if centroid.y >= 0 else f"EPSG:327{utm_zone:02d}"

        gdf_utm = prior.geometry.to_crs(utm_crs)

        # Buffer in meters (UTM)
        buffered = gdf_utm.buffer(buffer_m)
        corridor_geom = unary_union(buffered)

        # Convert target_crs to string if CRS object
        if hasattr(target_crs, "to_string"):
            target_crs_str = target_crs.to_string()
        else:
            target_crs_str = str(target_crs)

        # Reproject to target CRS for rasterization
        corridor_gdf = gpd.GeoDataFrame(geometry=[corridor_geom], crs=utm_crs).to_crs(target_crs_str)
        corridor_geom_target = corridor_gdf.geometry.iloc[0]

        # Rasterize in target CRS
        mask = rasterize(
            [(corridor_geom_target, 1)],
            out_shape=shape,
            transform=transform,
            fill=0,
            dtype=np.uint8,
        ).astype(bool)

        # Stats
        corridor_fraction = np.sum(mask) / mask.size

        logger.info(
            f"OSM corridor mask: buffer={buffer_m:.0f}m, "
            f"coverage={corridor_fraction*100:.1f}%, features={prior.feature_count}"
        )

        return CorridorMask(
            mask=mask,
            transform=transform,
            crs=CRS.from_string(target_crs_str) if isinstance(target_crs_str, str) else target_crs,
            buffer_m=buffer_m,
            source="osm",
            stats={
                "feature_count": prior.feature_count,
                "corridor_fraction": float(corridor_fraction),
                "utm_crs": utm_crs,
                "target_crs": target_crs_str,
            },
        )

    def get_sword_corridor_mask(
        self,
        sword,
        reach_id: int,
        transform: Affine,
        shape: Tuple[int, int],
        target_crs: Union[CRS, str] = "EPSG:4326",
        min_buffer_m: float = 200.0,
    ) -> CorridorMask:
        """
        Create corridor mask from SWORD centerline.

        Buffer = max(3 × reach_width, min_buffer_m)

        Args:
            sword: SWORD object
            reach_id: SWORD reach ID
            transform: Affine transform for output raster
            shape: (height, width) of output raster
            target_crs: Target CRS for output (should match OPERA data)
            min_buffer_m: Minimum buffer width in meters

        Returns:
            CorridorMask with binary mask
        """
        # Get SWORD centerline (in WGS84)
        prior = self.get_sword_centerline(sword, reach_id)

        # Calculate buffer distance
        width = prior.stats["width"]
        buffer_m = max(3 * width, min_buffer_m)

        # Get UTM CRS for buffering
        centroid = prior.geometry.unary_union.centroid
        utm_zone = int((centroid.x + 180) / 6) + 1
        utm_crs = f"EPSG:326{utm_zone:02d}" if centroid.y >= 0 else f"EPSG:327{utm_zone:02d}"

        gdf_utm = prior.geometry.to_crs(utm_crs)

        # Buffer in meters (UTM)
        buffered = gdf_utm.buffer(buffer_m)
        corridor_geom = unary_union(buffered)

        # Convert target_crs to string if CRS object
        if hasattr(target_crs, "to_string"):
            target_crs_str = target_crs.to_string()
        else:
            target_crs_str = str(target_crs)

        # Reproject to target CRS for rasterization
        corridor_gdf = gpd.GeoDataFrame(geometry=[corridor_geom], crs=utm_crs).to_crs(target_crs_str)
        corridor_geom_target = corridor_gdf.geometry.iloc[0]

        # Rasterize in target CRS
        mask = rasterize(
            [(corridor_geom_target, 1)],
            out_shape=shape,
            transform=transform,
            fill=0,
            dtype=np.uint8,
        ).astype(bool)

        # Stats
        corridor_fraction = np.sum(mask) / mask.size

        logger.info(
            f"SWORD corridor mask for reach {reach_id}: buffer={buffer_m:.0f}m, "
            f"coverage={corridor_fraction*100:.1f}%, target_crs={target_crs_str}"
        )

        return CorridorMask(
            mask=mask,
            transform=transform,
            crs=CRS.from_string(target_crs_str) if isinstance(target_crs_str, str) else target_crs,
            buffer_m=buffer_m,
            source="sword",
            stats={
                "reach_id": reach_id,
                "width": width,
                "corridor_fraction": float(corridor_fraction),
                "utm_crs": utm_crs,
                "target_crs": target_crs_str,
            },
        )
