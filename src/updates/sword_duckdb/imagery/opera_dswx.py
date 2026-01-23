# -*- coding: utf-8 -*-
"""
OPERA DSWx Water Mask Client
============================

Access NASA OPERA Dynamic Surface Water Extent (DSWx) products via CMR-STAC.

OPERA DSWx provides pre-computed, validated water masks from Harmonized
Landsat-Sentinel (HLS) imagery at 30m resolution.

Water Classification Values (B01_WTR):
- 0: Not Water
- 1: Open Water
- 2: Partial Surface Water
- 8: Snow/Ice
- 9: Cloud/Cloud Shadow
- 252: Ocean Masked
- 253: Cloud Masked (additional)
- 255: Fill/NoData
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Tuple, Optional, List, Dict, Any
import logging

import numpy as np
from pystac_client import Client
from shapely.geometry import box
import rasterio
from rasterio.windows import from_bounds
from rasterio.crs import CRS
from rasterio.warp import transform_bounds
from affine import Affine

from .exceptions import ImageryError, NoImageryFoundError

logger = logging.getLogger(__name__)

# OPERA DSWx STAC Configuration
OPERA_STAC_URL = "https://cmr.earthdata.nasa.gov/cloudstac"
OPERA_CATALOG = "POCLOUD"
OPERA_COLLECTION_HLS = "OPERA_L3_DSWX-HLS_V1_1.0"  # Optical (HLS)
OPERA_COLLECTION_S1 = "OPERA_L3_DSWX-S1_V1"  # SAR (Sentinel-1)
# Backwards compat
OPERA_COLLECTION = OPERA_COLLECTION_HLS

# Water classification values
WATER_CLASSES = {
    0: "Not Water",
    1: "Open Water",
    2: "Partial Surface Water",
    8: "Snow/Ice",
    9: "Cloud/Cloud Shadow",
    252: "Ocean Masked",
    253: "Additional Masked",
    255: "Fill/NoData",
}

# Values considered water
WATER_VALUES = [1, 2]
# Values considered invalid/masked
INVALID_VALUES = [9, 252, 253, 255]


@dataclass
class DSWxResult:
    """Result from OPERA DSWx water mask retrieval."""

    water_mask: np.ndarray
    valid_mask: np.ndarray
    transform: Affine
    crs: CRS
    bbox: Tuple[float, float, float, float]
    item_id: str
    datetime: datetime
    stats: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DSWxComposite:
    """Composite from multiple OPERA DSWx images."""

    water_frequency: np.ndarray  # Fraction of valid obs with water
    any_water: np.ndarray  # Water detected at least once
    persistent_water: np.ndarray  # Water in >threshold of observations
    valid_count: np.ndarray  # Number of valid observations per pixel
    transform: Affine
    crs: CRS
    bbox: Tuple[float, float, float, float]
    n_images: int
    date_range: Tuple[str, str]
    stats: Dict[str, Any] = field(default_factory=dict)


class OPERADSWxClient:
    """
    Client for accessing OPERA DSWx water masks via CMR-STAC.

    Supports both HLS (optical) and S1 (SAR) products.
    Requires NASA Earthdata authentication via earthaccess.
    """

    def __init__(self, authenticate: bool = True, collection: str = "hls"):
        """
        Initialize OPERA DSWx client.

        Args:
            authenticate: Whether to authenticate with NASA Earthdata
            collection: Which collection to use: "hls", "s1", or "both"
        """
        self.stac_url = OPERA_STAC_URL
        self._collection_mode = collection.lower()
        if self._collection_mode == "s1":
            self.collection = OPERA_COLLECTION_S1
        elif self._collection_mode == "both":
            self.collection = [OPERA_COLLECTION_HLS, OPERA_COLLECTION_S1]
        else:
            self.collection = OPERA_COLLECTION_HLS
        self._api = None
        self._authenticated = False

        # GDAL config for HTTPS access
        self.gdal_config = {
            'GDAL_HTTP_COOKIEFILE': './cookies.txt',
            'GDAL_HTTP_COOKIEJAR': './cookies.txt',
            'GDAL_DISABLE_READDIR_ON_OPEN': 'EMPTY_DIR',
        }

        if authenticate:
            self._authenticate()

    def _authenticate(self) -> None:
        """Authenticate with NASA Earthdata."""
        try:
            import earthaccess
            earthaccess.login(strategy="netrc")
            self._authenticated = True
            logger.info("Authenticated with NASA Earthdata")
        except Exception as e:
            logger.warning(f"Earthdata authentication failed: {e}")
            logger.warning("Some OPERA data may not be accessible")

    @property
    def api(self) -> Client:
        """Lazy-load STAC API client."""
        if self._api is None:
            self._api = Client.open(f"{self.stac_url}/{OPERA_CATALOG}/")
        return self._api

    def search(
        self,
        bbox: Tuple[float, float, float, float],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        max_items: int = 100,
    ) -> List:
        """
        Search for OPERA DSWx items.

        Args:
            bbox: (xmin, ymin, xmax, ymax) in WGS84
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            max_items: Maximum items to return

        Returns:
            List of STAC items
        """
        aoi = box(*bbox)

        # Handle single or multiple collections
        collections = self.collection if isinstance(self.collection, list) else [self.collection]

        search_params = {
            "collections": collections,
            "intersects": aoi.__geo_interface__,
            "max_items": max_items,
        }

        if start_date and end_date:
            search_params["datetime"] = [start_date, end_date]
        elif start_date:
            search_params["datetime"] = f"{start_date}/.."
        elif end_date:
            search_params["datetime"] = f"../{end_date}"

        search = self.api.search(**search_params)
        items = list(search.items())

        logger.info(f"Found {len(items)} OPERA DSWx items for bbox {bbox} (collections: {collections})")
        return items

    def get_water_mask(
        self,
        item,
        bbox: Tuple[float, float, float, float],
    ) -> DSWxResult:
        """
        Get water mask from a single OPERA DSWx item.

        Supports both HLS and S1 asset naming conventions.

        Args:
            item: STAC item
            bbox: Bounding box in WGS84 to clip to

        Returns:
            DSWxResult with water mask and metadata
        """
        # Try HLS asset name first, then S1
        wtr_asset = item.assets.get('0_B01_WTR')  # HLS naming
        if not wtr_asset:
            wtr_asset = item.assets.get('B01_WTR')  # S1 naming
        if not wtr_asset:
            # Try to find any asset with WTR in the name
            for key, asset in item.assets.items():
                if 'WTR' in key.upper():
                    wtr_asset = asset
                    break
        if not wtr_asset:
            raise ImageryError(f"No water mask asset found in item {item.id}")

        with rasterio.Env(**self.gdal_config):
            with rasterio.open(wtr_asset.href) as src:
                # Transform bbox to image CRS
                src_bounds = transform_bounds(
                    CRS.from_epsg(4326), src.crs, *bbox
                )

                window = from_bounds(*src_bounds, transform=src.transform)
                data = src.read(1, window=window)
                window_transform = src.window_transform(window)

                # Create masks
                water_mask = np.isin(data, WATER_VALUES)
                valid_mask = ~np.isin(data, INVALID_VALUES)

                # Compute stats
                valid_count = np.sum(valid_mask)
                water_count = np.sum(water_mask & valid_mask)
                water_frac = water_count / max(1, valid_count)

                stats = {
                    "water_fraction": float(water_frac),
                    "water_pixel_count": int(water_count),
                    "total_valid_pixels": int(valid_count),
                    "class_distribution": {},
                }

                # Class distribution
                unique, counts = np.unique(data, return_counts=True)
                for val, cnt in zip(unique, counts):
                    label = WATER_CLASSES.get(val, f"Class{val}")
                    stats["class_distribution"][label] = int(cnt)

                logger.debug(
                    f"DSWx {item.id}: {water_frac*100:.2f}% water, "
                    f"shape={data.shape}"
                )

                return DSWxResult(
                    water_mask=water_mask,
                    valid_mask=valid_mask,
                    transform=window_transform,
                    crs=src.crs,
                    bbox=bbox,
                    item_id=item.id,
                    datetime=item.datetime,
                    stats=stats,
                )

    def get_composite(
        self,
        bbox: Tuple[float, float, float, float],
        start_date: str,
        end_date: str,
        max_images: int = 30,
        persistence_threshold: float = 0.5,
    ) -> DSWxComposite:
        """
        Create temporal composite from multiple OPERA DSWx images.

        Args:
            bbox: Bounding box in WGS84
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            max_images: Maximum images to use
            persistence_threshold: Threshold for persistent water (0-1)

        Returns:
            DSWxComposite with aggregated water masks
        """
        items = self.search(bbox, start_date, end_date, max_items=max_images)

        if len(items) == 0:
            raise NoImageryFoundError(
                "No OPERA DSWx items found",
                bbox=bbox,
                date_range=(start_date, end_date),
            )

        water_stack = []
        valid_stack = []
        ref_transform = None
        ref_crs = None
        ref_shape = None

        for item in items[:max_images]:
            try:
                result = self.get_water_mask(item, bbox)

                if ref_transform is None:
                    ref_transform = result.transform
                    ref_crs = result.crs
                    ref_shape = result.water_mask.shape

                # Ensure consistent shape - crop or pad to match reference
                water_mask = result.water_mask
                valid_mask = result.valid_mask

                if water_mask.shape != ref_shape:
                    h_ref, w_ref = ref_shape
                    h_cur, w_cur = water_mask.shape

                    # Create arrays of reference shape
                    water_aligned = np.zeros(ref_shape, dtype=water_mask.dtype)
                    valid_aligned = np.zeros(ref_shape, dtype=valid_mask.dtype)

                    # Copy overlapping region
                    h_copy = min(h_ref, h_cur)
                    w_copy = min(w_ref, w_cur)
                    water_aligned[:h_copy, :w_copy] = water_mask[:h_copy, :w_copy]
                    valid_aligned[:h_copy, :w_copy] = valid_mask[:h_copy, :w_copy]

                    water_mask = water_aligned
                    valid_mask = valid_aligned

                water_stack.append(water_mask)
                valid_stack.append(valid_mask)

            except Exception as e:
                logger.warning(f"Failed to process {item.id}: {e}")
                continue

        if len(water_stack) == 0:
            raise ImageryError("Failed to load any DSWx images")

        # Stack arrays (now guaranteed same shape)
        water_array = np.stack(water_stack, axis=0).astype(np.float32)
        valid_array = np.stack(valid_stack, axis=0).astype(np.float32)

        # Composite calculations
        water_sum = np.sum(water_array, axis=0)
        valid_count = np.sum(valid_array, axis=0).astype(np.int32)

        # Water frequency (fraction of valid obs with water)
        water_frequency = np.divide(
            water_sum, valid_count,
            where=valid_count > 0,
            out=np.zeros_like(water_sum),
        )

        # Binary masks
        any_water = water_sum > 0
        persistent_water = water_frequency > persistence_threshold

        # Stats
        total_valid = valid_count > 0
        stats = {
            "any_water_fraction": float(
                np.sum(any_water & total_valid) / max(1, np.sum(total_valid))
            ),
            "persistent_water_fraction": float(
                np.sum(persistent_water & total_valid) / max(1, np.sum(total_valid))
            ),
            "mean_valid_observations": float(np.mean(valid_count[total_valid])),
            "images_used": len(water_stack),
        }

        logger.info(
            f"DSWx composite from {len(water_stack)} images: "
            f"{stats['any_water_fraction']*100:.2f}% any water, "
            f"{stats['persistent_water_fraction']*100:.2f}% persistent"
        )

        return DSWxComposite(
            water_frequency=water_frequency,
            any_water=any_water,
            persistent_water=persistent_water,
            valid_count=valid_count,
            transform=ref_transform,
            crs=ref_crs,
            bbox=bbox,
            n_images=len(water_stack),
            date_range=(start_date, end_date),
            stats=stats,
        )
