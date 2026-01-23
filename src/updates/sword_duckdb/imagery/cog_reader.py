# -*- coding: utf-8 -*-
"""
Cloud Optimized GeoTIFF (COG) Reader
====================================

Efficient streaming of COG data using rasterio windowed reads.
"""

from typing import Tuple, Optional, Dict, Any
import logging

import numpy as np
import rasterio
from rasterio.windows import from_bounds
from rasterio.crs import CRS
from rasterio.warp import calculate_default_transform, reproject, Resampling
from affine import Affine
from pystac import Item

from .config import ImageryConfig, DEFAULT_CONFIG, S2_BANDS, apply_gdal_env
from .exceptions import COGReadError, BandMismatchError

logger = logging.getLogger(__name__)

# Apply GDAL environment on module import
apply_gdal_env()


class COGReader:
    """
    Reader for Cloud Optimized GeoTIFFs.

    Supports windowed reads for efficient partial downloads.
    """

    def __init__(self, config: ImageryConfig = None):
        """
        Initialize COG reader.

        Args:
            config: Imagery configuration.
        """
        self.config = config or DEFAULT_CONFIG

    def read_window(
        self,
        cog_url: str,
        bbox: Tuple[float, float, float, float],
        target_crs: str = "EPSG:4326",
        resolution: Optional[float] = None,
    ) -> Tuple[np.ndarray, Affine, CRS]:
        """
        Read a window from a COG.

        Args:
            cog_url: URL to the COG file
            bbox: (xmin, ymin, xmax, ymax) in WGS84 (EPSG:4326)
            target_crs: Target coordinate reference system (for output)
            resolution: Target resolution in CRS units (None = native)

        Returns:
            Tuple of (data array, transform, CRS)
        """
        try:
            with rasterio.open(cog_url) as src:
                src_crs = src.crs

                # Transform bbox from WGS84 to source CRS
                src_bbox = transform_bbox(bbox, "EPSG:4326", str(src_crs))

                # Calculate window from bounds
                window = from_bounds(*src_bbox, transform=src.transform)

                # Ensure window has valid dimensions
                if window.width < 1 or window.height < 1:
                    raise COGReadError(
                        f"Window too small: {window.width}x{window.height}",
                        url=cog_url,
                    )

                # Read data
                data = src.read(1, window=window)
                window_transform = src.window_transform(window)

                logger.debug(
                    f"Read window from {cog_url}: shape={data.shape}, "
                    f"crs={src_crs}"
                )

                # Return in source CRS (no reprojection for simplicity)
                return data, window_transform, src_crs

        except rasterio.errors.RasterioIOError as e:
            raise COGReadError(f"Failed to read COG: {e}", url=cog_url)
        except COGReadError:
            raise
        except Exception as e:
            raise COGReadError(f"Unexpected error reading COG: {e}", url=cog_url)

    def read_band(
        self,
        item: Item,
        band_name: str,
        bbox: Tuple[float, float, float, float],
        **kwargs,
    ) -> Tuple[np.ndarray, Affine, CRS]:
        """
        Read a specific band from a STAC item.

        Args:
            item: STAC Item
            band_name: Band name (e.g., 'green', 'nir', 'B03', 'B08')
            bbox: Bounding box for window
            **kwargs: Additional arguments for read_window

        Returns:
            Tuple of (data array, transform, CRS)
        """
        # Resolve band name to asset key
        asset_key = self._resolve_band_asset(item, band_name)

        if asset_key not in item.assets:
            raise BandMismatchError(
                f"Band {band_name} not found in item {item.id}",
                expected=[band_name],
                available=list(item.assets.keys()),
            )

        asset = item.assets[asset_key]
        cog_url = asset.href

        logger.info(f"Reading band {band_name} ({asset_key}) from {item.id}")

        return self.read_window(cog_url, bbox, **kwargs)

    def read_bands_for_ndwi(
        self,
        item: Item,
        bbox: Tuple[float, float, float, float],
        **kwargs,
    ) -> Tuple[np.ndarray, np.ndarray, Affine, CRS]:
        """
        Read green and NIR bands for NDWI computation.

        Args:
            item: STAC Item
            bbox: Bounding box for window
            **kwargs: Additional arguments for read_window

        Returns:
            Tuple of (green array, nir array, transform, CRS)
        """
        green, transform, crs = self.read_band(item, "green", bbox, **kwargs)
        nir, _, _ = self.read_band(item, "nir", bbox, **kwargs)

        # Verify shapes match
        if green.shape != nir.shape:
            raise BandMismatchError(
                f"Band shape mismatch: green={green.shape}, nir={nir.shape}",
                expected=["matching shapes"],
                available=[f"green:{green.shape}", f"nir:{nir.shape}"],
            )

        return green, nir, transform, crs

    def get_item_metadata(self, item: Item) -> Dict[str, Any]:
        """
        Extract relevant metadata from a STAC item.

        Args:
            item: STAC Item

        Returns:
            Dictionary of metadata
        """
        props = item.properties
        return {
            "item_id": item.id,
            "collection": item.collection_id,
            "datetime": props.get("datetime"),
            "cloud_cover": props.get("eo:cloud_cover"),
            "sun_azimuth": props.get("view:sun_azimuth"),
            "sun_elevation": props.get("view:sun_elevation"),
            "platform": props.get("platform"),
            "instrument": props.get("instruments"),
            "processing_level": props.get("processing:level"),
            "bbox": item.bbox,
            "geometry": item.geometry,
        }

    def _resolve_band_asset(self, item: Item, band_name: str) -> str:
        """
        Resolve a band name to its asset key in the STAC item.

        Args:
            item: STAC Item
            band_name: Band name (friendly or raw)

        Returns:
            Asset key in item.assets
        """
        band_lower = band_name.lower()

        # Direct match first (Element 84 uses 'green', 'nir', etc.)
        if band_lower in item.assets:
            return band_lower

        # Check friendly name mapping
        if band_lower in S2_BANDS:
            raw_band = S2_BANDS[band_lower]
            # Try the raw band name
            if raw_band.lower() in item.assets:
                return raw_band.lower()
            if raw_band in item.assets:
                return raw_band

        # Try variations
        variations = [
            band_name,
            band_name.lower(),
            band_name.upper(),
        ]

        # If it looks like a band number (B03, B08), try friendly names
        if band_name.upper().startswith("B"):
            # Reverse lookup: B03 -> green, B08 -> nir
            reverse_map = {v: k for k, v in S2_BANDS.items()}
            if band_name.upper() in reverse_map:
                friendly = reverse_map[band_name.upper()]
                variations.insert(0, friendly)

        for var in variations:
            if var in item.assets:
                return var

        # Return original, let caller handle error
        return band_lower


def transform_bbox(
    bbox: Tuple[float, float, float, float],
    src_crs: str,
    dst_crs: str,
) -> Tuple[float, float, float, float]:
    """
    Transform a bounding box between CRS.

    Args:
        bbox: (xmin, ymin, xmax, ymax)
        src_crs: Source CRS
        dst_crs: Destination CRS

    Returns:
        Transformed bbox
    """
    from rasterio.warp import transform_bounds

    return transform_bounds(
        CRS.from_string(src_crs),
        CRS.from_string(dst_crs),
        *bbox,
    )


def reproject_array(
    data: np.ndarray,
    src_transform: Affine,
    src_crs: CRS,
    dst_crs: str,
    resolution: float,
) -> Tuple[np.ndarray, Affine]:
    """
    Reproject an array to a new CRS.

    Args:
        data: Source array
        src_transform: Source affine transform
        src_crs: Source CRS
        dst_crs: Destination CRS string
        resolution: Target resolution

    Returns:
        Tuple of (reprojected array, new transform)
    """
    dst_crs_obj = CRS.from_string(dst_crs)

    # Calculate output dimensions and transform
    dst_transform, width, height = calculate_default_transform(
        src_crs,
        dst_crs_obj,
        data.shape[1],
        data.shape[0],
        *rasterio.transform.array_bounds(data.shape[0], data.shape[1], src_transform),
        resolution=(resolution, resolution),
    )

    # Create output array
    dst_data = np.empty((height, width), dtype=data.dtype)

    # Reproject
    reproject(
        source=data,
        destination=dst_data,
        src_transform=src_transform,
        src_crs=src_crs,
        dst_transform=dst_transform,
        dst_crs=dst_crs_obj,
        resampling=Resampling.bilinear,
    )

    return dst_data, dst_transform
