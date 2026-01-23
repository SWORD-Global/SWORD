# -*- coding: utf-8 -*-
"""
OPERA + Sentinel-2 Fusion Centerline Extraction
================================================

Fuse OPERA SAR water detection with Sentinel-2 optical imagery to extract
river centerlines at 10m resolution.

Pipeline:
1. OPERA (30m) provides reliable water detection (SAR, all-weather)
2. Learn local S2 water signature from OPERA-labeled samples
3. Apply learned threshold to S2 at 10m, constrained to OPERA region
4. Fuse OPERA + S2 for continuous water mask
5. Skeletonize and route to extract single centerline

Usage:
    from sword_duckdb.imagery.fusion_centerline import FusionCenterlineExtractor

    extractor = FusionCenterlineExtractor()
    result = extractor.extract(
        bbox=(-98.35, 35.0, -98.25, 35.1),
        start_date="2023-01-01",
        end_date="2024-12-31",
        sword_endpoints=[(row1, col1), (row2, col2)]  # Optional
    )

    print(f"Centerline: {len(result.centerline)} points")
    print(f"Support: {result.support:.1%}")
"""

import numpy as np
import logging
from dataclasses import dataclass, field
from typing import Tuple, Optional, List, Dict, Any

from scipy.ndimage import zoom, binary_dilation, binary_erosion, binary_closing
from scipy.ndimage import distance_transform_edt, uniform_filter1d, gaussian_filter, label
from scipy.spatial import cKDTree
from skimage.morphology import skeletonize, remove_small_objects, disk
from skimage.graph import route_through_array
from skimage.measure import regionprops
from affine import Affine
from rasterio.crs import CRS

from .opera_dswx import OPERADSWxClient
from .stac_client import SentinelSTACClient
from .cog_reader import COGReader

logger = logging.getLogger(__name__)


def filter_elongated_features(
    water_mask: np.ndarray,
    min_elongation: float = 3.0,
    min_area: int = 50,
    large_feature_area: int = 5000,
) -> np.ndarray:
    """
    Filter water mask to keep only elongated features (rivers).

    Rivers are elongated (long and narrow), while ponds/lakes are round.
    Elongation = major_axis / minor_axis. Rivers typically have elongation > 3.

    Args:
        water_mask: Binary water mask
        min_elongation: Minimum elongation ratio to keep (default 3.0)
        min_area: Minimum area in pixels to consider (default 50)
        large_feature_area: Features larger than this are always kept (default 5000)

    Returns:
        Filtered water mask with only elongated features
    """
    labeled, n_features = label(water_mask)
    river_mask = np.zeros_like(water_mask, dtype=bool)

    for region in regionprops(labeled):
        # Skip tiny features
        if region.area < min_area:
            continue

        # Compute elongation (major / minor axis)
        if region.minor_axis_length > 0:
            elongation = region.major_axis_length / region.minor_axis_length
        else:
            elongation = 999  # Essentially a line

        # Keep if elongated OR very large (main river body)
        if elongation >= min_elongation or region.area > large_feature_area:
            river_mask[labeled == region.label] = True

    return river_mask


@dataclass
class FusionWaterMask:
    """Result from OPERA + S2 water mask fusion."""
    mask: np.ndarray  # Binary water mask at 10m
    opera_mask: np.ndarray  # Original OPERA mask (upsampled)
    s2_mask: np.ndarray  # S2-derived mask (constrained)
    transform: Affine
    crs: CRS
    shape: Tuple[int, int]
    stats: Dict[str, Any] = field(default_factory=dict)


@dataclass
class FusionCenterlineResult:
    """Result from fusion-based centerline extraction."""
    centerline: np.ndarray  # Nx2 array of (row, col) in pixel coords
    centerline_geo: np.ndarray  # Nx2 array of (x, y) in CRS coords
    water_mask: FusionWaterMask
    skeleton: np.ndarray
    support: float  # Fraction of centerline on water
    length_px: float
    transform: Affine
    crs: CRS
    stats: Dict[str, Any] = field(default_factory=dict)


class FusionCenterlineExtractor:
    """
    Extract river centerlines by fusing OPERA and Sentinel-2.

    OPERA provides reliable SAR-based water detection at 30m.
    S2 provides 10m resolution but struggles with turbid water.
    Fusion: use OPERA as training labels to learn local S2 water signature,
    then apply at 10m to fill gaps.
    """

    def __init__(
        self,
        opera_threshold: float = 0.6,  # Water 60%+ of time = persistent water
        opera_core_threshold: float = 0.2,
        opera_buffer_px: int = 15,
        min_water_size: int = 30,
        closing_radius: int = 0,  # Disabled - was causing inflation
        smoothing_size: int = 9,
        n_s2_scenes: int = 5,
        s2_max_cloud: float = 20.0,
        s2_fallback_threshold: float = 0.0,  # MNDWI threshold when OPERA empty
        min_opera_water_px: int = 100,  # Min OPERA pixels before using S2 fallback
        connectivity_blur_sigma: float = 0.0,  # No blur - keeps ponds separate from river
        connectivity_threshold: float = 0.15,  # Threshold after blur (unused when sigma=0)
        filter_elongation: bool = True,  # Filter to keep only elongated features (rivers)
        min_elongation: float = 3.0,  # Min elongation ratio for river features
    ):
        """
        Initialize extractor.

        Args:
            opera_threshold: OPERA frequency threshold for water mask
            opera_core_threshold: Higher threshold for training samples
            opera_buffer_px: Buffer around OPERA detections for S2 constraint (pixels at 10m)
            min_water_size: Minimum connected component size to keep
            closing_radius: Morphological closing radius
            smoothing_size: Centerline smoothing window
            n_s2_scenes: Number of S2 scenes for composite
            s2_max_cloud: Maximum cloud cover for S2 scenes
        """
        self.opera_threshold = opera_threshold
        self.opera_core_threshold = opera_core_threshold
        self.opera_buffer_px = opera_buffer_px
        self.min_water_size = min_water_size
        self.closing_radius = closing_radius
        self.smoothing_size = smoothing_size
        self.n_s2_scenes = n_s2_scenes
        self.s2_max_cloud = s2_max_cloud
        self.s2_fallback_threshold = s2_fallback_threshold
        self.min_opera_water_px = min_opera_water_px
        self.connectivity_blur_sigma = connectivity_blur_sigma
        self.connectivity_threshold = connectivity_threshold
        self.filter_elongation = filter_elongation
        self.min_elongation = min_elongation

        self.opera_client = OPERADSWxClient()
        self.s2_client = SentinelSTACClient()
        self.cog_reader = COGReader()

    def get_fused_water_mask(
        self,
        bbox: Tuple[float, float, float, float],
        start_date: str,
        end_date: str,
    ) -> FusionWaterMask:
        """
        Create fused water mask from OPERA + S2.

        Args:
            bbox: (xmin, ymin, xmax, ymax) in WGS84
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)

        Returns:
            FusionWaterMask with 10m resolution water mask
        """
        # 1. Get OPERA composite
        logger.info(f"Loading OPERA for bbox {bbox}")
        opera_composite = self.opera_client.get_composite(
            bbox=bbox, start_date=start_date, end_date=end_date
        )
        opera_freq = opera_composite.water_frequency
        opera_transform = opera_composite.transform
        opera_crs = opera_composite.crs

        logger.info(f"OPERA: {opera_freq.shape}, {np.sum(opera_freq > self.opera_threshold)} water px")

        # 2. Get S2 composite
        logger.info("Loading Sentinel-2 composite")
        s2_items = list(self.s2_client.search_by_bbox(
            bbox=bbox, start_date=start_date, end_date=end_date,
            max_cloud_cover=self.s2_max_cloud, limit=self.n_s2_scenes * 2
        ))
        s2_items.sort(key=lambda x: x.properties.get('eo:cloud_cover', 100))

        if not s2_items:
            logger.warning("No S2 scenes found, using OPERA only")
            # Return upsampled OPERA as fallback
            scale = 3.0  # Approximate 30m -> 10m
            opera_10m = zoom(opera_freq, scale, order=1)
            s2_transform = Affine(
                opera_transform.a / scale, opera_transform.b, opera_transform.c,
                opera_transform.d, opera_transform.e / scale, opera_transform.f
            )
            mask = opera_10m > self.opera_threshold
            return FusionWaterMask(
                mask=mask,
                opera_mask=opera_10m > self.opera_threshold,
                s2_mask=np.zeros_like(mask),
                transform=s2_transform,
                crs=opera_crs,
                shape=mask.shape,
                stats={"method": "opera_only", "n_s2_scenes": 0}
            )

        # Build S2 composite with all bands for MNDWI + AWEInsh
        green_sum, nir_sum, swir1_sum, swir2_sum, valid_count = None, None, None, None, None
        s2_shape, s2_transform = None, None

        for item in s2_items[:self.n_s2_scenes]:
            try:
                green_asset = item.assets.get('green') or item.assets.get('B03')
                nir_asset = item.assets.get('nir') or item.assets.get('B08')
                swir1_asset = item.assets.get('swir16') or item.assets.get('B11')
                swir2_asset = item.assets.get('swir22') or item.assets.get('B12')

                if not (green_asset and swir1_asset):
                    continue

                green, gt, _ = self.cog_reader.read_window(green_asset.href, bbox)
                swir1, _, _ = self.cog_reader.read_window(swir1_asset.href, bbox)

                # Try to get NIR and SWIR2 for AWEInsh (optional)
                nir, swir2 = None, None
                if nir_asset:
                    try:
                        nir, _, _ = self.cog_reader.read_window(nir_asset.href, bbox)
                    except:
                        pass
                if swir2_asset:
                    try:
                        swir2, _, _ = self.cog_reader.read_window(swir2_asset.href, bbox)
                    except:
                        pass

                # Resample SWIR bands to 10m
                if swir1.shape != green.shape:
                    swir1 = zoom(swir1, (green.shape[0]/swir1.shape[0],
                                         green.shape[1]/swir1.shape[1]), order=1)
                if nir is not None and nir.shape != green.shape:
                    nir = zoom(nir, (green.shape[0]/nir.shape[0],
                                     green.shape[1]/nir.shape[1]), order=1)
                if swir2 is not None and swir2.shape != green.shape:
                    swir2 = zoom(swir2, (green.shape[0]/swir2.shape[0],
                                         green.shape[1]/swir2.shape[1]), order=1)

                if green_sum is None:
                    s2_shape = green.shape
                    s2_transform = gt
                    green_sum = np.zeros(s2_shape, dtype=float)
                    nir_sum = np.zeros(s2_shape, dtype=float)
                    swir1_sum = np.zeros(s2_shape, dtype=float)
                    swir2_sum = np.zeros(s2_shape, dtype=float)
                    valid_count = np.zeros(s2_shape, dtype=int)

                # Align to reference shape if different
                if green.shape != s2_shape:
                    h_ref, w_ref = s2_shape
                    h_cur, w_cur = green.shape
                    h_use, w_use = min(h_ref, h_cur), min(w_ref, w_cur)

                    green_aligned = np.zeros(s2_shape, dtype=green.dtype)
                    swir1_aligned = np.zeros(s2_shape, dtype=swir1.dtype)
                    green_aligned[:h_use, :w_use] = green[:h_use, :w_use]
                    swir1_aligned[:h_use, :w_use] = swir1[:h_use, :w_use]
                    green = green_aligned
                    swir1 = swir1_aligned

                    if nir is not None:
                        nir_aligned = np.zeros(s2_shape, dtype=nir.dtype)
                        nir_aligned[:h_use, :w_use] = nir[:h_use, :w_use]
                        nir = nir_aligned
                    if swir2 is not None:
                        swir2_aligned = np.zeros(s2_shape, dtype=swir2.dtype)
                        swir2_aligned[:h_use, :w_use] = swir2[:h_use, :w_use]
                        swir2 = swir2_aligned

                valid = (green > 0) & (swir1 > 0) & (green < 10000)

                green_sum[valid] += green[valid].astype(float)
                swir1_sum[valid] += swir1[valid].astype(float)
                if nir is not None:
                    nir_sum[valid] += nir[valid].astype(float)
                if swir2 is not None:
                    swir2_sum[valid] += swir2[valid].astype(float)
                valid_count[valid] += 1

            except Exception as e:
                logger.warning(f"Error reading S2 scene: {e}")
                continue

        if valid_count is None or np.sum(valid_count > 0) == 0:
            logger.warning("Failed to build S2 composite, using OPERA only")
            scale = 3.0
            opera_10m = zoom(opera_freq, scale, order=1)
            s2_transform = Affine(
                opera_transform.a / scale, opera_transform.b, opera_transform.c,
                opera_transform.d, opera_transform.e / scale, opera_transform.f
            )
            mask = opera_10m > self.opera_threshold
            return FusionWaterMask(
                mask=mask,
                opera_mask=opera_10m > self.opera_threshold,
                s2_mask=np.zeros_like(mask),
                transform=s2_transform,
                crs=opera_crs,
                shape=mask.shape,
                stats={"method": "opera_only", "n_s2_scenes": 0}
            )

        # Compute mean bands
        with np.errstate(invalid='ignore', divide='ignore'):
            green_mean = green_sum / np.maximum(valid_count, 1)
            nir_mean = nir_sum / np.maximum(valid_count, 1)
            swir1_mean = swir1_sum / np.maximum(valid_count, 1)
            swir2_mean = swir2_sum / np.maximum(valid_count, 1)

            # MNDWI = (Green - SWIR1) / (Green + SWIR1)
            mndwi = (green_mean - swir1_mean) / (green_mean + swir1_mean)

            # AWEInsh = 4 * (Green - SWIR1) - (0.25 * NIR + 2.75 * SWIR2)
            awei = 4.0 * (green_mean - swir1_mean) - (0.25 * nir_mean + 2.75 * swir2_mean)

        mndwi = np.nan_to_num(mndwi, nan=0)
        awei = np.nan_to_num(awei, nan=-9999)
        s2_valid = valid_count > 0

        # Check if we have all bands for AWEInsh
        has_awei = np.sum(nir_sum) > 0 and np.sum(swir2_sum) > 0

        logger.info(f"S2 composite: {s2_shape}, {np.sum(s2_valid)} valid px, AWEInsh={'yes' if has_awei else 'no'}")

        # 3. Upsample OPERA to 10m and align to S2 grid
        scale = opera_transform.a / s2_transform.a
        opera_10m = zoom(opera_freq, scale, order=1)

        # Ensure OPERA matches S2 shape - crop or pad as needed
        h_target, w_target = s2_shape
        h_opera, w_opera = opera_10m.shape

        if opera_10m.shape != s2_shape:
            opera_aligned = np.zeros(s2_shape, dtype=opera_10m.dtype)
            h_use, w_use = min(h_opera, h_target), min(w_opera, w_target)
            opera_aligned[:h_use, :w_use] = opera_10m[:h_use, :w_use]
            opera_10m = opera_aligned

        # 4. Check if OPERA has enough water - if not, use S2 standalone
        opera_water_px = np.sum(opera_10m > self.opera_threshold)

        if opera_water_px < self.min_opera_water_px:
            logger.warning(f"OPERA has only {opera_water_px} water px (min={self.min_opera_water_px}), using S2 standalone")
            # Use combined MNDWI + AWEInsh as fallback
            mndwi_water = (mndwi > self.s2_fallback_threshold) & s2_valid

            # AWEInsh threshold (positive = water)
            awei_threshold = 0.0
            if has_awei:
                awei_water = (awei > awei_threshold) & s2_valid
                s2_water = mndwi_water | awei_water
                logger.info(f"S2 standalone: MNDWI={np.sum(mndwi_water)}, AWEInsh={np.sum(awei_water)}, combined={np.sum(s2_water)}")
            else:
                s2_water = mndwi_water
                logger.info(f"S2 standalone (MNDWI only): {np.sum(s2_water)} px")

            s2_water = remove_small_objects(s2_water, min_size=self.min_water_size)
            if self.closing_radius > 0:
                s2_water = binary_closing(s2_water, disk(self.closing_radius))

            logger.info(f"S2 standalone mask after cleanup: {np.sum(s2_water)} water px at 10m")

            # Apply elongation filter to remove round ponds
            pre_filter_px = int(np.sum(s2_water))
            if self.filter_elongation:
                s2_water = filter_elongated_features(
                    s2_water,
                    min_elongation=self.min_elongation,
                    min_area=self.min_water_size,
                )
                logger.info(f"After elongation filter (min={self.min_elongation}): {np.sum(s2_water)} px (was {pre_filter_px})")

            return FusionWaterMask(
                mask=s2_water,
                opera_mask=opera_10m > self.opera_threshold,
                s2_mask=s2_water,
                transform=s2_transform,
                crs=opera_crs,
                shape=s2_shape,
                stats={
                    "method": "s2_standalone_combined" if has_awei else "s2_standalone_mndwi",
                    "n_s2_scenes": int(np.max(valid_count)),
                    "mndwi_threshold": float(self.s2_fallback_threshold),
                    "awei_threshold": float(awei_threshold) if has_awei else None,
                    "opera_water_px": int(opera_water_px),
                    "mndwi_water_px": int(np.sum(mndwi_water)),
                    "awei_water_px": int(np.sum(awei_water)) if has_awei else 0,
                    "s2_water_px": int(np.sum(s2_water)),
                    "elongation_filtered": self.filter_elongation,
                }
            )

        # 5. Learn water threshold from OPERA samples
        opera_water_core = binary_erosion(opera_10m > self.opera_core_threshold, disk(2))
        opera_land = opera_10m < 0.05

        water_mndwi = mndwi[opera_water_core & s2_valid]
        land_mndwi = mndwi[opera_land & s2_valid]

        if len(water_mndwi) < 10 or len(land_mndwi) < 10:
            logger.warning("Not enough samples for threshold learning, using default")
            best_thresh = -0.3
        else:
            # Find optimal threshold
            all_vals = np.concatenate([water_mndwi, land_mndwi])
            thresholds = np.percentile(all_vals, np.arange(10, 90, 5))

            best_thresh, best_score = -0.3, 0
            for t in thresholds:
                score = np.mean(water_mndwi > t) + np.mean(land_mndwi <= t)
                if score > best_score:
                    best_score = score
                    best_thresh = t

            logger.info(f"Learned threshold: {best_thresh:.3f} (water={np.mean(water_mndwi):.3f}, land={np.mean(land_mndwi):.3f})")

        # 5. Apply threshold constrained to OPERA region
        s2_water = (mndwi > best_thresh) & s2_valid
        opera_region = binary_dilation(opera_10m > 0.05, disk(self.opera_buffer_px))
        s2_water_constrained = s2_water & opera_region

        # 6. Fuse: OPERA OR constrained S2
        combined = (opera_10m > self.opera_threshold) | s2_water_constrained
        combined = remove_small_objects(combined, min_size=self.min_water_size)
        if self.closing_radius > 0:
            combined = binary_closing(combined, disk(self.closing_radius))

        pre_blur_px = int(np.sum(combined))
        logger.info(f"Fused mask before blur: {pre_blur_px} water px at 10m")

        # 7. Apply connectivity blur to connect fragments
        if self.connectivity_blur_sigma > 0:
            blurred = gaussian_filter(combined.astype(float), sigma=self.connectivity_blur_sigma)
            combined_blurred = blurred > self.connectivity_threshold
            # Clean up after blur
            combined_blurred = remove_small_objects(combined_blurred, min_size=self.min_water_size)
            if self.closing_radius > 0:
                combined_blurred = binary_closing(combined_blurred, disk(self.closing_radius))
            logger.info(f"After connectivity blur (σ={self.connectivity_blur_sigma}): {np.sum(combined_blurred)} px")
            combined = combined_blurred

        # 8. Apply elongation filter to remove round ponds, keep linear rivers
        pre_filter_px = int(np.sum(combined))
        if self.filter_elongation:
            combined = filter_elongated_features(
                combined,
                min_elongation=self.min_elongation,
                min_area=self.min_water_size,
            )
            logger.info(f"After elongation filter (min={self.min_elongation}): {np.sum(combined)} px (was {pre_filter_px})")

        return FusionWaterMask(
            mask=combined,
            opera_mask=opera_10m > self.opera_threshold,
            s2_mask=s2_water_constrained,
            transform=s2_transform,
            crs=opera_crs,
            shape=s2_shape,
            stats={
                "method": "opera_s2_fusion",
                "n_s2_scenes": int(np.max(valid_count)),
                "learned_threshold": float(best_thresh),
                "opera_water_px": int(np.sum(opera_10m > self.opera_threshold)),
                "s2_water_px": int(np.sum(s2_water_constrained)),
                "pre_blur_water_px": pre_blur_px,
                "fused_water_px": int(np.sum(combined)),
                "blur_sigma": self.connectivity_blur_sigma,
                "elongation_filtered": self.filter_elongation,
                "min_elongation": self.min_elongation if self.filter_elongation else None,
            }
        )

    def extract(
        self,
        bbox: Tuple[float, float, float, float],
        start_date: str = "2023-01-01",
        end_date: str = "2024-12-31",
        endpoints: Optional[Tuple[Tuple[int, int], Tuple[int, int]]] = None,
        sword_coords: Optional[np.ndarray] = None,
    ) -> FusionCenterlineResult:
        """
        Extract centerline from fused OPERA + S2 water mask.

        Args:
            bbox: (xmin, ymin, xmax, ymax) in WGS84
            start_date: Start date for imagery
            end_date: End date for imagery
            endpoints: Optional ((r1,c1), (r2,c2)) endpoints in pixel coords
            sword_coords: Optional Nx2 array of SWORD coords in WGS84 (x, y)
                         Used to derive endpoints if not provided

        Returns:
            FusionCenterlineResult with extracted centerline
        """
        # Get fused water mask
        water_mask = self.get_fused_water_mask(bbox, start_date, end_date)

        # Skeletonize
        skeleton = skeletonize(water_mask.mask)
        logger.info(f"Skeleton: {np.sum(skeleton)} px")

        if np.sum(skeleton) == 0:
            logger.error("Empty skeleton, cannot extract centerline")
            return FusionCenterlineResult(
                centerline=np.array([]),
                centerline_geo=np.array([]),
                water_mask=water_mask,
                skeleton=skeleton,
                support=0.0,
                length_px=0,
                transform=water_mask.transform,
                crs=water_mask.crs,
                stats={"error": "empty_skeleton"}
            )

        # Determine endpoints
        if endpoints is None and sword_coords is not None:
            # Transform SWORD to pixel coords
            from pyproj import Transformer
            xform = Transformer.from_crs('EPSG:4326', water_mask.crs, always_xy=True)
            sword_utm = np.array([xform.transform(x, y) for x, y in sword_coords])
            inv_t = ~water_mask.transform
            sword_px = np.array([inv_t * (x, y) for x, y in sword_utm])
            sword_px = sword_px[:, ::-1]  # (col, row) -> (row, col)

            # Clip to bounds
            h, w = water_mask.shape
            sword_px[:, 0] = np.clip(sword_px[:, 0], 0, h - 1)
            sword_px[:, 1] = np.clip(sword_px[:, 1], 0, w - 1)

            endpoints = (
                (int(sword_px[0, 0]), int(sword_px[0, 1])),
                (int(sword_px[-1, 0]), int(sword_px[-1, 1]))
            )

        if endpoints is None:
            # Find endpoints from skeleton extremes
            skel_r, skel_c = np.where(skeleton)
            endpoints = (
                (skel_r[0], skel_c[0]),
                (skel_r[-1], skel_c[-1])
            )

        # Find nearest skeleton points to endpoints
        skel_r, skel_c = np.where(skeleton)
        start_pt, end_pt = endpoints

        dists = (skel_r - start_pt[0])**2 + (skel_c - start_pt[1])**2
        nearest_start = (skel_r[np.argmin(dists)], skel_c[np.argmin(dists)])

        dists = (skel_r - end_pt[0])**2 + (skel_c - end_pt[1])**2
        nearest_end = (skel_r[np.argmin(dists)], skel_c[np.argmin(dists)])

        logger.info(f"Routing from {nearest_start} to {nearest_end}")

        # Build cost surface
        dt = distance_transform_edt(water_mask.mask)
        dt_norm = dt / (dt.max() + 1e-6)
        skel_cost = np.where(skeleton, 0.1, 10.0)
        skel_cost = skel_cost - dt_norm * 0.5
        skel_cost = np.clip(skel_cost, 0.01, 100)

        # Route through skeleton
        try:
            path_indices, path_cost = route_through_array(
                skel_cost, nearest_start, nearest_end, fully_connected=True
            )
            centerline_raw = np.array(path_indices)
            logger.info(f"Path: {len(centerline_raw)} points, cost={path_cost:.1f}")
        except Exception as e:
            logger.error(f"Routing failed: {e}")
            return FusionCenterlineResult(
                centerline=np.array([]),
                centerline_geo=np.array([]),
                water_mask=water_mask,
                skeleton=skeleton,
                support=0.0,
                length_px=0,
                transform=water_mask.transform,
                crs=water_mask.crs,
                stats={"error": str(e)}
            )

        # Smooth
        if len(centerline_raw) > 10:
            centerline_smooth = centerline_raw.copy().astype(float)
            centerline_smooth[:, 0] = uniform_filter1d(
                centerline_smooth[:, 0], size=self.smoothing_size, mode='nearest')
            centerline_smooth[:, 1] = uniform_filter1d(
                centerline_smooth[:, 1], size=self.smoothing_size, mode='nearest')
        else:
            centerline_smooth = centerline_raw.astype(float)

        # Subsample
        if len(centerline_smooth) > 200:
            indices = np.linspace(0, len(centerline_smooth)-1, 200).astype(int)
            centerline_final = centerline_smooth[indices]
        else:
            centerline_final = centerline_smooth

        # Convert to geographic coordinates
        centerline_geo = np.array([
            water_mask.transform * (c, r)
            for r, c in centerline_final
        ])

        # Compute metrics
        support = np.mean([
            water_mask.mask[int(r), int(c)]
            for r, c in centerline_final
            if 0 <= int(r) < water_mask.shape[0] and 0 <= int(c) < water_mask.shape[1]
        ])

        # Length in pixels
        diffs = np.diff(centerline_final, axis=0)
        length_px = np.sum(np.sqrt(diffs[:, 0]**2 + diffs[:, 1]**2))

        logger.info(f"Final centerline: {len(centerline_final)} pts, support={support:.1%}, length={length_px:.0f}px")

        return FusionCenterlineResult(
            centerline=centerline_final,
            centerline_geo=centerline_geo,
            water_mask=water_mask,
            skeleton=skeleton,
            support=support,
            length_px=length_px,
            transform=water_mask.transform,
            crs=water_mask.crs,
            stats={
                "n_points": len(centerline_final),
                "path_cost": float(path_cost),
                **water_mask.stats
            }
        )


@dataclass
class WaterMaskClassification:
    """Classification of SWORD accuracy based on water mask corridor overlap."""
    reach_id: int
    raw_water_px: int  # Total water detected in bbox
    corridor_water_px: int  # Water within SWORD corridor
    corridor_ratio: float  # corridor_water / raw_water
    classification: str  # STABLE, DRIFT, or MISMATCH
    confidence: str  # HIGH, MEDIUM, LOW based on water count
    corridor_buffer_m: float
    stats: Dict[str, Any] = field(default_factory=dict)


def classify_sword_accuracy(
    water_mask: np.ndarray,
    sword_px: np.ndarray,
    corridor_buffer_px: int = 20,
    stable_threshold: float = 0.5,
    drift_threshold: float = 0.15,
    min_water_for_high_conf: int = 10000,
    min_water_for_med_conf: int = 1000,
) -> WaterMaskClassification:
    """
    Classify SWORD accuracy based on water mask corridor overlap.

    No centerline fitting required - just measures what fraction of
    detected water falls within the SWORD corridor.

    Args:
        water_mask: Binary water mask
        sword_px: SWORD centerline in pixel coords (Nx2: row, col)
        corridor_buffer_px: Buffer around SWORD line in pixels
        stable_threshold: Ratio above which SWORD is STABLE
        drift_threshold: Ratio above which SWORD has DRIFT (below = MISMATCH)
        min_water_for_high_conf: Min water px for HIGH confidence
        min_water_for_med_conf: Min water px for MEDIUM confidence

    Returns:
        WaterMaskClassification with ratio and classification
    """
    h, w = water_mask.shape

    # Create corridor mask from SWORD line
    sword_line = np.zeros((h, w), dtype=bool)
    for r, c in sword_px:
        ri, ci = int(r), int(c)
        if 0 <= ri < h and 0 <= ci < w:
            sword_line[ri, ci] = True

    corridor = binary_dilation(sword_line, disk(corridor_buffer_px))

    # Compute overlap
    raw_water_px = int(np.sum(water_mask))
    corridor_water_px = int(np.sum(water_mask & corridor))
    corridor_ratio = corridor_water_px / max(raw_water_px, 1)

    # Classify
    if corridor_ratio > stable_threshold:
        classification = "STABLE"
    elif corridor_ratio > drift_threshold:
        classification = "DRIFT"
    else:
        classification = "MISMATCH"

    # Confidence based on water count
    if raw_water_px >= min_water_for_high_conf:
        confidence = "HIGH"
    elif raw_water_px >= min_water_for_med_conf:
        confidence = "MEDIUM"
    else:
        confidence = "LOW"

    return WaterMaskClassification(
        reach_id=0,  # Caller should set
        raw_water_px=raw_water_px,
        corridor_water_px=corridor_water_px,
        corridor_ratio=corridor_ratio,
        classification=classification,
        confidence=confidence,
        corridor_buffer_m=corridor_buffer_px * 10,  # Assume 10m pixels
        stats={
            "water_outside_corridor_px": raw_water_px - corridor_water_px,
            "water_outside_ratio": 1.0 - corridor_ratio,
        }
    )


def extract_fusion_centerline(
    bbox: Tuple[float, float, float, float],
    start_date: str = "2023-01-01",
    end_date: str = "2024-12-31",
    sword_coords: Optional[np.ndarray] = None,
) -> FusionCenterlineResult:
    """
    Convenience function to extract centerline using OPERA+S2 fusion.

    Args:
        bbox: (xmin, ymin, xmax, ymax) in WGS84
        start_date: Start date
        end_date: End date
        sword_coords: Optional SWORD centerline coordinates for endpoints

    Returns:
        FusionCenterlineResult
    """
    extractor = FusionCenterlineExtractor()
    return extractor.extract(bbox, start_date, end_date, sword_coords=sword_coords)
