# -*- coding: utf-8 -*-
"""
Water Mask Post-Processing
==========================

Turn OPERA water frequency into clean, connected river masks.

Two-stage approach:
1. Core channel from high-frequency pixels
2. Connectivity-limited growth to fill gaps

Key insight: Use SWORD/OSM corridor as constraint, not as the water polygon.
Derive the actual mask from OPERA observations.
"""

from dataclasses import dataclass, field
from typing import Tuple, Optional, Dict, Any

import numpy as np
from scipy import ndimage
from scipy.ndimage import label, binary_dilation, binary_erosion, binary_closing, binary_opening
from affine import Affine
from rasterio.crs import CRS


def _skeletonize(mask: np.ndarray) -> np.ndarray:
    """Simple skeletonization using morphological thinning."""
    from scipy.ndimage import binary_hit_or_miss

    skeleton = mask.copy()
    # Thinning structuring elements
    patterns = [
        (np.array([[0, 0, 0], [0, 1, 0], [1, 1, 1]]), np.array([[1, 1, 1], [0, 0, 0], [0, 0, 0]])),
        (np.array([[0, 0, 0], [1, 1, 0], [0, 1, 0]]), np.array([[0, 1, 1], [0, 0, 1], [0, 0, 0]])),
    ]
    # Add rotations
    all_patterns = []
    for p1, p2 in patterns:
        for _ in range(4):
            all_patterns.append((p1.copy(), p2.copy()))
            p1, p2 = np.rot90(p1), np.rot90(p2)

    changed = True
    while changed:
        changed = False
        for p1, p2 in all_patterns:
            hits = binary_hit_or_miss(skeleton, p1, p2)
            if np.any(hits):
                skeleton = skeleton & ~hits
                changed = True
    return skeleton


def _remove_small_objects(mask: np.ndarray, min_size: int) -> np.ndarray:
    """Remove connected components smaller than min_size."""
    labeled, n = label(mask)
    if n == 0:
        return mask
    sizes = ndimage.sum(mask, labeled, range(1, n + 1))
    keep = np.zeros(n + 1, dtype=bool)
    keep[1:] = sizes >= min_size
    return keep[labeled]


def _compute_azimuth(geometry) -> float:
    """Compute mean azimuth (degrees) from a LineString geometry."""
    if hasattr(geometry, 'coords'):
        coords = list(geometry.coords)
    else:
        # GeoDataFrame - get first geometry
        coords = list(geometry.geometry.iloc[0].coords)

    if len(coords) < 2:
        return 0.0

    # Compute segment azimuths and weight by length
    azimuths = []
    weights = []
    for i in range(len(coords) - 1):
        dx = coords[i+1][0] - coords[i][0]
        dy = coords[i+1][1] - coords[i][1]
        length = np.sqrt(dx**2 + dy**2)
        if length > 0:
            az = np.degrees(np.arctan2(dx, dy)) % 180  # 0-180 range
            azimuths.append(az)
            weights.append(length)

    if not azimuths:
        return 0.0

    # Weighted mean (handle circular mean for angles)
    weights = np.array(weights)
    azimuths = np.array(azimuths)

    # Convert to radians for circular mean
    az_rad = np.radians(azimuths * 2)  # Double to handle 0/180 wrap
    mean_sin = np.average(np.sin(az_rad), weights=weights)
    mean_cos = np.average(np.cos(az_rad), weights=weights)
    mean_az = np.degrees(np.arctan2(mean_sin, mean_cos)) / 2

    return mean_az % 180


def _anisotropic_structuring_element(azimuth_deg: float, length: int = 5, width: int = 1) -> np.ndarray:
    """
    Create anisotropic structuring element aligned with azimuth.

    Args:
        azimuth_deg: Direction in degrees (0=N, 90=E)
        length: Length of element in pixels
        width: Width of element in pixels

    Returns:
        Binary structuring element
    """
    # Create line in the azimuth direction
    size = length * 2 + 1
    element = np.zeros((size, size), dtype=bool)
    center = length

    az_rad = np.radians(azimuth_deg)

    for i in range(-length, length + 1):
        # Position along azimuth direction
        dy = i * np.cos(az_rad)
        dx = i * np.sin(az_rad)

        # Add width perpendicular to azimuth
        for w in range(-width // 2, width // 2 + 1):
            wy = w * np.sin(az_rad)
            wx = -w * np.cos(az_rad)

            y = int(round(center + dy + wy))
            x = int(round(center + dx + wx))

            if 0 <= y < size and 0 <= x < size:
                element[y, x] = True

    return element


def _remove_small_holes(mask: np.ndarray, area_threshold: int) -> np.ndarray:
    """Fill holes smaller than area_threshold."""
    # Invert, remove small objects, invert back
    inverted = ~mask
    # Label holes (connected background regions not touching border)
    labeled, n = label(inverted)
    if n == 0:
        return mask
    # Find border-touching labels
    border_labels = set()
    border_labels.update(labeled[0, :])
    border_labels.update(labeled[-1, :])
    border_labels.update(labeled[:, 0])
    border_labels.update(labeled[:, -1])
    # Fill holes that are small and not border-touching
    result = mask.copy()
    for i in range(1, n + 1):
        if i not in border_labels:
            component = labeled == i
            if np.sum(component) < area_threshold:
                result[component] = True
    return result

import logging

logger = logging.getLogger(__name__)


@dataclass
class WaterMaskResult:
    """Result from water mask extraction."""

    mask: np.ndarray  # Clean binary water mask
    core: np.ndarray  # High-confidence core pixels
    skeleton: np.ndarray  # Centerline skeleton
    width_map: np.ndarray  # Width estimate per pixel (meters)
    transform: Affine
    crs: CRS
    stats: Dict[str, Any] = field(default_factory=dict)


def adaptive_thresholds(
    n_valid: float,
    t_high_base: float = 0.45,
    t_low_base: float = 0.12,
) -> Tuple[float, float]:
    """
    Compute adaptive thresholds based on observation count.

    Lower thresholds when fewer observations (core would disappear).
    Higher thresholds when many observations (reduce overgrowth).

    Args:
        n_valid: Mean valid observations per pixel
        t_high_base: Base high threshold
        t_low_base: Base low threshold

    Returns:
        (t_high, t_low) thresholds
    """
    # Adaptive formula: lower thresholds for fewer obs
    # T_high = clamp(0.20 + 5/N_valid, 0.20, 0.45)
    # Cap at 0.45 to handle intermittent/narrow channels
    t_high = np.clip(0.20 + 5.0 / max(n_valid, 1), 0.20, 0.45)
    t_low = 0.35 * t_high  # Slightly lower ratio for wider candidate band

    logger.info(f"Adaptive thresholds for N_valid={n_valid:.1f}: T_high={t_high:.2f}, T_low={t_low:.2f}")
    return t_high, t_low


def connected_to_core(candidates: np.ndarray, core: np.ndarray) -> np.ndarray:
    """
    Keep only candidate pixels that connect to core.

    Args:
        candidates: Binary mask of candidate water pixels
        core: Binary mask of high-confidence core pixels

    Returns:
        Binary mask of candidates connected to core
    """
    # Label connected components in candidates
    labeled, n_components = label(candidates)

    if n_components == 0:
        return np.zeros_like(candidates, dtype=bool)

    # Find which components overlap with core
    core_labels = set(np.unique(labeled[core])) - {0}

    # Keep only those components
    result = np.isin(labeled, list(core_labels))

    logger.debug(f"Connected to core: {len(core_labels)}/{n_components} components retained")
    return result


def morphological_reconstruction_fill(
    seed: np.ndarray,
    frequency: np.ndarray,
    threshold: float = 0.1,
) -> np.ndarray:
    """
    Fill gaps using morphological reconstruction guided by water frequency.

    Uses the water frequency as a grayscale "basin" - reconstruction fills
    from seeds through connected high-frequency regions.

    Args:
        seed: Binary mask of high-confidence water (starting points)
        frequency: Water frequency [0,1] as filling guide
        threshold: Minimum frequency to include in result

    Returns:
        Binary mask with gaps filled via reconstruction
    """
    from skimage.morphology import reconstruction

    # Create marker (seed) and mask (guide) for reconstruction
    # Marker: seed pixels at their frequency values
    marker = np.where(seed, frequency, 0).astype(np.float64)

    # Mask: frequency field (reconstruction can't exceed this)
    mask = frequency.astype(np.float64)

    # Morphological reconstruction by dilation
    # This "fills" from seed through connected high-frequency regions
    reconstructed = reconstruction(marker, mask, method='dilation')

    # Threshold the result
    result = reconstructed >= threshold

    logger.info(f"Reconstruction: {np.sum(seed)} seed px -> {np.sum(result)} filled px")
    return result


def extract_river_network(
    frequency: np.ndarray,
    corridor_mask: np.ndarray,
    threshold: float = 0.15,
    closing_radius: int = 5,
    min_size: int = 50,
) -> np.ndarray:
    """
    Extract connected river network using grayscale closing to bridge gaps.

    Pipeline:
    1. Apply corridor constraint
    2. Grayscale morphological closing on frequency (bridges gaps)
    3. Threshold to binary
    4. Clean up small objects/holes
    5. Keep largest connected component

    Args:
        frequency: Water frequency [0,1]
        corridor_mask: Binary corridor constraint
        threshold: Water frequency threshold
        closing_radius: Radius for grayscale closing (pixels)
        min_size: Minimum component size to keep

    Returns:
        Binary river mask
    """
    from skimage.morphology import closing, disk, remove_small_objects, remove_small_holes

    # Apply corridor constraint
    freq = frequency * corridor_mask

    # Step 1: Grayscale closing to bridge gaps
    # This fills small gaps in the frequency field before thresholding
    if closing_radius > 0:
        freq_closed = closing(freq, disk(closing_radius))
        logger.info(f"Grayscale closing with radius={closing_radius}")
    else:
        freq_closed = freq

    # Step 2: Threshold
    mask = freq_closed >= threshold
    initial_count = np.sum(mask)
    labeled, n_components = label(mask)
    logger.info(f"After threshold (>={threshold}): {initial_count} px, {n_components} components")

    if initial_count == 0:
        logger.warning("No water pixels found")
        return np.zeros_like(frequency, dtype=bool)

    # Step 3: Clean up
    cleaned = remove_small_objects(mask, min_size=min_size)
    cleaned = remove_small_holes(cleaned, area_threshold=min_size)

    # Step 4: Keep largest connected component
    labeled, n = label(cleaned)
    if n > 1:
        sizes = ndimage.sum(cleaned, labeled, range(1, n + 1))
        largest = np.argmax(sizes) + 1
        cleaned = labeled == largest
        logger.info(f"Kept largest of {n} components")

    final_count = np.sum(cleaned)
    logger.info(f"Final river network: {final_count} px")

    return cleaned


def keep_main_component(
    mask: np.ndarray,
    centerline_mask: Optional[np.ndarray] = None,
) -> np.ndarray:
    """
    Keep the component most overlapping with centerline, or largest.

    Args:
        mask: Binary water mask
        centerline_mask: Optional thin buffer around centerline

    Returns:
        Binary mask with only main component
    """
    labeled, n_components = label(mask)

    if n_components == 0:
        return mask

    if n_components == 1:
        return mask

    if centerline_mask is not None:
        # Score by overlap with centerline
        best_label = 0
        best_overlap = 0
        for i in range(1, n_components + 1):
            overlap = np.sum((labeled == i) & centerline_mask)
            if overlap > best_overlap:
                best_overlap = overlap
                best_label = i
    else:
        # Keep largest component
        component_sizes = ndimage.sum(mask, labeled, range(1, n_components + 1))
        best_label = np.argmax(component_sizes) + 1

    result = labeled == best_label
    logger.debug(f"Kept component {best_label}/{n_components}")
    return result


def extract_water_mask(
    water_frequency: np.ndarray,
    corridor_mask: np.ndarray,
    valid_count: np.ndarray,
    transform: Affine,
    crs: CRS,
    centerline_buffer: Optional[np.ndarray] = None,
    centerline_geometry=None,  # For computing flow azimuth
    t_high: Optional[float] = None,
    t_low: Optional[float] = None,
    pixel_size_m: float = 30.0,
    min_hole_size: int = 50,
    min_object_size: int = 20,
    closing_length: int = 5,  # Anisotropic closing length along flow
    closing_width: int = 1,   # Anisotropic closing width perpendicular to flow
) -> WaterMaskResult:
    """
    Extract clean water mask from OPERA frequency using hysteresis + connectivity.

    Pipeline:
    1. Apply corridor constraint
    2. Dual-threshold (core + candidates)
    3. Connectivity-limited growth
    4. Morphological cleanup
    5. Keep main component
    6. Skeletonize + width estimation

    Args:
        water_frequency: OPERA water frequency [0,1]
        corridor_mask: Binary corridor constraint (from SWORD/OSM buffer)
        valid_count: Number of valid observations per pixel
        transform: Affine transform
        crs: CRS
        centerline_buffer: Optional thin buffer around centerline for component selection
        centerline_geometry: Optional LineString or GeoDataFrame for computing flow azimuth
        t_high: High threshold (core), or None for adaptive
        t_low: Low threshold (candidates), or None for adaptive
        pixel_size_m: Pixel size in meters
        min_hole_size: Minimum hole size to fill (pixels)
        min_object_size: Minimum object size to keep (pixels)
        closing_length: Anisotropic closing length along flow (pixels)
        closing_width: Anisotropic closing width perpendicular to flow (pixels)

    Returns:
        WaterMaskResult with clean mask and statistics
    """
    # Step 0: Compute adaptive thresholds if not provided
    mean_valid = float(np.mean(valid_count[corridor_mask])) if np.any(corridor_mask) else 1.0

    if t_high is None or t_low is None:
        t_high_auto, t_low_auto = adaptive_thresholds(mean_valid)
        t_high = t_high if t_high is not None else t_high_auto
        t_low = t_low if t_low is not None else t_low_auto

    # Step 1: Apply corridor constraint
    freq_corridor = water_frequency * corridor_mask

    # Step 2: Threshold to get candidates
    # Use both high and low - high for reference, low for extent
    core = (freq_corridor >= t_high)
    candidates = (freq_corridor >= t_low)

    core_count = np.sum(core)
    cand_count = np.sum(candidates)
    logger.info(f"Thresholding: core={core_count} px (>={t_high:.2f}), candidates={cand_count} px (>={t_low:.2f})")

    if cand_count == 0:
        logger.warning("No candidate pixels found - returning empty mask")
        return WaterMaskResult(
            mask=np.zeros_like(water_frequency, dtype=bool),
            core=core,
            skeleton=np.zeros_like(water_frequency, dtype=bool),
            width_map=np.zeros_like(water_frequency, dtype=np.float32),
            transform=transform,
            crs=crs,
            stats={"error": "no_candidate_pixels", "t_high": t_high, "t_low": t_low},
        )

    # Step 3: Bridge disconnected components using water frequency as cost
    # Cost = 1 - frequency (lower frequency = higher cost to bridge)
    cost_field = 1.0 - water_frequency
    cost_field[~corridor_mask] = 10.0  # High cost outside corridor

    mask = bridge_components(
        candidates=candidates,
        cost_field=cost_field,
        max_bridge_cost=5.0,
        max_bridge_length=20,
    )
    bridged_count = np.sum(mask)
    logger.info(f"After bridging: {bridged_count} px ({bridged_count/max(1,cand_count)*100:.1f}% of candidates)")

    # Step 4: Morphological cleanup
    # Anisotropic closing to bridge gaps along flow direction
    if closing_length > 0:
        if centerline_geometry is not None:
            azimuth = _compute_azimuth(centerline_geometry)
            logger.info(f"Flow azimuth: {azimuth:.1f}°, using anisotropic closing")
            struct = _anisotropic_structuring_element(azimuth, length=closing_length, width=closing_width)
        else:
            # Fallback to isotropic if no geometry
            logger.info("No centerline geometry, using isotropic closing")
            struct = ndimage.generate_binary_structure(2, 1)
        mask = binary_closing(mask, structure=struct)

    # Remove small objects
    if min_object_size > 0:
        mask = _remove_small_objects(mask, min_size=min_object_size)

    # Fill small holes
    if min_hole_size > 0:
        mask = _remove_small_holes(mask, area_threshold=min_hole_size)

    # Step 5: Keep main component
    mask = keep_main_component(mask, centerline_buffer)

    final_count = np.sum(mask)
    logger.info(f"Final mask: {final_count} px")

    # Step 6: Skeletonize + width estimation
    skeleton = _skeletonize(mask)

    # Distance transform for width
    distance = ndimage.distance_transform_edt(mask)
    width_map = np.zeros_like(distance)
    width_map[skeleton] = 2 * distance[skeleton] * pixel_size_m

    # Compute statistics
    skeleton_pixels = np.sum(skeleton)
    if skeleton_pixels > 0:
        mean_width = float(np.mean(width_map[skeleton]))
        max_width = float(np.max(width_map[skeleton]))
    else:
        mean_width = 0.0
        max_width = 0.0

    # Area in sq km
    area_km2 = final_count * (pixel_size_m ** 2) / 1e6

    stats = {
        "t_high": float(t_high),
        "t_low": float(t_low),
        "mean_valid_obs": float(mean_valid),
        "core_pixels": int(core_count),
        "candidate_pixels": int(cand_count),
        "bridged_pixels": int(bridged_count),
        "final_pixels": int(final_count),
        "skeleton_pixels": int(skeleton_pixels),
        "mean_width_m": mean_width,
        "max_width_m": max_width,
        "area_km2": float(area_km2),
        "pixel_size_m": pixel_size_m,
    }

    logger.info(
        f"Water mask extracted: {final_count} px, "
        f"mean_width={mean_width:.1f}m, area={area_km2:.3f} km²"
    )

    return WaterMaskResult(
        mask=mask,
        core=core,
        skeleton=skeleton,
        width_map=width_map,
        transform=transform,
        crs=crs,
        stats=stats,
    )
