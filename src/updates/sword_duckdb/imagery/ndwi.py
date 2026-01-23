# -*- coding: utf-8 -*-
"""
NDWI (Normalized Difference Water Index) Computation
====================================================

Compute water indices from satellite imagery bands.
"""

from typing import Tuple, Optional
from dataclasses import dataclass
import logging

import numpy as np

from .config import ImageryConfig, DEFAULT_CONFIG
from .exceptions import NDWIComputationError

logger = logging.getLogger(__name__)


@dataclass
class NDWIResult:
    """Result of NDWI computation."""

    ndwi: np.ndarray  # NDWI values (-1 to 1)
    water_mask: np.ndarray  # Binary water mask
    threshold: float  # Threshold used
    stats: dict  # Statistics


class NDWIComputer:
    """
    Compute NDWI (Normalized Difference Water Index).

    NDWI = (Green - NIR) / (Green + NIR)

    Water typically has NDWI > 0 (or > 0.3 for higher confidence).
    """

    def __init__(self, config: ImageryConfig = None):
        """
        Initialize NDWI computer.

        Args:
            config: Imagery configuration.
        """
        self.config = config or DEFAULT_CONFIG

    def compute(
        self,
        green: np.ndarray,
        nir: np.ndarray,
        nodata_value: Optional[float] = None,
    ) -> np.ndarray:
        """
        Compute NDWI from green and NIR bands.

        Args:
            green: Green band array (B03 for Sentinel-2)
            nir: NIR band array (B08 for Sentinel-2)
            nodata_value: Value to use for invalid pixels

        Returns:
            NDWI array (float32, range -1 to 1)
        """
        nodata = nodata_value if nodata_value is not None else self.config.ndwi_nodata

        # Validate inputs
        if green.shape != nir.shape:
            raise NDWIComputationError(
                f"Shape mismatch: green={green.shape}, nir={nir.shape}",
                green_shape=green.shape,
                nir_shape=nir.shape,
            )

        # Convert to float for computation
        green_f = green.astype(np.float32)
        nir_f = nir.astype(np.float32)

        # Compute NDWI with safe division
        denominator = green_f + nir_f

        with np.errstate(divide="ignore", invalid="ignore"):
            ndwi = (green_f - nir_f) / denominator

        # Handle invalid values
        invalid_mask = (denominator == 0) | np.isnan(ndwi) | np.isinf(ndwi)
        ndwi = np.where(invalid_mask, nodata, ndwi)

        # Also mark original nodata as nodata
        if np.issubdtype(green.dtype, np.integer):
            # Assume 0 is nodata for integer bands
            ndwi = np.where((green == 0) | (nir == 0), nodata, ndwi)

        logger.debug(
            f"Computed NDWI: shape={ndwi.shape}, "
            f"range=[{np.nanmin(ndwi):.3f}, {np.nanmax(ndwi):.3f}]"
        )

        return ndwi

    def classify_water(
        self,
        ndwi: np.ndarray,
        threshold: Optional[float] = None,
        use_otsu: bool = False,
    ) -> Tuple[np.ndarray, float]:
        """
        Classify water from NDWI values.

        Args:
            ndwi: NDWI array
            threshold: Classification threshold (NDWI > threshold = water)
            use_otsu: Use Otsu's method for automatic thresholding

        Returns:
            Tuple of (binary water mask, threshold used)
        """
        if use_otsu or self.config.use_otsu_threshold:
            threshold = otsu_threshold(ndwi, self.config.ndwi_nodata)
            logger.info(f"Otsu threshold: {threshold:.3f}")
        elif threshold is None:
            threshold = self.config.ndwi_threshold

        # Create water mask
        valid_mask = ndwi != self.config.ndwi_nodata
        water_mask = (ndwi > threshold) & valid_mask

        return water_mask.astype(np.uint8), threshold

    def compute_with_mask(
        self,
        green: np.ndarray,
        nir: np.ndarray,
        threshold: Optional[float] = None,
        use_otsu: bool = False,
    ) -> NDWIResult:
        """
        Compute NDWI and water mask together.

        Args:
            green: Green band array
            nir: NIR band array
            threshold: Classification threshold
            use_otsu: Use automatic thresholding

        Returns:
            NDWIResult with NDWI, mask, threshold, and statistics
        """
        ndwi = self.compute(green, nir)
        water_mask, threshold_used = self.classify_water(ndwi, threshold, use_otsu)

        # Compute statistics
        valid_mask = ndwi != self.config.ndwi_nodata
        valid_ndwi = ndwi[valid_mask]

        stats = {
            "ndwi_mean": float(np.mean(valid_ndwi)) if len(valid_ndwi) > 0 else None,
            "ndwi_std": float(np.std(valid_ndwi)) if len(valid_ndwi) > 0 else None,
            "ndwi_min": float(np.min(valid_ndwi)) if len(valid_ndwi) > 0 else None,
            "ndwi_max": float(np.max(valid_ndwi)) if len(valid_ndwi) > 0 else None,
            "water_pixel_count": int(np.sum(water_mask)),
            "total_valid_pixels": int(np.sum(valid_mask)),
            "water_fraction": float(np.sum(water_mask) / max(1, np.sum(valid_mask))),
        }

        return NDWIResult(
            ndwi=ndwi,
            water_mask=water_mask,
            threshold=threshold_used,
            stats=stats,
        )


def otsu_threshold(
    ndwi: np.ndarray,
    nodata_value: float = -9999.0,
    n_bins: int = 256,
) -> float:
    """
    Compute Otsu's threshold for NDWI water classification.

    Args:
        ndwi: NDWI array
        nodata_value: Value to exclude from computation
        n_bins: Number of histogram bins

    Returns:
        Optimal threshold value
    """
    # Get valid values only
    valid_mask = ndwi != nodata_value
    valid_values = ndwi[valid_mask]

    if len(valid_values) == 0:
        logger.warning("No valid NDWI values for Otsu thresholding, using default 0.0")
        return 0.0

    # Clip to valid NDWI range
    valid_values = np.clip(valid_values, -1, 1)

    # Compute histogram
    hist, bin_edges = np.histogram(valid_values, bins=n_bins, range=(-1, 1))
    bin_centers = (bin_edges[:-1] + bin_edges[1:]) / 2

    # Normalize histogram
    hist = hist.astype(np.float32)
    hist /= hist.sum()

    # Otsu's method
    best_threshold = 0.0
    best_variance = 0.0

    for i in range(1, n_bins):
        # Class probabilities
        w0 = hist[:i].sum()
        w1 = hist[i:].sum()

        if w0 == 0 or w1 == 0:
            continue

        # Class means
        mu0 = (hist[:i] * bin_centers[:i]).sum() / w0
        mu1 = (hist[i:] * bin_centers[i:]).sum() / w1

        # Between-class variance
        variance = w0 * w1 * (mu0 - mu1) ** 2

        if variance > best_variance:
            best_variance = variance
            best_threshold = bin_centers[i]

    return float(best_threshold)


def compute_mndwi(
    green: np.ndarray,
    swir: np.ndarray,
    nodata_value: float = -9999.0,
) -> np.ndarray:
    """
    Compute MNDWI (Modified NDWI) using SWIR band.

    MNDWI = (Green - SWIR) / (Green + SWIR)

    Better at suppressing built-up area noise than standard NDWI.

    Args:
        green: Green band array
        swir: SWIR band array (B11 or B12 for Sentinel-2)
        nodata_value: Value for invalid pixels

    Returns:
        MNDWI array
    """
    green_f = green.astype(np.float32)
    swir_f = swir.astype(np.float32)

    denominator = green_f + swir_f

    with np.errstate(divide="ignore", invalid="ignore"):
        mndwi = (green_f - swir_f) / denominator

    invalid_mask = (denominator == 0) | np.isnan(mndwi) | np.isinf(mndwi)
    mndwi = np.where(invalid_mask, nodata_value, mndwi)

    return mndwi


def compute_awei_nsh(
    green: np.ndarray,
    nir: np.ndarray,
    swir1: np.ndarray,
    swir2: np.ndarray,
    nodata_value: float = -9999.0,
) -> np.ndarray:
    """
    Compute AWEInsh (Automated Water Extraction Index - no shadow).

    AWEInsh = 4 * (Green - SWIR1) - (0.25 * NIR + 2.75 * SWIR2)

    Better at distinguishing water from shadows and dark surfaces.
    Positive values indicate water.

    Args:
        green: Green band array (B03 for Sentinel-2)
        nir: NIR band array (B08 for Sentinel-2)
        swir1: SWIR1 band array (B11 for Sentinel-2, ~1610nm)
        swir2: SWIR2 band array (B12 for Sentinel-2, ~2190nm)
        nodata_value: Value for invalid pixels

    Returns:
        AWEInsh array (positive = water)
    """
    green_f = green.astype(np.float32)
    nir_f = nir.astype(np.float32)
    swir1_f = swir1.astype(np.float32)
    swir2_f = swir2.astype(np.float32)

    # AWEInsh = 4 * (Green - SWIR1) - (0.25 * NIR + 2.75 * SWIR2)
    awei = 4.0 * (green_f - swir1_f) - (0.25 * nir_f + 2.75 * swir2_f)

    # Handle nodata
    invalid_mask = (
        (green == 0) | (nir == 0) | (swir1 == 0) | (swir2 == 0) |
        np.isnan(awei) | np.isinf(awei)
    )
    awei = np.where(invalid_mask, nodata_value, awei)

    return awei


def compute_combined_water_index(
    green: np.ndarray,
    nir: np.ndarray,
    swir1: np.ndarray,
    swir2: np.ndarray,
    mndwi_threshold: float = 0.0,
    awei_threshold: float = 0.0,
    nodata_value: float = -9999.0,
) -> Tuple[np.ndarray, dict]:
    """
    Compute combined water mask using MNDWI and AWEInsh.

    Water is detected where EITHER index exceeds its threshold.
    This provides more robust detection across different water types.

    Args:
        green: Green band (B03)
        nir: NIR band (B08)
        swir1: SWIR1 band (B11)
        swir2: SWIR2 band (B12)
        mndwi_threshold: MNDWI threshold (default 0)
        awei_threshold: AWEInsh threshold (default 0)
        nodata_value: Nodata value

    Returns:
        Tuple of (combined water mask, stats dict)
    """
    mndwi = compute_mndwi(green, swir1, nodata_value)
    awei = compute_awei_nsh(green, nir, swir1, swir2, nodata_value)

    valid = (mndwi != nodata_value) & (awei != nodata_value)

    mndwi_water = (mndwi > mndwi_threshold) & valid
    awei_water = (awei > awei_threshold) & valid

    # Combined: water if either index detects it
    combined = mndwi_water | awei_water

    stats = {
        "mndwi_water_px": int(np.sum(mndwi_water)),
        "awei_water_px": int(np.sum(awei_water)),
        "combined_water_px": int(np.sum(combined)),
        "mndwi_only_px": int(np.sum(mndwi_water & ~awei_water)),
        "awei_only_px": int(np.sum(awei_water & ~mndwi_water)),
        "both_px": int(np.sum(mndwi_water & awei_water)),
    }

    return combined, stats
