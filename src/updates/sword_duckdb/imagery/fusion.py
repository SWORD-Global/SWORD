# -*- coding: utf-8 -*-
"""
River Prior + OPERA Fusion
==========================

Fuse river priors (OSM/NHD) with OPERA water observations to detect
channel changes.

Four-state classification per pixel:
- UPLAND (0): Neither prior nor OPERA shows water
- STABLE (1): Both prior and OPERA agree on water
- NEW (2): OPERA sees water, prior doesn't → migration/avulsion
- ABANDONED (3): Prior expects water, OPERA doesn't → dried/outdated

High NEW% suggests channel migration or flooding.
High ABANDONED% suggests outdated prior database or dried channel.
"""

from dataclasses import dataclass, field
from typing import Tuple, Optional, Dict, Any
import logging

import numpy as np
from affine import Affine
from rasterio.crs import CRS

logger = logging.getLogger(__name__)

# Classification values
UPLAND = 0
STABLE = 1
NEW = 2
ABANDONED = 3

CLASS_NAMES = {
    UPLAND: "upland",
    STABLE: "stable",
    NEW: "new",
    ABANDONED: "abandoned",
}


@dataclass
class FusionResult:
    """Result from prior + observation fusion."""

    classification: np.ndarray  # 2D array with values 0-3
    transform: Affine
    crs: CRS
    bbox: Tuple[float, float, float, float]
    stats: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ReachFusionStats:
    """Aggregated fusion statistics for a single reach."""

    reach_id: int
    pct_stable: float
    pct_new: float
    pct_abandoned: float
    pct_upland: float
    pixel_count: int
    corridor_pixel_count: int
    water_fraction_opera: float
    water_fraction_prior: float
    confidence: float  # Based on valid observation count
    flagged: bool  # True if exceeds threshold
    flag_reason: Optional[str] = None


class FusionEngine:
    """
    Engine for fusing river priors with OPERA observations.

    Usage:
        engine = FusionEngine(flag_threshold=0.2)

        # Classify agreement between prior corridor and OPERA water
        result = engine.classify(prior_mask, opera_mask, transform, crs, bbox)

        # Get reach-level statistics
        stats = engine.aggregate_reach_stats(result, reach_id=74226000371)
    """

    def __init__(
        self,
        flag_threshold: float = 0.2,
        min_corridor_pixels: int = 100,
    ):
        """
        Initialize fusion engine.

        Args:
            flag_threshold: Fraction of NEW or ABANDONED to trigger flag (default 20%)
            min_corridor_pixels: Minimum corridor pixels for valid analysis
        """
        self.flag_threshold = flag_threshold
        self.min_corridor_pixels = min_corridor_pixels

    def classify(
        self,
        prior_mask: np.ndarray,
        opera_mask: np.ndarray,
        transform: Affine,
        crs: CRS,
        bbox: Tuple[float, float, float, float],
        valid_mask: Optional[np.ndarray] = None,
    ) -> FusionResult:
        """
        Classify agreement between prior and observation.

        Args:
            prior_mask: Binary mask from corridor (True = expected water)
            opera_mask: Binary mask from OPERA (True = observed water)
            transform: Affine transform
            crs: Coordinate reference system
            bbox: Bounding box
            valid_mask: Optional mask of valid OPERA observations

        Returns:
            FusionResult with 4-class classification
        """
        # Ensure boolean masks
        prior = prior_mask.astype(bool)
        opera = opera_mask.astype(bool)

        # Initialize classification
        classification = np.zeros(prior.shape, dtype=np.uint8)

        # Apply 4-state logic
        # UPLAND: neither (already 0)
        # STABLE: both
        classification[prior & opera] = STABLE
        # NEW: opera only
        classification[~prior & opera] = NEW
        # ABANDONED: prior only
        classification[prior & ~opera] = ABANDONED

        # Apply valid mask if provided
        if valid_mask is not None:
            # Mark invalid pixels as UPLAND (conservative)
            classification[~valid_mask] = UPLAND

        # Compute stats
        total = classification.size
        corridor_pixels = np.sum(prior)

        stats = {
            "total_pixels": int(total),
            "corridor_pixels": int(corridor_pixels),
            "upland_count": int(np.sum(classification == UPLAND)),
            "stable_count": int(np.sum(classification == STABLE)),
            "new_count": int(np.sum(classification == NEW)),
            "abandoned_count": int(np.sum(classification == ABANDONED)),
        }

        # Fractions (relative to corridor)
        if corridor_pixels > 0:
            stats["pct_stable"] = float(stats["stable_count"] / corridor_pixels * 100)
            stats["pct_new"] = float(stats["new_count"] / corridor_pixels * 100)
            stats["pct_abandoned"] = float(stats["abandoned_count"] / corridor_pixels * 100)
        else:
            stats["pct_stable"] = 0.0
            stats["pct_new"] = 0.0
            stats["pct_abandoned"] = 0.0

        logger.info(
            f"Fusion: {stats['pct_stable']:.1f}% stable, "
            f"{stats['pct_new']:.1f}% new, {stats['pct_abandoned']:.1f}% abandoned"
        )

        return FusionResult(
            classification=classification,
            transform=transform,
            crs=crs,
            bbox=bbox,
            stats=stats,
        )

    def aggregate_reach_stats(
        self,
        result: FusionResult,
        reach_id: int,
        corridor_mask: Optional[np.ndarray] = None,
        valid_count: Optional[np.ndarray] = None,
    ) -> ReachFusionStats:
        """
        Aggregate fusion statistics for a reach.

        Args:
            result: FusionResult from classify()
            reach_id: SWORD reach ID
            corridor_mask: Optional corridor mask for more precise stats
            valid_count: Optional array of valid observation counts per pixel

        Returns:
            ReachFusionStats with aggregated statistics
        """
        cls = result.classification

        # Use provided corridor mask or infer from non-upland
        if corridor_mask is not None:
            mask = corridor_mask.astype(bool)
        else:
            # Corridor = anywhere that was either prior or observed
            mask = cls > UPLAND

        corridor_pixels = int(np.sum(mask))

        if corridor_pixels < self.min_corridor_pixels:
            logger.warning(f"Reach {reach_id}: only {corridor_pixels} corridor pixels, below minimum")

        # Count classes within corridor
        stable = np.sum(cls[mask] == STABLE)
        new = np.sum(cls[mask] == NEW)
        abandoned = np.sum(cls[mask] == ABANDONED)
        upland = np.sum(cls[mask] == UPLAND)

        total_corridor = stable + new + abandoned + upland
        if total_corridor == 0:
            total_corridor = 1  # Avoid division by zero

        pct_stable = float(stable / total_corridor)
        pct_new = float(new / total_corridor)
        pct_abandoned = float(abandoned / total_corridor)
        pct_upland = float(upland / total_corridor)

        # Water fractions
        water_opera = float((stable + new) / total_corridor)
        water_prior = float((stable + abandoned) / total_corridor)

        # Confidence based on observation count
        if valid_count is not None:
            mean_obs = float(np.mean(valid_count[mask]))
            confidence = min(1.0, mean_obs / 10)  # Full confidence at 10+ observations
        else:
            confidence = 0.5  # Unknown

        # Flag check
        flagged = False
        flag_reason = None

        if pct_new > self.flag_threshold:
            flagged = True
            flag_reason = f"HIGH_NEW ({pct_new*100:.1f}%)"
        elif pct_abandoned > self.flag_threshold:
            flagged = True
            flag_reason = f"HIGH_ABANDONED ({pct_abandoned*100:.1f}%)"

        return ReachFusionStats(
            reach_id=reach_id,
            pct_stable=pct_stable,
            pct_new=pct_new,
            pct_abandoned=pct_abandoned,
            pct_upland=pct_upland,
            pixel_count=cls.size,
            corridor_pixel_count=corridor_pixels,
            water_fraction_opera=water_opera,
            water_fraction_prior=water_prior,
            confidence=confidence,
            flagged=flagged,
            flag_reason=flag_reason,
        )


def fuse_reach(
    opera_composite,
    corridor_mask,
    reach_id: int,
    transform: Affine,
    crs: CRS,
    bbox: Tuple[float, float, float, float],
    flag_threshold: float = 0.2,
) -> Tuple[FusionResult, ReachFusionStats]:
    """
    Convenience function to fuse OPERA composite with corridor mask.

    Args:
        opera_composite: DSWxComposite from OPERA client
        corridor_mask: CorridorMask from river prior client
        reach_id: SWORD reach ID
        transform: Affine transform
        crs: CRS
        bbox: Bounding box
        flag_threshold: Threshold for flagging (default 20%)

    Returns:
        Tuple of (FusionResult, ReachFusionStats)
    """
    engine = FusionEngine(flag_threshold=flag_threshold)

    # Use "any water" from OPERA composite
    opera_mask = opera_composite.any_water
    prior_mask = corridor_mask.mask

    # Align shapes if needed
    if opera_mask.shape != prior_mask.shape:
        logger.warning(
            f"Shape mismatch: OPERA {opera_mask.shape} vs prior {prior_mask.shape}. "
            f"Using intersection."
        )
        min_h = min(opera_mask.shape[0], prior_mask.shape[0])
        min_w = min(opera_mask.shape[1], prior_mask.shape[1])
        opera_mask = opera_mask[:min_h, :min_w]
        prior_mask = prior_mask[:min_h, :min_w]

    # Classify
    result = engine.classify(
        prior_mask=prior_mask,
        opera_mask=opera_mask,
        transform=transform,
        crs=crs,
        bbox=bbox,
        valid_mask=opera_composite.valid_count > 0,
    )

    # Aggregate stats
    stats = engine.aggregate_reach_stats(
        result=result,
        reach_id=reach_id,
        corridor_mask=prior_mask,
        valid_count=opera_composite.valid_count[:min_h, :min_w] if opera_mask.shape != prior_mask.shape else opera_composite.valid_count,
    )

    return result, stats
