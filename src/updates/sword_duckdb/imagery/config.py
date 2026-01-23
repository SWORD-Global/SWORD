# -*- coding: utf-8 -*-
"""
Imagery Pipeline Configuration
==============================

Configuration settings for the SWORD satellite imagery inspection pipeline.
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional
import os


@dataclass
class ImageryConfig:
    """Configuration for imagery pipeline."""

    # STAC settings
    stac_url: str = "https://earth-search.aws.element84.com/v1"
    collection: str = "sentinel-2-l2a"
    max_cloud_cover: float = 20.0

    # Temporal settings
    default_date_range_days: int = 365
    prefer_summer: bool = True  # Prefer ice-free season

    # COG streaming settings
    resolution_m: float = 10.0  # Sentinel-2 native resolution
    buffer_m: float = 500.0  # Buffer around reach bbox

    # Cache settings
    cache_dir: Path = field(default_factory=lambda: Path.home() / ".sword_cache" / "imagery")
    max_cache_gb: float = 10.0
    cache_enabled: bool = True

    # NDWI settings
    ndwi_threshold: float = 0.0  # Default water threshold
    ndwi_nodata: float = -9999.0
    use_otsu_threshold: bool = False  # Auto-threshold option

    # Processing settings
    retry_attempts: int = 3
    retry_delay_seconds: float = 1.0

    def __post_init__(self):
        """Ensure cache directory exists."""
        if self.cache_enabled:
            self.cache_dir.mkdir(parents=True, exist_ok=True)


# GDAL environment settings for efficient COG streaming
GDAL_COG_ENV = {
    "GDAL_DISABLE_READDIR_ON_OPEN": "EMPTY_DIR",
    "AWS_NO_SIGN_REQUEST": "YES",  # Element 84 is public
    "GDAL_HTTP_MERGE_CONSECUTIVE_RANGES": "YES",
    "GDAL_HTTP_MULTIPLEX": "YES",
    "GDAL_HTTP_VERSION": "2",
    "GDAL_HTTP_MAX_RETRY": "3",
    "GDAL_HTTP_RETRY_DELAY": "1",
}


def apply_gdal_env():
    """Apply GDAL environment settings for COG streaming."""
    for key, value in GDAL_COG_ENV.items():
        os.environ[key] = value


# Sentinel-2 band mappings
S2_BANDS = {
    "blue": "B02",
    "green": "B03",
    "red": "B04",
    "nir": "B08",
    "swir1": "B11",
    "swir2": "B12",
    "scl": "SCL",  # Scene classification
}

# Default configuration instance
DEFAULT_CONFIG = ImageryConfig()
