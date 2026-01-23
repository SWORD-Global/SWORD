# -*- coding: utf-8 -*-
"""
Imagery Cache Management
========================

Local disk cache for satellite imagery tiles and computed indices.
"""

from pathlib import Path
from typing import Optional, Tuple, Dict, Any
from datetime import datetime
import hashlib
import json
import logging
import shutil
import sqlite3

import numpy as np
import rasterio
from rasterio.crs import CRS
from affine import Affine

from .config import ImageryConfig, DEFAULT_CONFIG
from .exceptions import CacheError

logger = logging.getLogger(__name__)


class ImageryCache:
    """
    Local disk cache for imagery data.

    Stores tiles as GeoTIFFs with SQLite metadata index.
    """

    def __init__(self, config: ImageryConfig = None):
        """
        Initialize imagery cache.

        Args:
            config: Imagery configuration.
        """
        self.config = config or DEFAULT_CONFIG
        self.cache_dir = self.config.cache_dir
        self.tiles_dir = self.cache_dir / "tiles"
        self.index_path = self.cache_dir / "index.db"

        if self.config.cache_enabled:
            self._init_cache()

    def _init_cache(self):
        """Initialize cache directory and index database."""
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.tiles_dir.mkdir(exist_ok=True)
        self._init_index()

    def _init_index(self):
        """Initialize SQLite index database."""
        with sqlite3.connect(self.index_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS cache_entries (
                    cache_key TEXT PRIMARY KEY,
                    item_id TEXT NOT NULL,
                    bbox_wkt TEXT,
                    band TEXT,
                    file_path TEXT NOT NULL,
                    size_bytes INTEGER,
                    created_at TEXT NOT NULL,
                    accessed_at TEXT NOT NULL,
                    metadata TEXT
                )
            """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_item_id ON cache_entries(item_id)
            """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_accessed_at ON cache_entries(accessed_at)
            """
            )

    def _cache_key(
        self,
        item_id: str,
        bbox: Tuple[float, float, float, float],
        band: str,
    ) -> str:
        """Generate unique cache key."""
        bbox_str = f"{bbox[0]:.6f}_{bbox[1]:.6f}_{bbox[2]:.6f}_{bbox[3]:.6f}"
        key_str = f"{item_id}_{bbox_str}_{band}"
        return hashlib.md5(key_str.encode()).hexdigest()

    def _bbox_to_wkt(self, bbox: Tuple[float, float, float, float]) -> str:
        """Convert bbox to WKT polygon."""
        xmin, ymin, xmax, ymax = bbox
        return f"POLYGON(({xmin} {ymin}, {xmax} {ymin}, {xmax} {ymax}, {xmin} {ymax}, {xmin} {ymin}))"

    def get(
        self,
        item_id: str,
        bbox: Tuple[float, float, float, float],
        band: str,
    ) -> Optional[Tuple[np.ndarray, Affine, CRS]]:
        """
        Get cached tile if available.

        Args:
            item_id: STAC item ID
            bbox: Bounding box
            band: Band name

        Returns:
            Tuple of (data, transform, crs) or None if not cached
        """
        if not self.config.cache_enabled:
            return None

        cache_key = self._cache_key(item_id, bbox, band)

        with sqlite3.connect(self.index_path) as conn:
            cursor = conn.execute(
                "SELECT file_path FROM cache_entries WHERE cache_key = ?",
                (cache_key,),
            )
            row = cursor.fetchone()

            if row is None:
                return None

            file_path = Path(row[0])

            if not file_path.exists():
                # Stale entry, remove from index
                conn.execute(
                    "DELETE FROM cache_entries WHERE cache_key = ?",
                    (cache_key,),
                )
                return None

            # Update access time
            conn.execute(
                "UPDATE cache_entries SET accessed_at = ? WHERE cache_key = ?",
                (datetime.now().isoformat(), cache_key),
            )

        # Read cached file
        try:
            with rasterio.open(file_path) as src:
                data = src.read(1)
                transform = src.transform
                crs = src.crs

            logger.debug(f"Cache hit: {item_id}/{band}")
            return data, transform, crs

        except Exception as e:
            logger.warning(f"Failed to read cached file {file_path}: {e}")
            return None

    def put(
        self,
        item_id: str,
        bbox: Tuple[float, float, float, float],
        band: str,
        data: np.ndarray,
        transform: Affine,
        crs: CRS = None,
        metadata: Dict[str, Any] = None,
    ) -> Path:
        """
        Store tile in cache.

        Args:
            item_id: STAC item ID
            bbox: Bounding box
            band: Band name
            data: Array data to cache
            transform: Affine transform
            crs: Coordinate reference system
            metadata: Optional metadata dict

        Returns:
            Path to cached file
        """
        if not self.config.cache_enabled:
            raise CacheError("Cache is disabled")

        cache_key = self._cache_key(item_id, bbox, band)
        crs = crs or CRS.from_string("EPSG:4326")

        # Create item directory
        item_dir = self.tiles_dir / item_id.replace("/", "_")
        item_dir.mkdir(exist_ok=True)

        # Write GeoTIFF
        file_path = item_dir / f"{band}_{cache_key[:8]}.tif"

        with rasterio.open(
            file_path,
            "w",
            driver="GTiff",
            height=data.shape[0],
            width=data.shape[1],
            count=1,
            dtype=data.dtype,
            crs=crs,
            transform=transform,
            compress="lzw",
        ) as dst:
            dst.write(data, 1)

        # Update index
        size_bytes = file_path.stat().st_size
        now = datetime.now().isoformat()

        with sqlite3.connect(self.index_path) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO cache_entries
                (cache_key, item_id, bbox_wkt, band, file_path, size_bytes,
                 created_at, accessed_at, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    cache_key,
                    item_id,
                    self._bbox_to_wkt(bbox),
                    band,
                    str(file_path),
                    size_bytes,
                    now,
                    now,
                    json.dumps(metadata) if metadata else None,
                ),
            )

        logger.debug(f"Cached: {item_id}/{band} ({size_bytes / 1024:.1f} KB)")

        # Check if eviction needed
        self._maybe_evict()

        return file_path

    def _maybe_evict(self):
        """Evict old entries if cache exceeds size limit."""
        max_bytes = self.config.max_cache_gb * 1024 * 1024 * 1024

        with sqlite3.connect(self.index_path) as conn:
            cursor = conn.execute("SELECT SUM(size_bytes) FROM cache_entries")
            total_size = cursor.fetchone()[0] or 0

            if total_size <= max_bytes:
                return

            # Evict LRU entries until under limit
            target_size = max_bytes * 0.8  # Evict to 80% of limit

            cursor = conn.execute(
                """
                SELECT cache_key, file_path, size_bytes
                FROM cache_entries
                ORDER BY accessed_at ASC
            """
            )

            evicted_count = 0
            for cache_key, file_path, size_bytes in cursor:
                if total_size <= target_size:
                    break

                # Delete file
                try:
                    Path(file_path).unlink(missing_ok=True)
                except Exception as e:
                    logger.warning(f"Failed to delete {file_path}: {e}")

                # Remove from index
                conn.execute(
                    "DELETE FROM cache_entries WHERE cache_key = ?",
                    (cache_key,),
                )

                total_size -= size_bytes
                evicted_count += 1

            if evicted_count > 0:
                logger.info(f"Evicted {evicted_count} cache entries (LRU)")

    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        with sqlite3.connect(self.index_path) as conn:
            cursor = conn.execute(
                """
                SELECT
                    COUNT(*) as entry_count,
                    SUM(size_bytes) as total_bytes,
                    COUNT(DISTINCT item_id) as unique_items
                FROM cache_entries
            """
            )
            row = cursor.fetchone()

        return {
            "entry_count": row[0] or 0,
            "total_bytes": row[1] or 0,
            "total_mb": (row[1] or 0) / (1024 * 1024),
            "unique_items": row[2] or 0,
            "max_gb": self.config.max_cache_gb,
            "cache_dir": str(self.cache_dir),
        }

    def clear(self):
        """Clear entire cache."""
        if not self.config.cache_enabled:
            return

        # Delete all tiles
        if self.tiles_dir.exists():
            shutil.rmtree(self.tiles_dir)
            self.tiles_dir.mkdir()

        # Clear index
        with sqlite3.connect(self.index_path) as conn:
            conn.execute("DELETE FROM cache_entries")

        logger.info("Cache cleared")

    def clear_item(self, item_id: str):
        """Clear all cached data for a specific item."""
        with sqlite3.connect(self.index_path) as conn:
            cursor = conn.execute(
                "SELECT cache_key, file_path FROM cache_entries WHERE item_id = ?",
                (item_id,),
            )

            for cache_key, file_path in cursor:
                try:
                    Path(file_path).unlink(missing_ok=True)
                except Exception:
                    pass

            conn.execute(
                "DELETE FROM cache_entries WHERE item_id = ?",
                (item_id,),
            )

        # Remove item directory if empty
        item_dir = self.tiles_dir / item_id.replace("/", "_")
        if item_dir.exists() and not any(item_dir.iterdir()):
            item_dir.rmdir()

        logger.debug(f"Cleared cache for item {item_id}")
