# -*- coding: utf-8 -*-
"""
Water Detection Ensemble
========================

Multi-method water detection using spectral indices + deep learning.
Per-pixel voting across 6 methods for robust water mask generation.

Methods:
1. NDWI - Normalized Difference Water Index
2. MNDWI - Modified NDWI (uses SWIR1)
3. AWEI_nsh - Automated Water Extraction Index (no shadow)
4. AWEI_sh - AWEI with shadow correction
5. ML4Floods - Deep learning flood/water detector
6. DeepWaterMap - CNN water segmentation

Speed optimizations:
- Vectorized index computation (all 4 indices in one pass)
- Model caching (load once, reuse)
- Tiled batch inference for DL models
- Optional GPU/Metal acceleration

Cloud filtering:
- Pixel-level masking using Sentinel-2 SCL band
- Multi-scene compositing to fill cloud gaps
"""

from dataclasses import dataclass, field
from typing import Dict, Optional, Tuple, Any, List
from pathlib import Path
import logging
import os
import time

import numpy as np
import requests

logger = logging.getLogger(__name__)

# Sentinel-2 Scene Classification Layer (SCL) values
SCL_NO_DATA = 0
SCL_SATURATED = 1
SCL_DARK_AREA = 2
SCL_CLOUD_SHADOW = 3
SCL_VEGETATION = 4
SCL_BARE_SOIL = 5
SCL_WATER = 6
SCL_UNCLASSIFIED = 7
SCL_CLOUD_MEDIUM = 8
SCL_CLOUD_HIGH = 9
SCL_THIN_CIRRUS = 10
SCL_SNOW_ICE = 11

# Bad pixels to mask (clouds, shadows, snow)
SCL_BAD_PIXELS = {
    SCL_NO_DATA,
    SCL_SATURATED,
    SCL_CLOUD_SHADOW,
    SCL_CLOUD_MEDIUM,
    SCL_CLOUD_HIGH,
    SCL_THIN_CIRRUS,
    SCL_SNOW_ICE,
}

# ESRI Land Cover 10m - Water class value
ESRI_WATER_CLASS = 80

# OSM Overpass API
OVERPASS_URL = "https://overpass-api.de/api/interpreter"


@dataclass
class EnsembleResult:
    """Result from water ensemble detection."""

    masks: Dict[str, np.ndarray]  # Per-method binary masks
    probabilities: Dict[str, np.ndarray]  # Per-method probability maps
    vote_counts: np.ndarray  # Per-pixel vote count
    ensemble_mask: np.ndarray  # Final voted mask
    stats: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CompositeResult:
    """Result from multi-scene cloud-free compositing."""

    bands: Dict[str, np.ndarray]  # Composited band arrays
    valid_mask: np.ndarray  # Boolean mask of valid (cloud-free) pixels
    n_scenes_used: int  # Number of scenes used
    coverage_pct: float  # Percentage of AOI with valid data
    scene_ids: List[str]  # IDs of scenes used


class CloudFilter:
    """
    Sentinel-2 cloud filtering using SCL band.

    Masks clouds, shadows, snow, and saturated pixels for cleaner
    water detection.
    """

    def __init__(
        self,
        bad_classes: set = None,
        include_dark_area: bool = False,
    ):
        """
        Initialize cloud filter.

        Args:
            bad_classes: Set of SCL values to mask. Defaults to SCL_BAD_PIXELS.
            include_dark_area: Whether to mask dark area pixels (SCL=2).
                               Sometimes useful but can mask water shadows.
        """
        self.bad_classes = bad_classes or SCL_BAD_PIXELS.copy()
        if include_dark_area:
            self.bad_classes.add(SCL_DARK_AREA)

    def get_valid_mask(self, scl: np.ndarray) -> np.ndarray:
        """
        Create boolean mask of valid (cloud-free) pixels.

        Args:
            scl: Scene Classification Layer array

        Returns:
            Boolean array where True = valid, False = cloudy/bad
        """
        valid = np.ones(scl.shape, dtype=bool)
        for bad_val in self.bad_classes:
            valid &= (scl != bad_val)
        return valid

    def get_cloud_pct(self, scl: np.ndarray) -> float:
        """Get percentage of pixels that are cloudy."""
        valid = self.get_valid_mask(scl)
        return 100.0 * (1 - np.mean(valid))

    def mask_bands(
        self,
        bands: Dict[str, np.ndarray],
        scl: np.ndarray,
        fill_value: float = np.nan,
    ) -> Dict[str, np.ndarray]:
        """
        Apply cloud mask to band arrays.

        Args:
            bands: Dict of band name -> array
            scl: Scene Classification Layer
            fill_value: Value for masked pixels

        Returns:
            Dict of masked band arrays
        """
        valid = self.get_valid_mask(scl)
        masked = {}
        for name, arr in bands.items():
            out = arr.astype(np.float32).copy()
            out[~valid] = fill_value
            masked[name] = out
        return masked


class SceneCompositor:
    """
    Multi-scene compositing for cloud-free imagery.

    Combines multiple Sentinel-2 scenes, prioritizing clearer pixels
    to produce a cloud-free composite.
    """

    def __init__(
        self,
        cloud_filter: CloudFilter = None,
        min_valid_pct: float = 50.0,
        max_scenes: int = 10,
    ):
        """
        Initialize compositor.

        Args:
            cloud_filter: CloudFilter instance
            min_valid_pct: Minimum valid pixel % per scene to use it
            max_scenes: Maximum scenes to composite
        """
        self.cloud_filter = cloud_filter or CloudFilter()
        self.min_valid_pct = min_valid_pct
        self.max_scenes = max_scenes

    def composite(
        self,
        items: List[Any],
        bbox: Tuple[float, float, float, float],
        cog_reader: Any,
        bands: List[str] = None,
    ) -> CompositeResult:
        """
        Create cloud-free composite from multiple scenes.

        Uses a "first valid pixel" approach - iterates through scenes
        sorted by cloud cover, taking the first clear pixel for each location.

        Args:
            items: List of pystac Items sorted by quality
            bbox: (min_lon, min_lat, max_lon, max_lat)
            cog_reader: COGReader instance
            bands: Band names to composite. Defaults to S2 bands needed.

        Returns:
            CompositeResult with cloud-free band arrays
        """
        from scipy.ndimage import zoom

        if bands is None:
            bands = ["blue", "green", "red", "nir", "swir16", "swir22", "scl"]

        ref_shape = None
        composite_bands = {}
        composite_valid = None
        scenes_used = []

        for i, item in enumerate(items[:self.max_scenes]):
            item_id = getattr(item, 'id', str(i))

            try:
                # Read SCL first to check cloud coverage in AOI
                scl_data, _, _ = cog_reader.read_band(item, "scl", bbox)

                if ref_shape is None:
                    ref_shape = scl_data.shape
                elif scl_data.shape != ref_shape:
                    scale = (ref_shape[0] / scl_data.shape[0],
                             ref_shape[1] / scl_data.shape[1])
                    scl_data = zoom(scl_data, scale, order=0)

                valid_mask = self.cloud_filter.get_valid_mask(scl_data)
                valid_pct = 100.0 * np.mean(valid_mask)

                logger.info(f"Scene {item_id}: {valid_pct:.1f}% cloud-free")

                if valid_pct < self.min_valid_pct:
                    logger.info(f"  Skipping (below {self.min_valid_pct}% threshold)")
                    continue

                # Initialize composite arrays on first valid scene
                if composite_valid is None:
                    composite_valid = np.zeros(ref_shape, dtype=bool)
                    for band in bands:
                        if band != "scl":
                            composite_bands[band] = np.full(
                                ref_shape, np.nan, dtype=np.float32
                            )

                # Find pixels we still need
                need_pixels = ~composite_valid & valid_mask
                if not np.any(need_pixels):
                    logger.info(f"  Composite already complete")
                    break

                # Read and fill needed bands
                for band in bands:
                    if band == "scl":
                        continue

                    try:
                        data, _, _ = cog_reader.read_band(item, band, bbox)
                        if data.shape != ref_shape:
                            scale = (ref_shape[0] / data.shape[0],
                                     ref_shape[1] / data.shape[1])
                            data = zoom(data, scale, order=1)

                        composite_bands[band][need_pixels] = data[need_pixels].astype(np.float32)
                    except Exception as e:
                        logger.warning(f"  Band {band} read failed: {e}")

                composite_valid |= need_pixels
                scenes_used.append(item_id)

                coverage = 100.0 * np.mean(composite_valid)
                logger.info(f"  Composite coverage: {coverage:.1f}%")

                if coverage >= 99.0:
                    break

            except Exception as e:
                logger.warning(f"Scene {item_id} failed: {e}")
                continue

        if composite_valid is None:
            raise ValueError("No valid scenes found for compositing")

        coverage_pct = 100.0 * np.mean(composite_valid)
        logger.info(f"Final composite: {len(scenes_used)} scenes, {coverage_pct:.1f}% coverage")

        return CompositeResult(
            bands=composite_bands,
            valid_mask=composite_valid,
            n_scenes_used=len(scenes_used),
            coverage_pct=coverage_pct,
            scene_ids=scenes_used,
        )


class WaterIndexComputer:
    """Vectorized computation of water spectral indices."""

    @staticmethod
    def compute_all(
        blue: np.ndarray,
        green: np.ndarray,
        red: np.ndarray,
        nir: np.ndarray,
        swir1: np.ndarray,
        swir2: np.ndarray,
        eps: float = 1e-8,
    ) -> Dict[str, np.ndarray]:
        """
        Compute all water indices in a single vectorized pass.

        Returns dict with 'ndwi', 'mndwi', 'awei_nsh', 'awei_sh' arrays.
        """
        return {
            'ndwi': (green - nir) / (green + nir + eps),
            'mndwi': (green - swir1) / (green + swir1 + eps),
            'awei_nsh': blue + 2.5*green - 1.5*(nir + swir1) - 0.25*swir2,
            'awei_sh': 4*(green - swir1) - (0.25*nir + 2.75*swir2),
        }

    @staticmethod
    def threshold_indices(
        indices: Dict[str, np.ndarray],
        thresholds: Optional[Dict[str, float]] = None,
    ) -> Dict[str, np.ndarray]:
        """Apply thresholds to create binary masks."""
        if thresholds is None:
            thresholds = {'ndwi': 0, 'mndwi': 0, 'awei_nsh': 0, 'awei_sh': 0}

        return {
            name: (idx > thresholds.get(name, 0)).astype(np.uint8)
            for name, idx in indices.items()
        }


class ML4FloodsInference:
    """Cached ML4Floods model for fast inference."""

    _model = None
    _config = None
    _normalization = None

    def __init__(
        self,
        model_dir: str = "ml4floods_models/models/WF2_unetv2_bgriswirs",
    ):
        self.model_dir = Path(model_dir)

    def _load_model(self):
        """Load model once and cache."""
        if ML4FloodsInference._model is not None:
            return

        import json
        import torch
        from ml4floods.models.model_setup import get_model
        from ml4floods.preprocess.worldfloods.normalize import get_normalisation

        config_path = self.model_dir / "config.json"
        with open(config_path) as f:
            config = json.load(f)

        channel_config = config["model_params"]["hyperparameters"]["channel_configuration"]
        mean, std = get_normalisation(channel_config)

        model = get_model(config["model_params"])
        checkpoint = torch.load(
            self.model_dir / "model.pt",
            map_location="cpu",
            weights_only=False
        )
        model.load_state_dict(checkpoint)
        model.eval()

        ML4FloodsInference._model = model
        ML4FloodsInference._config = config
        ML4FloodsInference._normalization = (mean.squeeze(), std.squeeze())

        logger.info("ML4Floods model loaded and cached")

    def predict(
        self,
        image: np.ndarray,
        tile_size: int = 256,
        stride: int = 192,
        threshold: float = 0.6,
    ) -> Tuple[np.ndarray, np.ndarray]:
        """
        Run tiled inference.

        Args:
            image: (6, H, W) array [blue, green, red, nir, swir1, swir2]
            tile_size: Tile size for inference
            stride: Stride between tiles
            threshold: Water probability threshold

        Returns:
            (water_probs, water_mask) arrays
        """
        import torch

        self._load_model()

        mean, std = ML4FloodsInference._normalization
        model = ML4FloodsInference._model

        # Normalize
        normalized = image.copy().astype(np.float32)
        for i in range(normalized.shape[0]):
            normalized[i] = (normalized[i] - mean[i]) / (std[i] + 1e-6)

        h, w = normalized.shape[1], normalized.shape[2]
        water_probs = np.zeros((h, w), dtype=np.float32)
        counts = np.zeros((h, w), dtype=np.float32)

        with torch.no_grad():
            for y in range(0, h, stride):
                for x in range(0, w, stride):
                    y_end = min(y + tile_size, h)
                    x_end = min(x + tile_size, w)

                    tile = normalized[:, y:y_end, x:x_end]
                    tile_h, tile_w = tile.shape[1], tile.shape[2]

                    if tile_h < tile_size or tile_w < tile_size:
                        padded = np.zeros((6, tile_size, tile_size), dtype=np.float32)
                        padded[:, :tile_h, :tile_w] = tile
                        tile = padded

                    tile_t = torch.from_numpy(tile).unsqueeze(0)
                    logits = model(tile_t)
                    probs = torch.sigmoid(logits).numpy()[0]

                    water_probs[y:y_end, x:x_end] += probs[1, :tile_h, :tile_w]
                    counts[y:y_end, x:x_end] += 1

        water_probs /= np.maximum(counts, 1)
        water_mask = (water_probs > threshold).astype(np.uint8)

        return water_probs, water_mask


class DeepWaterMapInference:
    """Cached DeepWaterMap model for fast inference."""

    _model = None
    _weights_loaded = False

    def __init__(
        self,
        checkpoint_path: str = "deepwatermap/checkpoints/cp.135.ckpt",
    ):
        self.checkpoint_path = checkpoint_path

    def _load_model(self):
        """Load model once and cache."""
        if DeepWaterMapInference._model is not None and DeepWaterMapInference._weights_loaded:
            return

        import tensorflow as tf
        import keras

        # Suppress TF warnings
        os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'

        # Import DeepWaterMap model architecture
        import sys
        dwm_path = Path(__file__).parent.parent.parent.parent.parent / "deepwatermap"
        if str(dwm_path) not in sys.path:
            sys.path.insert(0, str(dwm_path))

        import deepwatermap

        model = deepwatermap.model()

        # Load weights from TF2 checkpoint
        reader = tf.train.load_checkpoint(self.checkpoint_path)
        keras_layers = [layer for layer in model.layers if layer.weights]

        for i, layer in enumerate(keras_layers):
            prefix = f"layer_with_weights-{i}"
            for weight in layer.weights:
                weight_name = weight.name.split('/')[-1].replace(':0', '')
                if weight_name in ['kernel', 'gamma', 'beta', 'moving_mean', 'moving_variance']:
                    ckpt_key = f"{prefix}/{weight_name}/.ATTRIBUTES/VARIABLE_VALUE"
                    try:
                        value = reader.get_tensor(ckpt_key)
                        weight.assign(value)
                    except Exception:
                        pass  # Skip if not found

        DeepWaterMapInference._model = model
        DeepWaterMapInference._weights_loaded = True

        logger.info("DeepWaterMap model loaded and cached")

    def predict(
        self,
        image: np.ndarray,
        threshold: float = 0.5,
    ) -> Tuple[np.ndarray, np.ndarray]:
        """
        Run DeepWaterMap inference.

        Args:
            image: (6, H, W) array [blue, green, red, nir, swir1, swir2]
            threshold: Water probability threshold

        Returns:
            (water_probs, water_mask) arrays
        """
        self._load_model()
        model = DeepWaterMapInference._model

        # Transpose to (H, W, C)
        img = np.transpose(image, (1, 2, 0)).astype(np.float32)

        # Pad to multiple of 32
        h, w = img.shape[:2]
        pad_h = (32 - h % 32) % 32
        pad_w = (32 - w % 32) % 32

        if pad_h > 0 or pad_w > 0:
            img = np.pad(img, ((0, pad_h), (0, pad_w), (0, 0)), mode='reflect')

        # Normalize to [0, 1]
        img = np.nan_to_num(img, nan=0.0, posinf=0.0, neginf=0.0)
        img = img - np.min(img)
        img = img / np.maximum(np.max(img), 1)

        # Inference
        img_batch = np.expand_dims(img, axis=0)
        dwm = model.predict(img_batch, verbose=0)
        dwm = np.squeeze(dwm)

        # Crop back to original size
        dwm = dwm[:h, :w]

        # Soft threshold (from original implementation)
        dwm = 1.0 / (1.0 + np.exp(-(16 * (dwm - 0.5))))
        dwm = np.clip(dwm, 0, 1)

        water_mask = (dwm > threshold).astype(np.uint8)

        return dwm, water_mask


class OSMWaterSource:
    """Query and rasterize OSM water features."""

    def __init__(self, timeout: int = 30, retries: int = 2):
        self.timeout = timeout
        self.retries = retries

    def get_water_mask(
        self,
        bbox: Tuple[float, float, float, float],
        shape: Tuple[int, int],
        buffer_m: float = 30.0,
    ) -> Tuple[np.ndarray, np.ndarray]:
        """
        Query OSM for water features and rasterize.

        Args:
            bbox: (min_lon, min_lat, max_lon, max_lat)
            shape: (height, width) of output raster
            buffer_m: Buffer width for line features in meters

        Returns:
            (water_mask, confidence) - binary mask and confidence (1.0 for OSM)
        """
        import geopandas as gpd
        from shapely.geometry import LineString, Polygon, box
        from shapely.ops import unary_union
        from rasterio.features import rasterize
        from affine import Affine

        xmin, ymin, xmax, ymax = bbox
        h, w = shape

        # Build Overpass query for water polygons AND waterways
        query = f"""
        [out:json][timeout:{self.timeout}];
        (
          way["natural"="water"]({ymin},{xmin},{ymax},{xmax});
          way["water"]({ymin},{xmin},{ymax},{xmax});
          way["waterway"~"river|stream|canal"]({ymin},{xmin},{ymax},{xmax});
          relation["natural"="water"]({ymin},{xmin},{ymax},{xmax});
        );
        out geom;
        """

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
            except Exception as e:
                if attempt < self.retries - 1:
                    time.sleep(1)
                else:
                    logger.warning(f"OSM query failed: {e}")
                    return np.zeros(shape, dtype=np.uint8), np.zeros(shape, dtype=np.float32)

        # Parse geometries
        polygons = []
        lines = []

        for element in data.get("elements", []):
            if "geometry" not in element:
                continue

            coords = [(p["lon"], p["lat"]) for p in element["geometry"]]
            if len(coords) < 2:
                continue

            tags = element.get("tags", {})

            # Check if it's a closed polygon (water body)
            if (coords[0] == coords[-1] and len(coords) >= 4) or \
               tags.get("natural") == "water" or tags.get("water"):
                try:
                    poly = Polygon(coords)
                    if poly.is_valid:
                        polygons.append(poly)
                except:
                    pass
            else:
                # It's a line (waterway)
                lines.append(LineString(coords))

        if not polygons and not lines:
            logger.info("OSM: No water features found")
            return np.zeros(shape, dtype=np.uint8), np.zeros(shape, dtype=np.float32)

        # Create transform
        pixel_width = (xmax - xmin) / w
        pixel_height = (ymax - ymin) / h
        transform = Affine(pixel_width, 0, xmin, 0, -pixel_height, ymax)

        # Convert buffer from meters to degrees (approximate)
        buffer_deg = buffer_m / 111000.0

        # Buffer lines and combine with polygons
        all_geoms = polygons.copy()
        for line in lines:
            all_geoms.append(line.buffer(buffer_deg))

        if not all_geoms:
            return np.zeros(shape, dtype=np.uint8), np.zeros(shape, dtype=np.float32)

        # Rasterize
        try:
            combined = unary_union(all_geoms)
            mask = rasterize(
                [(combined, 1)],
                out_shape=shape,
                transform=transform,
                fill=0,
                dtype=np.uint8,
            )
        except Exception as e:
            logger.warning(f"OSM rasterization failed: {e}")
            return np.zeros(shape, dtype=np.uint8), np.zeros(shape, dtype=np.float32)

        n_features = len(polygons) + len(lines)
        pct = 100 * np.mean(mask)
        logger.info(f"OSM: {n_features} features, {pct:.1f}% water")

        # Confidence is 1.0 where we have data (binary source)
        confidence = mask.astype(np.float32)

        return mask, confidence


class ESRILandCoverSource:
    """Query ESRI/IO 10m Land Cover for water class via Planetary Computer."""

    # ESRI/Impact Observatory 10m Land Cover - Water class = 1
    # Classes: 0=nodata, 1=water, 2=trees, 4=flooded_veg, 5=crops,
    #          7=built, 8=bare, 9=snow, 10=clouds, 11=rangeland
    WATER_CLASS = 1

    # Planetary Computer STAC endpoint
    STAC_URL = "https://planetarycomputer.microsoft.com/api/stac/v1"

    def __init__(self):
        self._client = None

    def get_water_mask(
        self,
        bbox: Tuple[float, float, float, float],
        shape: Tuple[int, int],
        year: int = 2022,
    ) -> Tuple[np.ndarray, np.ndarray]:
        """
        Query ESRI/IO Land Cover and extract water class.

        Args:
            bbox: (min_lon, min_lat, max_lon, max_lat)
            shape: (height, width) of output raster
            year: Year of land cover data (2017-2023)

        Returns:
            (water_mask, confidence) arrays
        """
        try:
            import rasterio
            from rasterio.windows import from_bounds
            from rasterio.warp import reproject, Resampling
            from affine import Affine
            import pystac_client
            import planetary_computer
        except ImportError as e:
            logger.warning(f"Dependencies not available for ESRI land cover: {e}")
            return np.zeros(shape, dtype=np.uint8), np.zeros(shape, dtype=np.float32)

        xmin, ymin, xmax, ymax = bbox
        h, w = shape

        try:
            # Connect to Planetary Computer
            catalog = pystac_client.Client.open(
                self.STAC_URL,
                modifier=planetary_computer.sign_inplace,
            )

            # Search for IO land cover - items are named like "16S-2023"
            search = catalog.search(
                collections=["io-lulc-annual-v02"],
                bbox=bbox,
            )

            items = list(search.items())

            # Filter to requested year (item IDs end with -YYYY)
            year_items = [it for it in items if it.id.endswith(f"-{year}")]
            if not year_items and items:
                # Fallback to most recent year
                year_items = sorted(items, key=lambda x: x.id, reverse=True)[:1]

            if not year_items:
                logger.warning(f"ESRI: No land cover found for {year}")
                return np.zeros(shape, dtype=np.uint8), np.zeros(shape, dtype=np.float32)

            item = year_items[0]
            logger.info(f"ESRI: Using {item.id}")
            href = item.assets["data"].href

            # Read the data - handle CRS transformation
            with rasterio.open(href) as src:
                # Transform bbox from WGS84 to source CRS if needed
                from rasterio.warp import transform_bounds
                if src.crs and src.crs != "EPSG:4326":
                    src_bounds = transform_bounds("EPSG:4326", src.crs, xmin, ymin, xmax, ymax)
                else:
                    src_bounds = (xmin, ymin, xmax, ymax)

                # Calculate window for reprojected bbox
                window = from_bounds(*src_bounds, src.transform)
                # Ensure window is valid
                window = window.round_offsets().round_lengths()
                if window.width < 1 or window.height < 1:
                    logger.warning("ESRI: Window too small after reprojection")
                    return np.zeros(shape, dtype=np.uint8), np.zeros(shape, dtype=np.float32)

                data = src.read(1, window=window)

            # Extract water class
            water = (data == self.WATER_CLASS).astype(np.uint8)

            # Resample to target shape if needed
            from scipy.ndimage import zoom as scipy_zoom
            if water.shape != (h, w) and water.shape[0] > 0 and water.shape[1] > 0:
                scale = (h / water.shape[0], w / water.shape[1])
                water = scipy_zoom(water, scale, order=0)

            # Ensure correct shape
            water = water[:h, :w]
            if water.shape != (h, w):
                result = np.zeros((h, w), dtype=np.uint8)
                result[:water.shape[0], :water.shape[1]] = water
                water = result

            pct = 100 * np.mean(water)
            logger.info(f"ESRI Land Cover: {pct:.1f}% water")

            return water, water.astype(np.float32)

        except Exception as e:
            logger.warning(f"ESRI Land Cover failed: {e}")
            return np.zeros(shape, dtype=np.uint8), np.zeros(shape, dtype=np.float32)


class WaterEnsemble:
    """
    Multi-method water detection ensemble.

    Combines spectral indices, deep learning, and external data sources
    for robust water detection. Uses per-pixel voting across all methods.

    Methods (up to 8):
    - 4 spectral indices: NDWI, MNDWI, AWEI_nsh, AWEI_sh
    - 2 deep learning: ML4Floods, DeepWaterMap
    - 2 external: OSM water features, ESRI Land Cover 10m
    """

    def __init__(
        self,
        ml4floods_dir: str = "ml4floods_models/models/WF2_unetv2_bgriswirs",
        deepwatermap_ckpt: str = "deepwatermap/checkpoints/cp.135.ckpt",
        use_ml4floods: bool = True,
        use_deepwatermap: bool = True,
        use_osm: bool = False,
        use_esri: bool = False,
    ):
        self.use_ml4floods = use_ml4floods
        self.use_deepwatermap = use_deepwatermap
        self.use_osm = use_osm
        self.use_esri = use_esri

        if use_ml4floods:
            self.ml4floods = ML4FloodsInference(ml4floods_dir)
        if use_deepwatermap:
            self.deepwatermap = DeepWaterMapInference(deepwatermap_ckpt)
        if use_osm:
            self.osm = OSMWaterSource()
        if use_esri:
            self.esri = ESRILandCoverSource()

    def detect_water(
        self,
        blue: np.ndarray,
        green: np.ndarray,
        red: np.ndarray,
        nir: np.ndarray,
        swir1: np.ndarray,
        swir2: np.ndarray,
        bbox: Optional[Tuple[float, float, float, float]] = None,
        min_votes: int = 4,
        index_thresholds: Optional[Dict[str, float]] = None,
        ml4floods_threshold: float = 0.6,
        deepwatermap_threshold: float = 0.5,
    ) -> EnsembleResult:
        """
        Run ensemble water detection.

        Args:
            blue, green, red, nir, swir1, swir2: Band arrays (same shape)
            bbox: (min_lon, min_lat, max_lon, max_lat) for external sources
            min_votes: Minimum votes for pixel to be classified as water
            index_thresholds: Custom thresholds for spectral indices
            ml4floods_threshold: ML4Floods probability threshold
            deepwatermap_threshold: DeepWaterMap probability threshold

        Returns:
            EnsembleResult with all masks and voting results
        """
        h, w = blue.shape
        masks = {}
        probabilities = {}

        # 1. Compute all spectral indices (vectorized)
        logger.info("Computing spectral indices...")
        indices = WaterIndexComputer.compute_all(
            blue, green, red, nir, swir1, swir2
        )
        index_masks = WaterIndexComputer.threshold_indices(indices, index_thresholds)

        for name, idx in indices.items():
            probabilities[name] = idx
            masks[name] = index_masks[name]
            pct = 100 * np.mean(masks[name])
            logger.info(f"  {name}: {pct:.1f}% water")

        # 2. ML4Floods inference
        if self.use_ml4floods:
            logger.info("Running ML4Floods...")
            image = np.stack([blue, green, red, nir, swir1, swir2], axis=0)
            ml4f_probs, ml4f_mask = self.ml4floods.predict(
                image, threshold=ml4floods_threshold
            )
            probabilities['ml4floods'] = ml4f_probs
            masks['ml4floods'] = ml4f_mask
            logger.info(f"  ml4floods: {100*np.mean(ml4f_mask):.1f}% water")

        # 3. DeepWaterMap inference
        if self.use_deepwatermap:
            logger.info("Running DeepWaterMap...")
            image = np.stack([blue, green, red, nir, swir1, swir2], axis=0)
            dwm_probs, dwm_mask = self.deepwatermap.predict(
                image, threshold=deepwatermap_threshold
            )
            probabilities['deepwatermap'] = dwm_probs
            masks['deepwatermap'] = dwm_mask
            logger.info(f"  deepwatermap: {100*np.mean(dwm_mask):.1f}% water")

        # 4. OSM water features (requires bbox)
        if self.use_osm and bbox is not None:
            logger.info("Querying OSM water...")
            osm_mask, osm_conf = self.osm.get_water_mask(bbox, (h, w))
            probabilities['osm'] = osm_conf
            masks['osm'] = osm_mask
            logger.info(f"  osm: {100*np.mean(osm_mask):.1f}% water")

        # 5. ESRI Land Cover (requires bbox) - use 2023 as latest stable year
        if self.use_esri and bbox is not None:
            logger.info("Querying ESRI Land Cover (2023)...")
            esri_mask, esri_conf = self.esri.get_water_mask(bbox, (h, w), year=2023)
            probabilities['esri_lc'] = esri_conf
            masks['esri_lc'] = esri_mask
            logger.info(f"  esri_lc: {100*np.mean(esri_mask):.1f}% water")

        # 6. Per-pixel voting
        n_methods = len(masks)
        stack = np.stack(list(masks.values()), axis=0)
        vote_counts = stack.sum(axis=0)
        ensemble_mask = (vote_counts >= min_votes).astype(np.uint8)

        logger.info(f"Ensemble voting (>={min_votes}/{n_methods}): "
                   f"{100*np.mean(ensemble_mask):.1f}% water")

        # Compute stats
        stats = {
            'n_methods': n_methods,
            'min_votes': min_votes,
            'methods': list(masks.keys()),
            'per_method_pct': {k: float(100*np.mean(v)) for k, v in masks.items()},
            'ensemble_pct': float(100 * np.mean(ensemble_mask)),
            'vote_distribution': {
                i: float(np.sum(vote_counts == i) / vote_counts.size * 100)
                for i in range(n_methods + 1)
            }
        }

        return EnsembleResult(
            masks=masks,
            probabilities=probabilities,
            vote_counts=vote_counts,
            ensemble_mask=ensemble_mask,
            stats=stats,
        )

    def detect_water_from_stac(
        self,
        item,
        bbox: Tuple[float, float, float, float],
        cog_reader,
        min_votes: int = 4,
        **kwargs,
    ) -> EnsembleResult:
        """
        Run ensemble detection from a STAC item.

        Args:
            item: pystac Item
            bbox: (min_lon, min_lat, max_lon, max_lat)
            cog_reader: COGReader instance
            min_votes: Minimum votes for water
            **kwargs: Passed to detect_water

        Returns:
            EnsembleResult
        """
        from scipy.ndimage import zoom

        band_map = {
            "blue": "blue",
            "green": "green",
            "red": "red",
            "nir": "nir",
            "swir1": "swir16",
            "swir2": "swir22",
        }

        bands = {}
        ref_shape = None

        for name, stac_name in band_map.items():
            data, _, _ = cog_reader.read_band(item, stac_name, bbox)

            if ref_shape is None:
                ref_shape = data.shape
            elif data.shape != ref_shape:
                scale = (ref_shape[0] / data.shape[0], ref_shape[1] / data.shape[1])
                data = zoom(data, scale, order=1)

            bands[name] = data.astype(np.float32)

        return self.detect_water(
            bands['blue'], bands['green'], bands['red'],
            bands['nir'], bands['swir1'], bands['swir2'],
            bbox=bbox,
            min_votes=min_votes,
            **kwargs,
        )

    def detect_water_from_composite(
        self,
        items: List[Any],
        bbox: Tuple[float, float, float, float],
        cog_reader,
        min_votes: int = 4,
        min_valid_pct: float = 30.0,
        max_scenes: int = 10,
        **kwargs,
    ) -> Tuple[EnsembleResult, CompositeResult]:
        """
        Run ensemble detection on a cloud-free composite from multiple scenes.

        This is the recommended method for robust water detection. It:
        1. Iterates through scenes sorted by cloud cover
        2. Uses SCL band to identify clear pixels
        3. Composites clear pixels from multiple scenes
        4. Runs ensemble water detection on the composite

        Args:
            items: List of pystac Items (ideally sorted by cloud cover)
            bbox: (min_lon, min_lat, max_lon, max_lat)
            cog_reader: COGReader instance
            min_votes: Minimum votes for water
            min_valid_pct: Min clear pixel % per scene to use it
            max_scenes: Maximum scenes to composite
            **kwargs: Passed to detect_water

        Returns:
            (EnsembleResult, CompositeResult) tuple
        """
        compositor = SceneCompositor(
            cloud_filter=CloudFilter(),
            min_valid_pct=min_valid_pct,
            max_scenes=max_scenes,
        )

        # Create cloud-free composite
        logger.info(f"Creating cloud-free composite from {len(items)} scenes...")
        composite = compositor.composite(
            items=items,
            bbox=bbox,
            cog_reader=cog_reader,
        )

        # Map band names
        band_map = {
            "blue": "blue",
            "green": "green",
            "red": "red",
            "nir": "nir",
            "swir1": "swir16",
            "swir2": "swir22",
        }

        # Run ensemble on composite
        result = self.detect_water(
            blue=composite.bands.get(band_map["blue"], composite.bands.get("blue")),
            green=composite.bands.get(band_map["green"], composite.bands.get("green")),
            red=composite.bands.get(band_map["red"], composite.bands.get("red")),
            nir=composite.bands.get(band_map["nir"], composite.bands.get("nir")),
            swir1=composite.bands.get(band_map["swir1"], composite.bands.get("swir1")),
            swir2=composite.bands.get(band_map["swir2"], composite.bands.get("swir2")),
            bbox=bbox,
            min_votes=min_votes,
            **kwargs,
        )

        # Apply valid mask - NaN pixels were cloudy/bad
        result.stats['composite'] = {
            'n_scenes': composite.n_scenes_used,
            'coverage_pct': composite.coverage_pct,
            'scene_ids': composite.scene_ids,
        }

        return result, composite


def remove_small_blobs(
    mask: np.ndarray,
    min_size: int = 100,
) -> np.ndarray:
    """
    Remove small connected components from binary mask.

    Args:
        mask: Binary water mask
        min_size: Minimum component size in pixels to keep

    Returns:
        Cleaned binary mask
    """
    from skimage.morphology import remove_small_objects

    cleaned = remove_small_objects(mask.astype(bool), min_size=min_size)
    n_removed = np.sum(mask) - np.sum(cleaned)
    logger.info(f"Removed small blobs: {n_removed} pixels ({min_size}px threshold)")
    return cleaned.astype(np.uint8)


def extract_channel_mask(
    water_mask: np.ndarray,
    corridor_mask: np.ndarray = None,
    centerline_mask: np.ndarray = None,
    min_size: int = 100,
    min_elongation: float = 3.0,
) -> Tuple[np.ndarray, Dict[str, Any]]:
    """
    Extract main river channel from water mask.

    Pipeline:
    1. Intersect with corridor (SWORD buffer)
    2. Remove small objects
    3. Filter by elongation (keep linear features, remove blobs)
    4. Keep component overlapping centerline (or largest)

    Args:
        water_mask: Binary water mask from ensemble
        corridor_mask: Binary corridor from SWORD buffer
        centerline_mask: Optional thin centerline buffer for selection
        min_size: Minimum component size in pixels
        min_elongation: Minimum elongation ratio (length/width) to keep

    Returns:
        (channel_mask, stats) tuple
    """
    from scipy.ndimage import label, find_objects
    from scipy.ndimage import distance_transform_edt
    from skimage.morphology import remove_small_objects

    stats = {
        'input_water_pct': float(100 * np.mean(water_mask)),
        'corridor_pct': float(100 * np.mean(corridor_mask)) if corridor_mask is not None else 100.0,
    }

    # Step 1: Intersect with corridor (if provided)
    if corridor_mask is not None:
        masked = water_mask.astype(bool) & corridor_mask.astype(bool)
        stats['after_corridor_pct'] = float(100 * np.mean(masked))
    else:
        masked = water_mask.astype(bool)
        stats['after_corridor_pct'] = stats['input_water_pct']

    if not np.any(masked):
        return np.zeros_like(water_mask, dtype=np.uint8), stats

    # Step 2: Remove small objects
    cleaned = remove_small_objects(masked, min_size=min_size)
    stats['after_small_removal_pct'] = float(100 * np.mean(cleaned))

    # Step 3: Filter by elongation (keep rivers, remove blobs)
    labeled, n_components = label(cleaned)

    if n_components == 0:
        return np.zeros_like(water_mask, dtype=np.uint8), stats

    keep_labels = []

    for i in range(1, n_components + 1):
        component = labeled == i
        area = np.sum(component)

        # Compute elongation using bounding box
        slices = find_objects(component.astype(int))[0]
        if slices is None:
            continue

        height = slices[0].stop - slices[0].start
        width = slices[1].stop - slices[1].start

        # Elongation = max(h,w) / min(h,w)
        elongation = max(height, width) / max(min(height, width), 1)

        if elongation >= min_elongation:
            keep_labels.append(i)
            logger.debug(f"Component {i}: area={area}, elong={elongation:.1f} - KEEP")
        else:
            logger.debug(f"Component {i}: area={area}, elong={elongation:.1f} - DROP (blob)")

    stats['n_components_total'] = n_components
    stats['n_components_elongated'] = len(keep_labels)

    if not keep_labels:
        # If elongation filter removes everything, fall back to largest
        logger.warning("Elongation filter too strict, keeping largest component")
        sizes = [np.sum(labeled == i) for i in range(1, n_components + 1)]
        keep_labels = [np.argmax(sizes) + 1]

    # Build filtered mask
    filtered = np.isin(labeled, keep_labels)

    # Step 4: Keep main component (overlapping centerline or largest)
    if centerline_mask is not None and np.any(centerline_mask):
        labeled_filt, n_filt = label(filtered)
        if n_filt > 1:
            best_label = 0
            best_overlap = 0
            for i in range(1, n_filt + 1):
                overlap = np.sum((labeled_filt == i) & centerline_mask)
                if overlap > best_overlap:
                    best_overlap = overlap
                    best_label = i
            if best_label > 0:
                filtered = labeled_filt == best_label
                logger.info(f"Selected component {best_label} (overlap={best_overlap})")

    channel = filtered.astype(np.uint8)
    stats['output_channel_pct'] = float(100 * np.mean(channel))

    logger.info(
        f"Channel extraction: {stats['input_water_pct']:.1f}% water -> "
        f"{stats['output_channel_pct']:.1f}% channel"
    )

    return channel, stats


def create_corridor_from_geometry(
    geometry,
    shape: Tuple[int, int],
    transform,
    buffer_m: float = 500.0,
) -> np.ndarray:
    """
    Create corridor mask from SWORD/centerline geometry.

    Args:
        geometry: Shapely geometry or GeoDataFrame
        shape: (height, width) of output raster
        transform: Affine transform
        buffer_m: Buffer distance in meters

    Returns:
        Binary corridor mask
    """
    from shapely.geometry import mapping
    from rasterio.features import rasterize

    # Handle GeoDataFrame
    if hasattr(geometry, 'geometry'):
        geom = geometry.geometry.unary_union
    else:
        geom = geometry

    # Buffer (approximate degrees)
    buffer_deg = buffer_m / 111000.0
    buffered = geom.buffer(buffer_deg)

    # Rasterize
    corridor = rasterize(
        [(mapping(buffered), 1)],
        out_shape=shape,
        transform=transform,
        fill=0,
        dtype=np.uint8,
    )

    return corridor.astype(bool)
