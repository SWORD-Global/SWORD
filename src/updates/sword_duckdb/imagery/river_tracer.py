"""
River Tracer - Patch-based water classification with stitched RivGraph extraction.

Pipeline:
1. Define river corridor (from SWORD or bbox)
2. Divide into patches
3. Classify water per-patch (parallelizable)
4. Mosaic patches into continuous mask
5. Run RivGraph once on full mosaic
"""

import numpy as np
from dataclasses import dataclass, field
from typing import List, Dict, Tuple, Optional
import logging
import tempfile
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

from scipy.ndimage import binary_closing, label as scipy_label
from skimage.morphology import disk
from rasterio.transform import from_bounds
import rasterio

logger = logging.getLogger(__name__)


@dataclass
class Patch:
    """A processing patch."""
    idx: int
    row: int
    col: int
    bbox: Tuple[float, float, float, float]  # (min_lon, min_lat, max_lon, max_lat)
    water_mask: Optional[np.ndarray] = None
    processed: bool = False
    error: Optional[str] = None


@dataclass
class MosaicResult:
    """Result of mosaicking patches."""
    mask: np.ndarray
    transform: rasterio.Affine
    crs: str
    bbox: Tuple[float, float, float, float]
    n_patches: int
    coverage_pct: float


@dataclass
class RiverTraceResult:
    """Result of tracing a river."""
    mosaic: MosaicResult
    centerline_coords: Optional[np.ndarray]  # (N, 2) as [lon, lat]
    centerline_px: Optional[np.ndarray]  # (N, 2) as [row, col]
    n_branches: int
    total_length_km: float
    metadata: Dict = field(default_factory=dict)


class RiverTracer:
    """
    Traces a river corridor using patch-based water classification
    and stitched RivGraph extraction.
    """

    def __init__(
        self,
        patch_size_deg: float = 0.1,  # ~11km patches
        overlap_deg: float = 0.01,  # ~1km overlap
        min_votes: int = 4,
        max_cloud_cover: float = 30.0,
        date_range: Tuple[str, str] = ("2024-01-01", "2024-12-31"),
        min_blob_size: int = 200,
        component_threshold: float = 0.05,  # Keep components >5% of largest
        n_workers: int = 4,
    ):
        self.patch_size = patch_size_deg
        self.overlap = overlap_deg
        self.min_votes = min_votes
        self.max_cloud = max_cloud_cover
        self.start_date, self.end_date = date_range
        self.min_blob_size = min_blob_size
        self.component_threshold = component_threshold
        self.n_workers = n_workers

        # Lazy-load
        self._stac = None
        self._cog = None
        self._ensemble = None

    @property
    def stac(self):
        if self._stac is None:
            from .stac_client import SentinelSTACClient
            self._stac = SentinelSTACClient()
        return self._stac

    @property
    def cog(self):
        if self._cog is None:
            from .cog_reader import COGReader
            self._cog = COGReader()
        return self._cog

    @property
    def ensemble(self):
        if self._ensemble is None:
            from .water_ensemble import WaterEnsemble
            self._ensemble = WaterEnsemble(use_osm=False, use_esri=False)
        return self._ensemble

    def create_patches(
        self,
        bbox: Tuple[float, float, float, float]
    ) -> List[Patch]:
        """Divide bbox into overlapping patches."""
        min_lon, min_lat, max_lon, max_lat = bbox
        step = self.patch_size - self.overlap

        patches = []
        idx = 0
        row = 0
        lat = min_lat

        while lat < max_lat:
            col = 0
            lon = min_lon

            while lon < max_lon:
                patch_bbox = (
                    lon,
                    lat,
                    min(lon + self.patch_size, max_lon),
                    min(lat + self.patch_size, max_lat),
                )
                patches.append(Patch(
                    idx=idx,
                    row=row,
                    col=col,
                    bbox=patch_bbox,
                ))
                idx += 1
                col += 1
                lon += step

            row += 1
            lat += step

        return patches

    def process_patch(self, patch: Patch) -> Patch:
        """Process a single patch - water classification."""
        try:
            logger.debug(f"Processing patch {patch.idx} at {patch.bbox}")

            # Search for scenes
            items = self.stac.search_by_bbox(
                patch.bbox,
                start_date=self.start_date,
                end_date=self.end_date,
                max_cloud_cover=self.max_cloud,
                limit=20,
            )

            if len(items) == 0:
                patch.error = "No imagery found"
                patch.processed = True
                return patch

            items_sorted = sorted(
                items,
                key=lambda x: x.properties.get("eo:cloud_cover", 100)
            )

            # Run ensemble
            result, composite = self.ensemble.detect_water_from_composite(
                items=items_sorted,
                bbox=patch.bbox,
                cog_reader=self.cog,
                min_votes=self.min_votes,
                min_valid_pct=30.0,
                max_scenes=10,
            )

            # Post-process
            mask = self._postprocess_mask(result.ensemble_mask)
            patch.water_mask = mask
            patch.processed = True

            logger.debug(f"Patch {patch.idx}: {100*np.mean(mask):.1f}% water")

        except Exception as e:
            logger.warning(f"Patch {patch.idx} failed: {e}")
            patch.error = str(e)
            patch.processed = True

        return patch

    def _postprocess_mask(self, mask: np.ndarray) -> np.ndarray:
        """Clean up water mask."""
        from .water_ensemble import remove_small_blobs

        # Close small gaps
        closed = binary_closing(mask, structure=disk(2))

        # Remove small blobs
        cleaned = remove_small_blobs(closed.astype(np.uint8), min_size=self.min_blob_size)

        return cleaned.astype(np.uint8)

    def process_patches_parallel(self, patches: List[Patch]) -> List[Patch]:
        """Process patches in parallel."""
        logger.info(f"Processing {len(patches)} patches with {self.n_workers} workers")

        with ThreadPoolExecutor(max_workers=self.n_workers) as executor:
            futures = {executor.submit(self.process_patch, p): p for p in patches}

            completed = 0
            for future in as_completed(futures):
                patch = future.result()
                completed += 1
                if completed % 5 == 0:
                    logger.info(f"  Progress: {completed}/{len(patches)} patches")

        return patches

    def mosaic_patches(
        self,
        patches: List[Patch],
        full_bbox: Tuple[float, float, float, float],
        resolution_m: float = 10.0,
    ) -> MosaicResult:
        """Stitch patches into a single mosaic."""
        min_lon, min_lat, max_lon, max_lat = full_bbox

        # Calculate mosaic dimensions
        width_deg = max_lon - min_lon
        height_deg = max_lat - min_lat

        # Approximate meters per degree at this latitude
        lat_center = (min_lat + max_lat) / 2
        m_per_deg_lon = 111000 * np.cos(np.radians(lat_center))
        m_per_deg_lat = 111000

        width_m = width_deg * m_per_deg_lon
        height_m = height_deg * m_per_deg_lat

        width_px = int(width_m / resolution_m)
        height_px = int(height_m / resolution_m)

        logger.info(f"Mosaic dimensions: {width_px}x{height_px} pixels")

        # Create empty mosaic
        mosaic = np.zeros((height_px, width_px), dtype=np.uint8)
        counts = np.zeros((height_px, width_px), dtype=np.uint8)

        # Create transform
        transform = from_bounds(min_lon, min_lat, max_lon, max_lat, width_px, height_px)
        inv_transform = ~transform

        # Place each patch
        for patch in patches:
            if patch.water_mask is None:
                continue

            p_min_lon, p_min_lat, p_max_lon, p_max_lat = patch.bbox
            p_h, p_w = patch.water_mask.shape

            # Calculate pixel bounds in mosaic
            col_start, row_end = [int(x) for x in inv_transform * (p_min_lon, p_min_lat)]
            col_end, row_start = [int(x) for x in inv_transform * (p_max_lon, p_max_lat)]

            # Clamp to mosaic bounds
            row_start = max(0, row_start)
            row_end = min(height_px, row_end)
            col_start = max(0, col_start)
            col_end = min(width_px, col_end)

            # Calculate corresponding patch region
            mosaic_h = row_end - row_start
            mosaic_w = col_end - col_start

            if mosaic_h <= 0 or mosaic_w <= 0:
                continue

            # Resize patch to fit (simple nearest neighbor)
            from skimage.transform import resize
            patch_resized = resize(
                patch.water_mask,
                (mosaic_h, mosaic_w),
                order=0,
                preserve_range=True,
            ).astype(np.uint8)

            # Add to mosaic (max for overlapping regions)
            mosaic[row_start:row_end, col_start:col_end] = np.maximum(
                mosaic[row_start:row_end, col_start:col_end],
                patch_resized
            )
            counts[row_start:row_end, col_start:col_end] += 1

        # Calculate coverage
        coverage_pct = 100 * np.sum(counts > 0) / counts.size

        # Final cleanup on mosaic
        mosaic = self._cleanup_mosaic(mosaic)

        return MosaicResult(
            mask=mosaic,
            transform=transform,
            crs="EPSG:4326",
            bbox=full_bbox,
            n_patches=len([p for p in patches if p.water_mask is not None]),
            coverage_pct=coverage_pct,
        )

    def _cleanup_mosaic(self, mosaic: np.ndarray) -> np.ndarray:
        """Final cleanup on stitched mosaic."""
        # Close gaps at patch boundaries
        closed = binary_closing(mosaic, structure=disk(3))

        # Remove small components, keep large connected network
        labeled, n_components = scipy_label(closed)
        if n_components == 0:
            return mosaic.astype(np.uint8)

        component_sizes = [(i, np.sum(labeled == i)) for i in range(1, n_components + 1)]
        component_sizes.sort(key=lambda x: x[1], reverse=True)

        largest = component_sizes[0][1]
        keep = [l for l, s in component_sizes if s >= largest * self.component_threshold]

        return np.isin(labeled, keep).astype(np.uint8)

    def extract_centerline(
        self,
        mosaic: MosaicResult,
        exit_sides: str = "auto",
    ) -> Tuple[Optional[np.ndarray], Optional[np.ndarray]]:
        """Run RivGraph on mosaic to extract centerline."""
        try:
            from rivgraph.classes import river
        except ImportError:
            logger.error("RivGraph not installed")
            return None, None

        h, w = mosaic.mask.shape

        with tempfile.TemporaryDirectory() as tmpdir:
            mask_path = os.path.join(tmpdir, 'mosaic.tif')

            with rasterio.open(
                mask_path, 'w', driver='GTiff',
                height=h, width=w, count=1, dtype='uint8',
                crs=mosaic.crs, transform=mosaic.transform,
            ) as dst:
                dst.write(mosaic.mask, 1)

            # Auto-detect exit sides based on mask
            if exit_sides == "auto":
                exit_sides = self._detect_exit_sides(mosaic.mask)

            logger.info(f"Running RivGraph with exit_sides={exit_sides}")

            riv = river(
                name='river_trace',
                path_to_mask=mask_path,
                results_folder=tmpdir,
                exit_sides=exit_sides,
            )

            riv.compute_network()
            riv.compute_centerline()

            if hasattr(riv, 'centerline') and riv.centerline is not None:
                cl = riv.centerline
                if isinstance(cl, tuple) and len(cl) >= 2:
                    x_coords, y_coords = cl[0], cl[1]
                    cl_wgs84 = np.column_stack([x_coords, y_coords])

                    # Convert to pixel coords
                    inv_t = ~mosaic.transform
                    cl_px = np.array([inv_t * (x, y) for x, y in cl_wgs84])
                    cl_px = cl_px[:, ::-1]  # (row, col)

                    return cl_wgs84, cl_px

        return None, None

    def _detect_exit_sides(self, mask: np.ndarray) -> str:
        """Auto-detect which sides the river exits from."""
        h, w = mask.shape
        margin = 10

        sides = []
        if np.any(mask[:margin, :]):  # Top
            sides.append('n')
        if np.any(mask[-margin:, :]):  # Bottom
            sides.append('s')
        if np.any(mask[:, :margin]):  # Left
            sides.append('w')
        if np.any(mask[:, -margin:]):  # Right
            sides.append('e')

        if not sides:
            sides = ['n', 's']  # Default

        return ','.join(sides)

    def trace(
        self,
        bbox: Tuple[float, float, float, float],
        exit_sides: str = "auto",
    ) -> RiverTraceResult:
        """
        Full pipeline: patches → mosaic → RivGraph.

        Args:
            bbox: (min_lon, min_lat, max_lon, max_lat)
            exit_sides: RivGraph exit sides or "auto"

        Returns:
            RiverTraceResult with centerline and metadata
        """
        logger.info(f"Tracing river in bbox {bbox}")

        # Phase 1: Create and process patches
        patches = self.create_patches(bbox)
        logger.info(f"Created {len(patches)} patches")

        patches = self.process_patches_parallel(patches)

        n_success = len([p for p in patches if p.water_mask is not None])
        n_failed = len([p for p in patches if p.error is not None])
        logger.info(f"Processed: {n_success} success, {n_failed} failed")

        # Phase 2: Mosaic
        mosaic = self.mosaic_patches(patches, bbox)
        logger.info(f"Mosaic: {mosaic.mask.shape}, {mosaic.coverage_pct:.1f}% coverage")

        # Phase 3: RivGraph
        cl_wgs84, cl_px = self.extract_centerline(mosaic, exit_sides)

        if cl_wgs84 is not None:
            # Calculate length
            diffs = np.diff(cl_wgs84, axis=0)
            lat_center = bbox[1] + (bbox[3] - bbox[1]) / 2
            m_per_deg = 111000 * np.cos(np.radians(lat_center))
            lengths_m = np.sqrt((diffs[:, 0] * m_per_deg) ** 2 + (diffs[:, 1] * 111000) ** 2)
            total_length_km = np.sum(lengths_m) / 1000

            logger.info(f"Centerline: {len(cl_wgs84)} points, {total_length_km:.1f} km")
        else:
            total_length_km = 0
            logger.warning("No centerline extracted")

        return RiverTraceResult(
            mosaic=mosaic,
            centerline_coords=cl_wgs84,
            centerline_px=cl_px,
            n_branches=1 if cl_wgs84 is not None else 0,
            total_length_km=total_length_km,
            metadata={
                'n_patches': len(patches),
                'n_patches_success': n_success,
                'n_patches_failed': n_failed,
                'bbox': bbox,
                'patch_size_deg': self.patch_size,
            }
        )
