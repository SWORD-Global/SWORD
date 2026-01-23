"""
Grid-based SWORD update processor.

Efficiently processes all SWORD reaches by:
1. Dividing globe into grid cells
2. Processing all reaches per cell with shared imagery
3. Running RivGraph once per cell on full water mask
"""

import numpy as np
from dataclasses import dataclass, field
from typing import List, Dict, Tuple, Optional, Iterator
import logging
from concurrent.futures import ProcessPoolExecutor, as_completed
import tempfile
import os

from scipy.ndimage import binary_closing, label as scipy_label, distance_transform_edt
from skimage.morphology import disk
from scipy.spatial import cKDTree

logger = logging.getLogger(__name__)


@dataclass
class GridCell:
    """A processing grid cell."""
    min_lon: float
    min_lat: float
    max_lon: float
    max_lat: float
    cell_id: str = ""

    def __post_init__(self):
        if not self.cell_id:
            self.cell_id = f"{self.min_lon:.1f}_{self.min_lat:.1f}"

    @property
    def bbox(self) -> Tuple[float, float, float, float]:
        return (self.min_lon, self.min_lat, self.max_lon, self.max_lat)

    @property
    def center(self) -> Tuple[float, float]:
        return ((self.min_lon + self.max_lon) / 2, (self.min_lat + self.max_lat) / 2)

    def contains(self, lon: float, lat: float) -> bool:
        return (self.min_lon <= lon <= self.max_lon and
                self.min_lat <= lat <= self.max_lat)


@dataclass
class ReachDrift:
    """Drift result for a single reach."""
    reach_id: int
    mean_drift_m: float
    max_drift_m: float
    n_nodes: int
    n_matched: int
    flag: int  # 0=ok, 1=minor, 2=major, -1=failed
    centerline_wgs84: Optional[np.ndarray] = None
    node_drifts_m: Optional[np.ndarray] = None


@dataclass
class CellResult:
    """Result of processing a grid cell."""
    cell: GridCell
    n_reaches: int
    n_successful: int
    n_failed: int
    reach_results: List[ReachDrift] = field(default_factory=list)
    water_coverage_pct: float = 0.0
    n_centerlines_extracted: int = 0
    error: Optional[str] = None


class GridProcessor:
    """
    Processes SWORD updates at grid-cell scale for efficiency.

    Instead of per-reach imagery fetching:
    - Fetches imagery once per cell
    - Builds one composite per cell
    - Runs RivGraph once on full cell mask
    - Matches extracted centerlines to SWORD reaches
    """

    def __init__(
        self,
        cell_size_deg: float = 0.5,
        min_votes: int = 4,
        min_water_pct: float = 0.1,
        minor_drift_m: float = 30.0,
        major_drift_m: float = 100.0,
        max_cloud_cover: float = 30.0,
        date_range: Tuple[str, str] = ("2024-01-01", "2024-12-31"),
    ):
        self.cell_size = cell_size_deg
        self.min_votes = min_votes
        self.min_water_pct = min_water_pct
        self.minor_drift = minor_drift_m
        self.major_drift = major_drift_m
        self.max_cloud = max_cloud_cover
        self.start_date, self.end_date = date_range

        # Lazy-load heavy dependencies
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

    def generate_grid(
        self,
        bounds: Tuple[float, float, float, float] = (-180, -60, 180, 75)
    ) -> Iterator[GridCell]:
        """Generate grid cells covering the given bounds."""
        min_lon, min_lat, max_lon, max_lat = bounds

        lat = min_lat
        while lat < max_lat:
            lon = min_lon
            while lon < max_lon:
                yield GridCell(
                    min_lon=lon,
                    min_lat=lat,
                    max_lon=min(lon + self.cell_size, max_lon),
                    max_lat=min(lat + self.cell_size, max_lat),
                )
                lon += self.cell_size
            lat += self.cell_size

    def get_reaches_in_cell(
        self,
        cell: GridCell,
        sword_data: Dict
    ) -> List[Dict]:
        """Get all SWORD reaches whose centroids fall within the cell."""
        reaches = []
        for reach in sword_data.get('reaches', []):
            # Check if reach centroid is in cell
            coords = np.array(reach['coordinates'])
            centroid_lon = coords[:, 0].mean()
            centroid_lat = coords[:, 1].mean()

            if cell.contains(centroid_lon, centroid_lat):
                reaches.append(reach)

        return reaches

    def process_cell(
        self,
        cell: GridCell,
        reaches: List[Dict],
    ) -> CellResult:
        """
        Process all reaches in a single grid cell.

        Args:
            cell: The grid cell to process
            reaches: List of SWORD reaches in this cell
                Each reach should have:
                - reach_id: int
                - coordinates: List of [lon, lat] pairs
                - width_m: float (optional)

        Returns:
            CellResult with per-reach drift analysis
        """
        if not reaches:
            return CellResult(
                cell=cell,
                n_reaches=0,
                n_successful=0,
                n_failed=0,
                error="No reaches in cell"
            )

        logger.info(f"Processing cell {cell.cell_id} with {len(reaches)} reaches")

        try:
            # Step 1: Fetch imagery and build composite
            items = self.stac.search_by_bbox(
                cell.bbox,
                start_date=self.start_date,
                end_date=self.end_date,
                max_cloud_cover=self.max_cloud,
                limit=30,
            )

            if len(items) == 0:
                return CellResult(
                    cell=cell,
                    n_reaches=len(reaches),
                    n_successful=0,
                    n_failed=len(reaches),
                    error="No imagery found"
                )

            items_sorted = sorted(
                items,
                key=lambda x: x.properties.get("eo:cloud_cover", 100)
            )

            # Step 2: Build water mask for entire cell
            result, composite = self.ensemble.detect_water_from_composite(
                items=items_sorted,
                bbox=cell.bbox,
                cog_reader=self.cog,
                min_votes=self.min_votes,
                min_valid_pct=30.0,
                max_scenes=10,
            )

            # Post-process mask
            water_mask = self._postprocess_mask(result.ensemble_mask)
            water_pct = 100 * np.mean(water_mask)

            if water_pct < self.min_water_pct:
                return CellResult(
                    cell=cell,
                    n_reaches=len(reaches),
                    n_successful=0,
                    n_failed=len(reaches),
                    water_coverage_pct=water_pct,
                    error=f"Insufficient water coverage: {water_pct:.2f}%"
                )

            # Step 3: Extract ALL centerlines with RivGraph
            h, w = water_mask.shape

            # Create transform from bbox
            from rasterio.transform import from_bounds
            transform = from_bounds(cell.bbox[0], cell.bbox[1], cell.bbox[2], cell.bbox[3], w, h)
            crs = "EPSG:4326"

            centerlines = self._extract_all_centerlines(
                water_mask, transform, crs, h, w
            )

            logger.info(f"  Extracted {len(centerlines)} centerlines")

            # Step 4: Match centerlines to SWORD reaches and compute drift
            reach_results = []
            n_success = 0
            n_fail = 0

            for reach in reaches:
                try:
                    drift_result = self._compute_reach_drift(
                        reach=reach,
                        centerlines=centerlines,
                        water_mask=water_mask,
                        transform=transform,
                    )
                    reach_results.append(drift_result)
                    if drift_result.flag >= 0:
                        n_success += 1
                    else:
                        n_fail += 1
                except Exception as e:
                    logger.warning(f"  Reach {reach['reach_id']} failed: {e}")
                    reach_results.append(ReachDrift(
                        reach_id=reach['reach_id'],
                        mean_drift_m=0,
                        max_drift_m=0,
                        n_nodes=len(reach['coordinates']),
                        n_matched=0,
                        flag=-1,
                    ))
                    n_fail += 1

            return CellResult(
                cell=cell,
                n_reaches=len(reaches),
                n_successful=n_success,
                n_failed=n_fail,
                reach_results=reach_results,
                water_coverage_pct=water_pct,
                n_centerlines_extracted=len(centerlines),
            )

        except Exception as e:
            logger.error(f"Cell {cell.cell_id} failed: {e}")
            return CellResult(
                cell=cell,
                n_reaches=len(reaches),
                n_successful=0,
                n_failed=len(reaches),
                error=str(e),
            )

    def _postprocess_mask(self, mask: np.ndarray) -> np.ndarray:
        """Clean up water mask."""
        from .water_ensemble import remove_small_blobs

        # Close gaps
        closed = binary_closing(mask, structure=disk(3))

        # Remove small blobs
        cleaned = remove_small_blobs(closed.astype(np.uint8), min_size=200)

        # Keep components > 20% of largest
        labeled, n_components = scipy_label(cleaned)
        if n_components == 0:
            return cleaned

        component_sizes = [(i, np.sum(labeled == i)) for i in range(1, n_components + 1)]
        component_sizes.sort(key=lambda x: x[1], reverse=True)

        largest = component_sizes[0][1]
        keep = [l for l, s in component_sizes if s >= largest * 0.20]

        return np.isin(labeled, keep).astype(np.uint8)

    def _extract_all_centerlines(
        self,
        water_mask: np.ndarray,
        transform,
        crs,
        h: int,
        w: int,
    ) -> List[np.ndarray]:
        """Extract all centerlines from water mask using RivGraph."""
        try:
            from rivgraph.classes import river
            import rasterio
        except ImportError:
            logger.error("RivGraph not installed")
            return []

        centerlines = []

        with tempfile.TemporaryDirectory() as tmpdir:
            mask_path = os.path.join(tmpdir, 'mask.tif')

            with rasterio.open(
                mask_path, 'w', driver='GTiff',
                height=h, width=w, count=1, dtype='uint8',
                crs=crs, transform=transform,
            ) as dst:
                dst.write(water_mask, 1)

            # Try different exit side combinations
            for exit_sides in ['n,s', 'e,w', 'n,s,e,w']:
                try:
                    riv = river(
                        name='grid_cell',
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
                            centerlines.append(cl_wgs84)
                            break  # Got a centerline, stop trying
                except Exception as e:
                    logger.debug(f"  RivGraph with {exit_sides} failed: {e}")
                    continue

        return centerlines

    def _compute_reach_drift(
        self,
        reach: Dict,
        centerlines: List[np.ndarray],
        water_mask: np.ndarray,
        transform,
    ) -> ReachDrift:
        """Compute drift between SWORD reach and nearest extracted centerline."""
        reach_id = reach['reach_id']
        sword_coords = np.array(reach['coordinates'])  # (N, 2) as [lon, lat]
        n_nodes = len(sword_coords)

        if not centerlines:
            return ReachDrift(
                reach_id=reach_id,
                mean_drift_m=0,
                max_drift_m=0,
                n_nodes=n_nodes,
                n_matched=0,
                flag=-1,
            )

        # Find nearest centerline to this reach
        reach_centroid = sword_coords.mean(axis=0)

        best_cl = None
        best_dist = float('inf')

        for cl in centerlines:
            cl_centroid = cl.mean(axis=0)
            dist = np.sqrt(np.sum((reach_centroid - cl_centroid) ** 2))
            if dist < best_dist:
                best_dist = dist
                best_cl = cl

        if best_cl is None:
            return ReachDrift(
                reach_id=reach_id,
                mean_drift_m=0,
                max_drift_m=0,
                n_nodes=n_nodes,
                n_matched=0,
                flag=-1,
            )

        # Build KDTree for nearest-neighbor matching
        cl_tree = cKDTree(best_cl)

        # Compute drift for each SWORD node
        drifts_deg = []
        for node in sword_coords:
            dist, idx = cl_tree.query(node)
            drifts_deg.append(dist)

        drifts_deg = np.array(drifts_deg)

        # Convert degrees to meters (rough: 1 deg ≈ 111km at equator)
        lat_mean = sword_coords[:, 1].mean()
        m_per_deg = 111000 * np.cos(np.radians(lat_mean))
        drifts_m = drifts_deg * m_per_deg

        mean_drift = float(np.mean(drifts_m))
        max_drift = float(np.max(drifts_m))

        # Flag based on thresholds
        if max_drift > self.major_drift:
            flag = 2
        elif max_drift > self.minor_drift:
            flag = 1
        else:
            flag = 0

        return ReachDrift(
            reach_id=reach_id,
            mean_drift_m=mean_drift,
            max_drift_m=max_drift,
            n_nodes=n_nodes,
            n_matched=n_nodes,
            flag=flag,
            centerline_wgs84=best_cl,
            node_drifts_m=drifts_m,
        )

    def process_cells_parallel(
        self,
        cells: List[GridCell],
        sword_data: Dict,
        max_workers: int = 4,
    ) -> List[CellResult]:
        """Process multiple cells in parallel."""
        results = []

        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = {}

            for cell in cells:
                reaches = self.get_reaches_in_cell(cell, sword_data)
                if reaches:
                    future = executor.submit(self.process_cell, cell, reaches)
                    futures[future] = cell

            for future in as_completed(futures):
                cell = futures[future]
                try:
                    result = future.result()
                    results.append(result)
                    logger.info(
                        f"Cell {cell.cell_id}: {result.n_successful}/{result.n_reaches} OK"
                    )
                except Exception as e:
                    logger.error(f"Cell {cell.cell_id} failed: {e}")
                    results.append(CellResult(
                        cell=cell,
                        n_reaches=0,
                        n_successful=0,
                        n_failed=0,
                        error=str(e),
                    ))

        return results
