"""
SWORD Centerline Updater

Uses RivGraph + DT snapping to detect where SWORD needs updating
and generate proposed new centerline coordinates.
"""

import numpy as np
from scipy.ndimage import distance_transform_edt, binary_dilation, binary_closing
from scipy.signal import savgol_filter
from scipy.spatial import cKDTree
from skimage.morphology import disk, remove_small_objects
from skimage.draw import line
import tempfile
import os

from pyproj import Transformer
import rasterio

from .fusion_centerline import FusionCenterlineExtractor


class SWORDUpdateResult:
    """Result of SWORD update analysis."""

    def __init__(
        self,
        reach_id: int,
        sword_wgs84: np.ndarray,
        sword_ids: np.ndarray,
        observed_wgs84: np.ndarray,
        proposed_wgs84: np.ndarray,
        drift_m: np.ndarray,
        flags: np.ndarray,
        metadata: dict,
    ):
        self.reach_id = reach_id
        self.sword_wgs84 = sword_wgs84  # Original SWORD coordinates (lon, lat)
        self.sword_ids = sword_ids  # SWORD cl_id values
        self.observed_wgs84 = observed_wgs84  # RivGraph centerline (lon, lat)
        self.proposed_wgs84 = proposed_wgs84  # Proposed updates for each SWORD node
        self.drift_m = drift_m  # Drift in meters for each node
        self.flags = flags  # 0=ok, 1=minor, 2=major
        self.metadata = metadata

    @property
    def mean_drift(self) -> float:
        return float(np.mean(self.drift_m))

    @property
    def max_drift(self) -> float:
        return float(np.max(self.drift_m))

    @property
    def nodes_needing_update(self) -> int:
        return int(np.sum(self.flags > 0))

    @property
    def pct_ok(self) -> float:
        return 100 * np.sum(self.flags == 0) / len(self.flags)

    @property
    def pct_minor(self) -> float:
        return 100 * np.sum(self.flags == 1) / len(self.flags)

    @property
    def pct_major(self) -> float:
        return 100 * np.sum(self.flags == 2) / len(self.flags)

    def get_update_segments(self) -> list:
        """Get contiguous segments with major drift."""
        major_mask = self.flags == 2
        segments = []
        in_segment = False
        seg_start = 0

        for i, is_major in enumerate(major_mask):
            if is_major and not in_segment:
                seg_start = i
                in_segment = True
            elif not is_major and in_segment:
                segments.append({
                    'start_idx': seg_start,
                    'end_idx': i - 1,
                    'start_cl_id': int(self.sword_ids[seg_start]),
                    'end_cl_id': int(self.sword_ids[i - 1]),
                    'n_nodes': i - seg_start,
                    'avg_drift_m': float(np.mean(self.drift_m[seg_start:i])),
                })
                in_segment = False

        if in_segment:
            segments.append({
                'start_idx': seg_start,
                'end_idx': len(major_mask) - 1,
                'start_cl_id': int(self.sword_ids[seg_start]),
                'end_cl_id': int(self.sword_ids[-1]),
                'n_nodes': len(major_mask) - seg_start,
                'avg_drift_m': float(np.mean(self.drift_m[seg_start:])),
            })

        return segments

    def to_dict(self) -> dict:
        """Export as dictionary."""
        return {
            'reach_id': self.reach_id,
            'n_nodes': len(self.sword_ids),
            'mean_drift_m': self.mean_drift,
            'max_drift_m': self.max_drift,
            'pct_ok': self.pct_ok,
            'pct_minor': self.pct_minor,
            'pct_major': self.pct_major,
            'nodes_needing_update': self.nodes_needing_update,
            'update_segments': self.get_update_segments(),
            'metadata': self.metadata,
        }


class SWORDUpdater:
    """
    Detects SWORD centerline drift and generates update proposals.

    Uses RivGraph for centerline extraction + DT snapping for centering.
    """

    def __init__(
        self,
        minor_drift_threshold_m: float = 30.0,
        major_drift_threshold_m: float = 100.0,
        corridor_buffer_factor: float = 5.0,
        min_corridor_buffer_m: float = 400.0,
        dt_snap_radius_px: int = 8,
        min_slope: float = 1e-5,
        max_centerline_escape_pct: float = 0.1,
        braided_corridor_factor: float = 3.0,
    ):
        self.minor_threshold = minor_drift_threshold_m
        self.major_threshold = major_drift_threshold_m
        self.corridor_factor = corridor_buffer_factor
        self.min_corridor_m = min_corridor_buffer_m
        self.dt_snap_radius = dt_snap_radius_px
        self.min_slope = min_slope  # Skip ultra-flat reaches (RivGraph can't determine flow)
        self.max_escape_pct = max_centerline_escape_pct  # Max % of centerline outside corridor
        self.braided_factor = braided_corridor_factor  # Tighter corridor for braided
        self.extractor = FusionCenterlineExtractor()

    def analyze_reach(
        self,
        sword_wgs84: np.ndarray,
        sword_ids: np.ndarray,
        reach_width_m: float,
        reach_id: int,
        start_date: str = "2023-01-01",
        end_date: str = "2024-12-31",
        slope: float = None,
        n_chan: int = 1,
    ) -> SWORDUpdateResult:
        """
        Analyze a SWORD reach and generate update proposals.

        Args:
            sword_wgs84: Array of (lon, lat) coordinates for SWORD nodes
            sword_ids: Array of cl_id values for each node
            reach_width_m: SWORD-reported reach width in meters
            reach_id: SWORD reach ID
            start_date: Start of observation period
            end_date: End of observation period
            slope: Reach slope (skip if < min_slope)
            n_chan: Number of channels (tighter corridor if braided)

        Returns:
            SWORDUpdateResult with drift analysis and proposed updates

        Raises:
            ValueError: If slope is below minimum threshold
        """
        # Check minimum slope
        if slope is not None and slope < self.min_slope:
            raise ValueError(
                f"Slope {slope:.2e} below minimum {self.min_slope:.2e}. "
                "Ultra-flat reaches cannot be reliably processed by RivGraph."
            )
        # Create bbox with buffer
        buffer = 0.02
        bbox = (
            sword_wgs84[:, 0].min() - buffer,
            sword_wgs84[:, 1].min() - buffer,
            sword_wgs84[:, 0].max() + buffer,
            sword_wgs84[:, 1].max() + buffer,
        )

        # Get water mask
        water_result = self.extractor.get_fused_water_mask(bbox, start_date, end_date)
        water_mask_raw = water_result.mask
        h, w = water_mask_raw.shape

        # Transform SWORD to pixel coordinates
        xform = Transformer.from_crs('EPSG:4326', water_result.crs, always_xy=True)
        sword_utm = np.array([xform.transform(x, y) for x, y in sword_wgs84])
        inv_t = ~water_result.transform
        sword_px = np.array([inv_t * (x, y) for x, y in sword_utm])[:, ::-1]
        sword_px[:, 0] = np.clip(sword_px[:, 0], 0, h - 1)
        sword_px[:, 1] = np.clip(sword_px[:, 1], 0, w - 1)

        # Constrain to SWORD corridor (tighter for braided to avoid wrong channels)
        if n_chan > 2:
            # Braided: use tighter corridor to stay on main channel
            corridor_buffer_m = max(reach_width_m * self.braided_factor, self.min_corridor_m)
        else:
            corridor_buffer_m = max(reach_width_m * self.corridor_factor, self.min_corridor_m)
        corridor_buffer_px = int(corridor_buffer_m / 10)  # Assume 10m pixels

        sword_line = np.zeros((h, w), dtype=bool)
        for r, c in sword_px:
            ri, ci = int(r), int(c)
            if 0 <= ri < h and 0 <= ci < w:
                sword_line[ri, ci] = True

        corridor = binary_dilation(sword_line, disk(corridor_buffer_px))
        water_in_corridor = water_mask_raw & corridor

        # Bridge gaps along SWORD path
        water_filled = water_in_corridor.copy()
        bridge_width = 2

        for i in range(len(sword_px) - 1):
            r1, c1 = int(sword_px[i, 0]), int(sword_px[i, 1])
            r2, c2 = int(sword_px[i + 1, 0]), int(sword_px[i + 1, 1])

            rr, cc = line(r1, c1, r2, c2)
            rr = np.clip(rr, 0, h - 1)
            cc = np.clip(cc, 0, w - 1)

            gap_pixels = ~water_in_corridor[rr, cc]
            if np.any(gap_pixels):
                for dr in range(-bridge_width, bridge_width + 1):
                    for dc in range(-bridge_width, bridge_width + 1):
                        if dr * dr + dc * dc <= bridge_width * bridge_width:
                            rr_off = np.clip(rr + dr, 0, h - 1)
                            cc_off = np.clip(cc + dc, 0, w - 1)
                            water_filled[rr_off, cc_off] = True

        # Minimal closing and cleanup
        water_closed = binary_closing(water_filled, disk(2))
        water_mask = remove_small_objects(water_closed, min_size=100)

        # Extract centerline with RivGraph
        observed_cl_px = self._extract_rivgraph_centerline(
            water_mask, sword_px, water_result.crs, water_result.transform, h, w
        )

        if observed_cl_px is None:
            raise RuntimeError("RivGraph centerline extraction failed")

        # Validate centerline stays within corridor
        in_corridor = 0
        for r, c in observed_cl_px:
            ri, ci = int(r), int(c)
            if 0 <= ri < h and 0 <= ci < w and corridor[ri, ci]:
                in_corridor += 1
        escape_pct = 1.0 - (in_corridor / len(observed_cl_px))
        if escape_pct > self.max_escape_pct:
            raise RuntimeError(
                f"Centerline escaped corridor: {escape_pct:.1%} outside "
                f"(max allowed: {self.max_escape_pct:.1%}). "
                "RivGraph likely found wrong water body."
            )

        # Compute DT and snap centerline to ridge
        dt = distance_transform_edt(water_mask)
        observed_cl_px = self._snap_to_dt_ridge(observed_cl_px, dt, h, w)

        # Smooth the snapped centerline
        if len(observed_cl_px) > 20:
            observed_cl_px[:, 0] = savgol_filter(
                observed_cl_px[:, 0], min(15, len(observed_cl_px) // 2 * 2 + 1), 3
            )
            observed_cl_px[:, 1] = savgol_filter(
                observed_cl_px[:, 1], min(15, len(observed_cl_px) // 2 * 2 + 1), 3
            )

        # Convert observed centerline to WGS84
        observed_utm = np.array([
            water_result.transform * (c, r) for r, c in observed_cl_px
        ])
        inv_xform = Transformer.from_crs(water_result.crs, 'EPSG:4326', always_xy=True)
        observed_wgs84 = np.array([
            inv_xform.transform(x, y) for x, y in observed_utm
        ])

        # Compute drift from SWORD to observed
        observed_tree = cKDTree(observed_cl_px)
        drift_px = []
        drift_vectors = []
        nearest_observed_px = []

        for i, (r, c) in enumerate(sword_px):
            dist, idx = observed_tree.query([r, c])
            nearest_pt = observed_cl_px[idx]
            drift_px.append(dist)
            drift_vectors.append(nearest_pt - np.array([r, c]))
            nearest_observed_px.append(nearest_pt)

        drift_px = np.array(drift_px)
        drift_vectors = np.array(drift_vectors)
        nearest_observed_px = np.array(nearest_observed_px)

        # Convert to meters (assume 10m pixels)
        drift_m = drift_px * 10

        # Smooth drift vectors for stable corrections
        drift_smooth = np.zeros_like(drift_vectors)
        window = min(21, len(drift_vectors) // 2 * 2 + 1)
        if window >= 5:
            drift_smooth[:, 0] = savgol_filter(drift_vectors[:, 0], window, 3)
            drift_smooth[:, 1] = savgol_filter(drift_vectors[:, 1], window, 3)
        else:
            drift_smooth = drift_vectors

        # Compute proposed positions
        proposed_px = sword_px + drift_smooth
        proposed_utm = np.array([
            water_result.transform * (c, r) for r, c in proposed_px
        ])
        proposed_wgs84 = np.array([
            inv_xform.transform(x, y) for x, y in proposed_utm
        ])

        # Flag nodes
        flags = np.zeros(len(drift_m), dtype=int)
        flags[(drift_m > self.minor_threshold) & (drift_m <= self.major_threshold)] = 1
        flags[drift_m > self.major_threshold] = 2

        metadata = {
            'bbox': bbox,
            'start_date': start_date,
            'end_date': end_date,
            'reach_width_m': reach_width_m,
            'slope': slope,
            'n_chan': n_chan,
            'corridor_buffer_m': corridor_buffer_m,
            'water_px_raw': int(np.sum(water_mask_raw)),
            'water_px_corridor': int(np.sum(water_in_corridor)),
            'water_px_final': int(np.sum(water_mask)),
            'observed_cl_pts': len(observed_cl_px),
            'centerline_escape_pct': escape_pct,
            'mean_dt': float(np.mean([
                dt[int(r), int(c)] for r, c in observed_cl_px
                if 0 <= int(r) < h and 0 <= int(c) < w
            ])),
            'max_dt': float(dt.max()),
        }

        return SWORDUpdateResult(
            reach_id=reach_id,
            sword_wgs84=sword_wgs84,
            sword_ids=sword_ids,
            observed_wgs84=observed_wgs84,
            proposed_wgs84=proposed_wgs84,
            drift_m=drift_m,
            flags=flags,
            metadata=metadata,
        )

    def _extract_rivgraph_centerline(
        self, water_mask, sword_px, crs, transform, h, w
    ) -> np.ndarray:
        """Extract centerline using RivGraph."""
        try:
            from rivgraph.classes import river
        except ImportError:
            raise ImportError("RivGraph not installed. Install from: https://github.com/jonschwenk/RivGraph")

        with tempfile.TemporaryDirectory() as tmpdir:
            mask_path = os.path.join(tmpdir, 'mask.tif')
            with rasterio.open(
                mask_path, 'w', driver='GTiff',
                height=h, width=w, count=1, dtype='uint8',
                crs=crs, transform=transform,
            ) as dst:
                dst.write(water_mask.astype('uint8'), 1)

            # Determine exit sides from SWORD endpoints
            start_r, start_c = sword_px[0]
            end_r, end_c = sword_px[-1]
            exit_sides = set()

            for r, c in [(start_r, start_c), (end_r, end_c)]:
                if r < h * 0.15:
                    exit_sides.add('n')
                elif r > h * 0.85:
                    exit_sides.add('s')
                if c < w * 0.15:
                    exit_sides.add('w')
                elif c > w * 0.85:
                    exit_sides.add('e')

            if not exit_sides:
                exit_sides = {'n', 's'}

            riv = river(
                name='sword_update',
                path_to_mask=mask_path,
                results_folder=tmpdir,
                exit_sides=','.join(exit_sides),
            )
            riv.compute_network()

            try:
                riv.compute_centerline()
            except:
                pass

            # Extract centerline
            if hasattr(riv, 'centerline') and riv.centerline is not None:
                cl = riv.centerline
                if isinstance(cl, tuple) and len(cl) >= 2:
                    x_coords, y_coords = cl[0], cl[1]
                    cl_geo = np.column_stack([x_coords, y_coords])
                    inv_t = ~transform
                    return np.array([inv_t * (x, y) for x, y in cl_geo])[:, ::-1]

        return None

    def _snap_to_dt_ridge(self, centerline, dt, h, w) -> np.ndarray:
        """Snap each centerline point to local DT maximum."""
        snapped = centerline.copy()

        for i in range(len(centerline)):
            r, c = centerline[i]
            ri, ci = int(r), int(c)

            best_r, best_c = ri, ci
            best_dt = 0

            for dr in range(-self.dt_snap_radius, self.dt_snap_radius + 1):
                for dc in range(-self.dt_snap_radius, self.dt_snap_radius + 1):
                    nr, nc = ri + dr, ci + dc
                    if 0 <= nr < h and 0 <= nc < w:
                        if dt[nr, nc] > best_dt:
                            best_dt = dt[nr, nc]
                            best_r, best_c = nr, nc

            snapped[i] = [best_r, best_c]

        return snapped
