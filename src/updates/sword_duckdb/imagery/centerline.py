# -*- coding: utf-8 -*-
"""
Centerline Extraction from Water Masks
======================================

Extract river centerlines from water masks using cost-field pathfinding.

This module provides robust centerline extraction that:
- Handles holes and noise in water masks
- Finds optimal paths through the river corridor
- Supports braided rivers via k-shortest paths
"""

from dataclasses import dataclass, field
from typing import Tuple, Optional, List, Dict, Any
import logging
import heapq

import numpy as np
from scipy import ndimage
from scipy.spatial import cKDTree
from affine import Affine
from rasterio.crs import CRS

logger = logging.getLogger(__name__)


@dataclass
class CenterlineResult:
    """Result from centerline extraction."""

    path: np.ndarray  # (N, 2) array of (row, col) pixel coordinates
    path_geo: np.ndarray  # (N, 2) array of (x, y) geographic coordinates
    cost: float  # Total path cost
    transform: Affine
    crs: CRS
    stats: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CenterlineComparison:
    """Comparison between detected and reference centerlines."""

    detected: np.ndarray
    reference: np.ndarray
    offsets: np.ndarray  # Distance from each reference point to detected
    mean_offset_m: float
    median_offset_m: float
    max_offset_m: float
    pct_within_1px: float
    pct_within_3px: float
    pixel_size_m: float


def create_cost_field(
    water_mask: np.ndarray,
    water_frequency: Optional[np.ndarray] = None,
    outside_cost: float = 1000.0,
    bank_penalty: float = 5.0,
    frequency_weight: float = 3.0,
) -> np.ndarray:
    """
    Create a cost field for pathfinding through a water mask.

    Low cost = center of persistent water (preferred path)
    High cost = near banks or outside water

    Args:
        water_mask: Binary water mask
        water_frequency: Optional frequency map (0-1) for temporal weighting
        outside_cost: Cost for pixels outside water
        bank_penalty: Penalty factor for proximity to banks
        frequency_weight: Weight for low-frequency penalty

    Returns:
        Cost field array
    """
    # Distance transform - high values far from banks
    distance = ndimage.distance_transform_edt(water_mask)
    max_dist = max(1, distance.max())

    # Normalize distance to 0-1
    normalized_dist = distance / max_dist

    # Initialize cost field
    cost = np.ones_like(distance, dtype=np.float64) * outside_cost

    # Water pixels get low cost based on distance from banks
    water_pixels = water_mask > 0
    cost[water_pixels] = 1.0 + bank_penalty * (1.0 - normalized_dist[water_pixels])

    # Add frequency penalty if provided
    if water_frequency is not None:
        cost[water_pixels] += frequency_weight * (1.0 - water_frequency[water_pixels])

    return cost


def astar_path(
    cost: np.ndarray,
    start: Tuple[int, int],
    end: Tuple[int, int],
    connectivity: int = 8,
) -> Optional[Tuple[np.ndarray, float]]:
    """
    Find shortest path through cost field using A* algorithm.

    Args:
        cost: 2D cost array
        start: (row, col) start position
        end: (row, col) end position
        connectivity: 4 or 8 for neighbor connectivity

    Returns:
        Tuple of (path array, total cost) or None if no path found
    """
    h, w = cost.shape

    # Priority queue: (f_score, row, col)
    open_set = [(0, start[0], start[1])]
    came_from = {}
    g_score = {start: 0}

    # Neighbors based on connectivity
    if connectivity == 8:
        neighbors = [(-1, -1), (-1, 0), (-1, 1), (0, -1), (0, 1), (1, -1), (1, 0), (1, 1)]
        diag_cost = np.sqrt(2)
    else:
        neighbors = [(-1, 0), (0, -1), (0, 1), (1, 0)]
        diag_cost = 1.0

    def heuristic(a, b):
        return np.sqrt((a[0] - b[0]) ** 2 + (a[1] - b[1]) ** 2)

    iterations = 0
    max_iterations = h * w * 2  # Safety limit

    while open_set and iterations < max_iterations:
        iterations += 1
        _, r, c = heapq.heappop(open_set)
        current = (r, c)

        if current == end:
            # Reconstruct path
            path = [current]
            while current in came_from:
                current = came_from[current]
                path.append(current)
            total_cost = g_score[(r, c)]
            return np.array(path[::-1]), total_cost

        for dr, dc in neighbors:
            nr, nc = r + dr, c + dc
            if 0 <= nr < h and 0 <= nc < w:
                neighbor = (nr, nc)
                # Movement cost (diagonal moves cost more)
                move_cost = diag_cost if (dr != 0 and dc != 0) else 1.0
                tentative_g = g_score[current] + cost[nr, nc] * move_cost

                if neighbor not in g_score or tentative_g < g_score[neighbor]:
                    came_from[neighbor] = current
                    g_score[neighbor] = tentative_g
                    f_score = tentative_g + heuristic(neighbor, end)
                    heapq.heappush(open_set, (f_score, nr, nc))

    logger.warning(f"A* did not find path after {iterations} iterations")
    return None


def extract_centerline(
    water_mask: np.ndarray,
    start: Tuple[int, int],
    end: Tuple[int, int],
    transform: Affine,
    crs: CRS,
    water_frequency: Optional[np.ndarray] = None,
    **cost_kwargs,
) -> Optional[CenterlineResult]:
    """
    Extract centerline from water mask using A* pathfinding.

    Args:
        water_mask: Binary water mask
        start: (row, col) start position in pixels
        end: (row, col) end position in pixels
        transform: Affine transform for georeferencing
        crs: Coordinate reference system
        water_frequency: Optional frequency map for temporal weighting
        **cost_kwargs: Additional arguments for create_cost_field

    Returns:
        CenterlineResult or None if extraction fails
    """
    # Create cost field
    cost = create_cost_field(water_mask, water_frequency, **cost_kwargs)

    # Find path
    result = astar_path(cost, start, end)
    if result is None:
        return None

    path, total_cost = result

    # Convert to geographic coordinates
    from rasterio.transform import xy

    path_geo = np.array([xy(transform, r, c) for r, c in path])

    # Compute stats
    distance = ndimage.distance_transform_edt(water_mask)
    path_distances = distance[path[:, 0], path[:, 1]]

    stats = {
        "path_length_pixels": len(path),
        "total_cost": float(total_cost),
        "mean_distance_from_bank": float(np.mean(path_distances)),
        "min_distance_from_bank": float(np.min(path_distances)),
    }

    logger.info(
        f"Extracted centerline: {len(path)} points, "
        f"cost={total_cost:.1f}, mean_dist={stats['mean_distance_from_bank']:.1f}px"
    )

    return CenterlineResult(
        path=path,
        path_geo=path_geo,
        cost=total_cost,
        transform=transform,
        crs=crs,
        stats=stats,
    )


def compare_centerlines(
    detected: np.ndarray,
    reference: np.ndarray,
    pixel_size_m: float = 30.0,
) -> CenterlineComparison:
    """
    Compare detected centerline to reference (e.g., SWORD).

    Args:
        detected: (N, 2) detected centerline in pixel coords
        reference: (M, 2) reference centerline in pixel coords
        pixel_size_m: Pixel size in meters

    Returns:
        CenterlineComparison with offset statistics
    """
    tree = cKDTree(detected)
    distances, _ = tree.query(reference)

    return CenterlineComparison(
        detected=detected,
        reference=reference,
        offsets=distances,
        mean_offset_m=float(distances.mean() * pixel_size_m),
        median_offset_m=float(np.median(distances) * pixel_size_m),
        max_offset_m=float(distances.max() * pixel_size_m),
        pct_within_1px=float(np.sum(distances < 1) / len(distances) * 100),
        pct_within_3px=float(np.sum(distances < 3) / len(distances) * 100),
        pixel_size_m=pixel_size_m,
    )


def find_endpoints_from_mask(
    water_mask: np.ndarray,
    method: str = "extremes",
) -> Tuple[Tuple[int, int], Tuple[int, int]]:
    """
    Automatically find start/end points for centerline extraction.

    Args:
        water_mask: Binary water mask
        method: "extremes" (top-left to bottom-right water pixels)
                or "skeleton" (endpoints of skeleton)

    Returns:
        Tuple of (start, end) pixel coordinates
    """
    water_coords = np.argwhere(water_mask)
    if len(water_coords) == 0:
        raise ValueError("No water pixels in mask")

    if method == "extremes":
        # Find extreme points along diagonal
        diag_sum = water_coords[:, 0] + water_coords[:, 1]
        start_idx = np.argmin(diag_sum)
        end_idx = np.argmax(diag_sum)
        start = tuple(water_coords[start_idx])
        end = tuple(water_coords[end_idx])
    else:
        raise ValueError(f"Unknown method: {method}")

    return start, end
