# -*- coding: utf-8 -*-
"""
Centerline Solver
=================

Extract river centerlines as optimal paths through a cost surface.

Key insight: Don't rely on morphology to connect gaps - explicitly optimize
for continuity and "centeredness" using A* pathfinding on a cost raster.

Cost function: c(x) = 1 / (s(x) + epsilon)
Where s(x) = p(x) * d_DT(x)  -- high where water is likely AND interior

This finds paths that:
1. Stay in high-probability water
2. Stay centered (high distance-to-bank)
3. Are continuous through gaps
4. Have smooth direction (turn penalty)
"""

import numpy as np
from typing import Tuple, Optional, List
from dataclasses import dataclass
from heapq import heappush, heappop
import logging

from scipy.ndimage import distance_transform_edt, label
from affine import Affine

logger = logging.getLogger(__name__)


@dataclass
class CenterlinePath:
    """Result from centerline extraction."""
    path: np.ndarray  # Nx2 array of (row, col) coordinates
    cost: float
    length_px: int
    mean_support: float  # Mean s(x) along path
    stats: dict


def build_channel_likelihood(
    water_frequency: np.ndarray,
    corridor_mask: np.ndarray,
    t_high: float = 0.3,
    t_low: float = 0.1,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    Build soft channel likelihood field s(x) = p(x) * d_DT(x).

    Uses hysteresis thresholding to get a clean mask for distance transform.

    Args:
        water_frequency: Water frequency [0,1]
        corridor_mask: Binary corridor constraint
        t_high: High threshold for "sure" water
        t_low: Low threshold for "possible" water

    Returns:
        (channel_likelihood, distance_transform, hysteresis_mask)
    """
    # Apply corridor
    freq = water_frequency * corridor_mask

    # Hysteresis thresholding
    sure_water = freq >= t_high
    possible_water = freq >= t_low

    # Keep possible only if connected to sure
    labeled, n = label(possible_water)
    sure_labels = set(np.unique(labeled[sure_water])) - {0}
    hysteresis_mask = np.isin(labeled, list(sure_labels))

    sure_count = np.sum(sure_water)
    hyst_count = np.sum(hysteresis_mask)
    logger.info(f"Hysteresis: {sure_count} sure -> {hyst_count} connected px")

    # Distance transform on hysteresis mask
    # This gives distance to nearest non-water (i.e., distance to bank)
    d_dt = distance_transform_edt(hysteresis_mask)

    # Channel likelihood: high where water is likely AND interior
    # s(x) = p(x) * d_DT(x)
    s = freq * d_dt

    # Normalize to [0, 1] for stability
    s_max = np.max(s)
    if s_max > 0:
        s = s / s_max

    return s, d_dt, hysteresis_mask


def build_cost_surface(
    channel_likelihood: np.ndarray,
    corridor_mask: np.ndarray,
    epsilon: float = 0.01,
    out_of_corridor_cost: float = 50.0,
    corridor_base_cost: float = 2.0,
    corridor_boost: float = 10.0,
) -> np.ndarray:
    """
    Build cost surface for pathfinding.

    Inside corridor: cost = corridor_base_cost / (1 + s(x) * corridor_boost)
    - High s(x) → low cost (channel center)
    - Low s(x) → corridor_base_cost (still navigable)

    Outside corridor: high but not impassable cost.

    Args:
        channel_likelihood: s(x) field from build_channel_likelihood
        corridor_mask: Binary corridor constraint
        epsilon: Small value to avoid division by zero
        out_of_corridor_cost: Cost for pixels outside corridor
        corridor_base_cost: Base cost inside corridor (navigable even without water)
        corridor_boost: Multiplier for s(x) to reduce cost where water is detected

    Returns:
        Cost surface array
    """
    # Default: out of corridor cost
    cost = np.full_like(channel_likelihood, out_of_corridor_cost)

    # Inside corridor: base cost reduced by channel likelihood
    # cost = base / (1 + s * boost) → ranges from base (s=0) to base/(1+boost) (s=1)
    cost[corridor_mask] = corridor_base_cost / (1.0 + channel_likelihood[corridor_mask] * corridor_boost)

    return cost


def find_endpoints_from_mask(
    mask: np.ndarray,
    direction: str = "NS",
    min_separation: int = 50,
) -> Tuple[Tuple[int, int], Tuple[int, int]]:
    """
    Find start/end points by intersecting mask with image boundaries.

    Prioritizes opposite-edge pairs (top/bottom or left/right) for rivers
    that cross the image. Falls back to furthest-apart mask pixels.

    Args:
        mask: Binary mask (hysteresis or corridor)
        direction: Expected flow direction - "NS" (north-south),
                   "EW" (east-west), "NE", "NW", etc.
        min_separation: Minimum distance between endpoints in pixels

    Returns:
        (start_point, end_point) as (row, col) tuples
    """
    h, w = mask.shape

    # Find mask pixels on each edge
    top_cols = np.where(mask[0, :])[0]
    bottom_cols = np.where(mask[-1, :])[0]
    left_rows = np.where(mask[:, 0])[0]
    right_rows = np.where(mask[:, -1])[0]

    def dist(p1, p2):
        return np.sqrt((p1[0] - p2[0])**2 + (p1[1] - p2[1])**2)

    # Try to find opposite-edge pairs first (most likely for a river)
    candidates = []

    # Top-bottom pair (NS flow)
    if len(top_cols) > 0 and len(bottom_cols) > 0:
        top_pt = (0, int(np.median(top_cols)))
        bot_pt = (h-1, int(np.median(bottom_cols)))
        d = dist(top_pt, bot_pt)
        candidates.append((d, "NS", top_pt, bot_pt))

    # Left-right pair (EW flow)
    if len(left_rows) > 0 and len(right_rows) > 0:
        left_pt = (int(np.median(left_rows)), 0)
        right_pt = (int(np.median(right_rows)), w-1)
        d = dist(left_pt, right_pt)
        candidates.append((d, "EW", left_pt, right_pt))

    # Diagonal pairs
    if len(top_cols) > 0 and len(right_rows) > 0:
        top_pt = (0, int(np.median(top_cols)))
        right_pt = (int(np.median(right_rows)), w-1)
        d = dist(top_pt, right_pt)
        candidates.append((d, "NE", top_pt, right_pt))

    if len(top_cols) > 0 and len(left_rows) > 0:
        top_pt = (0, int(np.median(top_cols)))
        left_pt = (int(np.median(left_rows)), 0)
        d = dist(top_pt, left_pt)
        candidates.append((d, "NW", top_pt, left_pt))

    if len(bottom_cols) > 0 and len(right_rows) > 0:
        bot_pt = (h-1, int(np.median(bottom_cols)))
        right_pt = (int(np.median(right_rows)), w-1)
        d = dist(bot_pt, right_pt)
        candidates.append((d, "SE", bot_pt, right_pt))

    if len(bottom_cols) > 0 and len(left_rows) > 0:
        bot_pt = (h-1, int(np.median(bottom_cols)))
        left_pt = (int(np.median(left_rows)), 0)
        d = dist(bot_pt, left_pt)
        candidates.append((d, "SW", bot_pt, left_pt))

    # Filter by minimum separation and sort by distance (prefer longer paths)
    valid_candidates = [(d, dir_, p1, p2) for d, dir_, p1, p2 in candidates
                        if d >= min_separation]

    if valid_candidates:
        # Prefer direction hint if provided, otherwise take longest
        if direction != "NS":
            matching = [c for c in valid_candidates if direction in c[1] or c[1] in direction]
            if matching:
                valid_candidates = matching

        # Sort by distance (longest first)
        valid_candidates.sort(key=lambda x: -x[0])
        _, dir_found, start, end = valid_candidates[0]
        logger.info(f"Endpoints from edges ({dir_found}): {start} -> {end}, dist={dist(start, end):.0f}px")
        return start, end

    # Fallback: find two furthest-apart points in the mask
    logger.warning("No valid opposite-edge endpoints, using furthest-apart mask points")
    rows, cols = np.where(mask)
    if len(rows) == 0:
        raise ValueError("Empty mask - cannot find endpoints")

    # Find point furthest from centroid
    centroid = (np.mean(rows), np.mean(cols))
    dists = (rows - centroid[0])**2 + (cols - centroid[1])**2
    far_idx = np.argmax(dists)
    far_point = (rows[far_idx], cols[far_idx])

    # Find point furthest from far_point
    dists2 = (rows - far_point[0])**2 + (cols - far_point[1])**2
    far_idx2 = np.argmax(dists2)
    far_point2 = (rows[far_idx2], cols[far_idx2])

    # Ensure minimum separation
    if dist(far_point, far_point2) < min_separation:
        logger.warning(f"Endpoints too close ({dist(far_point, far_point2):.0f}px), may produce poor centerline")

    logger.info(f"Endpoints (fallback): {far_point} -> {far_point2}")
    return far_point, far_point2


def astar_with_turn_penalty(
    cost: np.ndarray,
    start: Tuple[int, int],
    end: Tuple[int, int],
    turn_penalty: float = 0.5,
    max_iterations: int = 1000000,
) -> Optional[List[Tuple[int, int]]]:
    """
    A* pathfinding with turn penalty for smooth centerlines.

    Args:
        cost: Cost surface
        start: (row, col) start point
        end: (row, col) end point
        turn_penalty: Penalty for changing direction
        max_iterations: Maximum iterations before giving up

    Returns:
        List of (row, col) points along path, or None if no path found
    """
    h, w = cost.shape

    # 8-connected neighbors with directions
    # Direction encoded as index 0-7
    neighbors = [
        (-1, 0, 0),   # N
        (-1, 1, 1),   # NE
        (0, 1, 2),    # E
        (1, 1, 3),    # SE
        (1, 0, 4),    # S
        (1, -1, 5),   # SW
        (0, -1, 6),   # W
        (-1, -1, 7),  # NW
    ]

    # Diagonal cost multiplier
    diag_mult = 1.414

    def heuristic(pos):
        return np.sqrt((pos[0] - end[0])**2 + (pos[1] - end[1])**2)

    def direction_change_cost(dir1, dir2):
        if dir1 is None:
            return 0
        # Circular difference
        diff = abs(dir1 - dir2)
        diff = min(diff, 8 - diff)
        return turn_penalty * diff

    # Priority queue: (f_score, g_score, row, col, direction, path)
    # Using g_score as tiebreaker
    start_h = heuristic(start)
    heap = [(start_h, 0, start[0], start[1], None, [start])]

    # Best g_score to reach each (row, col, direction) state
    visited = {}

    iterations = 0
    while heap and iterations < max_iterations:
        iterations += 1
        f, g, r, c, prev_dir, path = heappop(heap)

        # Check if reached goal
        if (r, c) == end:
            logger.info(f"A* found path: {len(path)} px, cost={g:.2f}, iterations={iterations}")
            return path

        # Skip if we've seen this state with better cost
        state = (r, c, prev_dir)
        if state in visited and visited[state] <= g:
            continue
        visited[state] = g

        # Explore neighbors
        for dr, dc, direction in neighbors:
            nr, nc = r + dr, c + dc

            if 0 <= nr < h and 0 <= nc < w:
                # Step cost
                step_cost = cost[nr, nc]
                if dr != 0 and dc != 0:  # Diagonal
                    step_cost *= diag_mult

                # Turn penalty
                step_cost += direction_change_cost(prev_dir, direction)

                new_g = g + step_cost
                new_f = new_g + heuristic((nr, nc))

                new_state = (nr, nc, direction)
                if new_state not in visited or visited[new_state] > new_g:
                    heappush(heap, (new_f, new_g, nr, nc, direction, path + [(nr, nc)]))

    logger.warning(f"A* failed to find path after {iterations} iterations")
    return None


def smooth_path(
    path: List[Tuple[int, int]],
    iterations: int = 2,
) -> np.ndarray:
    """
    Smooth path using Chaikin's algorithm.

    Args:
        path: List of (row, col) points
        iterations: Number of smoothing iterations

    Returns:
        Smoothed path as Nx2 array
    """
    points = np.array(path, dtype=np.float64)

    for _ in range(iterations):
        if len(points) < 3:
            break

        # Chaikin's corner cutting
        new_points = [points[0]]  # Keep start
        for i in range(len(points) - 1):
            p0, p1 = points[i], points[i + 1]
            q = 0.75 * p0 + 0.25 * p1
            r = 0.25 * p0 + 0.75 * p1
            new_points.extend([q, r])
        new_points.append(points[-1])  # Keep end
        points = np.array(new_points)

    return points


def snap_to_ridge(
    path: np.ndarray,
    channel_likelihood: np.ndarray,
    search_radius: int = 2,
) -> np.ndarray:
    """
    Snap path points to local maxima of channel likelihood.

    Args:
        path: Nx2 array of (row, col) points
        channel_likelihood: s(x) field
        search_radius: Radius to search for local maximum

    Returns:
        Snapped path as Nx2 array
    """
    h, w = channel_likelihood.shape
    snapped = path.copy()

    for i, (r, c) in enumerate(path):
        r, c = int(r), int(c)

        # Search in neighborhood
        best_r, best_c = r, c
        best_val = channel_likelihood[r, c] if 0 <= r < h and 0 <= c < w else 0

        for dr in range(-search_radius, search_radius + 1):
            for dc in range(-search_radius, search_radius + 1):
                nr, nc = r + dr, c + dc
                if 0 <= nr < h and 0 <= nc < w:
                    val = channel_likelihood[nr, nc]
                    if val > best_val:
                        best_val = val
                        best_r, best_c = nr, nc

        snapped[i] = [best_r, best_c]

    return snapped


def extract_centerline(
    water_frequency: np.ndarray,
    corridor_mask: np.ndarray,
    t_high: float = 0.3,
    t_low: float = 0.1,
    turn_penalty: float = 0.5,
    smooth_iterations: int = 2,
    snap_radius: int = 2,
    endpoints: Optional[Tuple[Tuple[int, int], Tuple[int, int]]] = None,
    waypoints: Optional[List[Tuple[int, int]]] = None,
    waypoint_spacing: int = 20,
) -> CenterlinePath:
    """
    Extract river centerline as optimal path through cost surface.

    Pipeline:
    1. Build channel likelihood s(x) = p(x) * d_DT(x)
    2. Build cost surface c(x) = 1 / (s(x) + epsilon)
    3. Find endpoints (from mask edges or provided)
    4. Run A* with turn penalty (optionally through waypoints)
    5. Smooth path (Chaikin)
    6. Snap to ridge maxima

    Args:
        water_frequency: Water frequency [0,1]
        corridor_mask: Binary corridor constraint
        t_high: High threshold for hysteresis
        t_low: Low threshold for hysteresis
        turn_penalty: A* turn penalty
        smooth_iterations: Chaikin smoothing iterations
        snap_radius: Radius for snapping to ridge
        endpoints: Optional (start, end) points, or None to auto-detect
        waypoints: Optional list of (row, col) intermediate waypoints to force
                   the path through (e.g., from SWORD centerline). Path will
                   be computed as segments between consecutive waypoints.
        waypoint_spacing: Subsample waypoints to every N-th point (default 20)

    Returns:
        CenterlinePath with extracted centerline
    """
    # Step 1: Build channel likelihood
    s, d_dt, hyst_mask = build_channel_likelihood(
        water_frequency, corridor_mask, t_high, t_low
    )

    # Step 2: Build cost surface
    cost = build_cost_surface(s, corridor_mask)

    # Step 3: Find endpoints
    if endpoints is None:
        start, end = find_endpoints_from_mask(hyst_mask | corridor_mask)
    else:
        start, end = endpoints

    # Step 4: A* pathfinding (with optional waypoints)
    if waypoints is not None and len(waypoints) > 2:
        # Subsample waypoints for efficiency
        if waypoint_spacing > 1:
            sampled_waypoints = [waypoints[0]]
            for i in range(waypoint_spacing, len(waypoints) - 1, waypoint_spacing):
                sampled_waypoints.append(waypoints[i])
            sampled_waypoints.append(waypoints[-1])
        else:
            sampled_waypoints = list(waypoints)

        # Clamp waypoints to image bounds
        h, w = cost.shape
        sampled_waypoints = [
            (max(0, min(h-1, int(r))), max(0, min(w-1, int(c))))
            for r, c in sampled_waypoints
        ]

        logger.info(f"Using {len(sampled_waypoints)} waypoints for pathfinding")

        # Run A* between consecutive waypoints
        path = []
        total_iterations = 0
        for i in range(len(sampled_waypoints) - 1):
            wp_start = sampled_waypoints[i]
            wp_end = sampled_waypoints[i + 1]

            segment = astar_with_turn_penalty(
                cost, wp_start, wp_end, turn_penalty,
                max_iterations=200000  # Lower limit per segment
            )

            if segment is None:
                logger.warning(f"Failed to find path for segment {i}: {wp_start} -> {wp_end}")
                # Fallback: straight line
                segment = [wp_start, wp_end]

            # Add segment (skip first point except for first segment to avoid duplicates)
            if i == 0:
                path.extend(segment)
            else:
                path.extend(segment[1:])

        if len(path) < 2:
            path = [start, end]
    else:
        # Single A* from start to end
        path = astar_with_turn_penalty(cost, start, end, turn_penalty)

        if path is None:
            logger.error("Failed to find centerline path")
            return CenterlinePath(
                path=np.array([[start[0], start[1]], [end[0], end[1]]]),
                cost=float('inf'),
                length_px=2,
                mean_support=0,
                stats={"error": "no_path_found"},
            )

    path_array = np.array(path)

    # Compute path cost
    total_cost = sum(cost[r, c] for r, c in path)

    # Step 5: Smooth
    if smooth_iterations > 0:
        path_smooth = smooth_path(path, smooth_iterations)
    else:
        path_smooth = path_array.astype(np.float64)

    # Step 6: Snap to ridge
    if snap_radius > 0:
        path_final = snap_to_ridge(path_smooth, s, snap_radius)
    else:
        path_final = path_smooth

    # Compute stats
    mean_support = np.mean([s[int(r), int(c)] for r, c in path_final
                           if 0 <= int(r) < s.shape[0] and 0 <= int(c) < s.shape[1]])

    # Compute length in pixels
    diffs = np.diff(path_final, axis=0)
    length_px = np.sum(np.sqrt(diffs[:, 0]**2 + diffs[:, 1]**2))

    stats = {
        "t_high": t_high,
        "t_low": t_low,
        "turn_penalty": turn_penalty,
        "total_cost": total_cost,
        "path_points": len(path_final),
        "hysteresis_pixels": int(np.sum(hyst_mask)),
        "mean_channel_likelihood": float(mean_support),
    }

    logger.info(f"Centerline extracted: {len(path_final)} points, length={length_px:.0f}px, support={mean_support:.3f}")

    return CenterlinePath(
        path=path_final,
        cost=total_cost,
        length_px=int(length_px),
        mean_support=mean_support,
        stats=stats,
    )


def adjust_centerline_nodes(
    centerline: np.ndarray,
    channel_likelihood: np.ndarray,
    search_radius: int = 5,
    max_shift: int = 3,
    smooth_shifts: bool = True,
) -> Tuple[np.ndarray, np.ndarray]:
    """
    Adjust existing centerline nodes toward channel likelihood ridge.

    Instead of redrawing the centerline, this nudges each node toward
    local maxima of s(x) while preserving topology.

    Args:
        centerline: Nx2 array of (row, col) points
        channel_likelihood: s(x) field
        search_radius: Radius to search for better position
        max_shift: Maximum pixels to move per node
        smooth_shifts: Apply smoothing to shifts to avoid zigzag

    Returns:
        (adjusted_centerline, shifts) - adjusted points and shift vectors
    """
    h, w = channel_likelihood.shape
    n_points = len(centerline)
    adjusted = centerline.copy().astype(float)
    shifts = np.zeros((n_points, 2))

    for i, (r, c) in enumerate(centerline):
        r, c = int(r), int(c)

        # Search neighborhood for maximum s(x)
        best_r, best_c = r, c
        best_val = channel_likelihood[r, c] if 0 <= r < h and 0 <= c < w else 0

        for dr in range(-search_radius, search_radius + 1):
            for dc in range(-search_radius, search_radius + 1):
                nr, nc = r + dr, c + dc
                if 0 <= nr < h and 0 <= nc < w:
                    val = channel_likelihood[nr, nc]
                    # Only consider if better and within max_shift
                    dist = np.sqrt(dr**2 + dc**2)
                    if val > best_val and dist <= max_shift:
                        best_val = val
                        best_r, best_c = nr, nc

        shifts[i] = [best_r - r, best_c - c]
        adjusted[i] = [best_r, best_c]

    # Smooth shifts to avoid zigzag patterns
    if smooth_shifts and n_points > 5:
        from scipy.ndimage import uniform_filter1d
        shifts[:, 0] = uniform_filter1d(shifts[:, 0], size=5, mode='nearest')
        shifts[:, 1] = uniform_filter1d(shifts[:, 1], size=5, mode='nearest')

        # Apply smoothed shifts
        adjusted = centerline.astype(float) + shifts

        # Clamp to bounds
        adjusted[:, 0] = np.clip(adjusted[:, 0], 0, h - 1)
        adjusted[:, 1] = np.clip(adjusted[:, 1], 0, w - 1)

    # Stats
    shift_magnitudes = np.sqrt(shifts[:, 0]**2 + shifts[:, 1]**2)
    n_moved = np.sum(shift_magnitudes > 0.5)

    logger.info(f"Adjusted {n_moved}/{n_points} nodes, mean shift={np.mean(shift_magnitudes):.1f}px, max={np.max(shift_magnitudes):.1f}px")

    return adjusted, shifts


def active_contour_adjust(
    centerline: np.ndarray,
    channel_likelihood: np.ndarray,
    iterations: int = 50,
    alpha: float = 0.5,      # Smoothness weight (tension)
    beta: float = 0.3,       # Attraction to ridge weight
    gamma: float = 0.1,      # Step size
    pin_endpoints: bool = True,
) -> Tuple[np.ndarray, List[float]]:
    """
    Deform centerline toward channel likelihood ridge using active contour.

    The line is iteratively adjusted by balancing:
    - Smoothness: Points pulled toward neighbors (prevents zigzag)
    - Attraction: Points pulled toward high s(x) values (gradient ascent)

    Args:
        centerline: Nx2 array of (row, col) points
        channel_likelihood: s(x) field to attract toward
        iterations: Number of iterations
        alpha: Smoothness weight (higher = smoother line)
        beta: Attraction weight (higher = more pulled to ridges)
        gamma: Step size per iteration
        pin_endpoints: If True, don't move first/last points

    Returns:
        (adjusted_centerline, energy_history)
    """
    from scipy.ndimage import sobel

    h, w = channel_likelihood.shape
    n = len(centerline)
    pts = centerline.copy().astype(np.float64)

    # Compute gradient of channel likelihood (attraction field)
    grad_r = sobel(channel_likelihood, axis=0)  # d/dr
    grad_c = sobel(channel_likelihood, axis=1)  # d/dc

    energy_history = []

    for iteration in range(iterations):
        new_pts = pts.copy()

        for i in range(n):
            if pin_endpoints and (i == 0 or i == n - 1):
                continue

            r, c = pts[i]
            ri, ci = int(np.clip(r, 0, h-1)), int(np.clip(c, 0, w-1))

            # Smoothness force: pull toward average of neighbors (Laplacian)
            if i > 0 and i < n - 1:
                smooth_r = (pts[i-1, 0] + pts[i+1, 0]) / 2 - r
                smooth_c = (pts[i-1, 1] + pts[i+1, 1]) / 2 - c
            else:
                smooth_r, smooth_c = 0, 0

            # Attraction force: gradient of s(x) - move uphill
            attract_r = grad_r[ri, ci]
            attract_c = grad_c[ri, ci]

            # Normalize attraction to prevent explosion
            attract_mag = np.sqrt(attract_r**2 + attract_c**2)
            if attract_mag > 1e-6:
                attract_r /= attract_mag
                attract_c /= attract_mag

            # Combined force
            force_r = alpha * smooth_r + beta * attract_r
            force_c = alpha * smooth_c + beta * attract_c

            # Update position
            new_pts[i, 0] = r + gamma * force_r
            new_pts[i, 1] = c + gamma * force_c

        # Clamp to bounds
        new_pts[:, 0] = np.clip(new_pts[:, 0], 0, h - 1)
        new_pts[:, 1] = np.clip(new_pts[:, 1], 0, w - 1)

        pts = new_pts

        # Compute energy (negative mean s(x) - we want to maximize)
        energy = -np.mean([channel_likelihood[int(r), int(c)]
                          for r, c in pts
                          if 0 <= int(r) < h and 0 <= int(c) < w])
        energy_history.append(-energy)  # Store as positive (higher = better)

    # Final stats
    final_support = energy_history[-1] if energy_history else 0
    logger.info(f"Active contour: {iterations} iters, support {energy_history[0]:.4f} -> {final_support:.4f}")

    return pts, energy_history


def attract_to_mask(
    centerline: np.ndarray,
    channel_likelihood: np.ndarray,
    iterations: int = 100,
    attraction_strength: float = 2.0,
    smoothing_strength: float = 0.5,
    step_size: float = 0.5,
    convergence_threshold: float = 0.01,
    dilate_radius: int = 3,
) -> Tuple[np.ndarray, dict]:
    """
    Iteratively deform centerline to flow through highest mask values.

    Uses a spring model where:
    - Each point is connected to neighbors (smoothness springs)
    - Each point is attracted to nearby high-value regions

    Args:
        centerline: Nx2 array of (row, col) points
        channel_likelihood: s(x) field
        iterations: Max iterations
        attraction_strength: Pull toward high values
        smoothing_strength: Keep line smooth
        step_size: Movement per iteration
        convergence_threshold: Stop if mean movement below this
        dilate_radius: Dilation radius to expand attraction field (0 to disable)

    Returns:
        (adjusted_centerline, stats_dict)
    """
    from scipy.ndimage import maximum_filter, gaussian_filter, grey_dilation
    from skimage.morphology import disk

    h, w = channel_likelihood.shape
    n = len(centerline)
    pts = centerline.copy().astype(np.float64)

    # Dilate channel likelihood to create broader attraction field
    if dilate_radius > 0:
        s_dilated = grey_dilation(channel_likelihood, footprint=disk(dilate_radius))
    else:
        s_dilated = channel_likelihood

    # Precompute attraction field - direction toward local maximum
    smooth_s = gaussian_filter(s_dilated, sigma=2)

    # For each pixel, compute direction to local maximum
    window = 7
    local_max = maximum_filter(smooth_s, size=window)

    history = {'support': [], 'movement': []}

    for it in range(iterations):
        forces = np.zeros_like(pts)

        for i in range(1, n - 1):  # Skip endpoints
            r, c = pts[i]
            ri, ci = int(np.clip(r, 0, h-1)), int(np.clip(c, 0, w-1))

            # Smoothness: spring force toward midpoint of neighbors
            mid_r = (pts[i-1, 0] + pts[i+1, 0]) / 2
            mid_c = (pts[i-1, 1] + pts[i+1, 1]) / 2
            smooth_force = np.array([mid_r - r, mid_c - c])

            # Attraction: search local neighborhood for maximum
            search_r = 5
            best_r, best_c = ri, ci
            best_val = smooth_s[ri, ci]

            for dr in range(-search_r, search_r + 1):
                for dc in range(-search_r, search_r + 1):
                    nr, nc = ri + dr, ci + dc
                    if 0 <= nr < h and 0 <= nc < w:
                        if smooth_s[nr, nc] > best_val:
                            best_val = smooth_s[nr, nc]
                            best_r, best_c = nr, nc

            attract_force = np.array([best_r - r, best_c - c])

            # Normalize attraction
            attract_mag = np.linalg.norm(attract_force)
            if attract_mag > 1:
                attract_force = attract_force / attract_mag

            # Combined force
            forces[i] = (smoothing_strength * smooth_force +
                        attraction_strength * attract_force)

        # Apply forces
        movement = step_size * forces
        pts += movement

        # Clamp
        pts[:, 0] = np.clip(pts[:, 0], 0, h - 1)
        pts[:, 1] = np.clip(pts[:, 1], 0, w - 1)

        # Track convergence
        mean_movement = np.mean(np.linalg.norm(movement, axis=1))
        support = np.mean([channel_likelihood[int(r), int(c)]
                         for r, c in pts
                         if 0 <= int(r) < h and 0 <= int(c) < w])

        history['support'].append(support)
        history['movement'].append(mean_movement)

        if mean_movement < convergence_threshold:
            logger.info(f"Converged at iteration {it}")
            break

    final_support = history['support'][-1] if history['support'] else 0
    initial_support = history['support'][0] if history['support'] else 0

    logger.info(f"Attract to mask: {len(history['support'])} iters, "
                f"support {initial_support:.4f} -> {final_support:.4f}")

    return pts, {
        'iterations': len(history['support']),
        'initial_support': initial_support,
        'final_support': final_support,
        'history': history,
    }


def adjust_to_cross_section(
    centerline: np.ndarray,
    channel_likelihood: np.ndarray,
    search_width: int = 10,
) -> Tuple[np.ndarray, np.ndarray]:
    """
    Adjust centerline nodes along perpendicular cross-sections.

    For each node, searches along the perpendicular direction to find
    the position with maximum channel likelihood.

    Args:
        centerline: Nx2 array of (row, col) points
        channel_likelihood: s(x) field
        search_width: Half-width of cross-section search (pixels)

    Returns:
        (adjusted_centerline, offsets) - adjusted points and perpendicular offsets
    """
    h, w = channel_likelihood.shape
    n_points = len(centerline)
    adjusted = centerline.copy().astype(float)
    offsets = np.zeros(n_points)

    for i in range(n_points):
        r, c = centerline[i]

        # Compute local direction (tangent)
        if i == 0:
            tangent = centerline[1] - centerline[0]
        elif i == n_points - 1:
            tangent = centerline[-1] - centerline[-2]
        else:
            tangent = centerline[i + 1] - centerline[i - 1]

        # Normalize tangent
        tangent_len = np.sqrt(tangent[0]**2 + tangent[1]**2)
        if tangent_len < 1e-6:
            continue
        tangent = tangent / tangent_len

        # Perpendicular (normal) direction
        normal = np.array([-tangent[1], tangent[0]])

        # Search along cross-section
        best_offset = 0
        best_val = 0

        for offset in range(-search_width, search_width + 1):
            nr = int(r + offset * normal[0])
            nc = int(c + offset * normal[1])

            if 0 <= nr < h and 0 <= nc < w:
                val = channel_likelihood[nr, nc]
                if val > best_val:
                    best_val = val
                    best_offset = offset

        offsets[i] = best_offset
        adjusted[i] = [r + best_offset * normal[0], c + best_offset * normal[1]]

    # Smooth offsets
    if n_points > 5:
        from scipy.ndimage import uniform_filter1d
        offsets_smooth = uniform_filter1d(offsets, size=5, mode='nearest')

        # Recompute adjusted positions with smoothed offsets
        for i in range(n_points):
            r, c = centerline[i]
            if i == 0:
                tangent = centerline[1] - centerline[0]
            elif i == n_points - 1:
                tangent = centerline[-1] - centerline[-2]
            else:
                tangent = centerline[i + 1] - centerline[i - 1]

            tangent_len = np.sqrt(tangent[0]**2 + tangent[1]**2)
            if tangent_len < 1e-6:
                continue
            tangent = tangent / tangent_len
            normal = np.array([-tangent[1], tangent[0]])

            adjusted[i] = [r + offsets_smooth[i] * normal[0],
                          c + offsets_smooth[i] * normal[1]]

        offsets = offsets_smooth

    # Clamp to bounds
    adjusted[:, 0] = np.clip(adjusted[:, 0], 0, h - 1)
    adjusted[:, 1] = np.clip(adjusted[:, 1], 0, w - 1)

    n_moved = np.sum(np.abs(offsets) > 0.5)
    logger.info(f"Cross-section adjusted {n_moved}/{n_points} nodes, mean offset={np.mean(np.abs(offsets)):.1f}px")

    return adjusted, offsets


def path_to_mask(
    path: np.ndarray,
    shape: Tuple[int, int],
    width: int = 1,
) -> np.ndarray:
    """
    Convert path to binary mask by drawing lines.

    Args:
        path: Nx2 array of (row, col) points
        shape: (height, width) of output mask
        width: Line width in pixels

    Returns:
        Binary mask with path drawn
    """
    from skimage.draw import line

    mask = np.zeros(shape, dtype=bool)

    for i in range(len(path) - 1):
        r0, c0 = int(path[i, 0]), int(path[i, 1])
        r1, c1 = int(path[i + 1, 0]), int(path[i + 1, 1])

        # Clamp to bounds
        r0, c0 = max(0, min(shape[0]-1, r0)), max(0, min(shape[1]-1, c0))
        r1, c1 = max(0, min(shape[0]-1, r1)), max(0, min(shape[1]-1, c1))

        rr, cc = line(r0, c0, r1, c1)
        mask[rr, cc] = True

    # Dilate for width
    if width > 1:
        from scipy.ndimage import binary_dilation, generate_binary_structure
        struct = generate_binary_structure(2, 1)
        for _ in range(width // 2):
            mask = binary_dilation(mask, structure=struct)

    return mask
