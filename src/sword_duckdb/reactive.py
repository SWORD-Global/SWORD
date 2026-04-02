# -*- coding: utf-8 -*-
"""
SWORD Reactive Update System
----------------------------

Automatic recalculation of derived attributes when upstream values change.
Implements a dependency graph with topological ordering for cascading updates.

Architecture:
    1. Dependency Graph - Defines which attributes depend on others
    2. Change Tracker - Records which values have changed
    3. Recalculation Engine - Executes updates in dependency order

Example Usage:
    from sword_duckdb.reactive import SWORDReactive

    sword = SWORD('data/duckdb/sword_v17b.duckdb', 'NA', 'v17b')
    reactive = SWORDReactive(sword)

    # Edit geometry
    sword.centerlines.x[idx] = new_x
    sword.centerlines.y[idx] = new_y

    # Trigger recalculations
    reactive.mark_dirty('centerline.geometry', reach_ids=[12345678901])
    reactive.recalculate()  # Cascades to reach.len, node.len, dist_out, etc.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import TYPE_CHECKING, Dict, List, Optional, Set, Tuple
import numpy as np

logger = logging.getLogger(__name__)

try:
    from geopy import distance as geodesic_distance
except ImportError:
    geodesic_distance = None

if TYPE_CHECKING:
    from .sword_class import SWORD


def _get_geodesic_distances(lon: np.ndarray, lat: np.ndarray) -> np.ndarray:
    """
    Calculate geodesic distances between consecutive points.

    Parameters
    ----------
    lon : np.ndarray
        Longitude values (WGS84)
    lat : np.ndarray
        Latitude values (WGS84)

    Returns
    -------
    np.ndarray
        Distances in meters, with leading 0 for first point
    """
    if geodesic_distance is None:
        raise ImportError(
            "geopy is required for distance calculations. Install with: pip install geopy"
        )

    n_points = len(lon)
    if n_points < 2:
        return np.array([0.0])

    distances = np.zeros(n_points - 1)
    for i in range(n_points - 1):
        start = (lat[i], lon[i])
        finish = (lat[i + 1], lon[i + 1])
        distances[i] = geodesic_distance.geodesic(start, finish).m

    return np.concatenate([[0.0], distances])


class ChangeType(Enum):
    """Types of changes that can trigger recalculations."""

    GEOMETRY = auto()  # Centerline x,y coordinates changed
    TOPOLOGY = auto()  # Reach connectivity (up/down) changed
    ATTRIBUTE = auto()  # Non-geometric attribute changed
    STRUCTURE = auto()  # Reaches/nodes added, deleted, or split


@dataclass
class DirtySet:
    """Tracks which entities need recalculation."""

    reach_ids: Set[int] = field(default_factory=set)
    node_ids: Set[int] = field(default_factory=set)
    cl_ids: Set[int] = field(default_factory=set)
    all_reaches: bool = False
    all_nodes: bool = False
    all_centerlines: bool = False


@dataclass
class DependencyNode:
    """A node in the dependency graph."""

    name: str
    table: str  # reaches, nodes, centerlines
    depends_on: List[str] = field(default_factory=list)
    recalc_func: Optional[str] = None  # Method name to call for recalc
    change_types: List[ChangeType] = field(default_factory=list)


class DependencyGraph:
    """
    Defines attribute dependencies and recalculation order.

    Based on analysis of SWORD codebase:
    - Geometry changes → lengths → dist_out → end_rch/main_side
    - Topology changes → counts → dist_out → end_rch/main_side
    """

    def __init__(self) -> None:
        self.nodes: Dict[str, DependencyNode] = {}
        self._build_graph()

    def _build_graph(self) -> None:
        """Build the dependency graph based on SWORD attribute relationships."""

        # === CENTERLINE ATTRIBUTES ===
        self.add_node(
            DependencyNode(
                name="centerline.geometry",
                table="centerlines",
                depends_on=[],  # Root - manual input
                change_types=[ChangeType.GEOMETRY],
            )
        )

        # === REACH ATTRIBUTES ===
        self.add_node(
            DependencyNode(
                name="reach.len",
                table="reaches",
                depends_on=["centerline.geometry"],
                recalc_func="_recalc_reach_lengths",
                change_types=[ChangeType.GEOMETRY],
            )
        )

        self.add_node(
            DependencyNode(
                name="reach.bounds",  # x, y, x_min, x_max, y_min, y_max
                table="reaches",
                depends_on=["centerline.geometry"],
                recalc_func="_recalc_reach_bounds",
                change_types=[ChangeType.GEOMETRY],
            )
        )

        self.add_node(
            DependencyNode(
                name="reach.topology",  # rch_id_up, rch_id_down, n_rch_up, n_rch_down
                table="reaches",
                depends_on=[],  # Root - manual input
                change_types=[ChangeType.TOPOLOGY],
            )
        )

        self.add_node(
            DependencyNode(
                name="reach.dist_out",
                table="reaches",
                depends_on=["reach.len", "reach.topology"],
                recalc_func="_recalc_reach_dist_out",
                change_types=[ChangeType.GEOMETRY, ChangeType.TOPOLOGY],
            )
        )

        self.add_node(
            DependencyNode(
                name="reach.end_rch",
                table="reaches",
                depends_on=["reach.topology"],
                recalc_func="_recalc_reach_end_rch",
                change_types=[ChangeType.TOPOLOGY],
            )
        )

        self.add_node(
            DependencyNode(
                name="reach.main_side",
                table="reaches",
                depends_on=["reach.topology", "reach.dist_out"],
                recalc_func="_recalc_reach_main_side",
                change_types=[ChangeType.TOPOLOGY],
            )
        )

        # === NODE ATTRIBUTES ===
        self.add_node(
            DependencyNode(
                name="node.len",
                table="nodes",
                depends_on=["centerline.geometry"],
                recalc_func="_recalc_node_lengths",
                change_types=[ChangeType.GEOMETRY],
            )
        )

        self.add_node(
            DependencyNode(
                name="node.xy",  # x, y coordinates
                table="nodes",
                depends_on=["centerline.geometry"],
                recalc_func="_recalc_node_xy",
                change_types=[ChangeType.GEOMETRY],
            )
        )

        self.add_node(
            DependencyNode(
                name="node.dist_out",
                table="nodes",
                depends_on=["reach.dist_out", "node.len"],
                recalc_func="_recalc_node_dist_out",
                change_types=[ChangeType.GEOMETRY, ChangeType.TOPOLOGY],
            )
        )

        self.add_node(
            DependencyNode(
                name="node.end_rch",
                table="nodes",
                depends_on=["reach.end_rch"],
                recalc_func="_recalc_node_end_rch",
                change_types=[ChangeType.TOPOLOGY],
            )
        )

        self.add_node(
            DependencyNode(
                name="node.main_side",
                table="nodes",
                depends_on=["reach.main_side"],
                recalc_func="_recalc_node_main_side",
                change_types=[ChangeType.TOPOLOGY],
            )
        )

        self.add_node(
            DependencyNode(
                name="node.cl_id_bounds",  # cl_id[0] (min), cl_id[1] (max)
                table="nodes",
                depends_on=["centerline.geometry"],
                recalc_func="_recalc_node_cl_id_bounds",
                change_types=[ChangeType.GEOMETRY, ChangeType.STRUCTURE],
            )
        )

        # === CENTERLINE NEIGHBOR ATTRIBUTES ===
        self.add_node(
            DependencyNode(
                name="centerline.node_id_neighbors",  # node_id[1:4] via KDTree
                table="centerlines",
                depends_on=["centerline.geometry", "node.xy"],
                recalc_func="_recalc_centerline_neighbors",
                change_types=[ChangeType.GEOMETRY],
            )
        )

    def add_node(self, node: DependencyNode) -> None:
        """Add a node to the dependency graph."""
        self.nodes[node.name] = node

    def get_downstream_deps(self, attr_name: str) -> List[str]:
        """Get all attributes that depend on the given attribute (directly or transitively)."""
        result = []
        visited = set()

        def dfs(name):
            for node_name, node in self.nodes.items():
                if name in node.depends_on and node_name not in visited:
                    visited.add(node_name)
                    result.append(node_name)
                    dfs(node_name)

        dfs(attr_name)
        return result

    def topological_sort(self, dirty_attrs: Set[str]) -> List[str]:
        """
        Return attributes in recalculation order (dependencies first).

        Uses Kahn's algorithm for topological sorting.
        """
        # Build subgraph of dirty attributes and their dependencies
        relevant = set()
        for attr in dirty_attrs:
            relevant.add(attr)
            relevant.update(self.get_downstream_deps(attr))

        # Calculate in-degree for relevant nodes
        in_degree = defaultdict(int)
        for name in relevant:
            node = self.nodes.get(name)
            if node:
                for dep in node.depends_on:
                    if dep in relevant:
                        in_degree[name] += 1

        # Start with nodes that have no dependencies (in relevant set)
        queue = [name for name in relevant if in_degree[name] == 0]
        result = []

        while queue:
            name = queue.pop(0)
            result.append(name)

            # Reduce in-degree of dependent nodes
            for node_name in relevant:
                node = self.nodes.get(node_name)
                if node and name in node.depends_on:
                    in_degree[node_name] -= 1
                    if in_degree[node_name] == 0:
                        queue.append(node_name)

        return result


class SWORDReactive:
    """
    Reactive update manager for SWORD database.

    Tracks changes and automatically recalculates dependent attributes.
    """

    def __init__(self, sword: "SWORD") -> None:
        self.sword = sword
        self.graph = DependencyGraph()
        self.dirty = DirtySet()
        self._dirty_attrs: Set[str] = set()
        self._pending_changes: List[Tuple[str, ChangeType, DirtySet]] = []

    def mark_dirty(
        self,
        attr_name: str,
        change_type: ChangeType = ChangeType.ATTRIBUTE,
        reach_ids: Optional[List[int]] = None,
        node_ids: Optional[List[int]] = None,
        cl_ids: Optional[List[int]] = None,
        all_entities: bool = False,
    ) -> None:
        """
        Mark an attribute as dirty (needing recalculation of dependents).

        Parameters
        ----------
        attr_name : str
            Name of the changed attribute (e.g., 'centerline.geometry', 'reach.topology')
        change_type : ChangeType
            Type of change that occurred
        reach_ids : list of int, optional
            Specific reach IDs affected
        node_ids : list of int, optional
            Specific node IDs affected
        cl_ids : list of int, optional
            Specific centerline IDs affected
        all_entities : bool
            If True, marks all entities as dirty
        """
        dirty_set = DirtySet()

        if all_entities:
            dirty_set.all_reaches = True
            dirty_set.all_nodes = True
            dirty_set.all_centerlines = True
        else:
            if reach_ids:
                dirty_set.reach_ids.update(reach_ids)
            if node_ids:
                dirty_set.node_ids.update(node_ids)
            if cl_ids:
                dirty_set.cl_ids.update(cl_ids)

        self._dirty_attrs.add(attr_name)
        self._pending_changes.append((attr_name, change_type, dirty_set))

        # Merge into global dirty set
        self.dirty.reach_ids.update(dirty_set.reach_ids)
        self.dirty.node_ids.update(dirty_set.node_ids)
        self.dirty.cl_ids.update(dirty_set.cl_ids)
        self.dirty.all_reaches = self.dirty.all_reaches or dirty_set.all_reaches
        self.dirty.all_nodes = self.dirty.all_nodes or dirty_set.all_nodes
        self.dirty.all_centerlines = (
            self.dirty.all_centerlines or dirty_set.all_centerlines
        )

    def get_recalc_plan(self) -> List[Tuple[str, str]]:
        """
        Get the planned recalculation order.

        Returns list of (attribute_name, recalc_function_name) tuples.
        """
        if not self._dirty_attrs:
            return []

        sorted_attrs = self.graph.topological_sort(self._dirty_attrs)
        plan = []

        for attr_name in sorted_attrs:
            node = self.graph.nodes.get(attr_name)
            if node and node.recalc_func:
                plan.append((attr_name, node.recalc_func))

        return plan

    def recalculate(self, dry_run: bool = False) -> List[str]:
        """
        Execute recalculations for all dirty attributes.

        Parameters
        ----------
        dry_run : bool
            If True, only return what would be recalculated without doing it.

        Returns
        -------
        list of str
            Names of attributes that were (or would be) recalculated.
        """
        plan = self.get_recalc_plan()
        recalculated = []

        if dry_run:
            return [attr for attr, _ in plan]

        for attr_name, func_name in plan:
            logger.info(f"Recalculating: {attr_name}")
            func = getattr(self, func_name, None)
            if func:
                func()
                recalculated.append(attr_name)
            else:
                logger.warning(f"No implementation for {func_name}")

        # Clear dirty state
        self._dirty_attrs.clear()
        self._pending_changes.clear()
        self.dirty = DirtySet()

        return recalculated

    # === RECALCULATION IMPLEMENTATIONS ===

    def _recalc_reach_lengths(self) -> None:
        """
        Recalculate reach lengths from centerline geometry.

        Logic: For each reach, get all centerlines sorted by cl_id,
        calculate geodesic distances between consecutive points,
        and set reach.len to the total distance.
        """
        logger.debug("Recalculating reach.len from centerline x,y")

        sword = self.sword
        reaches = sword.reaches
        centerlines = sword.centerlines

        # Get affected reach IDs
        if self.dirty.all_reaches:
            reach_ids = reaches.id[:]
        else:
            reach_ids = list(self.dirty.reach_ids)

        for reach_id in reach_ids:
            # Find centerlines for this reach
            cl_mask = centerlines.reach_id[0, :] == reach_id
            cl_indices = np.where(cl_mask)[0]

            if len(cl_indices) == 0:
                continue

            # Sort by cl_id
            sorted_indices = cl_indices[np.argsort(centerlines.cl_id[cl_indices])]

            # Get coordinates
            x_coords = centerlines.x[sorted_indices]
            y_coords = centerlines.y[sorted_indices]

            # Calculate distances
            distances = _get_geodesic_distances(x_coords, y_coords)
            total_length = np.sum(distances)

            # Update reach length
            reach_idx = np.where(reaches.id == reach_id)[0]
            if len(reach_idx) > 0:
                reaches.len[reach_idx[0]] = total_length

    def _recalc_reach_bounds(self) -> None:
        """
        Recalculate reach bounding box from centerlines.

        Updates: x, y (centroid), x_min, x_max, y_min, y_max
        """
        logger.debug("Recalculating reach x, y, x_min, x_max, y_min, y_max")

        sword = self.sword
        reaches = sword.reaches
        centerlines = sword.centerlines

        if self.dirty.all_reaches:
            reach_ids = reaches.id[:]
        else:
            reach_ids = list(self.dirty.reach_ids)

        for reach_id in reach_ids:
            cl_mask = centerlines.reach_id[0, :] == reach_id
            cl_indices = np.where(cl_mask)[0]

            if len(cl_indices) == 0:
                continue

            x_coords = centerlines.x[cl_indices]
            y_coords = centerlines.y[cl_indices]

            reach_idx = np.where(reaches.id == reach_id)[0]
            if len(reach_idx) > 0:
                idx = reach_idx[0]
                reaches.x[idx] = np.mean(x_coords)
                reaches.y[idx] = np.mean(y_coords)
                reaches.x_min[idx] = np.min(x_coords)
                reaches.x_max[idx] = np.max(x_coords)
                reaches.y_min[idx] = np.min(y_coords)
                reaches.y_max[idx] = np.max(y_coords)

    def _recalc_reach_dist_out(self) -> None:
        """
        Recalculate reach dist_out from topology and lengths.

        Algorithm: Traverse from outlets (n_rch_down=0) upstream.
        Outlet dist_out = reach.len
        Upstream dist_out = reach.len + max(downstream dist_out values)
        """
        logger.debug("Recalculating reach.dist_out (outlet-to-headwater order)")

        sword = self.sword
        reaches = sword.reaches
        n_reaches = len(reaches.id)

        # Initialize dist_out array
        dist_out = np.full(n_reaches, np.nan)

        # Find outlets (no downstream neighbors)
        outlet_mask = reaches.n_rch_down[:] == 0
        outlet_indices = np.where(outlet_mask)[0]

        # Process queue - start from outlets
        processed = set()
        queue = list(outlet_indices)

        while queue:
            idx = queue.pop(0)
            reach_id = reaches.id[idx]  # noqa: F841 — kept for debugging/logging

            if idx in processed:
                continue

            n_down = reaches.n_rch_down[idx]

            if n_down == 0:
                # Outlet reach - dist_out is just its own length
                dist_out[idx] = reaches.len[idx]
            else:
                # Get downstream neighbors
                dn_ids = reaches.rch_id_down[:, idx]
                dn_ids = dn_ids[dn_ids > 0]

                # Check if all downstream reaches are processed
                all_dn_processed = True
                max_dn_dist = 0

                for dn_id in dn_ids:
                    dn_idx = np.where(reaches.id == dn_id)[0]
                    if len(dn_idx) > 0:
                        dn_idx = dn_idx[0]
                        if dn_idx not in processed:
                            all_dn_processed = False
                            break
                        max_dn_dist = max(max_dn_dist, dist_out[dn_idx])

                if not all_dn_processed:
                    # Put back in queue and try later
                    queue.append(idx)
                    continue

                dist_out[idx] = reaches.len[idx] + max_dn_dist

            processed.add(idx)

            # Add upstream neighbors to queue
            up_ids = reaches.rch_id_up[:, idx]
            up_ids = up_ids[up_ids > 0]
            for up_id in up_ids:
                up_idx = np.where(reaches.id == up_id)[0]
                if len(up_idx) > 0 and up_idx[0] not in processed:
                    queue.append(up_idx[0])

        # Update reach dist_out values
        for i in range(n_reaches):
            if not np.isnan(dist_out[i]):
                reaches.dist_out[i] = dist_out[i]

    def _recalc_reach_end_rch(self) -> None:
        """
        Recalculate reach end_rch flag from topology.

        Values:
        - 0: Normal reach
        - 1: Headwater (n_rch_up == 0)
        - 2: Outlet (n_rch_down == 0)
        - 3: Junction (n_rch_up > 1 or n_rch_down > 1)
        """
        logger.debug("Recalculating reach.end_rch from n_rch_up, n_rch_down")

        sword = self.sword
        reaches = sword.reaches
        n_reaches = len(reaches.id)

        # Default all to normal (0)
        end_rch = np.zeros(n_reaches, dtype=np.int32)

        # Mark junctions first (can be overridden by headwater/outlet)
        junction_mask = (reaches.n_rch_up[:] > 1) | (reaches.n_rch_down[:] > 1)
        end_rch[junction_mask] = 3

        # Mark headwaters
        headwater_mask = reaches.n_rch_up[:] == 0
        end_rch[headwater_mask] = 1

        # Mark outlets
        outlet_mask = reaches.n_rch_down[:] == 0
        end_rch[outlet_mask] = 2

        # Update
        for i in range(n_reaches):
            reaches.end_rch[i] = end_rch[i]

    def _recalc_reach_main_side(self) -> None:
        """
        Recalculate reach main_side from topology and path_freq.

        Classification:
        - main_side = 1: main channel (highest path_freq at junction)
        - main_side = 2: side channel

        At junctions, the upstream branch with higher path_freq is considered
        the main channel. Linear reaches (single upstream/downstream) are main.
        """
        logger.debug("Recalculating reach.main_side from path_freq")

        sword = self.sword
        reaches = sword.reaches
        n_reaches = len(reaches.id)

        # Build lookup tables
        reach_ids = reaches.id[:]
        path_freq = reaches.path_freq[:]
        n_rch_up = reaches.n_rch_up[:]
        n_rch_down = reaches.n_rch_down[:]
        rch_id_up = reaches.rch_id_up[:]  # Shape: (4, n_reaches)
        rch_id_dn = reaches.rch_id_dn[:]  # Shape: (4, n_reaches)

        # Create reach_id to index mapping
        id_to_idx = {rid: i for i, rid in enumerate(reach_ids)}

        # Initialize all as main channel (0=main, 1=side, 2=secondary outlet)
        main_side = np.zeros(n_reaches, dtype=np.int32)

        for i in range(n_reaches):
            # Linear reach - always main
            if n_rch_up[i] <= 1 and n_rch_down[i] <= 1:
                main_side[i] = 0
                continue

            # Junction: check if this reach is the main upstream branch
            # by comparing path_freq with siblings
            if n_rch_down[i] >= 1:
                # Get downstream reach(es)
                for d in range(min(n_rch_down[i], 4)):
                    dn_id = rch_id_dn[d, i]
                    if dn_id <= 0:
                        continue

                    dn_idx = id_to_idx.get(dn_id)
                    if dn_idx is None:
                        continue

                    # Get all upstream reaches of the downstream reach
                    n_up_of_dn = n_rch_up[dn_idx]
                    if n_up_of_dn <= 1:
                        # Single upstream = main
                        main_side[i] = 0
                        continue

                    # Multiple upstream branches - compare path_freq
                    my_path_freq = path_freq[i]
                    is_main = True

                    for u in range(min(n_up_of_dn, 4)):
                        up_id = rch_id_up[u, dn_idx]
                        if up_id <= 0 or up_id == reach_ids[i]:
                            continue

                        up_idx = id_to_idx.get(up_id)
                        if up_idx is not None:
                            # If sibling has higher path_freq, we're not main
                            if path_freq[up_idx] > my_path_freq:
                                is_main = False
                                break

                    main_side[i] = 0 if is_main else 1

        # Apply updates
        for i in range(n_reaches):
            reaches.main_side[i] = main_side[i]

        logger.info(
            f"Recalculated main_side: {np.sum(main_side == 0)} main, {np.sum(main_side == 1)} side"
        )

    def _recalc_node_lengths(self) -> None:
        """
        Recalculate node lengths from centerline geometry.

        For each node, calculate the cumulative distance of all
        centerlines belonging to that node.
        """
        logger.debug("Recalculating node.len from centerline x,y")

        sword = self.sword
        nodes = sword.nodes
        centerlines = sword.centerlines
        reaches = sword.reaches

        if self.dirty.all_reaches:
            reach_ids = reaches.id[:]
        else:
            reach_ids = list(self.dirty.reach_ids)

        for reach_id in reach_ids:
            # Get centerlines for this reach, sorted by cl_id
            cl_mask = centerlines.reach_id[0, :] == reach_id
            cl_indices = np.where(cl_mask)[0]

            if len(cl_indices) == 0:
                continue

            sorted_indices = cl_indices[np.argsort(centerlines.cl_id[cl_indices])]

            x_coords = centerlines.x[sorted_indices]
            y_coords = centerlines.y[sorted_indices]
            distances = _get_geodesic_distances(x_coords, y_coords)

            # Get unique nodes in sorted order
            node_ids_sorted = centerlines.node_id[0, sorted_indices]
            unique_nodes = []
            seen = set()
            for nid in node_ids_sorted:
                if nid not in seen:
                    unique_nodes.append(nid)
                    seen.add(nid)

            # Calculate node lengths
            for node_id in unique_nodes:
                node_cl_mask = centerlines.node_id[0, sorted_indices] == node_id
                node_distances = distances[node_cl_mask]
                node_length = np.sum(node_distances)

                node_idx = np.where(nodes.id == node_id)[0]
                if len(node_idx) > 0:
                    nodes.node_length[node_idx[0]] = node_length

    def _recalc_node_xy(self) -> None:
        """
        Recalculate node x,y as centroid of associated centerlines.
        """
        logger.debug("Recalculating node x, y (centroid of centerlines)")

        sword = self.sword
        nodes = sword.nodes
        centerlines = sword.centerlines

        if self.dirty.all_nodes:
            node_ids = nodes.id[:]
        else:
            node_ids = list(self.dirty.node_ids)

        for node_id in node_ids:
            cl_mask = centerlines.node_id[0, :] == node_id
            cl_indices = np.where(cl_mask)[0]

            if len(cl_indices) == 0:
                continue

            x_coords = centerlines.x[cl_indices]
            y_coords = centerlines.y[cl_indices]

            node_idx = np.where(nodes.id == node_id)[0]
            if len(node_idx) > 0:
                nodes.x[node_idx[0]] = np.mean(x_coords)
                nodes.y[node_idx[0]] = np.mean(y_coords)

    def _recalc_node_dist_out(self) -> None:
        """
        Recalculate node dist_out from reach dist_out and node lengths.

        node.dist_out = reach_base_dist + cumulative_node_length
        where reach_base_dist = reach.dist_out - reach.len
        """
        logger.debug(
            "Recalculating node.dist_out from reach base + cumulative node.len"
        )

        sword = self.sword
        nodes = sword.nodes
        reaches = sword.reaches

        for r in range(len(reaches.id)):
            reach_id = reaches.id[r]

            # Find nodes in this reach
            node_mask = nodes.reach_id[:] == reach_id
            node_indices = np.where(node_mask)[0]

            if len(node_indices) == 0:
                continue

            # Sort nodes by node_order (geometric order, 1=downstream)
            # This handles flow-corrected reaches where node_ids are in original order
            sorted_indices = node_indices[np.argsort(nodes.node_order[node_indices])]

            # Total length of all nodes in this reach
            total_node_len = nodes.node_length[sorted_indices].sum()

            # Base distance (upstream of this reach) = reach.dist_out - total_node_len
            base_dist = reaches.dist_out[r] - total_node_len

            # Cumulative node lengths from UPSTREAM to DOWNSTREAM
            # Reverse the sorted indices, cumsum, then reverse back
            reversed_lengths = nodes.node_length[sorted_indices[::-1]]
            reversed_cumsum = np.cumsum(reversed_lengths)
            cumsum = reversed_cumsum[::-1]

            # Update node dist_out
            # node_order=1 (downstream) gets base_dist + total_len = reach.dist_out (highest)
            # node_order=n (upstream) gets base_dist + its_length (lowest)
            for i, idx in enumerate(sorted_indices):
                nodes.dist_out[idx] = base_dist + cumsum[i]

    def _recalc_node_end_rch(self) -> None:
        """
        Propagate reach end_rch to nodes.

        Each node inherits the end_rch value from its parent reach.
        """
        logger.debug("Propagating reach.end_rch to node.end_rch")

        sword = self.sword
        nodes = sword.nodes
        reaches = sword.reaches

        for r in range(len(reaches.id)):
            reach_id = reaches.id[r]
            end_rch_val = reaches.end_rch[r]

            node_mask = nodes.reach_id[:] == reach_id
            node_indices = np.where(node_mask)[0]

            for idx in node_indices:
                nodes.end_rch[idx] = end_rch_val

    def _recalc_node_main_side(self) -> None:
        """
        Propagate reach main_side to nodes.

        Each node inherits the main_side value from its parent reach.
        """
        logger.debug("Propagating reach.main_side to node.main_side")

        sword = self.sword
        nodes = sword.nodes
        reaches = sword.reaches

        for r in range(len(reaches.id)):
            reach_id = reaches.id[r]
            main_side_val = reaches.main_side[r]

            node_mask = nodes.reach_id[:] == reach_id
            node_indices = np.where(node_mask)[0]

            for idx in node_indices:
                nodes.main_side[idx] = main_side_val

    def _recalc_node_cl_id_bounds(self) -> None:
        """
        Recalculate node cl_id min/max from centerlines.

        Updates nodes.cl_id[0] (min) and nodes.cl_id[1] (max)
        """
        logger.debug("Recalculating node.cl_id_min, node.cl_id_max")

        sword = self.sword
        nodes = sword.nodes
        centerlines = sword.centerlines

        for n in range(len(nodes.id)):
            node_id = nodes.id[n]

            cl_mask = centerlines.node_id[0, :] == node_id
            cl_indices = np.where(cl_mask)[0]

            if len(cl_indices) == 0:
                continue

            # Note: cl_id is a 2D array [2, N] where [0] is min and [1] is max
            # This requires special handling since cl_id may not be writable
            # For now, we skip this as it requires 2D WritableArray support

    def _recalc_centerline_neighbors(self) -> None:
        """
        Recalculate centerline node_id neighbors using KDTree.

        For each centerline, finds the 4 nearest node centroids:
        - node_id[0] = primary node (already set)
        - node_id[1:4] = 3 nearest neighbor nodes

        Updates the centerline_neighbors table with ranks 1, 2, 3.
        """
        try:
            from scipy.spatial import KDTree
        except ImportError:
            logger.warning(
                "scipy not available - skipping centerline neighbor recalculation"
            )
            return

        logger.info("Recalculating centerline.node_id[1:4] via KDTree spatial query")

        sword = self.sword
        nodes = sword.nodes
        centerlines = sword.centerlines
        db = sword.db
        region = sword._region

        # Get node centroids
        node_ids = nodes.id[:]
        node_x = nodes.x[:]
        node_y = nodes.y[:]

        # Filter out nodes with invalid coordinates
        valid_mask = np.isfinite(node_x) & np.isfinite(node_y)
        valid_node_ids = node_ids[valid_mask]
        valid_coords = np.column_stack([node_x[valid_mask], node_y[valid_mask]])

        if len(valid_coords) < 4:
            logger.warning("Not enough valid nodes for neighbor calculation")
            return

        # Build KDTree from node centroids
        logger.debug(f"Building KDTree with {len(valid_coords)} node centroids")
        tree = KDTree(valid_coords)

        # Get centerline coordinates
        cl_ids = centerlines.cl_id[:]
        cl_x = centerlines.x[:]
        cl_y = centerlines.y[:]

        # Determine which centerlines to process
        if self.dirty.all_reaches:
            cl_indices = np.arange(len(cl_ids))
        else:
            # Get centerlines for dirty reaches
            reach_ids = list(self.dirty.reach_ids)
            reach_id_arr = centerlines.reach_id[0, :]  # Primary reach_id
            cl_indices = np.where(np.isin(reach_id_arr, reach_ids))[0]

        if len(cl_indices) == 0:
            logger.debug("No centerlines to update")
            return

        logger.info(f"Updating neighbors for {len(cl_indices)} centerlines")

        # Query k=4 nearest nodes for each centerline
        cl_coords = np.column_stack([cl_x[cl_indices], cl_y[cl_indices]])
        distances, indices = tree.query(cl_coords, k=min(4, len(valid_coords)))

        # Prepare batch update data
        # Format: [(cl_id, region, rank, node_id), ...]
        neighbor_updates = []

        for i, cl_idx in enumerate(cl_indices):
            cl_id = cl_ids[cl_idx]
            # indices[i] contains the 4 nearest node indices
            # Skip the first (primary node), take next 3 as neighbors
            for rank in range(1, min(4, len(indices[i]))):
                neighbor_node_id = valid_node_ids[indices[i][rank]]
                neighbor_updates.append(
                    (int(cl_id), region, rank, int(neighbor_node_id))
                )

        if not neighbor_updates:
            return

        # Delete existing neighbors for these centerlines
        cl_ids_to_update = [int(cl_ids[i]) for i in cl_indices]
        placeholders = ", ".join(["?"] * len(cl_ids_to_update))

        db.execute(
            f"DELETE FROM centerline_neighbors WHERE cl_id IN ({placeholders}) AND region = ?",
            cl_ids_to_update + [region],
        )

        # Insert new neighbors in batches
        batch_size = 10000
        for batch_start in range(0, len(neighbor_updates), batch_size):
            batch = neighbor_updates[batch_start : batch_start + batch_size]
            db.execute(
                "INSERT INTO centerline_neighbors (cl_id, region, neighbor_rank, node_id) VALUES (?, ?, ?, ?)",
                batch,
            )

        logger.info(f"Updated {len(neighbor_updates)} centerline neighbor records")


# === CONVENIENCE FUNCTIONS ===


def mark_geometry_changed(
    reactive: SWORDReactive,
    reach_ids: Optional[List[int]] = None,
    all_reaches: bool = False,
) -> None:
    """Mark geometry as changed, triggering full recalculation cascade."""
    reactive.mark_dirty(
        "centerline.geometry",
        ChangeType.GEOMETRY,
        reach_ids=reach_ids,
        all_entities=all_reaches,
    )


def mark_topology_changed(
    reactive: SWORDReactive,
    reach_ids: Optional[List[int]] = None,
    all_reaches: bool = False,
) -> None:
    """Mark topology as changed, triggering topology recalculation cascade."""
    reactive.mark_dirty(
        "reach.topology",
        ChangeType.TOPOLOGY,
        reach_ids=reach_ids,
        all_entities=all_reaches,
    )


def full_recalculate(reactive: SWORDReactive) -> None:
    """Trigger full recalculation of all derived attributes."""
    reactive.mark_dirty("centerline.geometry", ChangeType.GEOMETRY, all_entities=True)
    reactive.mark_dirty("reach.topology", ChangeType.TOPOLOGY, all_entities=True)
    reactive.recalculate()
