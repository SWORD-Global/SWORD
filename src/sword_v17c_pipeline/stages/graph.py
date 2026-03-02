"""Graph construction stage for v17c pipeline."""

import math
from typing import Dict, Set, Tuple

import networkx as nx
import pandas as pd

from ._logging import log


def get_effective_width(attrs: Dict, min_obs: int = 5) -> float:
    """
    Get effective width for routing decisions.

    Prefers SWOT-observed width (width_obs_median) if n_obs >= min_obs,
    otherwise falls back to original GRWL width.
    """
    n_obs = attrs.get("n_obs", 0)
    if pd.isna(n_obs):
        n_obs = 0
    if n_obs >= min_obs:
        swot_width = attrs.get("width_obs_median")
        if swot_width is not None and not pd.isna(swot_width) and swot_width > 0:
            return swot_width
    width = attrs.get("width", 0)
    if pd.isna(width):
        return 0
    return width or 0


def build_reach_graph(
    topology_df: pd.DataFrame, reaches_df: pd.DataFrame
) -> nx.DiGraph:
    """
    Build directed graph where nodes=reaches, edges=flow connections.

    Flow direction from topology:
    - direction='up': neighbor is upstream -> neighbor -> reach
    - direction='down': neighbor is downstream -> reach -> neighbor
    """
    log("Building reach-level directed graph...")

    G = nx.DiGraph()

    # Create reach attributes dict
    reach_attrs = {}
    n_swot_width = 0

    for _, row in reaches_df.iterrows():
        rid = int(row["reach_id"])
        base_attrs = {
            "reach_length": row["reach_length"],
            "width": row["width"],
            "slope": row["slope"],
            "facc": row.get("facc", 0),
            "n_rch_up": row.get("n_rch_up", 0),
            "n_rch_down": row.get("n_rch_down", 0),
            "dist_out": row.get("dist_out", 0),
            "path_freq": row.get("path_freq", 1),
            "stream_order": row.get("stream_order", 1),
            "lakeflag": row.get("lakeflag", 0),
            "main_side": row.get("main_side", 0),
            "type": row.get("type", 1),
            "end_reach": row.get("end_reach", 0),
            "wse": row.get("wse"),
            "wse_obs_p50": row.get("wse_obs_p50"),
            "width_obs_median": row.get("width_obs_median"),
            "n_obs": row.get("n_obs", 0),
        }

        # Compute effective width (SWOT-preferred) and use facc directly (already corrected in DB)
        eff_width = get_effective_width(base_attrs)
        eff_facc = base_attrs.get("facc", 0)
        if pd.isna(eff_facc):
            eff_facc = 0

        # Track usage stats
        n_obs_val = base_attrs.get("n_obs", 0)
        width_obs_val = base_attrs.get("width_obs_median")
        if (
            not pd.isna(n_obs_val)
            and n_obs_val >= 5
            and width_obs_val is not None
            and not pd.isna(width_obs_val)
        ):
            n_swot_width += 1

        base_attrs["effective_width"] = eff_width
        base_attrs["effective_facc"] = eff_facc
        base_attrs["log_facc"] = math.log1p(eff_facc)

        reach_attrs[rid] = base_attrs

    log(f"Using SWOT width for {n_swot_width:,} reaches (n_obs >= 5)")

    # Add all reaches as nodes
    for rid, attrs in reach_attrs.items():
        G.add_node(rid, **attrs)

    # Add edges from topology
    edges_added = set()
    for _, row in topology_df.iterrows():
        reach_id = int(row["reach_id"])
        neighbor_id = int(row["neighbor_reach_id"])
        direction = row["direction"]

        if direction == "up":
            # neighbor is upstream: neighbor -> reach
            u, v = neighbor_id, reach_id
        else:
            # neighbor is downstream: reach -> neighbor
            u, v = reach_id, neighbor_id

        if (u, v) not in edges_added:
            edges_added.add((u, v))
            G.add_edge(u, v, reach_id=v)

    log(f"Reach graph: {G.number_of_nodes():,} nodes, {G.number_of_edges():,} edges")
    return G


def identify_junctions(G: nx.DiGraph) -> Set[int]:
    """
    Identify junction nodes in the reach graph.

    A junction is where:
    - in_degree > 1 (confluence)
    - out_degree > 1 (bifurcation)
    - in_degree == 0 (headwater)
    - out_degree == 0 (outlet)
    """
    junctions = set()

    for node in G.nodes():
        in_deg = G.in_degree(node)
        out_deg = G.out_degree(node)

        if in_deg != 1 or out_deg != 1:
            junctions.add(node)

    log(f"Identified {len(junctions):,} junctions")
    return junctions


def build_section_graph(
    G: nx.DiGraph, junctions: Set[int]
) -> Tuple[nx.DiGraph, pd.DataFrame]:
    """
    Build a section graph where each edge is a section (chain of reaches between junctions).

    Returns:
        R: DiGraph where nodes are junctions, edges are sections
        sections_df: DataFrame with section details
    """
    log("Building section graph...")

    R = nx.DiGraph()
    sections = []
    section_id = 0

    # Add junction nodes
    for j in junctions:
        node_data = G.nodes[j]
        in_deg = G.in_degree(j)
        out_deg = G.out_degree(j)

        if in_deg == 0:
            node_type = "Head_water"
        elif out_deg == 0:
            node_type = "Outlet"
        else:
            node_type = "Junction"

        R.add_node(j, node_type=node_type, **node_data)

    # For each junction, trace downstream to next junction
    for upstream_j in junctions:
        for first_reach in G.successors(upstream_j):
            # Trace chain until we hit another junction
            reach_ids = []
            cumulative_dist = 0
            current = first_reach

            while current not in junctions:
                reach_ids.append(current)
                cumulative_dist += G.nodes[current].get("reach_length", 0)

                succs = list(G.successors(current))
                if len(succs) == 0:
                    break
                current = succs[0]

            # current is now the downstream junction
            downstream_j = current
            if downstream_j in junctions:
                reach_ids.append(downstream_j)
                cumulative_dist += G.nodes[downstream_j].get("reach_length", 0)

                R.add_edge(
                    upstream_j,
                    downstream_j,
                    section_id=section_id,
                    reach_ids=reach_ids,
                    distance=cumulative_dist,
                    n_reaches=len(reach_ids),
                )

                sections.append(
                    {
                        "section_id": section_id,
                        "upstream_junction": upstream_j,
                        "downstream_junction": downstream_j,
                        "reach_ids": reach_ids,
                        "distance": cumulative_dist,
                        "n_reaches": len(reach_ids),
                    }
                )
                section_id += 1

    sections_df = pd.DataFrame(sections)
    log(
        f"Section graph: {R.number_of_nodes():,} junctions, {R.number_of_edges():,} sections"
    )
    return R, sections_df
