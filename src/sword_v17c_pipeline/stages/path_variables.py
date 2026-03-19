"""Path variable computation stage for v17c pipeline."""

import math
from collections import defaultdict, deque
from typing import Dict, List, Set, Tuple

import networkx as nx
import pandas as pd

from ._logging import log


def compute_path_variables(
    G: nx.DiGraph, sections_df: pd.DataFrame, region: str = ""
) -> Dict[int, Dict]:
    """
    Compute path_freq, stream_order, path_segs, and path_order for every reach.

    Parameters
    ----------
    G : nx.DiGraph
        Reach-level directed graph (nodes = reaches, edges = flow direction).
        Node attributes must include ``dist_out``.  ``main_side`` and ``type``
        are read with safe defaults (0 and 1 respectively).
    sections_df : pd.DataFrame
        Output of ``build_section_graph`` with columns
        ``section_id``, ``reach_ids``, etc.
    region : str
        Region code (e.g. 'NA'). When provided, path_segs IDs are offset by
        the Pfafstetter continent code for global uniqueness.

    Returns
    -------
    Dict[int, Dict]
        Mapping reach_id -> {"path_freq": int, "stream_order": int,
        "path_segs": int, "path_order": int}.
    """
    log("Computing path variables (path_freq, stream_order, path_segs, path_order)...")

    if G.number_of_nodes() == 0:
        log("Empty graph, returning empty results")
        return {}

    # ------------------------------------------------------------------
    # 1. path_freq  (topological summation)
    # ------------------------------------------------------------------
    pf: Dict[int, int] = {}

    # Ghost reaches (type=6): unreliable topology, get -9999 and skip.
    # Side channels (main_side 1,2): valid topology, get computed pf
    # but are excluded from confluence sums to prevent double-counting
    # at braid reconvergences. is_mainstem handles routing downstream.
    ghosts: Set[int] = set()
    skip_in_sums: Set[int] = set()
    for node in G.nodes():
        node_type = G.nodes[node].get("type", 1)
        main_side = G.nodes[node].get("main_side", 0)
        if node_type == 6:
            ghosts.add(node)
            skip_in_sums.add(node)
            pf[node] = -9999
        elif main_side in (1, 2):
            skip_in_sums.add(node)

    def _sum_predecessors(node: int) -> int:
        """Sum pf from predecessors, excluding side channels and ghosts."""
        total = sum(
            pf[pred]
            for pred in G.predecessors(node)
            if pred not in skip_in_sums and pf.get(pred, 0) > 0
        )
        if total > 0:
            return total
        # All main predecessors excluded — inherit from any valid predecessor
        fallback = sum(
            pf[pred]
            for pred in G.predecessors(node)
            if pred not in ghosts and pf.get(pred, 0) > 0
        )
        return fallback if fallback > 0 else 1

    is_dag = nx.is_directed_acyclic_graph(G)

    if is_dag:
        # Fast path: topological sort
        for node in nx.topological_sort(G):
            if node in ghosts:
                continue
            if G.in_degree(node) == 0:
                pf[node] = 1
            else:
                pf[node] = _sum_predecessors(node)
    else:
        # Fallback: iterative worklist propagation for graphs with cycles.
        log("WARNING: Graph has cycles, using iterative worklist for path_freq")
        for node in G.nodes():
            if node in ghosts:
                continue
            if G.in_degree(node) == 0:
                pf[node] = 1
            else:
                pf[node] = 0

        # BFS from headwaters downstream
        queue: deque[int] = deque()
        for node in G.nodes():
            if node not in ghosts and G.in_degree(node) == 0:
                queue.append(node)

        visited_count: Dict[int, int] = defaultdict(int)
        max_iterations = G.number_of_nodes() * 4  # safety bound
        iterations = 0

        while queue and iterations < max_iterations:
            node = queue.popleft()
            iterations += 1
            if node in ghosts:
                continue

            if G.in_degree(node) == 0:
                new_pf = 1
            else:
                new_pf = _sum_predecessors(node)

            if new_pf != pf.get(node, 0):
                pf[node] = new_pf
                for succ in G.successors(node):
                    if succ not in ghosts:
                        visited_count[succ] += 1
                        if visited_count[succ] < max_iterations:
                            queue.append(succ)

        # Any node still at 0 gets fallback 1
        for node in G.nodes():
            if node not in ghosts and pf.get(node, 0) <= 0:
                pf[node] = 1

    # ------------------------------------------------------------------
    # 2. stream_order  = round(ln(path_freq)) + 1
    # ------------------------------------------------------------------
    so: Dict[int, int] = {}
    for node in G.nodes():
        freq = pf.get(node, -9999)
        if freq > 0:
            so[node] = int(round(math.log(freq))) + 1
        else:
            so[node] = -9999

    # ------------------------------------------------------------------
    # 3. path_segs  (section-based segment identifier)
    # ------------------------------------------------------------------
    from ..pfaf_offsets import pfaf_offset

    seg_offset = pfaf_offset(region) if region else 0

    ps: Dict[int, int] = {}
    reach_to_section: Dict[int, int] = {}

    if sections_df is not None and len(sections_df) > 0:
        for _, row in sections_df.iterrows():
            sid = seg_offset + int(row["section_id"]) + 1  # 1-based + offset
            for rid in row["reach_ids"]:
                rid = int(rid)
                reach_to_section[rid] = sid
                ps[rid] = sid

    # Junction reaches not in any section get unique IDs
    max_seg = max(ps.values()) if ps else seg_offset
    next_seg = max_seg + 1
    for node in G.nodes():
        if node not in ps:
            ps[node] = next_seg
            next_seg += 1

    # ------------------------------------------------------------------
    # 4. path_order  (rank by dist_out ASC within path_freq groups)
    # ------------------------------------------------------------------
    po: Dict[int, int] = {}

    # Group nodes by path_freq
    freq_groups: Dict[int, List[Tuple[float, int]]] = defaultdict(list)
    for node in G.nodes():
        freq = pf.get(node)
        dist = G.nodes[node].get("dist_out", 0)
        if pd.isna(dist):
            dist = 0
        freq_groups[freq].append((dist, node))

    for freq, members in freq_groups.items():
        # Sort by dist_out ascending, assign 1-based rank
        members.sort(key=lambda x: x[0])
        for rank, (_, node) in enumerate(members, start=1):
            po[node] = rank

    # ------------------------------------------------------------------
    # 5. Assemble results
    # ------------------------------------------------------------------
    results: Dict[int, Dict] = {}
    for node in G.nodes():
        if node in ghosts:
            results[node] = {
                "path_freq": -9999,
                "stream_order": -9999,
                "path_segs": -9999,
                "path_order": -9999,
            }
        else:
            results[node] = {
                "path_freq": pf.get(node, -9999),
                "stream_order": so.get(node, -9999),
                "path_segs": ps.get(node, -9999),
                "path_order": po.get(node, -9999),
            }

    # ------------------------------------------------------------------
    # 6. Validation logging
    # ------------------------------------------------------------------
    n_valid = sum(1 for v in pf.values() if v > 0)
    n_ghost = len(ghosts)
    log(f"path_freq: {n_valid:,} valid (>0), {n_ghost:,} ghosts (-9999)")

    # T002 monotonicity check (log-only)
    mono_violations = 0
    for u, v in G.edges():
        pf_u = pf.get(u, -9999)
        pf_v = pf.get(v, -9999)
        if pf_u > 0 and pf_v > 0 and pf_v < pf_u:
            mono_violations += 1
    if mono_violations > 0:
        log(f"WARNING: T002 path_freq monotonicity violations: {mono_violations:,}")
    else:
        log("T002 path_freq monotonicity: OK")

    # T010 headwater check (log-only)
    hw_violations = 0
    for node in G.nodes():
        if G.in_degree(node) == 0 and node not in ghosts:
            if pf.get(node, 0) < 1:
                hw_violations += 1
    if hw_violations > 0:
        log(f"WARNING: T010 headwater path_freq violations: {hw_violations:,}")
    else:
        log("T010 headwater path_freq: OK")

    log("Path variables computed")
    return results
