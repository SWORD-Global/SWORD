"""Hydrologic distance computation stage for v17c pipeline."""

import math
from typing import Dict, Optional

import networkx as nx

from ._logging import log
from .graph import routing_score


def compute_dijkstra_distances(G: nx.DiGraph) -> Dict[int, Dict]:
    """
    Compute shortest-path distance to any outlet via Dijkstra.

    Returns ``{reach_id: {"dist_out_dijkstra": float}}``.
    """
    log("Computing Dijkstra distances to outlets...")

    if G.number_of_nodes() == 0:
        log("Empty graph, returning empty results")
        return {}

    outlets = [n for n in G.nodes() if G.out_degree(n) == 0]
    log(f"Found {len(outlets):,} outlets")

    R = G.reverse()

    dist_out: Dict[int, float] = {n: float("inf") for n in G.nodes()}
    for outlet in outlets:
        dist_out[outlet] = 0

    if outlets:
        lengths = nx.multi_source_dijkstra_path_length(
            R, outlets, weight=lambda u, v, d: G.nodes[v].get("reach_length", 0)
        )
        dist_out.update(lengths)

    results = {}
    for node in G.nodes():
        results[node] = {
            "dist_out_dijkstra": dist_out.get(node, float("inf")),
        }

    log("Dijkstra distances computed")
    return results


# Keep old name as alias for backwards compatibility during transition
compute_hydro_distances = compute_dijkstra_distances


def compute_mainstem_distances(
    G: nx.DiGraph,
    main_neighbors: Dict[int, Dict],
) -> Dict[int, Dict]:
    """
    Compute distance to best_outlet by walking the rch_id_dn_main chain.

    For each reach, follow rch_id_dn_main until a terminal reach (NULL
    downstream or missing from graph), accumulating reach_length (including
    self).  Convention matches v17b dist_out: outlet reach gets its own
    reach_length, not 0.

    Returns ``{reach_id: {"hydro_dist_out": float}}``.
    """
    log("Computing mainstem distances (rch_id_dn_main chain walk)...")

    if G.number_of_nodes() == 0:
        return {}

    # Build lookup: reach_id → rch_id_dn_main
    dn_main: Dict[int, Optional[int]] = {}
    for rid, nb in main_neighbors.items():
        dn = nb.get("rch_id_dn_main")
        if dn is not None and dn in G.nodes:
            dn_main[rid] = dn
        else:
            dn_main[rid] = None

    # Cache: once a reach's distance is known, reuse it
    cache: Dict[int, float] = {}

    def _walk(start: int) -> float:
        """Walk downstream from *start*, return total distance."""
        if start in cache:
            return cache[start]

        path: list[int] = []
        visited: set[int] = set()
        cur = start

        while cur is not None and cur not in cache:
            if cur in visited:
                raise RuntimeError(f"Cycle in rch_id_dn_main chain: {path + [cur]}")
            visited.add(cur)
            path.append(cur)
            cur = dn_main.get(cur)

        # cur is either None (terminal) or already cached
        suffix = cache[cur] if cur is not None else 0.0

        # Walk backwards through path, filling cache
        cumulative = suffix
        for rid in reversed(path):
            cumulative += G.nodes[rid].get("reach_length", 0)
            cache[rid] = cumulative

        return cache[start]

    results: Dict[int, Dict] = {}
    for rid in G.nodes():
        if rid not in dn_main:
            # Reach not in main_neighbors (e.g. ghost) — use own length
            dist = G.nodes[rid].get("reach_length", 0)
        else:
            dist = _walk(rid)
        results[rid] = {"hydro_dist_out": dist}

    n_terminal = sum(1 for r in dn_main.values() if r is None)
    log(
        f"Mainstem distances: {len(results):,} reaches, "
        f"{n_terminal:,} terminal (NULL rch_id_dn_main)"
    )
    return results


def compute_best_headwater_outlet(G: nx.DiGraph) -> Dict[int, Dict]:
    """
    Compute best headwater and outlet for each reach.

    Uses a weighted scalar score learned from 1,967 human-labeled junction
    decisions: 1.97*log1p(ew) + 0.23*log1p(facc) - 0.23*log1p(slope) +
    0.23*log1p(pathlen) + 0.29*stream_order.  See ``graph.ROUTING_WEIGHTS``.

    WARNING: Do NOT add an overrides parameter here. Forcing a predecessor in
    the upstream pass cascades through pathlen_hw/pathlen_out, changing
    best_outlet for thousands of downstream reaches. Overrides belong only
    in compute_main_neighbors, which directly sets rch_id_up_main/rch_id_dn_main
    without cascading.
    """

    log("Computing best headwater/outlet assignments...")

    if not nx.is_directed_acyclic_graph(G):
        log("WARNING: Graph has cycles, computing on largest DAG component")
        # Find strongly connected components and work on DAG of SCCs
        # For simplicity, we'll proceed but results may be incomplete
        pass

    try:
        topo = list(nx.topological_sort(G))
    except nx.NetworkXUnfeasible:
        log("ERROR: Graph has cycles, cannot compute topological sort")
        return {}

    # Upstream pass: track headwater sets and choose best
    hw_sets = {n: set() for n in G.nodes()}
    best_hw = {}
    pathlen_hw = {}

    for n in topo:
        preds = list(G.predecessors(n))

        if not preds:
            # Headwater
            hw_sets[n] = {n}
            best_hw[n] = n
            pathlen_hw[n] = 0
        else:
            # Merge headwater sets from predecessors
            union = set()
            candidates = []

            for p in preds:
                union |= hw_sets[p]
                reach_len = G.nodes[n].get("reach_length", 0)
                total_len = pathlen_hw.get(p, 0) + reach_len
                pattrs = G.nodes[p]
                ew = math.log1p(max(pattrs.get("effective_width", 0) or 0, 0))
                lf = pattrs.get("log_facc", 0) or 0
                sl = pattrs.get("slope", 0) or 0
                so = pattrs.get("stream_order", 0) or 0
                score = routing_score(ew, lf, sl, total_len, so)
                candidates.append((score, best_hw.get(p), p, total_len))

            hw_sets[n] = union

            best = max(candidates, key=lambda x: x[0])
            best_hw[n] = best[1]
            pathlen_hw[n] = best[3]

    # Downstream pass: track outlet and choose best
    best_out = {}
    pathlen_out = {}

    for n in reversed(topo):
        succs = list(G.successors(n))

        if not succs:
            # Outlet
            best_out[n] = n
            pathlen_out[n] = 0
        else:
            candidates = []

            for s in succs:
                reach_len = G.nodes[s].get("reach_length", 0)
                total_len = pathlen_out.get(s, 0) + reach_len
                sattrs = G.nodes[s]
                ew = math.log1p(max(sattrs.get("effective_width", 0) or 0, 0))
                lf = sattrs.get("log_facc", 0) or 0
                sl = sattrs.get("slope", 0) or 0
                so = sattrs.get("stream_order", 0) or 0
                score = routing_score(ew, lf, sl, total_len, so)
                candidates.append((score, best_out.get(s), s, total_len))

            best = max(candidates, key=lambda x: x[0])
            best_out[n] = best[1]
            pathlen_out[n] = best[3]

    results = {}
    for node in G.nodes():
        results[node] = {
            "best_headwater": best_hw.get(node),
            "best_outlet": best_out.get(node),
            "pathlen_hw": pathlen_hw.get(node, 0),
            "pathlen_out": pathlen_out.get(node, 0),
            "path_freq": len(hw_sets.get(node, set())),
        }

    log("Best headwater/outlet computed")
    return results
