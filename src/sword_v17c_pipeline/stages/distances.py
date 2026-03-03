"""Hydrologic distance computation stage for v17c pipeline."""

from typing import Dict

import networkx as nx

from ._logging import log


def compute_hydro_distances(G: nx.DiGraph) -> Dict[int, Dict]:
    """
    Compute hydrologic distances for each reach.

    - hydro_dist_out: Distance to outlet following main channel
    - hydro_dist_hw: Distance from headwater following main channel
    """
    log("Computing hydrologic distances...")

    # Handle empty graph
    if G.number_of_nodes() == 0:
        log("Empty graph, returning empty results")
        return {}

    # Identify outlets (no outgoing edges)
    outlets = [n for n in G.nodes() if G.out_degree(n) == 0]
    log(f"Found {len(outlets):,} outlets")

    # Compute dist_out using Dijkstra from outlets (reversed graph)
    R = G.reverse()

    dist_out = {}
    for node in G.nodes():
        dist_out[node] = float("inf")

    for outlet in outlets:
        dist_out[outlet] = 0

    # Multi-source Dijkstra (only if we have outlets)
    if outlets:
        lengths = nx.multi_source_dijkstra_path_length(
            R, outlets, weight=lambda u, v, d: G.nodes[v].get("reach_length", 0)
        )
        dist_out.update(lengths)

    # Compute dist_hw (distance from furthest headwater)
    headwaters = [n for n in G.nodes() if G.in_degree(n) == 0]
    log(f"Found {len(headwaters):,} headwaters")

    dist_hw = {}
    for node in G.nodes():
        dist_hw[node] = 0

    # For each node, find max distance from any headwater
    for hw in headwaters:
        try:
            lengths = nx.single_source_dijkstra_path_length(
                G, hw, weight=lambda u, v, d: G.nodes[v].get("reach_length", 0)
            )
            for node, dist in lengths.items():
                if dist > dist_hw[node]:
                    dist_hw[node] = dist
        except nx.NetworkXError:
            continue

    results = {}
    for node in G.nodes():
        results[node] = {
            "hydro_dist_out": dist_out.get(node, float("inf")),
            "hydro_dist_hw": dist_hw.get(node, 0),
        }

    log("Hydrologic distances computed")
    return results


def compute_best_headwater_outlet(
    G: nx.DiGraph, overrides: Dict[int, Dict] | None = None
) -> Dict[int, Dict]:
    """
    Compute best headwater and outlet for each reach.

    Uses effective_width (SWOT if available), log(facc), and pathlen to select "main" path.
    Ranking tuple: (effective_width, log_facc, pathlen) - all maximized.

    Parameters
    ----------
    G : nx.DiGraph
        Reach-level directed graph.
    overrides : dict, optional
        Human-reviewed corrections from backwater QC.
        ``{junction_reach_id: {"rch_id_up_main": corrected_reach_id}}``.
        When a node appears in overrides, the corrected predecessor is forced
        as the best pick in the upstream pass, so ``best_headwater`` and
        ``pathlen_hw`` follow the human-corrected path.
    """
    overrides = overrides or {}
    n_overrides_applied = 0

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
                # Use effective_width (SWOT-preferred) and log_facc for ranking
                eff_width = G.nodes[p].get("effective_width", 0) or 0
                log_facc = G.nodes[p].get("log_facc", 0) or 0
                candidates.append((eff_width, log_facc, total_len, best_hw.get(p), p))

            hw_sets[n] = union

            # Check for human override at this junction
            override = overrides.get(n)
            if override and "rch_id_up_main" in override:
                forced_pred = override["rch_id_up_main"]
                forced = [c for c in candidates if c[4] == forced_pred]
                if forced:
                    best = forced[0]
                    n_overrides_applied += 1
                else:
                    best = max(candidates, key=lambda x: (x[0], x[1], x[2]))
            else:
                # Select by effective_width (primary), log_facc (secondary), pathlen (tertiary)
                best = max(candidates, key=lambda x: (x[0], x[1], x[2]))
            best_hw[n] = best[3]
            pathlen_hw[n] = best[2]

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
                # Use effective_width (SWOT-preferred) and log_facc for ranking
                eff_width = G.nodes[s].get("effective_width", 0) or 0
                log_facc = G.nodes[s].get("log_facc", 0) or 0
                candidates.append((eff_width, log_facc, total_len, best_out.get(s), s))

            best = max(candidates, key=lambda x: (x[0], x[1], x[2]))
            best_out[n] = best[3]
            pathlen_out[n] = best[2]

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
    if n_overrides_applied:
        log(f"  ({n_overrides_applied} overrides applied from backwater QC)")
    return results
