"""Mainstem computation stage for v17c pipeline."""

from collections import defaultdict
from typing import Dict

import networkx as nx

from ._logging import log
from ..pfaf_offsets import pfaf_offset


def compute_main_paths(
    G: nx.DiGraph, hw_out_attrs: Dict[int, Dict], region: str = ""
) -> Dict[int, int]:
    """
    Compute main_path_id for each reach.

    Reaches are grouped by (best_headwater, best_outlet) pairs. Each group
    is then checked for connectivity; if a group forms multiple disconnected
    components, each component gets a unique ID.

    Parameters
    ----------
    G : nx.DiGraph
        Reach-level directed graph.
    hw_out_attrs : dict
        Output from ``compute_best_headwater_outlet``.
    region : str
        Region code (e.g. 'NA'). When provided, IDs are offset by the
        Pfafstetter continent code for global uniqueness.

    Returns dict {reach_id: main_path_id}.
    """
    log("Computing main paths (main_path_id)...")

    offset = pfaf_offset(region) if region else 0

    # Group nodes by (best_headwater, best_outlet)
    groups = defaultdict(list)
    for node, attrs in hw_out_attrs.items():
        key = (attrs["best_headwater"], attrs["best_outlet"])
        if key[0] is not None and key[1] is not None:
            groups[key].append(node)

    results = {}
    next_id = 1

    for (hw, out), nodes in groups.items():
        # Check connectivity within this (hw, out) group
        H = G.subgraph(nodes)
        components = list(nx.weakly_connected_components(H))

        for component in components:
            path_id = offset + next_id
            for node in component:
                results[node] = path_id
            next_id += 1

    log(f"Main paths: {next_id - 1:,} unique paths identified")
    return results


def compute_mainstem(
    G: nx.DiGraph,
    hw_out_attrs: Dict[int, Dict],
    main_neighbors: Dict[int, Dict] | None = None,
) -> Dict[int, bool]:
    """
    Compute is_mainstem for each reach.

    For each weakly-connected network, finds the outlet (no successors), looks
    up its best_headwater, then walks the rch_id_dn_main chain from that
    headwater to the outlet.  Only reaches on that single chain are mainstem.

    Parameters
    ----------
    G : nx.DiGraph
        Reach-level directed graph.
    hw_out_attrs : dict
        Output from ``compute_best_headwater_outlet``.
    main_neighbors : dict, optional
        Output from ``compute_main_neighbors``.  Required for the
        rch_id_dn_main chain walk.  If not provided, falls back to the
        legacy shortest-path approach (produces ~98% True — avoid).
    """
    log("Computing mainstem classification...")

    is_mainstem = {n: False for n in G.nodes()}

    if main_neighbors is None:
        log("WARNING: main_neighbors not provided, skipping mainstem (all False)")
        return is_mainstem

    # Build rch_id_dn_main lookup
    dn_main: Dict[int, int | None] = {}
    for rid, nb in main_neighbors.items():
        dn = nb.get("rch_id_dn_main")
        if dn is not None and dn in G.nodes:
            dn_main[rid] = dn
        else:
            dn_main[rid] = None

    # For each weakly-connected component, trace one mainstem
    n_networks = 0
    for component in nx.weakly_connected_components(G):
        # Find outlets (no successors in the full graph)
        outlets = [n for n in component if G.out_degree(n) == 0]
        if not outlets:
            continue

        # Pick the outlet with highest facc (primary), then width (tiebreak).
        # facc is more reliable than width for identifying the true
        # hydrological outlet — estuary/delta reaches are wide but short.
        best_outlet_node = max(
            outlets,
            key=lambda n: (
                G.nodes[n].get("log_facc", 0) or 0,
                G.nodes[n].get("effective_width", 0) or 0,
            ),
        )

        # Get best_headwater for this outlet
        hw_attrs = hw_out_attrs.get(best_outlet_node, {})
        headwater = hw_attrs.get("best_headwater")
        if headwater is None:
            continue

        # Walk rch_id_dn_main from headwater to outlet
        # Ghost reaches (type=6) are excluded — they should never be mainstem
        visited = set()
        cur = headwater
        while cur is not None and cur not in visited:
            if G.nodes[cur].get("type") != 6:
                is_mainstem[cur] = True
            visited.add(cur)
            cur = dn_main.get(cur)

        n_networks += 1

    n_ghosts = sum(1 for n in G.nodes() if G.nodes[n].get("type") == 6)
    n_mainstem = sum(is_mainstem.values())
    n_total = len(G.nodes())
    pct = 100 * n_mainstem / n_total if n_total > 0 else 0
    log(
        f"Mainstem reaches: {n_mainstem:,} / {n_total:,} ({pct:.1f}%) "
        f"across {n_networks:,} networks ({n_ghosts:,} ghosts excluded)"
    )

    return is_mainstem


def compute_main_neighbors(
    G: nx.DiGraph,
    hw_out_attrs: Dict[int, Dict] | None = None,
    overrides: Dict[int, Dict] | None = None,
) -> Dict[int, Dict]:
    """
    Compute rch_id_up_main and rch_id_dn_main for each reach.

    For each node, selects the main upstream predecessor and main downstream
    successor using the same (effective_width, log_facc, pathlen) ranking
    used by best_headwater/best_outlet, ensuring consistent routing across
    all v17c columns.

    Parameters
    ----------
    G : nx.DiGraph
        Reach-level directed graph.
    hw_out_attrs : dict, optional
        Output from ``compute_best_headwater_outlet``.  Provides
        ``pathlen_hw`` and ``pathlen_out`` per reach as a third tiebreaker
        so that width/facc ties are resolved identically to the best-outlet
        algorithm.
    overrides : dict, optional
        Human-reviewed corrections from backwater QC.
        ``{junction_reach_id: {"rch_id_up_main": corrected_reach_id}}``.
        When a node appears in overrides, the override value is used instead
        of the algorithmic ranking.

    Returns dict {reach_id: {'rch_id_up_main': int|None, 'rch_id_dn_main': int|None}}.
    """
    overrides = overrides or {}
    hw_out_attrs = hw_out_attrs or {}
    n_overrides_applied = 0

    log("Computing main neighbors (rch_id_up_main / rch_id_dn_main)...")

    def _up_key(n: int) -> tuple:
        return (
            G.nodes[n].get("effective_width", 0) or 0,
            G.nodes[n].get("log_facc", 0) or 0,
            (hw_out_attrs.get(n, {}).get("pathlen_hw", 0) or 0)
            + (G.nodes[n].get("reach_length", 0) or 0),
        )

    def _dn_key(n: int) -> tuple:
        return (
            G.nodes[n].get("effective_width", 0) or 0,
            G.nodes[n].get("log_facc", 0) or 0,
            (hw_out_attrs.get(n, {}).get("pathlen_out", 0) or 0)
            + (G.nodes[n].get("reach_length", 0) or 0),
        )

    results = {}

    for node in G.nodes():
        # Main upstream neighbor: pick best predecessor
        preds = list(G.predecessors(node))
        if preds:
            override = overrides.get(node)
            if override and "rch_id_up_main" in override:
                candidate = override["rch_id_up_main"]
                if candidate in preds:
                    rch_id_up_main = candidate
                    n_overrides_applied += 1
                else:
                    log(
                        f"WARNING: override for {node} specifies "
                        f"rch_id_up_main={candidate} but it is not a "
                        f"predecessor (preds={preds}), falling back to ranking"
                    )
                    rch_id_up_main = max(preds, key=_up_key)
            else:
                rch_id_up_main = max(preds, key=_up_key)
        else:
            rch_id_up_main = None

        # Main downstream neighbor: pick best successor
        succs = list(G.successors(node))
        if succs:
            rch_id_dn_main = max(succs, key=_dn_key)
        else:
            rch_id_dn_main = None

        results[node] = {
            "rch_id_up_main": rch_id_up_main,
            "rch_id_dn_main": rch_id_dn_main,
        }

    n_with_up = sum(1 for v in results.values() if v["rch_id_up_main"] is not None)
    n_with_dn = sum(1 for v in results.values() if v["rch_id_dn_main"] is not None)
    log(f"Main neighbors: {n_with_up:,} with up_main, {n_with_dn:,} with dn_main")
    if n_overrides_applied:
        log(f"  ({n_overrides_applied} overrides applied from backwater QC)")

    return results
