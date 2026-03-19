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
    main_paths: Dict[int, int] | None = None,
) -> tuple[Dict[int, bool], Dict[int, Dict]]:
    """
    Compute is_mainstem for each reach via greedy walk within each main_path_id group.

    For each main_path_id group, finds the shared best_headwater and walks
    downstream picking the successor with max(effective_width, log_facc,
    pathlen_out + reach_length) at each bifurcation.

    Parameters
    ----------
    G : nx.DiGraph
        Reach-level directed graph.
    hw_out_attrs : dict
        Output from compute_best_headwater_outlet.
    main_paths : dict, optional
        Output from compute_main_paths {reach_id: main_path_id}.

    Returns
    -------
    tuple[Dict[int, bool], Dict[int, Dict]]
        (is_mainstem, mainstem_chain)
        is_mainstem: {reach_id: True/False}
        mainstem_chain: {reach_id: {'chain_pred': int|None, 'chain_succ': int|None}}
        Only mainstem reaches appear in mainstem_chain.
    """
    log("Computing mainstem classification...")

    is_mainstem = {n: False for n in G.nodes()}
    mainstem_chain: Dict[int, Dict] = {}

    if main_paths is None:
        log("WARNING: main_paths not provided, skipping mainstem (all False)")
        return is_mainstem, mainstem_chain

    # Group reaches by main_path_id
    path_groups: Dict[int, list] = defaultdict(list)
    for rid, pid in main_paths.items():
        path_groups[pid].append(rid)

    def _dn_key(n: int) -> tuple:
        return (
            G.nodes[n].get("effective_width", 0) or 0,
            G.nodes[n].get("log_facc", 0) or 0,
            (hw_out_attrs.get(n, {}).get("pathlen_out", 0) or 0)
            + (G.nodes[n].get("reach_length", 0) or 0),
        )

    n_networks = 0
    for pid, members in path_groups.items():
        member_set = set(members)

        # Find the shared best_headwater for this group
        # All members should share the same best_headwater
        headwater = None
        for rid in members:
            hw = hw_out_attrs.get(rid, {}).get("best_headwater")
            if hw is not None:
                headwater = hw
                break

        if headwater is None:
            continue

        # Greedy walk from headwater through members of this group
        chain = []  # ordered list of reach_ids on the chain
        visited = set()
        cur = headwater

        while cur is not None and cur not in visited:
            visited.add(cur)
            if cur in member_set:
                # Ghost reaches (type=6) are excluded from mainstem
                if G.nodes[cur].get("type") != 6:
                    is_mainstem[cur] = True
                chain.append(cur)

            # Pick best successor that is in this group
            succs = [
                s for s in G.successors(cur) if s in member_set and s not in visited
            ]
            if not succs:
                # Also try successors outside the group (headwater might not be in group)
                if cur not in member_set:
                    succs = [s for s in G.successors(cur) if s not in visited]
                if not succs:
                    break
            cur = max(succs, key=_dn_key)

        # Build chain predecessor/successor lookup
        for i, rid in enumerate(chain):
            pred = chain[i - 1] if i > 0 else None
            succ = chain[i + 1] if i < len(chain) - 1 else None
            mainstem_chain[rid] = {"chain_pred": pred, "chain_succ": succ}

        n_networks += 1

    n_ghosts = sum(1 for n in G.nodes() if G.nodes[n].get("type") == 6)
    n_mainstem = sum(is_mainstem.values())
    n_total = len(G.nodes())
    pct = 100 * n_mainstem / n_total if n_total > 0 else 0
    log(
        f"Mainstem reaches: {n_mainstem:,} / {n_total:,} ({pct:.1f}%) "
        f"across {n_networks:,} networks ({n_ghosts:,} ghosts excluded)"
    )

    return is_mainstem, mainstem_chain


def compute_main_neighbors(
    G: nx.DiGraph,
    hw_out_attrs: Dict[int, Dict] | None = None,
    overrides: Dict[int, Dict] | None = None,
    mainstem_chain: Dict[int, Dict] | None = None,
) -> Dict[int, Dict]:
    """
    Compute rch_id_up_main and rch_id_dn_main for each reach.

    For mainstem reaches (present in mainstem_chain), neighbors are derived
    directly from the chain. For non-mainstem reaches, uses the
    (effective_width, log_facc, pathlen) ranking.

    Parameters
    ----------
    G : nx.DiGraph
        Reach-level directed graph.
    hw_out_attrs : dict, optional
        Output from compute_best_headwater_outlet.
    overrides : dict, optional
        Human-reviewed corrections from backwater QC.
    mainstem_chain : dict, optional
        Output from compute_mainstem. {reach_id: {'chain_pred': ..., 'chain_succ': ...}}.
        When provided, mainstem reaches get neighbors from the chain.

    Returns dict {reach_id: {'rch_id_up_main': int|None, 'rch_id_dn_main': int|None}}.
    """
    overrides = overrides or {}
    hw_out_attrs = hw_out_attrs or {}
    mainstem_chain = mainstem_chain or {}
    n_overrides_applied = 0
    n_from_chain = 0

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
        chain_info = mainstem_chain.get(node)

        # Check for overrides first
        override = overrides.get(node)

        if chain_info is not None and not override:
            # Mainstem reach: derive from chain
            chain_pred = chain_info.get("chain_pred")
            chain_succ = chain_info.get("chain_succ")

            # Verify chain_pred is actually a predecessor in the graph
            preds = set(G.predecessors(node))
            rch_id_up_main = chain_pred if chain_pred in preds else None
            # If chain_pred not in preds (shouldn't happen), fall back to ranking
            if rch_id_up_main is None and preds:
                rch_id_up_main = max(preds, key=_up_key)

            # Verify chain_succ is actually a successor in the graph
            succs = set(G.successors(node))
            rch_id_dn_main = chain_succ if chain_succ in succs else None
            if rch_id_dn_main is None and succs:
                rch_id_dn_main = max(succs, key=_dn_key)

            n_from_chain += 1
        else:
            # Non-mainstem reach or override: use ranking
            preds = list(G.predecessors(node))
            if preds:
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
    log(
        f"  ({n_from_chain:,} from mainstem chain, {len(results) - n_from_chain:,} from ranking)"
    )
    if n_overrides_applied:
        log(f"  ({n_overrides_applied} overrides applied from backwater QC)")

    return results
