import sys

print("[IMPORT] Starting imports...", file=sys.stderr, flush=True)

import networkx as nx

print("[IMPORT] networkx imported", file=sys.stderr, flush=True)

import numpy as np

print("[IMPORT] numpy imported", file=sys.stderr, flush=True)

from itertools import product
from collections import defaultdict
import pickle
import argparse

print("[IMPORT] Importing duckdb...", file=sys.stderr, flush=True)
import duckdb

print("[IMPORT] duckdb imported", file=sys.stderr, flush=True)

import os
import gc
from functools import lru_cache
from datetime import datetime

# Region offsets for globally unique main_path_id values.
# Each region gets a 1M-wide ID band so IDs never collide across regions.
REGION_OFFSETS = {
    "AF": 1_000_000,
    "AS": 2_000_000,
    "EU": 3_000_000,
    "NA": 4_000_000,
    "OC": 5_000_000,
    "SA": 6_000_000,
}

print("[IMPORT] Importing SWORD_graph...", file=sys.stderr, flush=True)
from SWORD_graph import load_sword_data, create_edges_gdf, create_network_nodes_gdf

print("[IMPORT] SWORD_graph imported", file=sys.stderr, flush=True)

print("[IMPORT] All imports complete", file=sys.stderr, flush=True)


def normalized_score(data, w=1, m=0.0, c=0.0, pl=0.0, minimize=[]):

    assert len({len(v) for v in data.values()}) == 1, (
        "All lists must have the same length"
    )
    weights = {"swot_width": w, "meand_len": m, "hw": c, "reach_len": pl}
    if len(minimize) > 0:
        for minVal in minimize:
            data[minVal] = [1 / v for v in data[minimize]]

    max_idx = data["swot_width"].index(max(data["swot_width"]))
    # 3. Normalize safely (avoid divide-by-zero)
    normalized = {}
    for k, v in data.items():
        ref_val = v[max_idx]
        if ref_val == 0:
            normalized[k] = [0 for _ in v]  # all zeros if denominator is zero
        else:
            normalized[k] = [x / ref_val for x in v]

    # 4. Apply weights directly and 5. sum to get scores per row
    scores = [
        sum(normalized[k][i] * weights[k] for k in data.keys())
        for i in range(len(next(iter(data.values()))))
    ]
    return scores


# add for downstream outlet the number of bifurcations
# check if it works correctly at multiple places
def compute_up_down_best(
    G,
    length_attr="reach_len",
    width_attr="swot_width",
    meand_attr="meand_len",
    widthW=0.6,
    meandW=0.0,
    hwW=0.2,
    path_lenHwW=0.2,
    path_lenOutW=0,
    outW=0.4,
):
    """
    Decisions in headwater/outlet selection:
    - First choice is number of headwaters
    - second decider is number of outlets (possible to combine them but is not currently done) --> only for outlet selection
    - third choice is length of path --> can be something like facc
    - tiebreaker ??
    Updates node attributes with (and create nodenetwork dataframe):
      - headwater_sets: node -> set(headwaters reaching node)
      - upstream_count: node -> len(headwater_sets[node])
      - best_hw: node -> chosen headwater upstream (by freq then pathlen then name)
      - pathlen_hw: node -> cumulative length along chosen headwater path
      - downstream_hw_sets: node -> set(headwaters reachable downstream (including joins))
      - down_count: node -> len(downstream_hw_sets[node])
      - best_out: node -> chosen outlet downstream (by additional headwaters then total then pathlen)
      - pathlen_out: node -> cumulative length to chosen outlet
    """
    from datetime import datetime

    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] compute_up_down_best: Starting...",
        flush=True,
    )
    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] compute_up_down_best: Checking if graph is DAG...",
        flush=True,
    )
    assert nx.is_directed_acyclic_graph(G), "Graph must be a DAG"
    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] compute_up_down_best: Graph is DAG",
        flush=True,
    )
    assert isinstance(G, nx.MultiDiGraph), (
        f"Expected MultiDiGraph, got {type(G).__name__}"
    )

    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] compute_up_down_best: Computing topological sort...",
        flush=True,
    )
    topo = list(nx.topological_sort(G))
    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] compute_up_down_best: Topological sort completed: {len(topo):,} nodes",
        flush=True,
    )

    # ---- Upstream pass ----
    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] compute_up_down_best: Initializing upstream pass data structures...",
        flush=True,
    )
    headwater_sets = {n: set() for n in G.nodes}
    upstream_count = {}
    best_hw = {}
    pathlen_hw = {}

    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] compute_up_down_best: Starting upstream pass over {len(topo):,} nodes...",
        flush=True,
    )
    node_count = 0
    for n in topo:
        node_count += 1
        if node_count % 10000 == 0:
            print(
                f"[{datetime.now().strftime('%H:%M:%S')}] compute_up_down_best:   Processed {node_count:,}/{len(topo):,} nodes in upstream pass...",
                flush=True,
            )
        preds = list(G.predecessors(n))
        if not preds:
            headwater_sets[n] = {n}
            upstream_count[n] = 1
            best_hw[n] = n
            pathlen_hw[n] = 0.0
        else:
            unionset = set()
            candidates = []
            values = {name: [] for name in [width_attr, meand_attr, length_attr, "hw"]}
            for p in preds:
                unionset |= headwater_sets[p]
                # length of edge p -> n (handle MultiDiGraph)
                u, v = p, n
                control_attr = []

                for attr in [width_attr, meand_attr, length_attr]:
                    if attr == "reach_len":
                        val = min(data.get(attr, 1) for key, data in G[u][v].items())
                    else:
                        val = max(data.get(attr, 1) for key, data in G[u][v].items())

                    if (attr == "swot_width") & (val == -9999):
                        val = max(data.get("width", 1) for key, data in G[u][v].items())
                    values[attr].append(val)
                    control_attr.append(val)

                length = pathlen_hw[p] + values["reach_len"][-1]
                values[length_attr][-1] += length
                values["hw"].append(upstream_count[p])
                candidates.append((length, best_hw[p]))

            headwater_sets[n] = unionset
            upstream_count[n] = len(unionset)

            scores = normalized_score(values, widthW, meandW, hwW, path_lenHwW)

            best = candidates[np.argmax(scores)]
            pathlen_hw[n] = best[0]
            best_hw[n] = best[1]

    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] compute_up_down_best: Upstream pass completed",
        flush=True,
    )

    # ---- Downstream pass ----
    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] compute_up_down_best: Initializing downstream pass data structures...",
        flush=True,
    )
    downstream_hw_sets = {n: set() for n in G.nodes}
    downstream_ot_sets = {n: set() for n in G.nodes}
    down_count_out = {}
    best_out_out = {}

    down_count = {}
    best_out = {}
    pathlen_out = {}

    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] compute_up_down_best: Starting downstream pass over {len(topo):,} nodes...",
        flush=True,
    )
    node_count = 0
    for n in reversed(topo):
        node_count += 1
        if node_count % 10000 == 0:
            print(
                f"[{datetime.now().strftime('%H:%M:%S')}] compute_up_down_best:   Processed {node_count:,}/{len(topo):,} nodes in downstream pass...",
                flush=True,
            )
        succs = list(G.successors(n))
        if not succs:
            # outlet: downstream headwaters are the headwaters that reach this outlet node
            downstream_ot_sets[n] = {n}
            down_count_out[n] = 1
            best_out_out[n] = n

            downstream_hw_sets[n] = set(headwater_sets[n])
            down_count[n] = len(downstream_hw_sets[n])
            best_out[n] = n
            pathlen_out[n] = 0.0
        else:
            unionset = set()
            unionsetO = set()
            candidates = []
            values = {name: [] for name in [width_attr, meand_attr, length_attr, "hw"]}
            for s in succs:
                unionset |= downstream_hw_sets[s]
                unionsetO |= downstream_ot_sets[s]

                u, v = n, s
                control_attr = []

                for attr in [width_attr, meand_attr, length_attr]:
                    if attr == "reach_len":
                        val = min(data.get(attr, 1) for key, data in G[u][v].items())
                    else:
                        val = max(data.get(attr, 1) for key, data in G[u][v].items())

                    if (attr == "swot_width") & (val == -9999):
                        val = max(data.get("width", 1) for key, data in G[u][v].items())
                    values[attr].append(val)
                    control_attr.append(val)
                length = pathlen_out[s] + control_attr[2]
                values[length_attr][-1] += length
                values["hw"].append(len(headwater_sets[s]) + down_count_out[s])
                # score = decider(control_attr[0], control_attr[1], upstream_count[p], length)
                candidates.append((length, best_out[s]))

                # how many new headwaters would this successor path contribute that are not already at n
                additional = len(downstream_hw_sets[s] - headwater_sets[n])
                total_down_at_s = len(downstream_hw_sets[s])

            # headwater sets in downstream direction
            downstream_hw_sets[n] = unionset
            down_count[n] = len(unionset)

            # outlet sets
            downstream_ot_sets[n] = unionsetO
            down_count_out[n] = len(unionsetO)

            # choose successor by (additional, total down count, pathlen) (final tie by outlet name)
            # best = max(candidates, key=lambda x: (x[0]))
            scores = normalized_score(values, widthW, meandW, outW, path_lenOutW)
            best = candidates[np.argmax(scores)]
            best_out[n] = best[1]  # best_out of the chosen successor
            pathlen_out[n] = best[0]

    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] compute_up_down_best: Downstream pass completed",
        flush=True,
    )

    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] compute_up_down_best: Creating upDownDict...",
        flush=True,
    )
    upDownDict = {
        "best_headwater": best_hw,
        "best_outlet": best_out,
        "path_freq": upstream_count,
        "outlet_count": down_count_out,
        "pathlen_hw": pathlen_hw,
        "pathlen_out": pathlen_out,
        # 'down_count': down_count
    }

    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] compute_up_down_best: Setting node attributes...",
        flush=True,
    )
    for attr_name, values in upDownDict.items():
        nx.set_node_attributes(G, values, attr_name)
    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] compute_up_down_best: Node attributes set, function complete",
        flush=True,
    )


def check_main_path_id_components(G, continent):
    # Validate continent is known (IDs were already offset by main_path())
    if continent.upper() not in REGION_OFFSETS:
        raise ValueError(f"Unknown continent: {continent}")
    # --- 1. Group edges by main_path_id ---
    groups = defaultdict(list)
    for u, v, k, d in G.edges(keys=True, data=True):
        groups[d["main_path_id"]].append((u, v, k))
    next_id = max(groups.keys()) + 1  # stays in this region's offset band
    # --- 2. Process each main_path_id group ---
    for m, edge_list in groups.items():
        H = G.edge_subgraph(edge_list)  # view containing only these edges
        # weak components returns sets of nodes
        components = list(nx.weakly_connected_components(H))
        if len(components) > 1:
            print(f"main_path_id {m} must be split into {len(components)} components")
            # --- 3. Assign new main_path_id values ---
            # Keep the first component under the original id (optional)
            for comp_idx, comp_nodes in enumerate(components):
                if comp_idx == 0:
                    new_id = m  # keep original for first component
                else:
                    new_id = next_id
                    next_id += 1
                # Update edges belonging to this component
                for u, v, k, d in G.edges(keys=True, data=True):
                    if d["main_path_id"] == m and u in comp_nodes and v in comp_nodes:
                        d["main_path_id"] = new_id


def main_path(DG, continent):
    from datetime import datetime

    offset = REGION_OFFSETS[continent.upper()]
    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] main_path: Starting (region={continent.upper()}, offset={offset:,})...",
        flush=True,
    )
    groups = {}
    edge_count = 0
    total_edges = DG.number_of_edges()
    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] main_path: Computing main_path_id for {total_edges:,} edges...",
        flush=True,
    )
    for u, v, k, data in DG.edges(keys=True, data=True):
        edge_count += 1
        if edge_count % 10000 == 0:
            print(
                f"    [{datetime.now().strftime('%H:%M:%S')}] Processed {edge_count:,}/{total_edges:,} edges ({100 * edge_count / total_edges:.1f}%)"
            )

        bhw, bow = DG.nodes[u].get("best_headwater"), DG.nodes[v]["best_outlet"]
        DG[u][v][k]["best_headwater"] = bhw
        DG[u][v][k]["best_outlet"] = bow

        DG[u][v][k]["pathlen_hw"] = DG.nodes[u]["pathlen_hw"]
        DG[u][v][k]["pathlen_out"] = DG.nodes[u]["pathlen_out"]
        DG[u][v][k]["path_freq"] = DG.nodes[u]["path_freq"]
        DG[u][v][k]["outlet_count"] = DG.nodes[v]["outlet_count"]
        # DG[u][v][k]["down_count"] = DG.nodes[v]['down_count']

        key = (bhw, bow)
        if key not in groups:
            groups[key] = []
        groups[key].append((u, v, k))

    # Step 2: assign a unique main_path_id to each group (offset by region)
    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] main_path: Assigning main_path_id to {len(groups):,} groups (offset={offset:,})...",
        flush=True,
    )
    for idx, ((bhw, bow), edges) in enumerate(groups.items(), start=1):
        if idx % 1000 == 0:
            print(
                f"[{datetime.now().strftime('%H:%M:%S')}] main_path:   Processed {idx:,}/{len(groups):,} groups...",
                flush=True,
            )
        path_id = offset + idx
        for u, v, k in edges:
            DG[u][v][k]["main_path_id"] = path_id

    print(f"[{datetime.now().strftime('%H:%M:%S')}] main_path: Complete", flush=True)
    check_main_path_id_components(DG, continent)


def all_simple_paths_with_keys(G, source, target, cutoff=None):
    """Return all simple node paths and their associated edge key lists.

    If cutoff=None, automatically uses cutoff = min(10 * shortest_path_length, 90) to prevent
    infinite hangs in exponential search spaces. You can override by passing a specific
    cutoff value.
    """
    from datetime import datetime
    import time

    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] all_simple_paths_with_keys: Starting - source={source}, target={target}, cutoff={cutoff}",
        flush=True,
    )

    # Check graph size
    num_nodes = G.number_of_nodes()
    num_edges = G.number_of_edges()
    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] all_simple_paths_with_keys: Graph has {num_nodes:,} nodes, {num_edges:,} edges",
        flush=True,
    )

    # Check if path exists and get shortest path length
    try:
        shortest_len = nx.shortest_path_length(G, source, target)
        print(
            f"[{datetime.now().strftime('%H:%M:%S')}] all_simple_paths_with_keys: Shortest path length: {shortest_len}",
            flush=True,
        )
    except nx.NetworkXNoPath:
        print(
            f"[{datetime.now().strftime('%H:%M:%S')}] all_simple_paths_with_keys: No path exists between source and target",
            flush=True,
        )
        return

    # Apply automatic cutoff if none provided: 10x shortest path length, but cap at 90
    if cutoff is None:
        cutoff = min(shortest_len * 10, 90)
        print(
            f"[{datetime.now().strftime('%H:%M:%S')}] all_simple_paths_with_keys: No cutoff provided, using automatic cutoff={cutoff} (10x shortest path length={shortest_len}, capped at 90)",
            flush=True,
        )

    # Check node degrees
    source_out = G.out_degree(source)
    target_in = G.in_degree(target)
    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] all_simple_paths_with_keys: Source out-degree: {source_out}, Target in-degree: {target_in}",
        flush=True,
    )

    # Estimate worst-case path count (very rough heuristic)
    # This is exponential in worst case, but gives a sense of scale
    if False:  # Disabled now that we always use cutoff
        avg_out_degree = num_edges / num_nodes if num_nodes > 0 else 0

        # Try to get a better sense of the graph structure
        # Count nodes reachable from source (gives sense of search space)
        try:
            reachable_from_source = (
                len(nx.descendants(G, source)) + 1
            )  # +1 for source itself
            # Estimate: if there are cycles or multiple paths, this can explode
            # Worst case: exponential in path length
            # With no cutoff, paths could be up to num_nodes long
            # But NetworkX limits to simple paths (no repeated nodes), so max is num_nodes
            max_path_len = min(num_nodes, shortest_len * 10)  # Reasonable upper bound

            # Very rough: if branching factor is avg_out_degree, and we have cycles,
            # number of paths could be exponential: branching_factor^path_length
            # But this is worst-case - actual depends heavily on graph structure
            if avg_out_degree > 1.5:
                # High branching - could be exponential
                worst_case_paths_estimate = int(
                    avg_out_degree ** min(max_path_len, 20)
                )  # Cap at 20 to avoid overflow
                if worst_case_paths_estimate > 1e12:
                    worst_case_paths_estimate = float("inf")
                    print(
                        f"[{datetime.now().strftime('%H:%M:%S')}] all_simple_paths_with_keys: ⚠️  WARNING - Exponential search space detected!",
                        flush=True,
                    )
                    print(
                        f"[{datetime.now().strftime('%H:%M:%S')}] all_simple_paths_with_keys: ⚠️  Graph: {num_nodes:,} nodes, {num_edges:,} edges, avg out-degree={avg_out_degree:.2f}",
                        flush=True,
                    )
                    print(
                        f"[{datetime.now().strftime('%H:%M:%S')}] all_simple_paths_with_keys: ⚠️  Reachable from source: {reachable_from_source:,} nodes",
                        flush=True,
                    )
                    print(
                        f"[{datetime.now().strftime('%H:%M:%S')}] all_simple_paths_with_keys: ⚠️  Without cutoff, this could take HOURS or DAYS (or hang forever)",
                        flush=True,
                    )
                    print(
                        f"[{datetime.now().strftime('%H:%M:%S')}] all_simple_paths_with_keys: ⚠️  STRONGLY RECOMMENDED: Use cutoff parameter (e.g., cutoff={shortest_len * 3})",
                        flush=True,
                    )
                else:
                    print(
                        f"[{datetime.now().strftime('%H:%M:%S')}] all_simple_paths_with_keys: ⚠️  ROUGH ESTIMATE - Could explore ~{worst_case_paths_estimate:,} paths",
                        flush=True,
                    )
                    print(
                        f"[{datetime.now().strftime('%H:%M:%S')}] all_simple_paths_with_keys: ⚠️  At 1000 paths/sec: ~{worst_case_paths_estimate / 1000:.1f}s ({worst_case_paths_estimate / 1000 / 60:.1f} min)",
                        flush=True,
                    )
                    print(
                        f"[{datetime.now().strftime('%H:%M:%S')}] all_simple_paths_with_keys: ⚠️  At 100 paths/sec: ~{worst_case_paths_estimate / 100:.1f}s ({worst_case_paths_estimate / 100 / 60:.1f} min)",
                        flush=True,
                    )
            else:
                # Low branching - probably linear or polynomial
                print(
                    f"[{datetime.now().strftime('%H:%M:%S')}] all_simple_paths_with_keys: Graph has low branching (avg out-degree={avg_out_degree:.2f}), should be manageable",
                    flush=True,
                )
        except Exception as e:
            print(
                f"[{datetime.now().strftime('%H:%M:%S')}] all_simple_paths_with_keys: Could not estimate path count: {e}",
                flush=True,
            )

    start_time = time.time()
    path_count = 0
    seen = set()
    longest_path_length = 0
    last_path_time = start_time
    last_progress_time = start_time

    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] all_simple_paths_with_keys: Calling nx.all_simple_paths (cutoff={cutoff})...",
        flush=True,
    )

    # Create generator - this returns immediately
    path_generator = nx.all_simple_paths(G, source=source, target=target, cutoff=cutoff)
    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] all_simple_paths_with_keys: Generator created, starting iteration...",
        flush=True,
    )

    try:
        for path in path_generator:
            path_count += 1
            longest_path_length = max(longest_path_length, len(path))
            elapsed = time.time() - start_time
            time_since_last_path = time.time() - last_path_time
            last_path_time = time.time()

            # Report progress more frequently, especially if paths are coming slowly
            if path_count == 1:
                print(
                    f"[{datetime.now().strftime('%H:%M:%S')}] all_simple_paths_with_keys: Found first path (length={len(path)}) after {elapsed:.2f}s",
                    flush=True,
                )
            elif path_count <= 10:
                # Report every path for first 10
                print(
                    f"[{datetime.now().strftime('%H:%M:%S')}] all_simple_paths_with_keys: Found path {path_count} (length={len(path)}, time_since_last={time_since_last_path:.2f}s)",
                    flush=True,
                )
            elif path_count % 10 == 0:
                rate = path_count / elapsed if elapsed > 0 else 0
                avg_time_per_path = elapsed / path_count
                print(
                    f"[{datetime.now().strftime('%H:%M:%S')}] all_simple_paths_with_keys: Found {path_count:,} paths (elapsed: {elapsed:.2f}s, rate: {rate:.1f} paths/sec, avg: {avg_time_per_path:.3f}s/path)",
                    flush=True,
                )
            elif path_count % 100 == 0:
                rate = path_count / elapsed if elapsed > 0 else 0
                print(
                    f"[{datetime.now().strftime('%H:%M:%S')}] all_simple_paths_with_keys: Found {path_count:,} paths (elapsed: {elapsed:.2f}s, rate: {rate:.1f} paths/sec)",
                    flush=True,
                )

            # Warn if paths are coming very slowly (more than 5 seconds between paths)
            if time_since_last_path > 5.0 and path_count > 1:
                print(
                    f"[{datetime.now().strftime('%H:%M:%S')}] all_simple_paths_with_keys: ⚠️  WARNING - {time_since_last_path:.1f}s since last path (path_count={path_count:,})",
                    flush=True,
                )
                print(
                    f"[{datetime.now().strftime('%H:%M:%S')}] all_simple_paths_with_keys: ⚠️  Generator is exploring large search space, this may take a while...",
                    flush=True,
                )

            # Progress heartbeat every 30 seconds if we're still iterating
            if time.time() - last_progress_time > 30.0:
                print(
                    f"[{datetime.now().strftime('%H:%M:%S')}] all_simple_paths_with_keys: Still iterating... {path_count:,} paths found so far (elapsed: {elapsed:.1f}s)",
                    flush=True,
                )
                last_progress_time = time.time()

            # Process key combinations
            key_start = time.time()
            key_lists = [list(G[u][v].keys()) for u, v in zip(path[:-1], path[1:])]
            num_combos = 1
            for kl in key_lists:
                num_combos *= len(kl)

            combo_count = 0
            for combo in product(*key_lists):
                combo_count += 1
                tup = (tuple(path), tuple(combo))
                if tup not in seen:  # avoid duplicates
                    seen.add(tup)
                    yield list(path), list(combo)

            if num_combos > 1 and path_count <= 5:
                key_elapsed = time.time() - key_start
                print(
                    f"[{datetime.now().strftime('%H:%M:%S')}] all_simple_paths_with_keys: Path {path_count} had {num_combos:,} key combinations, {combo_count:,} unique, took {key_elapsed:.3f}s",
                    flush=True,
                )

        total_elapsed = time.time() - start_time
        print(
            f"[{datetime.now().strftime('%H:%M:%S')}] all_simple_paths_with_keys: COMPLETE - Found {path_count:,} paths, {len(seen):,} unique path+key combinations in {total_elapsed:.2f}s",
            flush=True,
        )
    except Exception as e:
        elapsed = time.time() - start_time
        print(
            f"[{datetime.now().strftime('%H:%M:%S')}] all_simple_paths_with_keys: ERROR after {elapsed:.2f}s, {path_count:,} paths found: {e}",
            flush=True,
        )
        raise


def assign_mainstem_with_threshold_all(
    G,
    width_dict,
    width_col="width",
    length_attr="reach_len",
    reach_id_attr="reach_id",
    edge_rules=None,
    score_threshold=0.05,
    width_weight=0.6,
    hw_weight=0.4,
):
    """Core mainstem selector (unchanged recursive logic)."""
    from datetime import datetime

    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] assign_mainstem_with_threshold_all: Starting (graph has {G.number_of_nodes():,} nodes, {G.number_of_edges():,} edges)...",
        flush=True,
    )
    if edge_rules is None:
        edge_rules = [("reach_len", 1.0, "max")]

    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] assign_mainstem_with_threshold_all: Computing topological sort...",
        flush=True,
    )
    topo = list(nx.topological_sort(G))
    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] assign_mainstem_with_threshold_all: Topological sort completed",
        flush=True,
    )

    def edge_score(u, v, k):
        data = G[u][v][k]
        rid = data.get(reach_id_attr)
        w = width_dict.get(rid, [data.get(width_col, 0)])
        ml = data.get("meand_len", 0)
        return np.median(w), ml

    def aggregate_path_metrics(path):
        widths, meand, lens, hw = [], [], [], []
        for i, (u, v) in enumerate(zip(path[0][:-1], path[0][1:])):
            k = path[1][i]
            data = G[u][v][k]
            rid = data.get("reach_id")
            if rid in width_dict:
                widths.extend(width_dict[rid])
            meand.append(data.get("meand_len", 1))
            lens.append(data.get("reach_len", 1))
            hw.append(data.get("path_freq", 1))

        if len(widths) == 0:
            for i, (u, v) in enumerate(zip(path[0][:-1], path[0][1:])):
                k = path[1][i]
                data = G[u][v][k]
                w = data.get("width", 0)
                if w > 0:
                    widths.append(w)
        if len(widths) == 0:
            width = 0
        else:
            width = np.median(widths)

        return width, np.median(meand), np.sum(lens), np.max(hw)

    def find_first_common_descendant(G, nodes):
        print(
            f"[{datetime.now().strftime('%H:%M:%S')}] find_first_common_descendant: Finding common descendant for {len(nodes):,} nodes...",
            flush=True,
        )
        print(
            f"[{datetime.now().strftime('%H:%M:%S')}] find_first_common_descendant: Computing descendants (this may be slow)...",
            flush=True,
        )
        desc = [set(nx.descendants(G, n)) | {n} for n in nodes]
        print(
            f"[{datetime.now().strftime('%H:%M:%S')}] find_first_common_descendant: Descendants computed, finding intersection...",
            flush=True,
        )
        common = set.intersection(*desc)
        if not common:
            print(
                f"[{datetime.now().strftime('%H:%M:%S')}] find_first_common_descendant: No common descendant found",
                flush=True,
            )
            return None
        result = min(common, key=lambda x: topo.index(x))
        print(
            f"[{datetime.now().strftime('%H:%M:%S')}] find_first_common_descendant: Found common descendant: {result}",
            flush=True,
        )
        return result

    def select_best_parallel_edge(u, v):
        edges = list(G[u][v].items())
        if len(edges) == 1:
            return edges[0][0]

        data = {"swot_width": [], "meand_len": []}
        data["swot_width"], data["meand_len"] = map(
            list, zip(*(edge_score(u, v, k) for k, _ in edges))
        )
        scores = normalized_score(data, w=1)
        best_k = np.argmax(scores)

        return best_k

    def mark_path(path):
        # for u, v in zip(path[:-1], path[1:]):
        # best_k = select_best_parallel_edge(u, v)
        for i, (u, v) in enumerate(zip(path[0][:-1], path[0][1:])):
            # print(f"i: {i}")
            # print(f"u: {u}, v: {v}")
            # print(f"path: {path[1]}")
            # print(f"path: {path[1][i]}")
            k = path[1][i]
            G[u][v][k]["is_mainstem_edge"] = True
            G.nodes[v]["is_mainstem"] = True

    def best_path_to_confluence(G, s, t, return_score=False):
        print(
            f"[{datetime.now().strftime('%H:%M:%S')}] best_path_to_confluence: Finding paths from {s} to {t}...",
            flush=True,
        )
        branch_info = []
        if isinstance(s, str):
            print(
                f"[{datetime.now().strftime('%H:%M:%S')}] best_path_to_confluence: Calling all_simple_paths_with_keys (single source)...",
                flush=True,
            )
            paths = list(all_simple_paths_with_keys(G, source=s, target=t))
            print(
                f"[{datetime.now().strftime('%H:%M:%S')}] best_path_to_confluence: Found {len(paths):,} paths",
                flush=True,
            )
        else:
            print(
                f"[{datetime.now().strftime('%H:%M:%S')}] best_path_to_confluence: Calling all_simple_paths_with_keys (multiple sources: {len(s)})...",
                flush=True,
            )
            paths = []
            for idx, so in enumerate(s):
                if idx % 10 == 0:
                    print(
                        f"[{datetime.now().strftime('%H:%M:%S')}] best_path_to_confluence:   Processing source {idx + 1}/{len(s)}: {so}...",
                        flush=True,
                    )
                paths.extend(list(all_simple_paths_with_keys(G, source=so, target=t)))
            print(
                f"[{datetime.now().strftime('%H:%M:%S')}] best_path_to_confluence: Found {len(paths):,} total paths",
                flush=True,
            )
        values = {name: [] for name in ["swot_width", "meand_len", "hw", "reach_len"]}
        for path in paths:
            # try:#width, np.median(meand), np.sum(lens), np.max(hw)
            w, m, l, h = aggregate_path_metrics(path)
            values["swot_width"].append(w)
            values["meand_len"].append(m)
            values["hw"].append(h)
            values["reach_len"].append(1 / l)

            branch_info.append(path)
        # except nx.NetworkXNoPath:
        #     continue
        if not branch_info:
            return
        scores = normalized_score(values, w=width_weight, c=hw_weight)
        best_idx = np.argmax(scores)
        if return_score:
            return branch_info[best_idx], np.max(scores)
        else:
            return branch_info[best_idx]

    node_process_count = [0]  # Use list to allow modification in nested function

    def process_node(current):
        node_process_count[0] += 1
        if node_process_count[0] % 100 == 0:
            print(
                f"[{datetime.now().strftime('%H:%M:%S')}] process_node: Processed {node_process_count[0]:,} nodes...",
                flush=True,
            )
        G.nodes[current]["is_mainstem"] = True
        succs = list(G.successors(current))
        if not succs:
            return

        if len(succs) == 1:
            nxt = succs[0]
            best_k = select_best_parallel_edge(
                current, nxt
            )  ## parallel edge selection!!!!!!

            G[current][nxt][best_k]["is_mainstem_edge"] = True
            G.nodes[nxt]["is_mainstem"] = True
            # print(G[current][nxt][0]['is_mainstem_edge'])
            # print(G[current][nxt][1]['is_mainstem_edge'])

            process_node(nxt)
            return

        if len(succs) > 1:
            print(
                f"[{datetime.now().strftime('%H:%M:%S')}] process_node: Node {current} has {len(succs):,} successors, finding confluence...",
                flush=True,
            )
        confluence = find_first_common_descendant(G, succs)
        if confluence is None:
            # print(current, succs)
            candidates = []
            values = {name: [] for name in ["swot_width", "meand_len", "hw"]}
            ks = []
            for s in succs:
                edges = list(G[current][s].items())
                k = 0
                if len(edges) > 1:
                    k = select_best_parallel_edge(current, s)
                ks.append(k)
                for attr in ["swot_width", "meand_len"]:
                    val = G[current][s][k].get(attr, 1)
                    if (attr == "swot_width") & (val == -9999):
                        val = G[current][s][k].get("width", 1)
                    values[attr].append(val)

                values["hw"].append(
                    G[current][s][k].get("path_freq", 0)
                    + G[current][s][k].get("outlet_freq", 0)
                )

            bestidx = np.argmax(
                normalized_score(
                    values,
                    w=width_weight,
                    c=hw_weight,
                )
            )
            best_succ = succs[bestidx]
            G[current][best_succ][ks[bestidx]]["is_mainstem_edge"] = True
            G.nodes[best_succ]["is_mainstem"] = True
            process_node(best_succ)
            return

        bp = best_path_to_confluence(G, current, confluence)
        if bp is None:
            return
        # print(bp)
        mark_path(bp)
        process_node(confluence)

    headwater_node_types = [
        u
        for u, d in G.nodes(data=True)
        if G.in_degree(u) == 0 and d.get("node_type") == "Head_water"
    ]

    if headwater_node_types:
        # One or more found
        headwaters = headwater_node_types
    else:
        headwaters = [n for n in topo if G.in_degree(n) == 0]
        if len(headwaters) != 1:
            components = list(nx.weakly_connected_components(G))
            if len(components) > 1:
                headwaters = []
                for comp in components:
                    G1 = nx.subgraph(G, comp)
                    h1 = [n for n in topo if G1.in_degree(n) == 0]

                    if len(h1) == 1:
                        headwaters.append(h1[0])
                    else:
                        conf = find_first_common_descendant(G1, h1)

                        bp = best_path_to_confluence(G1, h1, conf)
                        headwaters.extend([n for n in bp[0] if G.in_degree(n) == 0])
            else:
                conf = find_first_common_descendant(G, headwaters)
                bp = best_path_to_confluence(G, headwaters, conf)
                headwaters = [n for n in bp[0] if G.in_degree(n) == 0]

    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] assign_mainstem_with_threshold_all: Processing {len(headwaters):,} headwaters...",
        flush=True,
    )
    for idx, hw in enumerate(headwaters, 1):
        if idx % 10 == 0 or idx == len(headwaters):
            print(
                f"[{datetime.now().strftime('%H:%M:%S')}] assign_mainstem_with_threshold_all: Processing headwater {idx}/{len(headwaters)}: {hw}...",
                flush=True,
            )
        process_node(hw)
    # process_node('81250500031-81250500041-81250500051')
    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] assign_mainstem_with_threshold_all: Complete",
        flush=True,
    )
    return G


def assign_mainstem_by_mainpath(
    G,
    width_dict,
    width_col="width",
    mainpath_attr="main_path_id",
    width_weight=0.6,
    hw_weight=0.4,
    **kwargs,
):
    """
    Apply the mainstem selection separately for each main_path_id group.

    This is cleaner and usually faster: each subnetwork is processed in isolation.
    """
    from datetime import datetime

    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] assign_mainstem_by_mainpath: Starting...",
        flush=True,
    )
    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] assign_mainstem_by_mainpath: Initializing attributes...",
        flush=True,
    )
    nx.set_node_attributes(G, False, "is_mainstem")
    for u, v, k in G.edges(keys=True):
        G[u][v][k]["is_mainstem_edge"] = False

    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] assign_mainstem_by_mainpath: Collecting main_path_ids...",
        flush=True,
    )
    all_ids = set(
        data.get(mainpath_attr)
        for _, _, _, data in G.edges(keys=True, data=True)
        if data.get(mainpath_attr) is not None
    )

    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] assign_mainstem_by_mainpath: Processing {len(all_ids):,} main path groups...",
        flush=True,
    )
    for idx, mpid in enumerate(all_ids, 1):
        if idx % 100 == 0 or idx == len(all_ids):
            print(
                f"[{datetime.now().strftime('%H:%M:%S')}] assign_mainstem_by_mainpath: Processing main path {idx:,}/{len(all_ids):,} (path_id={mpid})",
                flush=True,
            )

        print(
            f"[{datetime.now().strftime('%H:%M:%S')}] assign_mainstem_by_mainpath:   Extracting edges for path_id={mpid}...",
            flush=True,
        )
        # Extract subgraph for this main_path_id
        edges = [
            (u, v, k)
            for u, v, k, d in G.edges(keys=True, data=True)
            if d.get(mainpath_attr) == mpid
        ]
        if not edges:
            continue

        print(
            f"[{datetime.now().strftime('%H:%M:%S')}] assign_mainstem_by_mainpath:   Creating subgraph with {len(edges):,} edges...",
            flush=True,
        )
        H = G.edge_subgraph(edges).copy()
        print(
            f"[{datetime.now().strftime('%H:%M:%S')}] assign_mainstem_by_mainpath:   Subgraph created, calling assign_mainstem_with_threshold_all...",
            flush=True,
        )

        # Run the mainstem logic on this subgraph
        assign_mainstem_with_threshold_all(
            H,
            width_dict,
            width_col=width_col,
            width_weight=width_weight,
            hw_weight=hw_weight,
            **kwargs,
        )
        print(
            f"[{datetime.now().strftime('%H:%M:%S')}] assign_mainstem_by_mainpath:   assign_mainstem_with_threshold_all completed, copying results back...",
            flush=True,
        )

        # Copy the results back into the main graph
        for u, v, k, d in H.edges(keys=True, data=True):
            if d.get("is_mainstem_edge", False):
                G[u][v][k]["is_mainstem_edge"] = True
                G.nodes[v]["is_mainstem"] = True

        print(
            f"[{datetime.now().strftime('%H:%M:%S')}] assign_mainstem_by_mainpath:   Results copied back for path_id={mpid}",
            flush=True,
        )

    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] assign_mainstem_by_mainpath: Complete",
        flush=True,
    )


def distance_measures(G):
    # -----------------------------------------------------
    # Identify outlet and headwater nodes
    # -----------------------------------------------------
    outlet_nodes = [n for n in G.nodes() if G.out_degree(n) == 0]
    # headwater_nodes = [n for n in G.nodes() if G.in_degree(n) == 0]

    # -----------------------------------------------------
    # Helper: build a directed graph for Dijkstra
    # -----------------------------------------------------
    # DG = nx.DiGraph()
    # for u, v, k, d in G.edges(keys=True, data=True):
    #     DG.add_edge(u, v, weight=d.get("reach_len", 0.0))

    DG = G.copy()
    # -----------------------------------------------------
    # 1. Compute dist_out = shortest-path distance to any outlet
    # -----------------------------------------------------
    # Multi-source Dijkstra backwards from outlets
    dist_out_node = {n: float("inf") for n in G.nodes()}
    for outlet in outlet_nodes:
        dist_out_node[outlet] = 0.0

    # Run Dijkstra from all outlets
    # (We reverse edges so distances propagate upstream)
    revDG = DG.reverse()
    dist_out_node.update(
        nx.multi_source_dijkstra_path_length(revDG, outlet_nodes, weight="weight")
    )

    # -----------------------------------------------------
    # 2 & 3: Hydro distance functions (downstream & upstream)
    # -----------------------------------------------------

    # Use caching to avoid recomputation
    @lru_cache(None)
    def hydro_dist_out_from_node(node):
        """Follow rch_id_dn_main repeatedly until outlet."""
        # Stop if outlet
        if G.out_degree(node) == 0:
            return 0.0

        # Get all outgoing edges
        edges = [(u, v, k, d) for u, v, k, d in G.out_edges(node, keys=True, data=True)]

        # Try to find the chosen downstream main reach
        for u, v, k, d in edges:
            if d.get("rch_id_dn_main") == d.get("reach_id"):
                # main path edge found
                return d["reach_len"] + hydro_dist_out_from_node(v)

        # Fallback: pick the first (should be rare)
        u, v, k, d = edges[0]
        return d["reach_len"] + hydro_dist_out_from_node(v)

    @lru_cache(None)
    def hydro_dist_hw_from_node(node):
        """Follow rch_id_up_main repeatedly until headwater."""
        # Stop at headwater
        if G.in_degree(node) == 0:
            return 0.0

        # Get all incoming edges
        edges = [(u, v, k, d) for u, v, k, d in G.in_edges(node, keys=True, data=True)]

        # Try to find selected upstream main reach
        for u, v, k, d in edges:
            if d.get("rch_id_up_main") == d.get("reach_id"):
                return d["reach_len"] + hydro_dist_hw_from_node(u)

        # fallback
        u, v, k, d = edges[0]
        return d["reach_len"] + hydro_dist_hw_from_node(u)

    # -----------------------------------------------------
    # Assign hydro distances to each edge
    # -----------------------------------------------------
    for u, v, k, d in G.edges(keys=True, data=True):
        # downstream hydro pathlength
        d["hydro_dist_out"] = hydro_dist_out_from_node(v)

        # upstream hydro pathlength
        d["hydro_dist_hw"] = hydro_dist_hw_from_node(u)


def assign_main_connection(G):
    # ---------------------------------------------------------
    # Helper function for selecting main upstream/downstream reach
    # ---------------------------------------------------------
    def choose_main_reach(candidates):
        """
        candidates: list of (reach_id, attributes_dict)
        Applies your logic:
            1. If all have same main_path_id:
                a) If any has is_mainstem_edge == True → choose that one
                b) Else choose the one with largest width
            2. If not same main_path_id:
                → choose the one with is_mainstem_edge == True
        """
        if not candidates:
            return None

        # Unpack
        reach_ids = [c[0] for c in candidates]
        attrs = [c[1] for c in candidates]
        main_path_ids = {a["main_path_id"] for a in attrs}

        # CASE 1 — all candidates share same main_path_id
        if len(main_path_ids) == 1:
            # a) Prefer is_mainstem_edge == True
            stem_edges = [
                (rid, a) for rid, a in candidates if a.get("is_mainstem_edge") is True
            ]
            if stem_edges:
                return stem_edges[0][0]

            # b) Otherwise pick largest width
            sorted_by_width = sorted(
                candidates, key=lambda x: x[1].get("width", 0), reverse=True
            )
            return sorted_by_width[0][0]

        # CASE 2 — different main_path_id → pick is_mainstem_edge == True
        stem_edges = [
            (rid, a) for rid, a in candidates if a.get("is_mainstem_edge") is True
        ]
        if stem_edges:
            return stem_edges[0][0]

        # Fallback: pick largest width
        sorted_by_width = sorted(
            candidates, key=lambda x: x[1].get("width", 0), reverse=True
        )
        return sorted_by_width[0][0]

    # ---------------------------------------------------------
    # Compute rch_id_up_main and rch_id_dn_main for each reach
    # ---------------------------------------------------------
    for u, v, k, data in G.edges(keys=True, data=True):
        reach_id = data["reach_id"]

        # find upstream edges ending at u
        upstream_edges = [
            (attrs["reach_id"], attrs)
            for uu, vv, kk, attrs in G.in_edges(u, keys=True, data=True)
        ]

        # find downstream edges starting from v
        downstream_edges = [
            (attrs["reach_id"], attrs)
            for uu, vv, kk, attrs in G.out_edges(v, keys=True, data=True)
        ]

        # choose main upstream reach
        data["rch_id_up_main"] = choose_main_reach(upstream_edges)

        # choose main downstream reach
        data["rch_id_dn_main"] = choose_main_reach(downstream_edges)


def parquet_to_df_optimized(path):
    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] parquet_to_df_optimized: Starting DuckDB query...",
        flush=True,
    )
    df = duckdb.query(f"SELECT * FROM read_parquet('{path}')").to_df()
    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] parquet_to_df_optimized: DuckDB query completed, DataFrame shape: {df.shape}",
        flush=True,
    )

    # Convert to pandas-specific optimizations
    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] parquet_to_df_optimized: Converting junction_id to category...",
        flush=True,
    )
    df["junction_id"] = df["junction_id"].astype("category")
    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] parquet_to_df_optimized: Category conversion completed",
        flush=True,
    )

    # Downcast numeric types
    # NOTE: reach_id must be int64, not int32, because values like 71120000293 exceed int32 max (2,147,483,647)
    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] parquet_to_df_optimized: Downcasting numeric types...",
        flush=True,
    )
    df = df.astype(
        {
            "node_id": "int32",
            "section_id": "int32",
            "reach_id": "int64",  # Changed from int32 to prevent overflow
            "section_node_size": "int32",
            "pass_size": "float32",
            "distance": "float32",
        }
    )
    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] parquet_to_df_optimized: Type conversion completed",
        flush=True,
    )

    # timestamps already optimal
    return df


def main():
    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] Starting assign_attribute main()...",
        flush=True,
    )

    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] Setting up argument parser...",
        flush=True,
    )
    ap = argparse.ArgumentParser()
    ap.add_argument("--graph", required=True)
    ap.add_argument("--continent", required=True)
    ap.add_argument("--inputdir", required=True)
    ap.add_argument("--outdir", required=True)
    # ap.add_argument("--prefer_highs",action="store_true")
    ap.add_argument("--width-weight", type=float, default=0.6)
    ap.add_argument("--freq-hw-weight", type=float, default=0.2)
    ap.add_argument("--dist-hw-weight", type=float, default=0.2)
    ap.add_argument("--freq-out-weight", type=float, default=0.4)
    ap.add_argument("--dist-out-weight", type=float, default=0.0)

    print(f"[{datetime.now().strftime('%H:%M:%S')}] Parsing arguments...", flush=True)
    args = ap.parse_args()
    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] Arguments parsed: graph={args.graph}, continent={args.continent}",
        flush=True,
    )

    # Normalize continent to uppercase for consistent handling
    continent = args.continent.upper()
    directory = args.inputdir

    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] Creating output directory: {args.outdir}...",
        flush=True,
    )
    os.makedirs(args.outdir, exist_ok=True)
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Output directory ready", flush=True)

    graph_path = f"{directory}/output/{continent}/{args.graph}"
    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] Loading graph from {graph_path}...",
        flush=True,
    )

    # Check if file exists and get size
    if not os.path.exists(graph_path):
        raise FileNotFoundError(f"Graph file not found: {graph_path}")
    file_size = os.path.getsize(graph_path)
    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] Graph file exists: {file_size / (1024 * 1024):.2f} MB",
        flush=True,
    )

    print(f"[{datetime.now().strftime('%H:%M:%S')}] Opening file...", flush=True)
    with open(graph_path, "rb") as f:
        print(
            f"[{datetime.now().strftime('%H:%M:%S')}] File opened, starting pickle.load()...",
            flush=True,
        )
        G = pickle.load(f)
        print(
            f"[{datetime.now().strftime('%H:%M:%S')}] pickle.load() completed",
            flush=True,
        )

    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] Graph loaded: {G.number_of_nodes():,} nodes, {G.number_of_edges():,} edges",
        flush=True,
    )

    print(f"[{datetime.now().strftime('%H:%M:%S')}] Loading SWORD data...", flush=True)
    sword_data_dir = directory + "/data/"
    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] SWORD data directory: {sword_data_dir}",
        flush=True,
    )

    reaches_file = f"{sword_data_dir}{continent}_sword_reaches_v17b.gpkg"
    nodes_file = f"{sword_data_dir}{continent}_sword_nodes_v17b.gpkg"

    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] Checking SWORD files exist...",
        flush=True,
    )
    if not os.path.exists(reaches_file):
        raise FileNotFoundError(f"Reaches file not found: {reaches_file}")
    if not os.path.exists(nodes_file):
        raise FileNotFoundError(f"Nodes file not found: {nodes_file}")

    reaches_size = os.path.getsize(reaches_file) / (1024 * 1024)
    nodes_size = os.path.getsize(nodes_file) / (1024 * 1024)
    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] Reaches file: {reaches_size:.2f} MB, Nodes file: {nodes_size:.2f} MB",
        flush=True,
    )

    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] Calling load_sword_data()...",
        flush=True,
    )
    df_input, dfNode_input = load_sword_data(
        data_dir=sword_data_dir, continent=continent, include_nodes=True
    )
    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] load_sword_data() returned",
        flush=True,
    )

    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] Computing DataFrame lengths...",
        flush=True,
    )
    df_input_len = len(df_input)
    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] df_input length: {df_input_len:,}",
        flush=True,
    )
    dfNode_input_len = len(dfNode_input)
    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] dfNode_input length: {dfNode_input_len:,}",
        flush=True,
    )

    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] SWORD data loaded: {df_input_len:,} reaches, {dfNode_input_len:,} nodes",
        flush=True,
    )
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Moving to next step...", flush=True)
    # dfNetworkEdge = create_edges_gdf(G, df_input, directory = '', save = False)
    # dfNetworkNode = create_network_nodes_gdf(G, directory = '', save = False, explode = False)

    # df_swot_nodes = open_SWOT_files(dfNode_input['node_id'].to_list(), directory, continent=continent, uncertainty_threshold = None)
    # df_swot_nodes = merge_SWOT_node_distance(df_swot_nodes, dfNode_input, dfNetworkEdge, dfNetworkNode)

    swot_parquet_path = (
        directory + f"/output/{continent}/{continent}_swot_nodes.parquet"
    )
    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] Loading SWOT nodes parquet from {swot_parquet_path}...",
        flush=True,
    )

    # Check if file exists and get size
    if not os.path.exists(swot_parquet_path):
        raise FileNotFoundError(f"SWOT parquet file not found: {swot_parquet_path}")
    swot_file_size = os.path.getsize(swot_parquet_path) / (1024 * 1024)
    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] SWOT parquet file exists: {swot_file_size:.2f} MB",
        flush=True,
    )

    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] Calling parquet_to_df_optimized()...",
        flush=True,
    )
    df_swot_nodes = parquet_to_df_optimized(swot_parquet_path)
    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] parquet_to_df_optimized() returned",
        flush=True,
    )

    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] Computing SWOT nodes length...",
        flush=True,
    )
    swot_len = len(df_swot_nodes)
    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] SWOT nodes loaded: {swot_len:,} rows",
        flush=True,
    )

    # Fix corrupted negative reach_ids (caused by int32 overflow in previous versions)
    # If reach_id is negative and graph reach_ids are positive, convert negative to positive
    # This handles the case where int32 overflow occurred: large positive -> negative
    # if 'reach_id' in df_swot_nodes.columns:
    #     negative_count = (df_swot_nodes['reach_id'] < 0).sum()
    #     if negative_count > 0:
    #         print(f"WARNING: Found {negative_count:,} negative reach_ids (likely int32 overflow). Attempting correction...")
    #         # Convert negative int32 overflow back to positive by adding 2^32
    #         # This works if the original value was between 2^31 and 2^32-1
    #         mask_negative = df_swot_nodes['reach_id'] < 0
    #         df_swot_nodes.loc[mask_negative, 'reach_id'] = df_swot_nodes.loc[mask_negative, 'reach_id'] + (2**32)
    #         print(f"Corrected {negative_count:,} negative reach_ids by adding 2^32")

    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] Processing SWOT width data in DuckDB...",
        flush=True,
    )

    # Check if width column exists by querying the parquet file schema
    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] Checking for width column in parquet file...",
        flush=True,
    )
    schema_check = duckdb.query(
        f"DESCRIBE SELECT * FROM read_parquet('{swot_parquet_path}')"
    ).df()
    has_width = "width" in schema_check["column_name"].values

    if not has_width:
        print(
            f"WARNING: 'width' column not found in SWOT nodes. Available columns: {schema_check['column_name'].tolist()}",
            flush=True,
        )
        # Create empty dicts so all values will be -9999
        swotWidthDict = {}
        swotWidthMedianDict = {}
    else:
        # Use DuckDB to filter and aggregate directly from parquet file
        SENTINEL = -999_999_999_999.0

        # First, get initial count
        print(
            f"[{datetime.now().strftime('%H:%M:%S')}] Counting total rows...",
            flush=True,
        )
        initial_count = (
            duckdb.query(
                f"SELECT COUNT(*) as cnt FROM read_parquet('{swot_parquet_path}')"
            )
            .df()["cnt"]
            .iloc[0]
        )
        print(
            f"[{datetime.now().strftime('%H:%M:%S')}] Initial count: {initial_count:,}",
            flush=True,
        )

        # Filter and aggregate in DuckDB
        print(
            f"[{datetime.now().strftime('%H:%M:%S')}] Filtering and aggregating widths in DuckDB...",
            flush=True,
        )

        # Query to get lists of widths per reach_id (for swotWidthDict)
        # DuckDB's LIST() function collects values into an array
        width_list_query = f"""
            SELECT 
                CAST(reach_id AS BIGINT) AS reach_id,
                LIST(width) AS widths
            FROM read_parquet('{swot_parquet_path}')
            WHERE width IS NOT NULL 
                AND width != {SENTINEL}
                AND width >= 30
            GROUP BY reach_id
        """

        # Query to get medians per reach_id (for swotWidthMedianDict)
        width_median_query = f"""
            SELECT 
                CAST(reach_id AS BIGINT) AS reach_id,
                MEDIAN(width) AS width_median
            FROM read_parquet('{swot_parquet_path}')
            WHERE width IS NOT NULL 
                AND width != {SENTINEL}
                AND width >= 30
            GROUP BY reach_id
        """

        print(
            f"[{datetime.now().strftime('%H:%M:%S')}] Executing width list aggregation query...",
            flush=True,
        )
        width_list_df = duckdb.query(width_list_query).df()
        print(
            f"[{datetime.now().strftime('%H:%M:%S')}] Width list query completed: {len(width_list_df):,} reach_ids",
            flush=True,
        )

        print(
            f"[{datetime.now().strftime('%H:%M:%S')}] Executing width median aggregation query...",
            flush=True,
        )
        width_median_df = duckdb.query(width_median_query).df()
        print(
            f"[{datetime.now().strftime('%H:%M:%S')}] Width median query completed: {len(width_median_df):,} reach_ids",
            flush=True,
        )

        # Convert DuckDB results to Python dictionaries
        # DuckDB LIST() returns as a Python list, so we can use it directly
        print(
            f"[{datetime.now().strftime('%H:%M:%S')}] Converting to dictionaries...",
            flush=True,
        )
        swotWidthDict = dict(zip(width_list_df["reach_id"], width_list_df["widths"]))
        swotWidthMedianDict = dict(
            zip(width_median_df["reach_id"], width_median_df["width_median"])
        )

        filtered_count = sum(len(widths) for widths in swotWidthDict.values())
        print(
            f"[{datetime.now().strftime('%H:%M:%S')}] SWOT width processing complete: {initial_count:,} -> {filtered_count:,} valid measurements in {len(swotWidthDict):,} reach_ids",
            flush=True,
        )

    # Free memory: delete large DataFrame now that we have the dictionaries
    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] Freeing memory: deleting df_swot_nodes ({swot_len:,} rows)...",
        flush=True,
    )
    del df_swot_nodes
    gc.collect()
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Memory freed", flush=True)

    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] Starting debug checks...", flush=True
    )
    # Debug: Check reach_id types and sample values
    if len(swotWidthDict) > 0:
        print(
            f"[{datetime.now().strftime('%H:%M:%S')}] Getting sample SWOT reach_ids...",
            flush=True,
        )
        sample_swot_reach_ids = list(swotWidthDict.keys())[:5]
        sample_swot_types = [type(rid).__name__ for rid in sample_swot_reach_ids]
        print(
            f"Sample SWOT reach_ids: {sample_swot_reach_ids}, types: {sample_swot_types}",
            flush=True,
        )

        print(
            f"[{datetime.now().strftime('%H:%M:%S')}] Getting sample graph reach_ids...",
            flush=True,
        )
        # Check graph edge reach_ids
        graph_reach_ids = []
        graph_reach_types = []
        for u, v, k, d in list(G.edges(data=True, keys=True))[:10]:
            rid = d.get("reach_id")
            if rid is not None:
                graph_reach_ids.append(rid)
                graph_reach_types.append(type(rid).__name__)
        print(
            f"Sample graph reach_ids: {graph_reach_ids[:5]}, types: {graph_reach_types[:5]}",
            flush=True,
        )

        print(
            f"[{datetime.now().strftime('%H:%M:%S')}] Building graph_reach_set (iterating all edges)...",
            flush=True,
        )
        # Check overlap
        graph_reach_set = set()
        edge_count = 0
        for u, v, k, d in G.edges(data=True, keys=True):
            edge_count += 1
            if edge_count % 10000 == 0:
                print(
                    f"[{datetime.now().strftime('%H:%M:%S')}]   Processed {edge_count:,} edges...",
                    flush=True,
                )
            rid = d.get("reach_id")
            if rid is not None:
                # Normalize to int for comparison
                try:
                    if isinstance(rid, (float, np.floating)) and not np.isnan(rid):
                        rid = int(rid)
                    elif isinstance(rid, str):
                        rid = int(rid)
                    graph_reach_set.add(rid)
                except (ValueError, TypeError):
                    pass

        print(
            f"[{datetime.now().strftime('%H:%M:%S')}] Building swot_reach_set...",
            flush=True,
        )
        swot_reach_set = set(swotWidthDict.keys())
        print(
            f"[{datetime.now().strftime('%H:%M:%S')}] Computing overlap...", flush=True
        )
        overlap = graph_reach_set & swot_reach_set
        print(
            f"Graph has {len(graph_reach_set):,} unique reach_ids, SWOT has {len(swot_reach_set):,}, overlap: {len(overlap):,}",
            flush=True,
        )
        if len(overlap) == 0 and len(graph_reach_set) > 0 and len(swot_reach_set) > 0:
            print(
                f"WARNING: No overlap! Sample graph reach_ids: {sorted(list(graph_reach_set))[:10]}",
                flush=True,
            )
            print(
                f"WARNING: Sample SWOT reach_ids: {sorted(list(swot_reach_set))[:10]}",
                flush=True,
            )

    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] Assigning width values to Graph edges...",
        flush=True,
    )
    # Assign width values to Graph edges
    matched_count = 0
    edge_count = 0
    for u, v, k, d in G.edges(data=True, keys=True):
        edge_count += 1
        if edge_count % 10000 == 0:
            print(
                f"[{datetime.now().strftime('%H:%M:%S')}]   Processed {edge_count:,} edges, matched {matched_count:,}...",
                flush=True,
            )
        reach_id = d.get("reach_id")
        if reach_id is not None:
            # Normalize reach_id to int for matching
            try:
                if isinstance(reach_id, (float, np.floating)):
                    if np.isnan(reach_id):
                        reach_id = None
                    else:
                        reach_id = int(reach_id)
                elif isinstance(reach_id, str):
                    reach_id = int(reach_id)
                elif isinstance(reach_id, (int, np.integer)):
                    reach_id = int(reach_id)
                else:
                    reach_id = None
            except (ValueError, TypeError):
                reach_id = None

            if reach_id is not None and reach_id in swotWidthMedianDict:
                G[u][v][k]["swot_width"] = swotWidthMedianDict[
                    reach_id
                ]  # Store median as scalar
                matched_count += 1
            else:
                G[u][v][k]["swot_width"] = -9999
        else:
            G[u][v][k]["swot_width"] = -9999

    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] Assigned swot_width: {matched_count:,} matched, {len(G.edges()) - matched_count:,} defaulted to -9999",
        flush=True,
    )

    # Assign meander length values to Graph
    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] Computing meander lengths in DuckDB...",
        flush=True,
    )

    # DuckDB doesn't support geometry columns, so we need to select only the columns we need
    # Extract just reach_id and meand_len as a regular pandas DataFrame
    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] Extracting reach_id and meand_len columns...",
        flush=True,
    )
    nodes_df = dfNode_input[["reach_id", "meand_len"]].copy()

    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] Registering nodes DataFrame with DuckDB...",
        flush=True,
    )
    duckdb.register("nodes_df", nodes_df)

    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] Executing meander length aggregation query...",
        flush=True,
    )
    meander_len_query = """
        SELECT 
            CAST(reach_id AS BIGINT) AS reach_id,
            MEDIAN(meand_len) AS meand_len_median
        FROM nodes_df
        WHERE meand_len IS NOT NULL
        GROUP BY reach_id
    """

    meander_len_df = duckdb.query(meander_len_query).df()
    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] Meander length query completed: {len(meander_len_df):,} reach_ids",
        flush=True,
    )

    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] Converting to dictionary...",
        flush=True,
    )
    dfMeanLen = dict(
        zip(meander_len_df["reach_id"], meander_len_df["meand_len_median"])
    )
    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] Meander length dict created with {len(dfMeanLen):,} reach_ids",
        flush=True,
    )

    # Free memory: delete intermediate DataFrames and the large nodes DataFrame
    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] Freeing memory: deleting nodes_df, meander_len_df, and dfNode_input...",
        flush=True,
    )
    del nodes_df, meander_len_df, dfNode_input
    # Note: We keep df_input because it's used later in create_edges_gdf
    gc.collect()
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Memory freed", flush=True)

    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] Assigning meander lengths to graph edges...",
        flush=True,
    )
    edge_count = 0
    for u, v, k, d in G.edges(data=True, keys=True):
        edge_count += 1
        if edge_count % 10000 == 0:
            print(
                f"[{datetime.now().strftime('%H:%M:%S')}]   Processed {edge_count:,} edges...",
                flush=True,
            )
        G[u][v][k]["meand_len"] = dfMeanLen.get(d["reach_id"], -9999)
    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] Meander lengths assigned", flush=True
    )

    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] Computing up/down best paths...",
        flush=True,
    )
    compute_up_down_best(
        G,
        widthW=args.width_weight,
        hwW=args.freq_hw_weight,
        path_lenHwW=args.dist_hw_weight,
        outW=args.freq_out_weight,
        path_lenOutW=args.dist_out_weight,
    )
    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] Up/down best paths computed",
        flush=True,
    )

    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] Computing main paths...", flush=True
    )
    main_path(G, continent)
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Main paths computed", flush=True)

    # with open(os.path.join(directory + f'output/{continent}/{continent}_test.pkl'),"wb") as f:
    #     pickle.dump(G,f)
    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] Assigning mainstem by mainpath...",
        flush=True,
    )
    assign_mainstem_by_mainpath(
        G, swotWidthDict, width_weight=args.width_weight, hw_weight=args.freq_hw_weight
    )
    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] Mainstem assignment complete",
        flush=True,
    )

    print(f"[{datetime.now().strftime('%H:%M:%S')}] Assigning main connections...")
    assign_main_connection(G)
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Main connections assigned")

    print(f"[{datetime.now().strftime('%H:%M:%S')}] Computing distance measures...")
    distance_measures(G)
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Distance measures computed")

    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] Creating and saving network edges GeoPackage..."
    )
    dfNetworkEdge = create_edges_gdf(
        G,
        df_input,
        directory=directory,
        save=True,
        filename=f"{continent}/{continent}_network_edges",
    )
    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] Network edges saved: {len(dfNetworkEdge):,} edges"
    )

    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] Creating and saving network nodes GeoPackage..."
    )
    dfNetworkNode = create_network_nodes_gdf(
        G,
        directory=directory,
        save=True,
        explode=False,
        filename=f"{continent}/{continent}_network_nodes",
    )
    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] Network nodes saved: {len(dfNetworkNode):,} nodes"
    )

    print(f"[{datetime.now().strftime('%H:%M:%S')}] Saving final graph pickle...")
    with open(
        os.path.join(directory + f"/output/{continent}/{continent}.pkl"), "wb"
    ) as f:
        pickle.dump(G, f)
    print(f"[{datetime.now().strftime('%H:%M:%S')}] ✅ assign_attribute complete!")


if __name__ == "__main__":
    main()
