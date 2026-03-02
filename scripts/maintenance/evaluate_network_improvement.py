#!/usr/bin/env python3
"""
Global Network Health Simulator
===============================

Quantifies the 'Improvement' if we apply our proposed topological flips.
"""

import duckdb
import networkx as nx

DB_PATH = "/Users/jakegearon/projects/SWORD/data/duckdb/sword_v17c.duckdb"


def simulate_global_improvement(db_path: str, region: str = "AS"):
    print(f"Connecting to {db_path} (Region: {region})...")
    conn = duckdb.connect(db_path)

    topo = conn.execute(
        f"SELECT reach_id, neighbor_reach_id, direction FROM reach_topology WHERE region='{region}'"
    ).fetchdf()
    reaches = conn.execute(
        f"SELECT reach_id, wse, facc FROM reaches WHERE region='{region}'"
    ).fetchdf()
    r_map = reaches.set_index("reach_id").to_dict("index")

    def build_graph(topo_df):
        G = nx.DiGraph()
        for _, row in topo_df.iterrows():
            if row["direction"] == "down":
                G.add_edge(row["reach_id"], row["neighbor_reach_id"])
        return G

    G_initial = build_graph(topo)

    def score_health(graph, reach_data):
        uphill_m = 0
        facc_drop_count = 0

        for u, v in graph.edges():
            if u in reach_data and v in reach_data:
                if reach_data[v]["wse"] > reach_data[u]["wse"] + 0.1:
                    uphill_m += reach_data[v]["wse"] - reach_data[u]["wse"]
                if reach_data[v]["facc"] < reach_data[u]["facc"] * 0.5:
                    facc_drop_count += 1

        return {
            "uphill_penalty_m": round(uphill_m, 1),
            "facc_violations": facc_drop_count,
            "disconnected_components": nx.number_weakly_connected_components(graph),
        }

    initial_score = score_health(G_initial, r_map)
    print(f"Initial State: {initial_score}")

    candidates_query = f"""
    SELECT rt.reach_id, rt.neighbor_reach_id
    FROM reach_topology rt
    JOIN reaches r1 ON rt.reach_id = r1.reach_id AND rt.region = r1.region
    JOIN reaches r2 ON rt.neighbor_reach_id = r2.reach_id AND rt.region = r2.region
    WHERE rt.direction = 'down' AND rt.region = '{region}'
      AND (
        (r1.facc > r2.facc * 2)
        OR 
        ((r2.wse - r1.wse) > 5 AND r1.facc > r2.facc * 1.1)
      )
    """
    to_flip = conn.execute(candidates_query).fetchdf()
    flip_set = set(zip(to_flip["reach_id"], to_flip["neighbor_reach_id"]))
    print(f"Identified {len(flip_set)} high-confidence flip candidate edges.")

    G_flipped = build_graph(topo)
    for u, v in flip_set:
        if G_flipped.has_edge(u, v):
            G_flipped.remove_edge(u, v)
            G_flipped.add_edge(v, u)

    flipped_score = score_health(G_flipped, r_map)
    print(f"Flipped State: {flipped_score}")

    print("\n=== SYSTEMATIC IMPROVEMENT REPORT ===")
    print(
        f"  Reduction in 'Uphill' Flow (m)  : {initial_score['uphill_penalty_m'] - flipped_score['uphill_penalty_m']:.1f}"
    )
    print(
        f"  Reduction in FACC Violations    : {initial_score['facc_violations'] - flipped_score['facc_violations']}"
    )
    print(
        f"  Impact on Network Fragmentation : {flipped_score['disconnected_components'] - initial_score['disconnected_components']} new components"
    )

    improvement = (
        (initial_score["facc_violations"] - flipped_score["facc_violations"])
        / max(1, initial_score["facc_violations"])
    ) * 100
    print(f"\nOVERALL TOPOLOGICAL HARMONY IMPROVED BY: {improvement:.1f}%")

    conn.close()


if __name__ == "__main__":
    simulate_global_improvement(DB_PATH, "AS")
