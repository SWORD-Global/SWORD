"""
Tests for v17c pipeline attribute computation functions.

Tests the core attribute computation functions from v17c_pipeline.py:
- compute_dijkstra_distances(G) (shortest to any outlet)
- compute_mainstem_distances(G, main_neighbors) (follow rch_id_dn_main)
- compute_best_headwater_outlet(G)
- compute_mainstem(G, hw_out_attrs)

Uses the minimal test database with 100 reaches in a linear chain topology.
"""

import pytest
import duckdb
import networkx as nx
from pathlib import Path

from src.sword_v17c_pipeline.v17c_pipeline import (
    build_reach_graph,
    compute_dijkstra_distances,
    compute_best_headwater_outlet,
    compute_mainstem,
    compute_main_neighbors,
    compute_main_paths,
    compute_mainstem_distances,
    load_topology,
    load_reaches,
)

pytestmark = [pytest.mark.pipeline, pytest.mark.topology]


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def test_db_path():
    """Path to the minimal test database."""
    return Path(__file__).parent / "fixtures" / "sword_test_minimal.duckdb"


@pytest.fixture
def db_connection(test_db_path):
    """DuckDB connection to test database."""
    if not test_db_path.exists():
        pytest.skip(f"Test database not found: {test_db_path}")
    conn = duckdb.connect(str(test_db_path), read_only=True)
    yield conn
    conn.close()


@pytest.fixture
def topology_df(db_connection):
    """Load topology DataFrame from test database."""
    return load_topology(db_connection, "NA")


@pytest.fixture
def reaches_df(db_connection):
    """Load reaches DataFrame from test database."""
    return load_reaches(db_connection, "NA")


@pytest.fixture
def reach_graph(topology_df, reaches_df):
    """Build reach graph from test data."""
    return build_reach_graph(topology_df, reaches_df)


@pytest.fixture
def dijkstra_distances(reach_graph):
    """Compute Dijkstra distances to any outlet."""
    return compute_dijkstra_distances(reach_graph)


@pytest.fixture
def hw_out_attrs(reach_graph):
    """Compute headwater/outlet attributes."""
    return compute_best_headwater_outlet(reach_graph)


@pytest.fixture
def main_neighbors(reach_graph):
    """Compute main neighbors."""
    return compute_main_neighbors(reach_graph)


@pytest.fixture
def main_paths(reach_graph, hw_out_attrs):
    """Compute main paths."""
    return compute_main_paths(reach_graph, hw_out_attrs, region="NA")


@pytest.fixture
def mainstem(reach_graph, hw_out_attrs, main_paths):
    """Compute mainstem classification."""
    is_mainstem, _ = compute_mainstem(reach_graph, hw_out_attrs, main_paths=main_paths)
    return is_mainstem


@pytest.fixture
def mainstem_distances(reach_graph, main_neighbors):
    """Compute mainstem distances via rch_id_dn_main chain."""
    return compute_mainstem_distances(reach_graph, main_neighbors)


# =============================================================================
# Test Graph Construction
# =============================================================================


class TestBuildReachGraph:
    """Tests for build_reach_graph function."""

    def test_graph_is_directed(self, reach_graph):
        """Graph should be a DiGraph."""
        assert isinstance(reach_graph, nx.DiGraph)

    def test_graph_node_count(self, reach_graph):
        """Graph should have 100 nodes (reaches)."""
        assert reach_graph.number_of_nodes() == 100

    def test_graph_has_edges(self, reach_graph):
        """Graph should have edges representing flow connections."""
        assert reach_graph.number_of_edges() > 0

    def test_graph_nodes_have_attributes(self, reach_graph):
        """Nodes should have reach attributes."""
        for node in list(reach_graph.nodes())[:5]:
            attrs = reach_graph.nodes[node]
            assert "reach_length" in attrs
            assert "width" in attrs
            assert attrs["reach_length"] > 0

    def test_graph_is_dag(self, reach_graph):
        """Graph should be a directed acyclic graph (DAG)."""
        assert nx.is_directed_acyclic_graph(reach_graph)


# =============================================================================
# Test Hydrologic Distances
# =============================================================================


class TestComputeDijkstraDistances:
    """Tests for compute_dijkstra_distances function."""

    def test_returns_dict_for_all_nodes(self, reach_graph, dijkstra_distances):
        """Should return results for all nodes in graph."""
        assert len(dijkstra_distances) == reach_graph.number_of_nodes()

    def test_contains_required_keys(self, dijkstra_distances):
        """Each result should have dist_out_dijkstra."""
        for node, attrs in dijkstra_distances.items():
            assert "dist_out_dijkstra" in attrs

    def test_dist_out_dijkstra_at_outlet_is_zero(self, reach_graph, dijkstra_distances):
        """dist_out_dijkstra should be 0 at outlets (no outgoing edges)."""
        outlets = [n for n in reach_graph.nodes() if reach_graph.out_degree(n) == 0]
        assert len(outlets) > 0, "Should have at least one outlet"

        for outlet in outlets:
            dist_out = dijkstra_distances[outlet]["dist_out_dijkstra"]
            assert dist_out == 0, (
                f"Outlet {outlet} should have dist_out_dijkstra=0, got {dist_out}"
            )

    def test_dist_out_dijkstra_increases_upstream(
        self, reach_graph, dijkstra_distances
    ):
        """dist_out_dijkstra should increase as we go upstream."""
        outlets = [n for n in reach_graph.nodes() if reach_graph.out_degree(n) == 0]
        headwaters = [n for n in reach_graph.nodes() if reach_graph.in_degree(n) == 0]

        for outlet in outlets:
            outlet_dist = dijkstra_distances[outlet]["dist_out_dijkstra"]
            for hw in headwaters:
                hw_dist = dijkstra_distances[hw]["dist_out_dijkstra"]
                if hw != outlet:
                    assert hw_dist > outlet_dist, (
                        f"Headwater {hw} dist ({hw_dist}) should be > outlet {outlet} dist ({outlet_dist})"
                    )

    def test_dist_out_dijkstra_values_are_non_negative(self, dijkstra_distances):
        """All distance values should be non-negative."""
        for node, attrs in dijkstra_distances.items():
            dist_out = attrs["dist_out_dijkstra"]
            assert dist_out >= 0 or dist_out == float("inf"), (
                f"Node {node}: dist_out_dijkstra should be >= 0, got {dist_out}"
            )


class TestComputeMainstemDistances:
    """Tests for compute_mainstem_distances function."""

    def test_returns_dict_for_all_nodes(self, reach_graph, mainstem_distances):
        """Should return results for all nodes in graph."""
        assert len(mainstem_distances) == reach_graph.number_of_nodes()

    def test_contains_hydro_dist_out_key(self, mainstem_distances):
        """Each result should have hydro_dist_out."""
        for node, attrs in mainstem_distances.items():
            assert "hydro_dist_out" in attrs

    def test_hydro_dist_out_positive(self, mainstem_distances):
        """hydro_dist_out should be positive (includes own reach_length)."""
        for node, attrs in mainstem_distances.items():
            assert attrs["hydro_dist_out"] >= 0, (
                f"Node {node}: hydro_dist_out should be >= 0"
            )

    def test_outlet_hydro_dist_out_equals_reach_length(
        self, reach_graph, main_neighbors, mainstem_distances
    ):
        """Outlet (NULL rch_id_dn_main) hydro_dist_out = own reach_length."""
        for rid, nb in main_neighbors.items():
            if nb.get("rch_id_dn_main") is None:
                expected = reach_graph.nodes[rid].get("reach_length", 0)
                actual = mainstem_distances[rid]["hydro_dist_out"]
                assert actual == pytest.approx(expected, abs=0.01), (
                    f"Terminal reach {rid}: expected {expected}, got {actual}"
                )


# =============================================================================
# Test Best Headwater/Outlet Computation
# =============================================================================


class TestComputeBestHeadwaterOutlet:
    """Tests for compute_best_headwater_outlet function."""

    def test_returns_dict_for_all_nodes(self, reach_graph, hw_out_attrs):
        """Should return results for all nodes in graph."""
        assert len(hw_out_attrs) == reach_graph.number_of_nodes()

    def test_contains_required_keys(self, hw_out_attrs):
        """Each result should have required keys."""
        required_keys = [
            "best_headwater",
            "best_outlet",
            "pathlen_hw",
            "pathlen_out",
            "path_freq",
        ]
        for node, attrs in hw_out_attrs.items():
            for key in required_keys:
                assert key in attrs, f"Node {node} missing key: {key}"

    def test_best_headwater_is_valid_reach_id(self, reach_graph, hw_out_attrs):
        """best_headwater should be a valid node in the graph."""
        all_nodes = set(reach_graph.nodes())
        for node, attrs in hw_out_attrs.items():
            hw = attrs["best_headwater"]
            if hw is not None:
                assert hw in all_nodes, f"best_headwater {hw} not in graph nodes"

    def test_best_outlet_is_valid_reach_id(self, reach_graph, hw_out_attrs):
        """best_outlet should be a valid node in the graph."""
        all_nodes = set(reach_graph.nodes())
        for node, attrs in hw_out_attrs.items():
            out = attrs["best_outlet"]
            if out is not None:
                assert out in all_nodes, f"best_outlet {out} not in graph nodes"

    def test_headwater_best_headwater_is_itself(self, reach_graph, hw_out_attrs):
        """At a headwater node, best_headwater should be itself."""
        headwaters = [n for n in reach_graph.nodes() if reach_graph.in_degree(n) == 0]

        for hw in headwaters:
            assert hw_out_attrs[hw]["best_headwater"] == hw, (
                f"Headwater {hw} should have best_headwater == itself"
            )

    def test_outlet_best_outlet_is_itself(self, reach_graph, hw_out_attrs):
        """At an outlet node, best_outlet should be itself."""
        outlets = [n for n in reach_graph.nodes() if reach_graph.out_degree(n) == 0]

        for outlet in outlets:
            assert hw_out_attrs[outlet]["best_outlet"] == outlet, (
                f"Outlet {outlet} should have best_outlet == itself"
            )

    def test_pathlen_hw_at_headwater_is_zero(self, reach_graph, hw_out_attrs):
        """pathlen_hw should be 0 at headwaters."""
        headwaters = [n for n in reach_graph.nodes() if reach_graph.in_degree(n) == 0]

        for hw in headwaters:
            assert hw_out_attrs[hw]["pathlen_hw"] == 0, (
                f"Headwater {hw} should have pathlen_hw == 0"
            )

    def test_pathlen_out_at_outlet_is_zero(self, reach_graph, hw_out_attrs):
        """pathlen_out should be 0 at outlets."""
        outlets = [n for n in reach_graph.nodes() if reach_graph.out_degree(n) == 0]

        for outlet in outlets:
            assert hw_out_attrs[outlet]["pathlen_out"] == 0, (
                f"Outlet {outlet} should have pathlen_out == 0"
            )

    def test_pathlen_values_are_non_negative(self, hw_out_attrs):
        """All pathlen values should be non-negative."""
        for node, attrs in hw_out_attrs.items():
            assert attrs["pathlen_hw"] >= 0, (
                f"Node {node}: pathlen_hw should be >= 0, got {attrs['pathlen_hw']}"
            )
            assert attrs["pathlen_out"] >= 0, (
                f"Node {node}: pathlen_out should be >= 0, got {attrs['pathlen_out']}"
            )

    def test_path_freq_is_positive(self, hw_out_attrs):
        """path_freq should be at least 1 for all nodes."""
        for node, attrs in hw_out_attrs.items():
            assert attrs["path_freq"] >= 1, (
                f"Node {node}: path_freq should be >= 1, got {attrs['path_freq']}"
            )


# =============================================================================
# Test Mainstem Computation
# =============================================================================


class TestComputeMainstem:
    """Tests for compute_mainstem function."""

    def test_returns_dict_for_all_nodes(self, reach_graph, mainstem):
        """Should return results for all nodes in graph."""
        assert len(mainstem) == reach_graph.number_of_nodes()

    def test_returns_boolean_values(self, mainstem):
        """All values should be boolean."""
        for node, is_main in mainstem.items():
            assert isinstance(is_main, bool), (
                f"Node {node}: is_mainstem should be bool, got {type(is_main)}"
            )

    def test_at_least_one_mainstem_reach(self, mainstem):
        """At least one reach should be on the mainstem."""
        n_mainstem = sum(mainstem.values())
        assert n_mainstem >= 1, "Should have at least one mainstem reach"

    def test_mainstem_forms_connected_path(self, reach_graph, mainstem):
        """Mainstem reaches should form a connected path."""
        mainstem_nodes = [n for n, is_main in mainstem.items() if is_main]

        if len(mainstem_nodes) <= 1:
            # Single node or no mainstem is trivially connected
            return

        # Create subgraph of mainstem nodes
        subgraph = reach_graph.subgraph(mainstem_nodes)

        # Convert to undirected for connectivity check
        undirected = subgraph.to_undirected()

        # Should be connected
        assert nx.is_connected(undirected), "Mainstem should form a connected path"

    def test_mainstem_includes_outlet(self, reach_graph, mainstem, hw_out_attrs):
        """Mainstem should include the outlet of the main path."""
        # Find nodes where best_outlet equals their own outlet
        outlets = [n for n in reach_graph.nodes() if reach_graph.out_degree(n) == 0]

        # At least one outlet should be on mainstem
        outlet_on_mainstem = any(mainstem.get(o, False) for o in outlets)
        assert outlet_on_mainstem, "At least one outlet should be on the mainstem"

    def test_mainstem_includes_headwater(self, reach_graph, mainstem, hw_out_attrs):
        """Mainstem should include a headwater of the main path."""
        headwaters = [n for n in reach_graph.nodes() if reach_graph.in_degree(n) == 0]

        # At least one headwater should be on mainstem
        hw_on_mainstem = any(mainstem.get(hw, False) for hw in headwaters)
        assert hw_on_mainstem, "At least one headwater should be on the mainstem"


# =============================================================================
# Integration Tests
# =============================================================================


class TestIntegration:
    """Integration tests verifying consistency between computed attributes."""

    def test_mainstem_path_exists_between_best_hw_and_outlet(
        self, reach_graph, hw_out_attrs, mainstem
    ):
        """The mainstem should form a valid path from best_headwater to best_outlet."""
        mainstem_nodes = [n for n, is_main in mainstem.items() if is_main]

        if not mainstem_nodes:
            pytest.skip("No mainstem nodes found")

        mainstem_headwaters = [
            n for n in mainstem_nodes if reach_graph.in_degree(n) == 0
        ]
        mainstem_outlets = [n for n in mainstem_nodes if reach_graph.out_degree(n) == 0]

        if not mainstem_headwaters or not mainstem_outlets:
            return

        hw = mainstem_headwaters[0]
        outlet = mainstem_outlets[0]

        try:
            path = nx.shortest_path(reach_graph, hw, outlet)
            for node in path:
                assert mainstem[node], (
                    f"Node {node} on HW-outlet path should be mainstem"
                )
        except nx.NetworkXNoPath:
            pytest.fail(f"No path found from mainstem HW {hw} to outlet {outlet}")

    def test_dijkstra_decreases_along_topo_order(self, reach_graph, dijkstra_distances):
        """For a linear chain, dist_out_dijkstra should decrease downstream."""
        try:
            topo_order = list(nx.topological_sort(reach_graph))
        except nx.NetworkXUnfeasible:
            pytest.skip("Graph has cycles")

        if len(topo_order) < 2:
            return

        prev_dist_out = float("inf")
        for node in topo_order:
            curr_dist_out = dijkstra_distances[node]["dist_out_dijkstra"]
            if curr_dist_out < float("inf"):
                assert curr_dist_out <= prev_dist_out, (
                    f"dist_out_dijkstra should decrease downstream: {prev_dist_out} -> {curr_dist_out}"
                )
                prev_dist_out = curr_dist_out

    def test_mainstem_dist_ge_dijkstra(self, dijkstra_distances, mainstem_distances):
        """hydro_dist_out (mainstem walk) should be >= dist_out_dijkstra (shortest)."""
        for rid in dijkstra_distances:
            dij = dijkstra_distances[rid]["dist_out_dijkstra"]
            ms = mainstem_distances[rid]["hydro_dist_out"]
            if dij < float("inf"):
                assert ms >= dij - 0.01, f"Reach {rid}: mainstem {ms} < dijkstra {dij}"


# =============================================================================
# Edge Cases
# =============================================================================


class TestEdgeCases:
    """Tests for edge cases and boundary conditions."""

    def test_single_node_graph(self):
        """Test with a single-node graph."""
        G = nx.DiGraph()
        G.add_node(1, reach_length=1000, width=50)

        dij = compute_dijkstra_distances(G)
        assert 1 in dij
        assert dij[1]["dist_out_dijkstra"] == 0

        hw_out = compute_best_headwater_outlet(G)
        assert hw_out[1]["best_headwater"] == 1
        assert hw_out[1]["best_outlet"] == 1
        assert hw_out[1]["pathlen_hw"] == 0
        assert hw_out[1]["pathlen_out"] == 0

        mp = compute_main_paths(G, hw_out)
        ms, chain = compute_mainstem(G, hw_out, main_paths=mp)
        assert ms[1] is True

        mn = compute_main_neighbors(G, mainstem_chain=chain)
        md = compute_mainstem_distances(G, mn)
        # Single node, terminal → hydro_dist_out = own reach_length
        assert md[1]["hydro_dist_out"] == 1000

    def test_two_node_linear_graph(self):
        """Test with a simple two-node linear graph."""
        G = nx.DiGraph()
        G.add_node(1, reach_length=1000, width=50)
        G.add_node(2, reach_length=1500, width=60)
        G.add_edge(1, 2)

        dij = compute_dijkstra_distances(G)
        assert dij[2]["dist_out_dijkstra"] == 0

        hw_out = compute_best_headwater_outlet(G)
        assert hw_out[1]["best_headwater"] == 1
        assert hw_out[2]["best_outlet"] == 2

        mp = compute_main_paths(G, hw_out)
        ms, chain = compute_mainstem(G, hw_out, main_paths=mp)
        assert ms[1] is True
        assert ms[2] is True

        mn = compute_main_neighbors(G, mainstem_chain=chain)
        md = compute_mainstem_distances(G, mn)
        # Node 2 is terminal: 1500
        assert md[2]["hydro_dist_out"] == 1500
        # Node 1 walks to 2: 1000 + 1500 = 2500
        assert md[1]["hydro_dist_out"] == 2500

    def test_y_shaped_network(self):
        """Test with a Y-shaped network (two tributaries merging)."""
        G = nx.DiGraph()
        G.add_node(1, reach_length=1000, width=30, effective_width=30, log_facc=0)
        G.add_node(2, reach_length=1200, width=40, effective_width=40, log_facc=0)
        G.add_node(3, reach_length=800, width=60, effective_width=60, log_facc=0)
        G.add_node(4, reach_length=900, width=70, effective_width=70, log_facc=0)

        G.add_edge(1, 3)
        G.add_edge(2, 3)
        G.add_edge(3, 4)

        dij = compute_dijkstra_distances(G)
        assert dij[4]["dist_out_dijkstra"] == 0

        hw_out = compute_best_headwater_outlet(G)
        assert hw_out[3]["best_headwater"] == 2
        assert hw_out[4]["best_outlet"] == 4

        mp = compute_main_paths(G, hw_out)
        ms, chain = compute_mainstem(G, hw_out, main_paths=mp)
        # Node 2 is the best_headwater for the outlet (node 4).
        # Greedy walk from 2: 2 → 3 → 4
        assert ms[2] is True
        assert ms[3] is True
        assert ms[4] is True
        # Node 1 is mainstem of its own main_path_id group (best_hw=1, best_out=4)
        assert ms[1] is True

        mn = compute_main_neighbors(G, mainstem_chain=chain)
        md = compute_mainstem_distances(G, mn)
        # Node 4 terminal: 900
        assert md[4]["hydro_dist_out"] == 900
        # Node 3 → 4: 800 + 900 = 1700
        assert md[3]["hydro_dist_out"] == 1700

    def test_ghost_reaches_excluded_from_mainstem(self):
        """Ghost reaches (type=6) should never be marked as mainstem."""
        G = nx.DiGraph()
        G.add_node(
            1, reach_length=1000, width=50, effective_width=50, log_facc=5, type=1
        )
        G.add_node(
            2, reach_length=1000, width=50, effective_width=50, log_facc=5, type=6
        )  # ghost
        G.add_node(
            3, reach_length=1000, width=50, effective_width=50, log_facc=5, type=1
        )

        G.add_edge(1, 2)
        G.add_edge(2, 3)

        hw_out = compute_best_headwater_outlet(G)
        mp = compute_main_paths(G, hw_out)
        ms, chain = compute_mainstem(G, hw_out, main_paths=mp)

        assert ms[1] is True
        assert ms[2] is False, "Ghost reach (type=6) must not be mainstem"
        assert ms[3] is True

    def test_empty_graph(self):
        """Test with an empty graph."""
        G = nx.DiGraph()

        dij = compute_dijkstra_distances(G)
        assert len(dij) == 0

        hw_out = compute_best_headwater_outlet(G)
        assert len(hw_out) == 0

        mp = compute_main_paths(G, hw_out)
        ms, chain = compute_mainstem(G, hw_out, main_paths=mp)
        assert len(ms) == 0

        mn = compute_main_neighbors(G, mainstem_chain=chain)
        md = compute_mainstem_distances(G, mn)
        assert len(md) == 0
