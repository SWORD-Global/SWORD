"""
Tests for v17c pipeline graph construction functions.

Tests the core graph building and junction identification logic used
by the v17c pipeline to compute hydrologic attributes.
"""

import pytest
import duckdb
import networkx as nx
import pandas as pd
from pathlib import Path

from src.sword_v17c_pipeline.stages.graph import routing_score, ROUTING_WEIGHTS
from src.sword_v17c_pipeline.v17c_pipeline import (
    build_reach_graph,
    identify_junctions,
    build_section_graph,
    load_topology,
    load_reaches,
)

pytestmark = [pytest.mark.pipeline, pytest.mark.topology]


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def test_db_path():
    """Path to test database."""
    return Path(__file__).parent / "fixtures" / "sword_test_minimal.duckdb"


@pytest.fixture
def db_conn(test_db_path):
    """DuckDB connection to test database."""
    conn = duckdb.connect(str(test_db_path), read_only=True)
    yield conn
    conn.close()


@pytest.fixture
def topology_df(db_conn):
    """Load topology DataFrame from test database."""
    return db_conn.execute("""
        SELECT reach_id, direction, neighbor_rank, neighbor_reach_id
        FROM reach_topology
        WHERE region = 'NA'
    """).fetchdf()


@pytest.fixture
def reaches_df(db_conn):
    """Load reaches DataFrame from test database."""
    return db_conn.execute("""
        SELECT
            reach_id, region, reach_length, width, slope, facc,
            n_rch_up, n_rch_down, dist_out, path_freq, stream_order,
            lakeflag, trib_flag
        FROM reaches
        WHERE region = 'NA'
    """).fetchdf()


@pytest.fixture
def reach_graph(topology_df, reaches_df):
    """Build reach graph from test data."""
    return build_reach_graph(topology_df, reaches_df)


@pytest.fixture
def junctions(reach_graph):
    """Identify junctions in the test graph."""
    return identify_junctions(reach_graph)


# =============================================================================
# Synthetic Test Data Fixtures
# =============================================================================


@pytest.fixture
def simple_linear_topology():
    """Simple linear chain: 0 -> 1 -> 2 -> 3."""
    return pd.DataFrame(
        {
            "reach_id": [0, 1, 1, 2, 2, 3],
            "direction": ["down", "down", "up", "down", "up", "up"],
            "neighbor_rank": [0, 0, 0, 0, 0, 0],
            "neighbor_reach_id": [1, 2, 0, 3, 1, 2],
        }
    )


@pytest.fixture
def simple_linear_reaches():
    """Reaches for simple linear chain."""
    return pd.DataFrame(
        {
            "reach_id": [0, 1, 2, 3],
            "reach_length": [1000.0, 1000.0, 1000.0, 1000.0],
            "width": [50.0, 55.0, 60.0, 65.0],
            "slope": [0.001, 0.001, 0.001, 0.001],
            "facc": [100, 200, 300, 400],
            "n_rch_up": [0, 1, 1, 1],
            "n_rch_down": [1, 1, 1, 0],
            "dist_out": [3000.0, 2000.0, 1000.0, 0.0],
            "path_freq": [1, 1, 1, 1],
            "stream_order": [1, 1, 1, 1],
            "lakeflag": [0, 0, 0, 0],
        }
    )


@pytest.fixture
def confluence_topology():
    """
    Confluence topology: two tributaries merging.

        0 -----+
               +--> 2 --> 3
        1 -----+
    """
    return pd.DataFrame(
        {
            "reach_id": [0, 1, 2, 2, 2, 3],
            "direction": ["down", "down", "up", "up", "down", "up"],
            "neighbor_rank": [0, 0, 0, 1, 0, 0],
            "neighbor_reach_id": [2, 2, 0, 1, 3, 2],
        }
    )


@pytest.fixture
def confluence_reaches():
    """Reaches for confluence topology."""
    return pd.DataFrame(
        {
            "reach_id": [0, 1, 2, 3],
            "reach_length": [1000.0, 1200.0, 800.0, 1500.0],
            "width": [40.0, 35.0, 80.0, 90.0],
            "slope": [0.002, 0.0025, 0.001, 0.0008],
            "facc": [100, 80, 200, 250],
            "n_rch_up": [0, 0, 2, 1],
            "n_rch_down": [1, 1, 1, 0],
            "dist_out": [4000.0, 4200.0, 2000.0, 0.0],
            "path_freq": [1, 1, 2, 2],
            "stream_order": [1, 1, 2, 2],
            "lakeflag": [0, 0, 0, 0],
        }
    )


@pytest.fixture
def bifurcation_topology():
    """
    Bifurcation topology: one reach splitting.

               +--> 1
        0 -----+
               +--> 2
    """
    return pd.DataFrame(
        {
            "reach_id": [0, 0, 1, 2],
            "direction": ["down", "down", "up", "up"],
            "neighbor_rank": [0, 1, 0, 0],
            "neighbor_reach_id": [1, 2, 0, 0],
        }
    )


@pytest.fixture
def bifurcation_reaches():
    """Reaches for bifurcation topology."""
    return pd.DataFrame(
        {
            "reach_id": [0, 1, 2],
            "reach_length": [2000.0, 1000.0, 1500.0],
            "width": [100.0, 60.0, 50.0],
            "slope": [0.001, 0.0015, 0.002],
            "facc": [500, 300, 200],
            "n_rch_up": [0, 1, 1],
            "n_rch_down": [2, 0, 0],
            "dist_out": [2000.0, 0.0, 0.0],
            "path_freq": [1, 1, 1],
            "stream_order": [2, 1, 1],
            "lakeflag": [0, 0, 0],
        }
    )


# =============================================================================
# Test: load_topology / load_reaches
# =============================================================================


class TestDataLoading:
    """Tests for data loading functions."""

    def test_load_topology_returns_dataframe(self, db_conn):
        """load_topology returns a pandas DataFrame."""
        df = load_topology(db_conn, "NA")
        assert isinstance(df, pd.DataFrame)

    def test_load_topology_correct_row_count(self, db_conn):
        """load_topology returns expected number of rows."""
        df = load_topology(db_conn, "NA")
        assert len(df) == 198

    def test_load_topology_required_columns(self, db_conn):
        """load_topology returns required columns."""
        df = load_topology(db_conn, "NA")
        required_cols = ["reach_id", "direction", "neighbor_rank", "neighbor_reach_id"]
        for col in required_cols:
            assert col in df.columns

    def test_load_reaches_returns_dataframe(self, db_conn):
        """load_reaches returns a pandas DataFrame."""
        df = load_reaches(db_conn, "NA")
        assert isinstance(df, pd.DataFrame)

    def test_load_reaches_correct_count(self, db_conn):
        """load_reaches returns expected number of rows."""
        df = load_reaches(db_conn, "NA")
        assert len(df) == 100

    def test_load_reaches_required_columns(self, db_conn):
        """load_reaches returns required columns."""
        df = load_reaches(db_conn, "NA")
        required_cols = ["reach_id", "region", "reach_length", "n_rch_up", "n_rch_down"]
        for col in required_cols:
            assert col in df.columns

    def test_load_topology_case_insensitive_region(self, db_conn):
        """load_topology works with lowercase region."""
        df_upper = load_topology(db_conn, "NA")
        df_lower = load_topology(db_conn, "na")
        assert len(df_upper) == len(df_lower)


# =============================================================================
# Test: build_reach_graph
# =============================================================================


class TestBuildReachGraph:
    """Tests for build_reach_graph function."""

    def test_returns_digraph(self, topology_df, reaches_df):
        """build_reach_graph returns a NetworkX DiGraph."""
        G = build_reach_graph(topology_df, reaches_df)
        assert isinstance(G, nx.DiGraph)

    def test_correct_node_count(self, reach_graph):
        """Graph has correct number of nodes (100 reaches)."""
        assert reach_graph.number_of_nodes() == 100

    def test_correct_edge_count(self, reach_graph):
        """Graph has correct number of edges (99 for linear chain)."""
        # Linear chain of 100 reaches -> 99 edges
        assert reach_graph.number_of_edges() == 99

    def test_reach_ids_as_nodes(self, reach_graph):
        """Reach IDs are used as node identifiers."""
        # Test fixture has IDs 11000000000-11000000099
        assert 11000000000 in reach_graph.nodes()
        assert 11000000099 in reach_graph.nodes()

    def test_nodes_have_attributes(self, reach_graph):
        """Graph nodes have reach attributes."""
        node_data = reach_graph.nodes[11000000000]
        assert "reach_length" in node_data
        assert "width" in node_data
        assert "slope" in node_data

    def test_edges_have_direction(self, reach_graph):
        """Edges follow flow direction (smaller dist_out downstream)."""
        # In the test fixture, reach 0 -> reach 1 -> ... -> reach 99
        # First reach flows to second
        assert reach_graph.has_edge(11000000000, 11000000001)
        # But not reverse
        assert not reach_graph.has_edge(11000000001, 11000000000)

    def test_simple_linear_graph(self, simple_linear_topology, simple_linear_reaches):
        """Test with simple synthetic linear chain."""
        G = build_reach_graph(simple_linear_topology, simple_linear_reaches)

        assert G.number_of_nodes() == 4
        assert G.number_of_edges() == 3
        assert G.has_edge(0, 1)
        assert G.has_edge(1, 2)
        assert G.has_edge(2, 3)

    def test_confluence_graph(self, confluence_topology, confluence_reaches):
        """Test confluence topology (two tribs merging)."""
        G = build_reach_graph(confluence_topology, confluence_reaches)

        assert G.number_of_nodes() == 4
        assert G.number_of_edges() == 3  # 0->2, 1->2, 2->3
        # Both 0 and 1 flow into 2
        assert G.has_edge(0, 2)
        assert G.has_edge(1, 2)
        # 2 flows into 3
        assert G.has_edge(2, 3)

    def test_bifurcation_graph(self, bifurcation_topology, bifurcation_reaches):
        """Test bifurcation topology (one reach splitting)."""
        G = build_reach_graph(bifurcation_topology, bifurcation_reaches)

        assert G.number_of_nodes() == 3
        assert G.number_of_edges() == 2
        # 0 splits into 1 and 2
        assert G.has_edge(0, 1)
        assert G.has_edge(0, 2)

    def test_empty_topology(self):
        """Empty topology produces graph with nodes but no edges."""
        topo = pd.DataFrame(
            columns=["reach_id", "direction", "neighbor_rank", "neighbor_reach_id"]
        )
        reaches = pd.DataFrame(
            {
                "reach_id": [1, 2, 3],
                "reach_length": [1000.0, 1000.0, 1000.0],
                "width": [50.0, 50.0, 50.0],
                "slope": [0.001, 0.001, 0.001],
            }
        )

        G = build_reach_graph(topo, reaches)
        assert G.number_of_nodes() == 3
        assert G.number_of_edges() == 0


# =============================================================================
# Test: identify_junctions
# =============================================================================


class TestIdentifyJunctions:
    """Tests for identify_junctions function."""

    def test_returns_set(self, reach_graph):
        """identify_junctions returns a set."""
        junctions = identify_junctions(reach_graph)
        assert isinstance(junctions, set)

    def test_headwater_is_junction(self, reach_graph):
        """Headwater (in_degree=0) identified as junction."""
        junctions = identify_junctions(reach_graph)
        # Reach 11000000000 is headwater (n_rch_up=0)
        assert 11000000000 in junctions

    def test_outlet_is_junction(self, reach_graph):
        """Outlet (out_degree=0) identified as junction."""
        junctions = identify_junctions(reach_graph)
        # Reach 11000000099 is outlet (n_rch_down=0)
        assert 11000000099 in junctions

    def test_linear_chain_only_endpoints(self, reach_graph):
        """Linear chain has only headwater and outlet as junctions."""
        junctions = identify_junctions(reach_graph)
        # Test fixture is linear chain: only 2 junctions (endpoints)
        assert len(junctions) == 2

    def test_confluence_identified(self, confluence_topology, confluence_reaches):
        """Confluence (in_degree > 1) identified as junction."""
        G = build_reach_graph(confluence_topology, confluence_reaches)
        junctions = identify_junctions(G)

        # Reach 2 is confluence (receives from 0 and 1)
        assert 2 in junctions

    def test_bifurcation_identified(self, bifurcation_topology, bifurcation_reaches):
        """Bifurcation (out_degree > 1) identified as junction."""
        G = build_reach_graph(bifurcation_topology, bifurcation_reaches)
        junctions = identify_junctions(G)

        # Reach 0 is bifurcation (flows to 1 and 2)
        assert 0 in junctions

    def test_all_headwaters_identified(self, confluence_topology, confluence_reaches):
        """All headwaters (in_degree=0) are identified."""
        G = build_reach_graph(confluence_topology, confluence_reaches)
        junctions = identify_junctions(G)

        # Reaches 0 and 1 are headwaters
        assert 0 in junctions
        assert 1 in junctions

    def test_all_outlets_identified(self, bifurcation_topology, bifurcation_reaches):
        """All outlets (out_degree=0) are identified."""
        G = build_reach_graph(bifurcation_topology, bifurcation_reaches)
        junctions = identify_junctions(G)

        # Reaches 1 and 2 are outlets
        assert 1 in junctions
        assert 2 in junctions

    def test_interior_non_junction(self, simple_linear_topology, simple_linear_reaches):
        """Interior nodes with in_degree=1 and out_degree=1 are not junctions."""
        G = build_reach_graph(simple_linear_topology, simple_linear_reaches)
        junctions = identify_junctions(G)

        # Reaches 1 and 2 are interior (pass-through)
        assert 1 not in junctions
        assert 2 not in junctions

    def test_empty_graph(self):
        """Empty graph returns empty junction set."""
        G = nx.DiGraph()
        junctions = identify_junctions(G)
        assert len(junctions) == 0

    def test_single_node_is_junction(self):
        """Single node (both HW and outlet) is a junction."""
        G = nx.DiGraph()
        G.add_node(1, reach_length=1000)
        junctions = identify_junctions(G)
        assert 1 in junctions


# =============================================================================
# Test: build_section_graph
# =============================================================================


class TestBuildSectionGraph:
    """Tests for build_section_graph function."""

    def test_returns_tuple(self, reach_graph, junctions):
        """build_section_graph returns (DiGraph, DataFrame) tuple."""
        result = build_section_graph(reach_graph, junctions)
        assert isinstance(result, tuple)
        assert len(result) == 2

    def test_returns_digraph(self, reach_graph, junctions):
        """First return value is NetworkX DiGraph."""
        R, _ = build_section_graph(reach_graph, junctions)
        assert isinstance(R, nx.DiGraph)

    def test_returns_dataframe(self, reach_graph, junctions):
        """Second return value is pandas DataFrame."""
        _, sections_df = build_section_graph(reach_graph, junctions)
        assert isinstance(sections_df, pd.DataFrame)

    def test_section_graph_nodes_are_junctions(self, reach_graph, junctions):
        """Section graph nodes are the junction nodes."""
        R, _ = build_section_graph(reach_graph, junctions)
        for node in R.nodes():
            assert node in junctions

    def test_linear_chain_one_section(self, reach_graph, junctions):
        """Linear chain produces one section (HW -> outlet)."""
        R, sections_df = build_section_graph(reach_graph, junctions)

        # Should have 2 junction nodes and 1 edge (section)
        assert R.number_of_nodes() == 2
        assert R.number_of_edges() == 1
        assert len(sections_df) == 1

    def test_section_contains_reach_ids(self, reach_graph, junctions):
        """Section edges contain list of reach IDs."""
        R, _ = build_section_graph(reach_graph, junctions)

        for u, v, data in R.edges(data=True):
            assert "reach_ids" in data
            assert isinstance(data["reach_ids"], list)
            assert len(data["reach_ids"]) > 0

    def test_section_has_distance(self, reach_graph, junctions):
        """Sections have cumulative distance attribute."""
        R, _ = build_section_graph(reach_graph, junctions)

        for u, v, data in R.edges(data=True):
            assert "distance" in data
            assert data["distance"] > 0

    def test_sections_df_columns(self, reach_graph, junctions):
        """Sections DataFrame has required columns."""
        _, sections_df = build_section_graph(reach_graph, junctions)

        required_cols = [
            "section_id",
            "upstream_junction",
            "downstream_junction",
            "reach_ids",
            "distance",
            "n_reaches",
        ]
        for col in required_cols:
            assert col in sections_df.columns

    def test_confluence_multiple_sections(
        self, confluence_topology, confluence_reaches
    ):
        """Confluence creates multiple sections."""
        G = build_reach_graph(confluence_topology, confluence_reaches)
        junctions = identify_junctions(G)
        R, sections_df = build_section_graph(G, junctions)

        # 0 -> 2 (section), 1 -> 2 (section), 2 -> 3 (section)
        assert R.number_of_edges() >= 2
        assert len(sections_df) >= 2

    def test_node_types_assigned(self, reach_graph, junctions):
        """Junction nodes have node_type attribute."""
        R, _ = build_section_graph(reach_graph, junctions)

        for node in R.nodes():
            assert "node_type" in R.nodes[node]
            assert R.nodes[node]["node_type"] in ["Head_water", "Outlet", "Junction"]

    def test_headwater_node_type(self, reach_graph, junctions):
        """Headwater has node_type='Head_water'."""
        R, _ = build_section_graph(reach_graph, junctions)

        # 11000000000 is the headwater
        assert R.nodes[11000000000]["node_type"] == "Head_water"

    def test_outlet_node_type(self, reach_graph, junctions):
        """Outlet has node_type='Outlet'."""
        R, _ = build_section_graph(reach_graph, junctions)

        # 11000000099 is the outlet
        assert R.nodes[11000000099]["node_type"] == "Outlet"

    def test_section_includes_downstream_junction(self, reach_graph, junctions):
        """Section reach_ids includes the downstream junction reach."""
        _, sections_df = build_section_graph(reach_graph, junctions)

        for _, row in sections_df.iterrows():
            downstream_j = row["downstream_junction"]
            reach_ids = row["reach_ids"]
            assert downstream_j in reach_ids

    def test_section_id_unique(self, reach_graph, junctions):
        """Each section has unique section_id."""
        _, sections_df = build_section_graph(reach_graph, junctions)
        assert sections_df["section_id"].nunique() == len(sections_df)


# =============================================================================
# Test: Integration / End-to-End
# =============================================================================


class TestGraphIntegration:
    """Integration tests for the full graph construction pipeline."""

    def test_full_pipeline_from_db(self, db_conn):
        """Full pipeline works with database data."""
        topology_df = load_topology(db_conn, "NA")
        reaches_df = load_reaches(db_conn, "NA")
        G = build_reach_graph(topology_df, reaches_df)
        junctions = identify_junctions(G)
        R, sections_df = build_section_graph(G, junctions)

        # Basic sanity checks
        assert G.number_of_nodes() == 100
        assert G.number_of_edges() == 99
        assert len(junctions) == 2
        assert R.number_of_nodes() == 2
        assert len(sections_df) == 1

    def test_graph_is_dag(self, reach_graph):
        """Reach graph is a directed acyclic graph (DAG)."""
        assert nx.is_directed_acyclic_graph(reach_graph)

    def test_topological_sort_possible(self, reach_graph):
        """Topological sort is possible on the graph."""
        topo_order = list(nx.topological_sort(reach_graph))
        assert len(topo_order) == reach_graph.number_of_nodes()

    def test_headwater_first_in_topo_sort(self, reach_graph):
        """Headwater appears first in topological sort."""
        topo_order = list(nx.topological_sort(reach_graph))
        # 11000000000 is the headwater
        assert topo_order[0] == 11000000000

    def test_outlet_last_in_topo_sort(self, reach_graph):
        """Outlet appears last in topological sort."""
        topo_order = list(nx.topological_sort(reach_graph))
        # 11000000099 is the outlet
        assert topo_order[-1] == 11000000099

    def test_all_reaches_reachable_from_headwater(self, reach_graph):
        """All reaches are reachable from headwater in linear chain."""
        reachable = set(nx.descendants(reach_graph, 11000000000))
        reachable.add(11000000000)  # Include the headwater itself
        assert len(reachable) == 100

    def test_section_reach_count_matches_graph(self, reach_graph, junctions):
        """Sum of reaches in sections excludes the upstream junction."""
        _, sections_df = build_section_graph(reach_graph, junctions)

        total_in_sections = sections_df["n_reaches"].sum()
        # In a linear chain from HW(0) to outlet(99), section traces from
        # first reach AFTER headwater (1) to outlet (99) = 99 reaches
        # The headwater junction itself is not included in the section
        assert total_in_sections == 99


# =============================================================================
# Test: routing_score
# =============================================================================


class TestRoutingScore:
    """Tests for the learned routing score function."""

    def test_wider_reach_scores_higher(self):
        """Wider reach should score higher, all else equal."""
        import math

        wide = routing_score(math.log1p(500), 10, 0.01, math.log1p(1000), 3)
        narrow = routing_score(math.log1p(50), 10, 0.01, math.log1p(1000), 3)
        assert wide > narrow

    def test_lower_slope_scores_higher(self):
        """Lower slope should score higher (negative weight)."""
        import math

        low_slope = routing_score(math.log1p(100), 10, 0.001, math.log1p(1000), 3)
        high_slope = routing_score(math.log1p(100), 10, 0.1, math.log1p(1000), 3)
        assert low_slope > high_slope

    def test_higher_facc_scores_higher(self):
        """Higher facc should score higher, all else equal."""
        import math

        big_facc = routing_score(math.log1p(100), 12, 0.01, math.log1p(1000), 3)
        small_facc = routing_score(math.log1p(100), 5, 0.01, math.log1p(1000), 3)
        assert big_facc > small_facc

    def test_width_dominates_slope(self):
        """6:1 width ratio should beat any slope difference."""
        import math

        # 4500m wide, steep slope
        wide_steep = routing_score(
            math.log1p(4500),
            math.log1p(200000),
            0.12,
            math.log1p(2000000),
            5,
        )
        # 700m wide, gentle slope
        narrow_gentle = routing_score(
            math.log1p(700),
            math.log1p(30000),
            0.007,
            math.log1p(2000000),
            5,
        )
        assert wide_steep > narrow_gentle

    def test_negative_width_treated_as_zero(self):
        """Negative width (GRWL fill=-1) should not crash."""
        import math

        score = routing_score(math.log1p(max(-1, 0)), 10, 0.01, math.log1p(1000), 3)
        assert score > 0

    def test_weights_dict_has_five_keys(self):
        """ROUTING_WEIGHTS has exactly 5 learned weights."""
        assert len(ROUTING_WEIGHTS) == 5
        assert set(ROUTING_WEIGHTS.keys()) == {
            "effective_width",
            "facc",
            "slope",
            "pathlen",
            "stream_order",
        }

    def test_slope_weight_is_negative(self):
        """Slope weight must be negative (prefer lower gradient)."""
        assert ROUTING_WEIGHTS["slope"] < 0

    def test_width_weight_is_largest(self):
        """Width weight should be the dominant signal."""
        assert ROUTING_WEIGHTS["effective_width"] > max(
            abs(v) for k, v in ROUTING_WEIGHTS.items() if k != "effective_width"
        )
