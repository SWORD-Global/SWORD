"""Tests for flow direction correction module."""

import duckdb
import networkx as nx
import numpy as np
import pandas as pd
import pytest

from src.sword_v17c_pipeline.flow_direction import (
    correct_flow_directions,
    create_flow_corrections_table,
    flip_section_topology,
    rollback_flow_corrections,
    score_section_confidence,
    snapshot_topology,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def conn():
    """In-memory DuckDB with reach_topology table."""
    c = duckdb.connect(":memory:")
    c.execute("""
        CREATE TABLE reach_topology (
            reach_id BIGINT, direction VARCHAR,
            neighbor_rank INTEGER, neighbor_reach_id BIGINT,
            region VARCHAR(2)
        )
    """)
    return c


def _insert_topology(conn, rows):
    """Insert topology rows: [(reach_id, direction, rank, neighbor, region), ...]"""
    for row in rows:
        conn.execute("INSERT INTO reach_topology VALUES (?, ?, ?, ?, ?)", list(row))


def _make_reaches_df(**overrides):
    """Build a minimal reaches DataFrame for scoring tests."""
    defaults = {
        "reach_id": [101, 102, 103],
        "wse_obs_mean": [100.0, 99.0, 98.0],
        "slope_obs_q": [0, 0, 0],
        "slope_obs_n_passes": [15, 12, 14],
        "n_obs": [50, 45, 48],
        "lakeflag": [0, 0, 0],
        "facc": [100.0, 200.0, 300.0],
    }
    defaults.update(overrides)
    return pd.DataFrame(defaults)


def _make_graph(reach_ids, lakeflag_map=None, facc_start=100):
    """Build a simple linear graph for testing."""
    G = nx.DiGraph()
    for i, rid in enumerate(reach_ids):
        lf = (lakeflag_map or {}).get(rid, 0)
        G.add_node(rid, lakeflag=lf, reach_length=1000, facc=facc_start + i * 100)
    for i in range(len(reach_ids) - 1):
        G.add_edge(reach_ids[i], reach_ids[i + 1])
    return G


# ---------------------------------------------------------------------------
# score_section_confidence
# ---------------------------------------------------------------------------


class TestScoreSectionConfidence:
    def test_valid_section_is_skip(self):
        vrow = {"direction_valid": True, "likely_cause": None}
        tier, meta = score_section_confidence(vrow, nx.DiGraph(), pd.DataFrame(), [])
        assert tier == "SKIP"

    def test_lake_section_is_skip(self):
        vrow = {
            "direction_valid": False,
            "likely_cause": "lake_section",
            "slope_from_upstream": 0.01,
            "slope_from_downstream": -0.01,
        }
        tier, _ = score_section_confidence(vrow, nx.DiGraph(), pd.DataFrame(), [])
        assert tier == "SKIP"

    def test_extreme_data_error_is_skip(self):
        vrow = {
            "direction_valid": False,
            "likely_cause": "extreme_slope_data_error",
        }
        tier, _ = score_section_confidence(vrow, nx.DiGraph(), pd.DataFrame(), [])
        assert tier == "SKIP"

    def test_tidal_section_is_skip(self):
        G = _make_graph([100, 101])
        G.nodes[100]["lakeflag"] = 3
        vrow = {
            "direction_valid": False,
            "likely_cause": "lake_section",
            "upstream_junction": 100,
            "downstream_junction": 101,
            "slope_from_upstream": 0.01,
            "slope_from_downstream": -0.01,
        }
        tier, _ = score_section_confidence(vrow, G, _make_reaches_df(), [101])
        assert tier == "SKIP"

    def test_high_confidence_both_wrong(self, monkeypatch):
        """Both slopes wrong, good quality, good facc snap -> HIGH."""
        import src.sword_v17c_pipeline.flow_direction as fd

        monkeypatch.setattr(fd, "_get_facc_confidence", lambda *a, **k: (1.0, 0.05))
        G = _make_graph([100, 101, 102, 103, 104])
        rdf = _make_reaches_df(
            reach_id=[101, 102, 103],
            wse_obs_mean=[100.0, 99.0, 98.0],
            slope_obs_q=[0, 0, 0],
            slope_obs_n_passes=[15, 12, 14],
            facc=[100.0, 200.0, 300.0],
        )
        vrow = {
            "direction_valid": False,
            "likely_cause": "potential_topology_error",
            "slope_from_upstream": 0.01,  # wrong: should be negative
            "slope_from_downstream": -0.01,  # wrong: should be positive
            "upstream_junction": 100,
            "downstream_junction": 104,
        }
        tier, meta = score_section_confidence(vrow, G, rdf, [101, 102, 103])
        assert tier == "HIGH"

    def test_medium_confidence_fewer_passes(self, monkeypatch):
        """Both slopes wrong, moderate facc snap -> MEDIUM."""
        import src.sword_v17c_pipeline.flow_direction as fd

        monkeypatch.setattr(fd, "_get_facc_confidence", lambda *a, **k: (0.7, 0.3))
        G = _make_graph([100, 101, 102, 103])
        rdf = _make_reaches_df(
            reach_id=[101, 102],
            wse_obs_mean=[100.0, 99.0],
            slope_obs_q=[0, 2],  # one has low n_passes flag
            slope_obs_n_passes=[5, 5],
            n_obs=[50, 45],
            lakeflag=[0, 0],
            facc=[100.0, 200.0],
        )
        vrow = {
            "direction_valid": False,
            "likely_cause": "potential_topology_error",
            "slope_from_upstream": 0.01,
            "slope_from_downstream": -0.01,
            "upstream_junction": 100,
            "downstream_junction": 103,
        }
        tier, _ = score_section_confidence(vrow, G, rdf, [101, 102])
        assert tier == "MEDIUM"

    def test_low_confidence_single_wrong(self):
        """Only one slope wrong -> LOW."""
        G = _make_graph([100, 101, 102, 103])
        rdf = _make_reaches_df(reach_id=[101, 102, 103], facc=[100.0, 200.0, 300.0])
        vrow = {
            "direction_valid": False,
            "likely_cause": "potential_topology_error",
            "slope_from_upstream": -0.01,  # correct sign
            "slope_from_downstream": -0.01,  # wrong sign
            "upstream_junction": 100,
            "downstream_junction": 103,
        }
        tier, _ = score_section_confidence(vrow, G, rdf, [101, 102, 103])
        assert tier == "LOW"

    def test_low_confidence_insufficient_wse(self):
        """Not enough WSE reaches -> LOW."""
        G = _make_graph([100, 101])
        rdf = _make_reaches_df(
            reach_id=[101],
            wse_obs_mean=[np.nan],
            slope_obs_q=[0],
            slope_obs_n_passes=[15],
            n_obs=[0],
            lakeflag=[0],
            facc=[100.0],
        )
        vrow = {
            "direction_valid": False,
            "likely_cause": "potential_topology_error",
            "slope_from_upstream": 0.01,
            "slope_from_downstream": -0.01,
            "upstream_junction": 100,
            "downstream_junction": 101,
        }
        tier, meta = score_section_confidence(vrow, G, rdf, [101])
        assert tier == "LOW"

    def test_low_confidence_extreme_flags(self):
        """Extreme quality flags -> LOW even if both wrong."""
        G = _make_graph([100, 101, 102, 103])
        rdf = _make_reaches_df(
            reach_id=[101, 102, 103],
            slope_obs_q=[8, 8, 0],  # bit 8 = extreme slope
            slope_obs_n_passes=[15, 12, 14],
            facc=[100.0, 200.0, 300.0],
        )
        vrow = {
            "direction_valid": False,
            "likely_cause": "potential_topology_error",
            "slope_from_upstream": 0.01,
            "slope_from_downstream": -0.01,
            "upstream_junction": 100,
            "downstream_junction": 103,
        }
        tier, _ = score_section_confidence(vrow, G, rdf, [101, 102, 103])
        assert tier == "LOW"


# ---------------------------------------------------------------------------
# flip_section_topology
# ---------------------------------------------------------------------------


class TestFlipSectionTopology:
    def test_flip_swaps_direction(self, conn):
        """Flipping swaps 'up' to 'down' and vice versa."""
        _insert_topology(
            conn,
            [
                (101, "up", 0, 102, "NA"),
                (102, "down", 0, 101, "NA"),
                (101, "down", 0, 103, "NA"),  # external — should NOT flip
            ],
        )
        n = flip_section_topology(conn, "NA", [101, 102], 101, 102)
        assert n == 2

        rows = conn.execute(
            "SELECT reach_id, direction, neighbor_reach_id FROM reach_topology ORDER BY reach_id, neighbor_reach_id"
        ).fetchall()
        # (101, down, 102) was 'up' -> now 'down'
        # (101, down, 103) external -> unchanged 'down'
        # (102, up, 101) was 'down' -> now 'up'
        directions = {(r[0], r[2]): r[1] for r in rows}
        assert directions[(101, 102)] == "down"  # was up
        assert directions[(102, 101)] == "up"  # was down
        assert directions[(101, 103)] == "down"  # external, unchanged

    def test_flip_preserves_external(self, conn):
        """External connections not in section_set are untouched."""
        _insert_topology(
            conn,
            [
                (200, "up", 0, 201, "NA"),
                (201, "down", 0, 200, "NA"),
                (201, "down", 0, 300, "NA"),  # external
                (300, "up", 0, 201, "NA"),  # external
            ],
        )
        flip_section_topology(conn, "NA", [200, 201], 200, 201)
        ext = conn.execute(
            "SELECT direction FROM reach_topology "
            "WHERE reach_id = 201 AND neighbor_reach_id = 300"
        ).fetchone()[0]
        assert ext == "down"  # unchanged


# ---------------------------------------------------------------------------
# snapshot_topology / rollback
# ---------------------------------------------------------------------------


class TestSnapshotRollback:
    def test_snapshot_and_rollback(self, conn):
        _insert_topology(
            conn,
            [
                (10, "up", 0, 20, "NA"),
                (20, "down", 0, 10, "NA"),
            ],
        )
        table = snapshot_topology(conn, "NA", "test123")
        assert "test123" in table

        # Modify topology
        conn.execute("DELETE FROM reach_topology WHERE reach_id = 10")
        assert conn.execute("SELECT COUNT(*) FROM reach_topology").fetchone()[0] == 1

        # Rollback
        n = rollback_flow_corrections(conn, "NA", "test123")
        assert n == 2
        assert conn.execute("SELECT COUNT(*) FROM reach_topology").fetchone()[0] == 2

    def test_rollback_missing_backup_raises(self, conn):
        with pytest.raises(ValueError, match="not found"):
            rollback_flow_corrections(conn, "NA", "nonexistent")


# ---------------------------------------------------------------------------
# correct_flow_directions (integration)
# ---------------------------------------------------------------------------


class TestCorrectFlowDirections:
    def _setup_correction_scenario(self, conn):
        """Set up a scenario with one invalid section."""
        _insert_topology(
            conn,
            [
                (1, "up", 0, 2, "NA"),
                (2, "down", 0, 1, "NA"),
                (2, "up", 0, 3, "NA"),
                (3, "down", 0, 2, "NA"),
            ],
        )
        create_flow_corrections_table(conn)

        G = _make_graph([1, 2, 3])
        sections_df = pd.DataFrame(
            [
                {
                    "section_id": 0,
                    "upstream_junction": 1,
                    "downstream_junction": 3,
                    "reach_ids": [1, 2, 3],
                    "distance": 3000,
                    "n_reaches": 3,
                }
            ]
        )
        validation_df = pd.DataFrame(
            [
                {
                    "section_id": 0,
                    "direction_valid": False,
                    "likely_cause": "potential_topology_error",
                    "slope_from_upstream": 0.01,
                    "slope_from_downstream": -0.01,
                    "upstream_junction": 1,
                    "downstream_junction": 3,
                    "n_reaches": 3,
                    "n_reaches_with_wse": 3,
                }
            ]
        )
        reaches_df = _make_reaches_df(
            reach_id=[1, 2, 3],
            wse_obs_mean=[100.0, 99.0, 98.0],
            slope_obs_q=[0, 0, 0],
            slope_obs_n_passes=[15, 12, 14],
            n_obs=[50, 45, 48],
            lakeflag=[0, 0, 0],
            facc=[100.0, 200.0, 300.0],
        )
        return G, sections_df, validation_df, reaches_df

    def test_single_pass_flips(self, conn, monkeypatch):
        """Single-pass mode flips HIGH section."""
        import src.sword_v17c_pipeline.flow_direction as fd

        monkeypatch.setattr(fd, "_get_facc_confidence", lambda *a, **k: (1.0, 0.05))
        G, sdf, vdf, rdf = self._setup_correction_scenario(conn)
        result = correct_flow_directions(
            conn,
            "NA",
            G,
            sdf,
            vdf,
            rdf,
            run_id="testrun1",
            rebuild_fn=None,
        )
        assert result["n_flipped"] == 1
        assert result["run_id"] == "testrun1"

        # Verify provenance log
        logs = conn.execute(
            "SELECT * FROM v17c_flow_corrections WHERE run_id = 'testrun1'"
        ).fetchdf()
        assert len(logs) == 1
        assert logs.iloc[0]["tier"] == "HIGH"

    def test_skip_sections_not_flipped(self, conn):
        """Lake/skip sections are not flipped."""
        create_flow_corrections_table(conn)
        _insert_topology(
            conn,
            [
                (1, "up", 0, 2, "NA"),
                (2, "down", 0, 1, "NA"),
            ],
        )
        G = _make_graph([1, 2])
        sdf = pd.DataFrame(
            [
                {
                    "section_id": 0,
                    "upstream_junction": 1,
                    "downstream_junction": 2,
                    "reach_ids": [1, 2],
                    "distance": 2000,
                    "n_reaches": 2,
                }
            ]
        )
        vdf = pd.DataFrame(
            [
                {
                    "section_id": 0,
                    "direction_valid": False,
                    "likely_cause": "lake_section",
                    "slope_from_upstream": 0.01,
                    "slope_from_downstream": -0.01,
                    "upstream_junction": 1,
                    "downstream_junction": 2,
                }
            ]
        )
        rdf = _make_reaches_df(
            reach_id=[1, 2],
            wse_obs_mean=[100.0, 99.0],
            slope_obs_q=[0, 0],
            slope_obs_n_passes=[15, 12],
            n_obs=[50, 45],
            lakeflag=[1, 1],
            facc=[100.0, 200.0],
        )
        result = correct_flow_directions(conn, "NA", G, sdf, vdf, rdf, run_id="skip1")
        assert result["n_flipped"] == 0

    def test_oscillation_guard(self, conn):
        """Section flipped twice gets demoted to LOW."""
        G, sdf, vdf, rdf = self._setup_correction_scenario(conn)
        call_count = [0]

        def mock_rebuild(c, r):
            call_count[0] += 1
            # Always return same invalid validation to force oscillation
            return G, sdf, vdf

        result = correct_flow_directions(
            conn,
            "NA",
            G,
            sdf,
            vdf,
            rdf,
            run_id="osc1",
            rebuild_fn=mock_rebuild,
            max_iterations=5,
        )
        # Should flip twice then stop (oscillation guard kicks in)
        assert result["n_flipped"] <= 2
        assert result["n_manual_review"] >= 1
