"""Tests for flow_verification module — multi-signal scoring + grouped flipping."""

import duckdb
import networkx as nx
import pandas as pd
import pytest

from src.sword_v17c_pipeline.flow_verification import (
    _compute_facc_fraction_decreasing,
    _compute_wse_slope,
    apply_verified_flips,
    build_flip_groups,
    score_reversal_signals,
    verify_flip_group,
)

pytestmark = [pytest.mark.topology, pytest.mark.pipeline]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _linear_graph(reach_ids, attrs=None):
    """Build a linear DAG: reach_ids[0] -> reach_ids[1] -> ..."""
    G = nx.DiGraph()
    defaults = {"reach_length": 1000, "facc": 100, "type": 1, "lakeflag": 0}
    for rid in reach_ids:
        node_attrs = dict(defaults)
        if attrs and rid in attrs:
            node_attrs.update(attrs[rid])
        G.add_node(rid, **node_attrs)
    for i in range(len(reach_ids) - 1):
        G.add_edge(reach_ids[i], reach_ids[i + 1])
    return G


def _make_section(sid, reach_ids, uj, dj, distance=None):
    """Build a section dict matching build_section_graph output."""
    return {
        "section_id": sid,
        "reach_ids": reach_ids,
        "upstream_junction": uj,
        "downstream_junction": dj,
        "distance": distance or len(reach_ids) * 1000,
        "n_reaches": len(reach_ids),
    }


def _make_sections_df(sections):
    return pd.DataFrame(sections)


def _empty_reaches_df():
    return pd.DataFrame(columns=["reach_id", "wse_obs_p50", "facc", "lakeflag", "type"])


# ---------------------------------------------------------------------------
# _compute_wse_slope
# ---------------------------------------------------------------------------


class TestComputeWseSlope:
    def test_decreasing_wse_negative_slope(self):
        """WSE decreasing downstream => negative slope => correct flow."""
        G = _linear_graph(
            [1, 2, 3],
            {1: {"wse": 100.0}, 2: {"wse": 95.0}, 3: {"wse": 90.0}},
        )
        slope = _compute_wse_slope(G, [1, 2, 3], col="wse")
        assert slope is not None
        assert slope < 0

    def test_increasing_wse_positive_slope(self):
        """WSE increasing downstream => positive slope => reversed flow."""
        G = _linear_graph(
            [1, 2, 3],
            {1: {"wse": 90.0}, 2: {"wse": 95.0}, 3: {"wse": 100.0}},
        )
        slope = _compute_wse_slope(G, [1, 2, 3], col="wse")
        assert slope is not None
        assert slope > 0

    def test_insufficient_data_returns_none(self):
        """< 2 valid values => None."""
        G = _linear_graph([1, 2], {1: {"wse": 100.0}})
        assert _compute_wse_slope(G, [1, 2], col="wse") is None

    def test_missing_column_returns_none(self):
        """Column not present on any node => None."""
        G = _linear_graph([1, 2, 3])
        assert _compute_wse_slope(G, [1, 2, 3], col="wse_obs_p50") is None


# ---------------------------------------------------------------------------
# _compute_facc_fraction_decreasing
# ---------------------------------------------------------------------------


class TestFaccFractionDecreasing:
    def test_all_increasing_returns_zero(self):
        """facc increasing downstream => fraction = 0 (correct flow)."""
        G = _linear_graph(
            [1, 2, 3],
            {1: {"facc": 100}, 2: {"facc": 200}, 3: {"facc": 300}},
        )
        frac = _compute_facc_fraction_decreasing(G, [1, 2, 3])
        assert frac == 0.0

    def test_all_decreasing_returns_one(self):
        """facc decreasing downstream => fraction = 1 (reversed)."""
        G = _linear_graph(
            [1, 2, 3],
            {1: {"facc": 300}, 2: {"facc": 200}, 3: {"facc": 100}},
        )
        frac = _compute_facc_fraction_decreasing(G, [1, 2, 3])
        assert frac == 1.0

    def test_mixed_returns_fraction(self):
        G = _linear_graph(
            [1, 2, 3, 4],
            {1: {"facc": 100}, 2: {"facc": 50}, 3: {"facc": 200}, 4: {"facc": 150}},
        )
        frac = _compute_facc_fraction_decreasing(G, [1, 2, 3, 4])
        assert frac == pytest.approx(2 / 3)

    def test_insufficient_data_returns_none(self):
        G = _linear_graph([1], {1: {"facc": 0}})
        assert _compute_facc_fraction_decreasing(G, [1]) is None


# ---------------------------------------------------------------------------
# score_reversal_signals
# ---------------------------------------------------------------------------


class TestScoreReversalSignals:
    def test_valid_section_is_skip(self):
        vrow = {"direction_valid": True, "likely_cause": None}
        tier, meta = score_reversal_signals(nx.DiGraph(), {}, pd.DataFrame(), vrow)
        assert tier == "SKIP"

    def test_lake_section_is_skip(self):
        vrow = {"direction_valid": False, "likely_cause": "lake_section"}
        tier, _ = score_reversal_signals(nx.DiGraph(), {}, pd.DataFrame(), vrow)
        assert tier == "SKIP"

    def test_all_ghost_is_skip(self):
        G = _linear_graph([1, 2], {1: {"type": 6}, 2: {"type": 6}})
        sec = _make_section(0, [1, 2], 0, 2)
        vrow = {
            "direction_valid": False,
            "likely_cause": "potential_topology_error",
            "slope_from_upstream": 0.01,
            "slope_from_downstream": -0.01,
        }
        tier, meta = score_reversal_signals(G, sec, _empty_reaches_df(), vrow)
        assert tier == "SKIP"
        assert meta["reason"] == "all_ghost_reaches"

    def test_single_reach_section_is_skip(self):
        """Single-reach sections are too noisy and should be skipped."""
        G = _linear_graph([1, 2], {1: {"type": 1}, 2: {"type": 1}})
        sec = _make_section(0, [2], 1, 2)
        vrow = {
            "direction_valid": False,
            "likely_cause": "potential_topology_error",
            "slope_from_upstream": 0.01,
            "slope_from_downstream": -0.01,
        }
        tier, meta = score_reversal_signals(G, sec, _empty_reaches_df(), vrow)
        assert tier == "SKIP"
        assert meta["reason"] == "single_reach_section"

    def test_downstream_junction_ghost_is_skip(self):
        """Section whose downstream junction is type=6 ghost should be skipped."""
        G = _linear_graph([1, 2, 3], {1: {"type": 1}, 2: {"type": 1}, 3: {"type": 6}})
        sec = _make_section(0, [2, 3], 1, 3)
        vrow = {
            "direction_valid": False,
            "likely_cause": "potential_topology_error",
            "slope_from_upstream": 0.01,
            "slope_from_downstream": -0.01,
        }
        tier, meta = score_reversal_signals(G, sec, _empty_reaches_df(), vrow)
        assert tier == "SKIP"
        assert meta["reason"] == "downstream_junction_type=6"

    def test_upstream_junction_ghost_is_skip(self):
        """Section whose upstream junction is type=6 ghost should be skipped."""
        G = _linear_graph([1, 2, 3], {1: {"type": 6}, 2: {"type": 1}, 3: {"type": 1}})
        sec = _make_section(0, [2, 3], 1, 3)
        vrow = {
            "direction_valid": False,
            "likely_cause": "potential_topology_error",
            "slope_from_upstream": 0.01,
            "slope_from_downstream": -0.01,
        }
        tier, meta = score_reversal_signals(G, sec, _empty_reaches_df(), vrow)
        assert tier == "SKIP"
        assert meta["reason"] == "upstream_junction_type=6"

    def test_downstream_junction_unreliable_is_skip(self):
        """Section whose downstream junction is type=5 unreliable should be skipped."""
        G = _linear_graph([1, 2, 3], {1: {"type": 1}, 2: {"type": 1}, 3: {"type": 5}})
        sec = _make_section(0, [2, 3], 1, 3)
        vrow = {
            "direction_valid": False,
            "likely_cause": "potential_topology_error",
            "slope_from_upstream": 0.01,
            "slope_from_downstream": -0.01,
        }
        tier, meta = score_reversal_signals(G, sec, _empty_reaches_df(), vrow)
        assert tier == "SKIP"
        assert meta["reason"] == "downstream_junction_type=5"

    def test_all_lake_reaches_is_skip(self):
        """Section where every reach is a lake (lakeflag>=1) should be skipped."""
        G = _linear_graph(
            [1, 2, 3],
            {
                1: {"lakeflag": 1, "type": 3},
                2: {"lakeflag": 1, "type": 3},
                3: {"lakeflag": 1, "type": 3},
            },
        )
        sec = _make_section(0, [2, 3], 1, 3)
        vrow = {
            "direction_valid": False,
            "likely_cause": "potential_topology_error",
            "slope_from_upstream": 0.01,
            "slope_from_downstream": -0.01,
        }
        tier, meta = score_reversal_signals(G, sec, _empty_reaches_df(), vrow)
        assert tier == "SKIP"
        assert meta["reason"] == "all_lake_reaches"

    def test_mixed_lake_river_not_skipped(self):
        """Section with mixed lake and river reaches should NOT be lake-skipped."""
        G = _linear_graph(
            [10, 11, 12, 13, 14],
            {
                10: {"wse": 90, "wse_obs_p50": 90, "facc": 500, "lakeflag": 0},
                11: {"wse": 92, "wse_obs_p50": 92, "facc": 400, "lakeflag": 1},
                12: {"wse": 94, "wse_obs_p50": 94, "facc": 300, "lakeflag": 0},
                13: {"wse": 96, "wse_obs_p50": 96, "facc": 200, "lakeflag": 0},
                14: {"wse": 98, "wse_obs_p50": 98, "facc": 100, "lakeflag": 0},
            },
        )
        sec = _make_section(0, [11, 12, 13, 14], 10, 14)
        vrow = {
            "direction_valid": False,
            "likely_cause": "potential_topology_error",
            "slope_from_upstream": 0.01,
            "slope_from_downstream": -0.01,
        }
        tier, _ = score_reversal_signals(G, sec, _empty_reaches_df(), vrow)
        assert tier != "SKIP"

    def test_high_multi_signal(self):
        """All 4 signals agree on reversal => HIGH."""
        G = _linear_graph(
            [10, 11, 12, 13, 14],
            {
                10: {"wse": 90, "wse_obs_p50": 90, "facc": 500},
                11: {"wse": 92, "wse_obs_p50": 92, "facc": 400},
                12: {"wse": 94, "wse_obs_p50": 94, "facc": 300},
                13: {"wse": 96, "wse_obs_p50": 96, "facc": 200},
                14: {"wse": 98, "wse_obs_p50": 98, "facc": 100},
            },
        )
        sec = _make_section(0, [11, 12, 13, 14], 10, 14)
        vrow = {
            "direction_valid": False,
            "likely_cause": "potential_topology_error",
            "slope_from_upstream": 0.01,
            "slope_from_downstream": -0.01,
        }
        tier, meta = score_reversal_signals(G, sec, _empty_reaches_df(), vrow)
        assert tier == "HIGH"
        assert meta["n_reversed"] >= 3

    def test_medium_two_signals(self):
        """Only 2 signals agree => MEDIUM."""
        G = _linear_graph(
            [10, 11, 12],
            {
                # WSE flat — no DEM signal
                10: {"facc": 300},
                11: {"facc": 200},
                12: {"facc": 100},
            },
        )
        sec = _make_section(0, [11, 12], 10, 12)
        vrow = {
            "direction_valid": False,
            "likely_cause": "potential_topology_error",
            "slope_from_upstream": 0.01,
            "slope_from_downstream": -0.01,
        }
        tier, meta = score_reversal_signals(G, sec, _empty_reaches_df(), vrow)
        assert tier in ("HIGH", "MEDIUM")

    def test_low_conflicting_signals(self):
        """Signals disagree => LOW."""
        G = _linear_graph(
            [10, 11, 12],
            {
                # WSE decreasing (correct), but facc also decreasing (wrong)
                10: {"wse": 100, "facc": 300},
                11: {"wse": 95, "facc": 200},
                12: {"wse": 90, "facc": 100},
            },
        )
        sec = _make_section(0, [11, 12], 10, 12)
        vrow = {
            "direction_valid": False,
            "likely_cause": "potential_topology_error",
            "slope_from_upstream": -0.01,  # correct sign
            "slope_from_downstream": 0.01,  # correct sign
        }
        tier, _ = score_reversal_signals(G, sec, _empty_reaches_df(), vrow)
        assert tier == "LOW"


# ---------------------------------------------------------------------------
# build_flip_groups
# ---------------------------------------------------------------------------


class TestBuildFlipGroups:
    def test_isolated_sections_separate_groups(self):
        """Non-adjacent sections form separate groups."""
        candidates = [
            _make_section(0, [2, 3], 1, 3) | {"tier": "HIGH", "diagnostics": {}},
            _make_section(1, [6, 7], 5, 7) | {"tier": "HIGH", "diagnostics": {}},
        ]
        groups = build_flip_groups(candidates, _make_sections_df(candidates))
        assert len(groups) == 2

    def test_adjacent_sections_same_group(self):
        """Sections sharing a junction form one group."""
        candidates = [
            _make_section(0, [2, 3], 1, 3) | {"tier": "HIGH", "diagnostics": {}},
            _make_section(1, [4, 5], 3, 5) | {"tier": "HIGH", "diagnostics": {}},
        ]
        groups = build_flip_groups(candidates, _make_sections_df(candidates))
        assert len(groups) == 1
        assert len(groups[0]) == 2

    def test_empty_candidates_empty_groups(self):
        assert build_flip_groups([], pd.DataFrame()) == []

    def test_groups_sorted_smallest_first(self):
        """Groups returned smallest-first."""
        candidates = [
            _make_section(0, [2, 3], 1, 3) | {"tier": "HIGH", "diagnostics": {}},
            _make_section(1, [4, 5], 3, 5) | {"tier": "HIGH", "diagnostics": {}},
            _make_section(2, [8, 9], 7, 9) | {"tier": "HIGH", "diagnostics": {}},
        ]
        groups = build_flip_groups(candidates, _make_sections_df(candidates))
        # Group of 2 (sections 0,1 share junction 3) and group of 1 (section 2)
        assert len(groups[0]) <= len(groups[-1])


# ---------------------------------------------------------------------------
# verify_flip_group
# ---------------------------------------------------------------------------


class TestVerifyFlipGroup:
    def test_simple_reversal_approved(self):
        """Reversing a reversed section preserves DAG and connectivity."""
        # Scenario: section is reversed (facc decreasing downstream).
        # The section sits between two external branches so both junctions
        # still reach the pre-flip outlet after flipping.
        #
        #   EXT_HW(1) -> UJ(10) -> 20 -> DJ(30) -> OUT(40)
        #                  ^                  |
        #                  |                  v
        #                  +--- EXT(50) <-----+  (DJ also -> 50 -> ... -> OUT(40))
        #
        # Section: [20, 30] with uj=10, dj=30
        # After flip: 30 -> 20 -> 10.  UJ(10) becomes a new sink, but
        # we need UJ(10) to still reach pre-flip outlet 40.
        # So add an external path: 10 -> 50 -> 40
        G = nx.DiGraph()
        for nid, facc in [
            (1, 50),
            (10, 500),
            (20, 400),
            (30, 300),
            (40, 200),
            (50, 250),
        ]:
            G.add_node(nid, facc=facc, reach_length=1000, type=1, lakeflag=0)
        G.add_edge(1, 10)
        G.add_edge(10, 20)
        G.add_edge(20, 30)
        G.add_edge(30, 40)
        G.add_edge(10, 50)  # external bypass so 10 can still reach 40
        G.add_edge(50, 40)

        section = _make_section(0, [20, 30], 10, 30)
        entry = {**section, "tier": "HIGH", "diagnostics": {}}

        ok, reason, diag = verify_flip_group(
            G, [entry], _make_sections_df([section]), _empty_reaches_df()
        )
        assert ok, f"Expected approved, got: {reason}"
        assert diag["post_facc_violations"] <= diag["pre_facc_violations"]

    def test_cycle_rejected(self):
        """Flip that creates a cycle is rejected."""
        # Build: HW(0) -> 1 -> 2 -> 3 -> OUT(4), plus 3 -> 1 (back-edge).
        # Section [2, 3] with uj=1, dj=3.
        # After flip: 3 -> 2 -> 1.  Combined with existing 3 -> 1,
        # we get 1 -> (external) ... and 3 -> 1, 3 -> 2 -> 1.  No cycle yet.
        # But add 1 -> 3 via an external edge to create a genuine post-flip cycle:
        #   After flip: 1 -> 3 (external) -> 2 -> 1 = cycle!
        G = nx.DiGraph()
        for n in [0, 1, 2, 3, 4]:
            G.add_node(n, facc=n * 100 + 100, reach_length=1000, type=1, lakeflag=0)
        G.add_edge(0, 1)
        G.add_edge(1, 2)
        G.add_edge(2, 3)
        G.add_edge(3, 4)
        G.add_edge(
            1, 3
        )  # external shortcut; after flip of 2->3 to 3->2, cycle: 1->3->2->1

        section = _make_section(0, [2, 3], 1, 3)
        entry = {**section, "tier": "HIGH", "diagnostics": {}}

        ok, reason, _ = verify_flip_group(
            G, [entry], _make_sections_df([section]), _empty_reaches_df()
        )
        assert not ok
        assert "cycle" in reason

    def test_group_too_large_rejected(self):
        """Group exceeding max size is rejected."""
        entries = []
        for i in range(35):
            sec = _make_section(i, [i * 10 + 1], i * 10, i * 10 + 1)
            entries.append({**sec, "tier": "HIGH", "diagnostics": {}})

        G = nx.DiGraph()
        ok, reason, _ = verify_flip_group(
            G, entries, pd.DataFrame(), pd.DataFrame(), max_group_size=30
        )
        assert not ok
        assert "group_too_large" in reason

    def test_facc_increase_rejected(self):
        """Flip that increases facc violations is rejected."""
        # Build correct-direction graph: facc increases downstream.
        # Add external bypass so junctions still reach outlet after flip.
        #   HW(0) -> 1 -> 2 -> 3 -> 4 -> OUT(5)
        #             \-> 6 -> 5  (bypass so 1 can reach outlet 5)
        G = nx.DiGraph()
        for n, f in [
            (0, 50),
            (1, 100),
            (2, 200),
            (3, 300),
            (4, 400),
            (5, 500),
            (6, 150),
        ]:
            G.add_node(n, facc=f, reach_length=1000, type=1, lakeflag=0)
        G.add_edge(0, 1)
        G.add_edge(1, 2)
        G.add_edge(2, 3)
        G.add_edge(3, 4)
        G.add_edge(4, 5)
        G.add_edge(1, 6)  # bypass
        G.add_edge(6, 5)

        # Section [2,3] with uj=1, dj=3 — facc is CORRECT here
        # Flipping would make facc go backwards => more violations
        section = _make_section(0, [2, 3], 1, 3)
        entry = {**section, "tier": "HIGH", "diagnostics": {}}

        ok, reason, diag = verify_flip_group(
            G, [entry], _make_sections_df([section]), _empty_reaches_df()
        )
        assert not ok
        assert "facc violations increased" in reason


# ---------------------------------------------------------------------------
# apply_verified_flips (integration with in-memory DuckDB)
# ---------------------------------------------------------------------------


class TestApplyVerifiedFlips:
    @pytest.fixture
    def conn(self):
        c = duckdb.connect(":memory:")
        c.execute("""
            CREATE TABLE reach_topology (
                reach_id BIGINT, direction VARCHAR,
                neighbor_rank INTEGER, neighbor_reach_id BIGINT,
                region VARCHAR(2)
            )
        """)
        return c

    def test_apply_flips_and_log(self, conn):
        """Flips are applied and logged to v17c_flow_corrections."""
        # Insert reciprocal topology: 1->2 (1 has up=2, 2 has down=1)
        conn.execute("INSERT INTO reach_topology VALUES (1, 'up', 0, 2, 'NA')")
        conn.execute("INSERT INTO reach_topology VALUES (2, 'down', 0, 1, 'NA')")

        section = _make_section(0, [1, 2], 1, 2)
        entry = {**section, "tier": "HIGH", "diagnostics": {}}
        result = apply_verified_flips(conn, "NA", [[entry]], "test_run_1")

        assert result["n_sections_flipped"] == 1
        assert result["n_topology_rows_flipped"] == 2

        # Check provenance log
        logs = conn.execute(
            "SELECT * FROM v17c_flow_corrections WHERE run_id = 'test_run_1'"
        ).fetchdf()
        assert len(logs) == 1
        assert logs.iloc[0]["tier"] == "HIGH"

    def test_snapshot_created(self, conn):
        """Snapshot backup table is created before flipping."""
        conn.execute("INSERT INTO reach_topology VALUES (1, 'up', 0, 2, 'NA')")

        section = _make_section(0, [1, 2], 1, 2)
        entry = {**section, "tier": "HIGH", "diagnostics": {}}
        apply_verified_flips(conn, "NA", [[entry]], "snap_test")

        # Backup table should exist
        tables = [
            r[0]
            for r in conn.execute(
                "SELECT table_name FROM information_schema.tables"
            ).fetchall()
        ]
        backup = [t for t in tables if "snap_test" in t]
        assert len(backup) == 1
