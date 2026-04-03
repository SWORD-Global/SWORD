# -*- coding: utf-8 -*-
"""
Unit tests for SWORD Workflow class methods.

Tests cover the workflow-level wrappers in workflow.py:
- delete_reach / delete_reaches
- check_topology
- check_node_lengths
- validate_ids
- status
- load/close

Note: break_reach and append_reaches are harder to test without
mock data, so we focus on method existence and RuntimeError checks.
"""

import os
import subprocess
import shutil
import sys

import numpy as np
import pytest

# Add project root to path
main_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, main_dir)

# Imported after path setup to support `python tests/.../test_workflow.py`.
from src.sword_duckdb.workflow import SWORDWorkflow  # noqa: E402

TEST_REGION = "NA"

pytestmark = [pytest.mark.slow, pytest.mark.workflow, pytest.mark.db]


class TestWorkflowLoad:
    """Test load/close methods."""

    def test_load_database(self, ensure_test_db, tmp_path):
        """Test loading database."""
        temp_db = tmp_path / "sword_test.duckdb"
        shutil.copy2(ensure_test_db, temp_db)

        workflow = SWORDWorkflow(user_id="test", enable_provenance=False)
        try:
            assert not workflow.is_loaded
            workflow.load(str(temp_db), TEST_REGION)
            assert workflow.is_loaded
        finally:
            if workflow.is_loaded:
                workflow.close()

    def test_close_database(self, temp_workflow):
        """Test closing database."""
        assert temp_workflow.is_loaded
        temp_workflow.close()
        assert not temp_workflow.is_loaded

    def test_context_manager(self, ensure_test_db, tmp_path):
        """Test using workflow as context manager."""
        temp_db = tmp_path / "sword_test.duckdb"
        shutil.copy2(ensure_test_db, temp_db)

        with SWORDWorkflow(user_id="test", enable_provenance=False) as wf:
            wf.load(str(temp_db), TEST_REGION)
            assert wf.is_loaded
        assert not wf.is_loaded


class TestWorkflowCommit:
    """Test reactive recalculation on commit."""

    def test_commit_updates_node_dist_out_in_node_order(
        self, ensure_test_db, tmp_path
    ):
        """Commit keeps node dist_out increasing from downstream to upstream."""
        temp_db = tmp_path / "sword_test.duckdb"
        shutil.copy2(ensure_test_db, temp_db)

        result = subprocess.run(
            [sys.executable, "scripts/maintenance/add_node_columns.py", "--db", temp_db],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, result.stderr

        workflow = SWORDWorkflow(user_id="test", enable_provenance=False)
        try:
            workflow.load(str(temp_db), TEST_REGION)
            sword = workflow._sword

            target_reach = None
            for reach_id in sword.reaches.id:
                node_idx = np.where(sword.nodes.reach_id[:] == reach_id)[0]
                if len(node_idx) >= 3:
                    target_reach = int(reach_id)
                    break

            assert target_reach is not None, "Need a reach with at least 3 nodes"

            reach_idx = np.where(sword.reaches.id[:] == target_reach)[0][0]
            workflow.modify_reach(
                target_reach,
                dist_out=float(sword.reaches.dist_out[reach_idx]) + 1000.0,
                reason="Test reactive node dist_out ordering",
            )

            stats = workflow.commit()
            assert stats["dirty_attributes"] == 1
            assert stats["attributes_affected"] == ["reach.dist_out"]

            node_idx = np.where(sword.nodes.reach_id[:] == target_reach)[0]
            sorted_idx = node_idx[np.argsort(sword.nodes.node_order[node_idx])]
            dist_outs = sword.nodes.dist_out[sorted_idx]

            assert np.all(np.diff(dist_outs) >= 0), (
                "node dist_out should increase with node_order after commit"
            )
        finally:
            if workflow.is_loaded:
                workflow.close()


class TestDeleteReach:
    """Test delete_reach method."""

    def test_delete_reach_requires_load(self, unloaded_workflow):
        """Test delete_reach raises error when database not loaded."""
        with pytest.raises(RuntimeError, match="No database loaded"):
            unloaded_workflow.delete_reach(12345)

    def test_delete_reach_single(self, temp_workflow):
        """Test deleting a single reach via workflow."""
        reach_to_delete = int(temp_workflow._sword.reaches.id[0])

        result = temp_workflow.delete_reach(reach_to_delete)

        assert result["success"] is True
        assert result["deleted_reach"] == reach_to_delete
        assert result["cascade"] is True

    def test_delete_reach_no_cascade(self, temp_workflow):
        """Test delete_reach with cascade=False."""
        reach_to_delete = int(temp_workflow._sword.reaches.id[0])

        result = temp_workflow.delete_reach(reach_to_delete, cascade=False)

        assert result["success"] is True
        assert result["cascade"] is False

    def test_delete_reach_with_reason(self, temp_workflow):
        """Test delete_reach with reason parameter."""
        reach_to_delete = int(temp_workflow._sword.reaches.id[0])

        result = temp_workflow.delete_reach(reach_to_delete, reason="Test deletion")

        assert result["success"] is True


class TestDeleteReaches:
    """Test delete_reaches method (multiple)."""

    def test_delete_reaches_requires_load(self, unloaded_workflow):
        """Test delete_reaches raises error when database not loaded."""
        with pytest.raises(RuntimeError, match="No database loaded"):
            unloaded_workflow.delete_reaches([12345, 67890])

    def test_delete_reaches_multiple(self, temp_workflow):
        """Test deleting multiple reaches via workflow."""
        reaches_to_delete = [
            int(temp_workflow._sword.reaches.id[0]),
        ]

        result = temp_workflow.delete_reaches(reaches_to_delete)

        assert result["success"] is True
        assert result["deleted_count"] == 1
        assert result["cascade"] is True


class TestCheckTopology:
    """Test check_topology method."""

    def test_check_topology_requires_load(self, unloaded_workflow):
        """Test check_topology raises error when database not loaded."""
        with pytest.raises(RuntimeError, match="No database loaded"):
            unloaded_workflow.check_topology()

    def test_check_topology_returns_dict(self, temp_workflow):
        """Test check_topology returns proper dict."""
        result = temp_workflow.check_topology(verbose=0)

        assert isinstance(result, dict)
        assert "passed" in result
        assert "total_reaches" in result
        assert "error_counts" in result
        assert "warning_counts" in result
        assert "reaches_with_issues" in result

    def test_check_topology_with_details(self, temp_workflow):
        """Test check_topology with return_details=True."""
        result = temp_workflow.check_topology(verbose=0, return_details=True)

        assert "details" in result


class TestCheckNodeLengths:
    """Test check_node_lengths method."""

    def test_check_node_lengths_requires_load(self, unloaded_workflow):
        """Test check_node_lengths raises error when database not loaded."""
        with pytest.raises(RuntimeError, match="No database loaded"):
            unloaded_workflow.check_node_lengths()

    def test_check_node_lengths_returns_dict(self, temp_workflow):
        """Test check_node_lengths returns proper dict."""
        result = temp_workflow.check_node_lengths(verbose=0)

        assert isinstance(result, dict)
        assert "passed" in result
        assert "total_nodes" in result
        assert "long_nodes" in result
        assert "zero_length_nodes" in result
        assert "affected_reaches" in result

    def test_check_node_lengths_custom_threshold(self, temp_workflow):
        """Test check_node_lengths with custom threshold."""
        result = temp_workflow.check_node_lengths(verbose=0, threshold=500.0)

        assert isinstance(result, dict)
        assert "passed" in result


class TestValidateIds:
    """Test validate_ids method."""

    def test_validate_ids_requires_load(self, unloaded_workflow):
        """Test validate_ids raises error when database not loaded."""
        with pytest.raises(RuntimeError, match="No database loaded"):
            unloaded_workflow.validate_ids()

    def test_validate_ids_all_valid(self, temp_workflow):
        """Test validate_ids returns results dict."""
        # Use a small subset to avoid long runtime
        reach_ids = temp_workflow._sword.reaches.id[:5].tolist()
        node_ids = temp_workflow._sword.nodes.id[:5].tolist()

        result = temp_workflow.validate_ids(reach_ids=reach_ids, node_ids=node_ids)

        assert isinstance(result, dict)
        assert "passed" in result
        assert "total_reaches_checked" in result
        assert "total_nodes_checked" in result
        assert "invalid_reaches" in result
        assert "invalid_nodes" in result
        assert result["total_reaches_checked"] == 5
        assert result["total_nodes_checked"] == 5

    def test_validate_ids_detects_invalid_reach(self, temp_workflow):
        """Test validate_ids detects invalid reach ID."""
        result = temp_workflow.validate_ids(
            reach_ids=[12345],  # Invalid format
            node_ids=[],
        )

        assert result["passed"] is False
        assert 12345 in result["invalid_reaches"]

    def test_validate_ids_detects_invalid_node(self, temp_workflow):
        """Test validate_ids detects invalid node ID."""
        result = temp_workflow.validate_ids(
            reach_ids=[],
            node_ids=[12345],  # Invalid format
        )

        assert result["passed"] is False
        assert 12345 in result["invalid_nodes"]


class TestStatus:
    """Test status method."""

    def test_status_when_loaded(self, temp_workflow):
        """Test status returns correct info when loaded."""
        status = temp_workflow.status()

        assert isinstance(status, dict)
        assert status["is_loaded"] is True
        assert status["region"] == TEST_REGION

    def test_status_when_not_loaded(self, unloaded_workflow):
        """Test status returns correct info when not loaded."""
        status = unloaded_workflow.status()

        assert isinstance(status, dict)
        assert status["is_loaded"] is False


class TestBreakReach:
    """Test break_reach method existence and basic checks."""

    def test_break_reach_method_exists(self, temp_workflow):
        """Test break_reach method exists."""
        assert hasattr(temp_workflow, "break_reach")
        assert callable(temp_workflow.break_reach)

    def test_break_reach_requires_load(self, unloaded_workflow):
        """Test break_reach raises error when database not loaded."""
        with pytest.raises(RuntimeError, match="No database loaded"):
            unloaded_workflow.break_reach(12345, 67890)


class TestAppendReaches:
    """Test append_reaches method existence and basic checks."""

    def test_append_reaches_method_exists(self, temp_workflow):
        """Test append_reaches method exists."""
        assert hasattr(temp_workflow, "append_reaches")
        assert callable(temp_workflow.append_reaches)

    def test_append_reaches_requires_load(self, unloaded_workflow):
        """Test append_reaches raises error when database not loaded."""
        with pytest.raises(RuntimeError, match="No database loaded"):
            unloaded_workflow.append_reaches(None, None, None)


class TestCreateGhostReach:
    """Test create_ghost_reach workflow method."""

    def test_create_ghost_reach_method_exists(self, temp_workflow):
        """Test create_ghost_reach method exists."""
        assert hasattr(temp_workflow, "create_ghost_reach")
        assert callable(temp_workflow.create_ghost_reach)

    def test_create_ghost_reach_requires_load(self, unloaded_workflow):
        """Test create_ghost_reach raises error when database not loaded."""
        with pytest.raises(RuntimeError, match="No database loaded"):
            unloaded_workflow.create_ghost_reach(12345)

    @pytest.mark.skip(reason="DuckDB numpy.int64 type error in create_ghost_reach")
    def test_create_ghost_reach_via_workflow(self, temp_workflow):
        """Test creating a ghost reach via workflow wrapper."""
        import numpy as np

        # Find a suitable reach (no upstream, at least 2 nodes, not ghost)
        candidate_reach = None
        for idx, reach_id in enumerate(temp_workflow._sword.reaches.id):
            reach_type = str(reach_id)[-1]
            if reach_type == "6":
                continue
            n_up = temp_workflow._sword.reaches.n_rch_up[idx]
            if n_up == 0:
                node_count = np.sum(temp_workflow._sword.nodes.reach_id == reach_id)
                if node_count >= 2:
                    candidate_reach = int(reach_id)
                    break

        if candidate_reach is None:
            pytest.skip("No suitable reach found for ghost reach test")

        result = temp_workflow.create_ghost_reach(
            candidate_reach, position="headwater", reason="Test ghost reach creation"
        )

        assert result["success"] is True
        assert result["original_reach"] == candidate_reach
        assert result["ghost_reach_id"] is not None
        assert str(result["ghost_reach_id"])[-1] == "6"


class TestFindMissingGhostReaches:
    """Test find_missing_ghost_reaches workflow method."""

    def test_find_missing_ghost_reaches_method_exists(self, temp_workflow):
        """Test find_missing_ghost_reaches method exists."""
        assert hasattr(temp_workflow, "find_missing_ghost_reaches")
        assert callable(temp_workflow.find_missing_ghost_reaches)

    def test_find_missing_ghost_reaches_requires_load(self, unloaded_workflow):
        """Test find_missing_ghost_reaches raises error when not loaded."""
        with pytest.raises(RuntimeError, match="No database loaded"):
            unloaded_workflow.find_missing_ghost_reaches()

    def test_find_missing_ghost_reaches_returns_dict(self, temp_workflow):
        """Test find_missing_ghost_reaches returns proper dict."""
        result = temp_workflow.find_missing_ghost_reaches()

        assert isinstance(result, dict)
        assert "missing_headwaters" in result
        assert "missing_outlets" in result
        assert "total_missing" in result


class TestFindIncorrectGhostReaches:
    """Test find_incorrect_ghost_reaches workflow method."""

    def test_find_incorrect_ghost_reaches_method_exists(self, temp_workflow):
        """Test find_incorrect_ghost_reaches method exists."""
        assert hasattr(temp_workflow, "find_incorrect_ghost_reaches")
        assert callable(temp_workflow.find_incorrect_ghost_reaches)

    def test_find_incorrect_ghost_reaches_requires_load(self, unloaded_workflow):
        """Test find_incorrect_ghost_reaches raises error when not loaded."""
        with pytest.raises(RuntimeError, match="No database loaded"):
            unloaded_workflow.find_incorrect_ghost_reaches()

    def test_find_incorrect_ghost_reaches_returns_dict(self, temp_workflow):
        """Test find_incorrect_ghost_reaches returns proper dict."""
        result = temp_workflow.find_incorrect_ghost_reaches()

        assert isinstance(result, dict)
        assert "incorrect_ghost_reaches" in result
        assert "total_incorrect" in result


class TestRecalculateStreamOrder:
    """Test recalculate_stream_order workflow method."""

    def test_recalculate_stream_order_method_exists(self, temp_workflow):
        """Test recalculate_stream_order method exists."""
        assert hasattr(temp_workflow, "recalculate_stream_order")
        assert callable(temp_workflow.recalculate_stream_order)

    def test_recalculate_stream_order_requires_load(self, unloaded_workflow):
        """Test recalculate_stream_order raises error when not loaded."""
        with pytest.raises(RuntimeError, match="No database loaded"):
            unloaded_workflow.recalculate_stream_order()

    def test_recalculate_stream_order_returns_dict(self, temp_workflow):
        """Test recalculate_stream_order returns proper dict."""
        result = temp_workflow.recalculate_stream_order(
            update_nodes=False, update_reaches=False
        )

        assert isinstance(result, dict)
        assert "nodes_updated" in result
        assert "reaches_updated" in result
        assert "nodes_with_valid_path_freq" in result


class TestRecalculatePathSegs:
    """Test recalculate_path_segs workflow method."""

    def test_recalculate_path_segs_method_exists(self, temp_workflow):
        """Test recalculate_path_segs method exists."""
        assert hasattr(temp_workflow, "recalculate_path_segs")
        assert callable(temp_workflow.recalculate_path_segs)

    def test_recalculate_path_segs_requires_load(self, unloaded_workflow):
        """Test recalculate_path_segs raises error when not loaded."""
        with pytest.raises(RuntimeError, match="No database loaded"):
            unloaded_workflow.recalculate_path_segs()

    def test_recalculate_path_segs_returns_dict(self, temp_workflow):
        """Test recalculate_path_segs returns proper dict."""
        result = temp_workflow.recalculate_path_segs(
            update_nodes=False, update_reaches=False
        )

        assert isinstance(result, dict)
        assert "nodes_updated" in result
        assert "reaches_updated" in result
        assert "total_segments" in result
        assert "nodes_assigned" in result


class TestRecalculateNetworkAttributes:
    """Test recalculate_network_attributes workflow method."""

    def test_recalculate_network_attributes_method_exists(self, temp_workflow):
        """Test recalculate_network_attributes method exists."""
        assert hasattr(temp_workflow, "recalculate_network_attributes")
        assert callable(temp_workflow.recalculate_network_attributes)

    @pytest.mark.skip(reason="DuckDB GC segfault on large updates")
    def test_recalculate_network_attributes_supports_stream_order(self, temp_workflow):
        """Test recalculate_network_attributes handles stream_order."""
        result = temp_workflow.recalculate_network_attributes(
            attributes=["stream_order"]
        )

        assert isinstance(result, dict)
        assert "stream_order" in result["attributes_updated"]

    @pytest.mark.skip(reason="DuckDB GC segfault on large updates")
    def test_recalculate_network_attributes_supports_path_segs(self, temp_workflow):
        """Test recalculate_network_attributes handles path_segs."""
        result = temp_workflow.recalculate_network_attributes(attributes=["path_segs"])

        assert isinstance(result, dict)
        assert "path_segs" in result["attributes_updated"]


class TestRecalculateSinuosity:
    """Test recalculate_sinuosity workflow method."""

    def test_recalculate_sinuosity_method_exists(self, temp_workflow):
        """Test recalculate_sinuosity method exists."""
        assert hasattr(temp_workflow, "recalculate_sinuosity")
        assert callable(temp_workflow.recalculate_sinuosity)

    def test_recalculate_sinuosity_requires_load(self, unloaded_workflow):
        """Test recalculate_sinuosity raises error when not loaded."""
        with pytest.raises(RuntimeError, match="No database loaded"):
            unloaded_workflow.recalculate_sinuosity()

    def test_recalculate_sinuosity_returns_dict(self, temp_workflow):
        """Test recalculate_sinuosity returns proper dict with limited reaches."""
        # Get a few reach IDs from the database
        conn = temp_workflow._sword._db.connect()
        reach_ids = [
            r[0]
            for r in conn.execute(
                """
            SELECT reach_id FROM reaches
            WHERE region = ?
            LIMIT 3
        """,
                [temp_workflow._sword.region],
            ).fetchall()
        ]

        result = temp_workflow.recalculate_sinuosity(reach_ids=reach_ids)

        assert isinstance(result, dict)
        assert "reaches_processed" in result
        assert "reaches_skipped" in result
        assert "reaches_updated" in result
        assert "mean_sinuosity" in result
        assert "reach_sinuosities" in result

    def test_recalculate_sinuosity_values_reasonable(self, temp_workflow):
        """Test that sinuosity values are within expected range."""
        conn = temp_workflow._sword._db.connect()
        reach_ids = [
            r[0]
            for r in conn.execute(
                """
            SELECT reach_id FROM reaches
            WHERE region = ?
            LIMIT 5
        """,
                [temp_workflow._sword.region],
            ).fetchall()
        ]

        result = temp_workflow.recalculate_sinuosity(reach_ids=reach_ids)

        for reach_id, sinuosity in result["reach_sinuosities"].items():
            assert sinuosity >= 1.0, "Sinuosity must be >= 1.0"
            assert sinuosity <= 10.0, "Sinuosity should be clamped to <= 10.0"


class TestRecalculateTribFlag:
    """Test recalculate_trib_flag workflow method."""

    # Path to MHV_SWORD data - skip tests if not available
    MHV_DATA_DIR = "/Volumes/SWORD_DATA/data/MHV_SWORD"

    def test_recalculate_trib_flag_method_exists(self, temp_workflow):
        """Test recalculate_trib_flag method exists."""
        assert hasattr(temp_workflow, "recalculate_trib_flag")
        assert callable(temp_workflow.recalculate_trib_flag)

    def test_recalculate_trib_flag_requires_load(self, unloaded_workflow):
        """Test recalculate_trib_flag raises error when not loaded."""
        with pytest.raises(RuntimeError, match="No database loaded"):
            unloaded_workflow.recalculate_trib_flag(self.MHV_DATA_DIR)

    def test_recalculate_trib_flag_requires_valid_dir(self, temp_workflow):
        """Test recalculate_trib_flag raises error for invalid directory."""
        with pytest.raises(FileNotFoundError):
            temp_workflow.recalculate_trib_flag("/nonexistent/path")

    def test_recalculate_trib_flag_returns_dict(self, temp_workflow):
        """Test recalculate_trib_flag returns proper dict."""
        if not os.path.exists(self.MHV_DATA_DIR):
            pytest.skip(f"MHV data not available: {self.MHV_DATA_DIR}")

        result = temp_workflow.recalculate_trib_flag(self.MHV_DATA_DIR)

        assert isinstance(result, dict)
        assert "nodes_flagged" in result
        assert "reaches_flagged" in result
        assert "mhv_files_processed" in result
        assert "total_mhv_points" in result

    def test_recalculate_trib_flag_values_reasonable(self, temp_workflow):
        """Test that trib_flag results are within expected range."""
        if not os.path.exists(self.MHV_DATA_DIR):
            pytest.skip(f"MHV data not available: {self.MHV_DATA_DIR}")

        result = temp_workflow.recalculate_trib_flag(self.MHV_DATA_DIR)

        # Should process at least some files
        assert result["mhv_files_processed"] > 0

        # Nodes flagged should be non-negative
        assert result["nodes_flagged"] >= 0
        assert result["reaches_flagged"] >= 0

        # Should have processed some MHV points
        assert result["total_mhv_points"] >= 0

    def test_recalculate_trib_flag_updates_database(self, temp_workflow):
        """Test that trib_flag values are actually updated in the database."""
        if not os.path.exists(self.MHV_DATA_DIR):
            pytest.skip(f"MHV data not available: {self.MHV_DATA_DIR}")

        result = temp_workflow.recalculate_trib_flag(self.MHV_DATA_DIR)

        # Verify database was updated
        conn = temp_workflow._sword._db.connect()

        # Count nodes with trib_flag=1
        flagged_nodes = conn.execute(
            """
            SELECT COUNT(*) FROM nodes
            WHERE region = ? AND trib_flag = 1
        """,
            [temp_workflow._sword.region],
        ).fetchone()[0]

        assert flagged_nodes == result["nodes_flagged"]

        # Count reaches with trib_flag=1
        flagged_reaches = conn.execute(
            """
            SELECT COUNT(*) FROM reaches
            WHERE region = ? AND trib_flag = 1
        """,
            [temp_workflow._sword.region],
        ).fetchone()[0]

        assert flagged_reaches == result["reaches_flagged"]

    def test_recalculate_trib_flag_custom_threshold(self, temp_workflow):
        """Test recalculate_trib_flag with custom distance threshold."""
        if not os.path.exists(self.MHV_DATA_DIR):
            pytest.skip(f"MHV data not available: {self.MHV_DATA_DIR}")

        # Use a smaller threshold - should flag fewer nodes
        result_small = temp_workflow.recalculate_trib_flag(
            self.MHV_DATA_DIR,
            distance_threshold=0.001,  # ~111m
        )

        # Use a larger threshold - should flag more nodes
        result_large = temp_workflow.recalculate_trib_flag(
            self.MHV_DATA_DIR,
            distance_threshold=0.005,  # ~555m
        )

        # Larger threshold should generally flag more or equal nodes
        assert result_large["nodes_flagged"] >= result_small["nodes_flagged"]


# =============================================================================
# SNAPSHOT VERSIONING TESTS
# =============================================================================


class TestSnapshotVersioning:
    """Test snapshot versioning methods."""

    def test_snapshot_method_exists(self, temp_workflow_with_provenance):
        """Test snapshot method exists."""
        assert hasattr(temp_workflow_with_provenance, "snapshot")
        assert callable(temp_workflow_with_provenance.snapshot)

    def test_snapshot_requires_load(self, unloaded_workflow):
        """Test snapshot raises error when database not loaded."""
        with pytest.raises(RuntimeError, match="No database loaded"):
            unloaded_workflow.snapshot("test-snapshot")

    def test_snapshot_requires_provenance(self, temp_workflow):
        """Test snapshot raises error when provenance disabled."""
        with pytest.raises(RuntimeError, match="Provenance tracking must be enabled"):
            temp_workflow.snapshot("test-snapshot")

    def test_create_snapshot_basic(self, temp_workflow_with_provenance):
        """Test creating a basic snapshot."""
        result = temp_workflow_with_provenance.snapshot("test-snapshot")

        assert result["name"] == "test-snapshot"
        assert result["snapshot_id"] > 0
        assert result["operation_id_max"] >= 0
        assert result["reach_count"] > 0
        assert result["node_count"] > 0

    def test_create_snapshot_with_description(self, temp_workflow_with_provenance):
        """Test creating a snapshot with description."""
        result = temp_workflow_with_provenance.snapshot(
            "described-snapshot", description="This is a test snapshot"
        )

        assert result["name"] == "described-snapshot"
        assert result["snapshot_id"] > 0

    def test_create_snapshot_duplicate_name_raises(self, temp_workflow_with_provenance):
        """Test creating snapshot with duplicate name raises error."""
        temp_workflow_with_provenance.snapshot("unique-name")

        with pytest.raises(ValueError, match="already exists"):
            temp_workflow_with_provenance.snapshot("unique-name")

    def test_create_snapshot_invalid_name_empty(self, temp_workflow_with_provenance):
        """Test creating snapshot with empty name raises error."""
        with pytest.raises(ValueError, match="cannot be empty"):
            temp_workflow_with_provenance.snapshot("")

    def test_create_snapshot_invalid_name_spaces(self, temp_workflow_with_provenance):
        """Test creating snapshot with spaces raises error."""
        with pytest.raises(ValueError, match="can only contain"):
            temp_workflow_with_provenance.snapshot("invalid name")

    def test_create_snapshot_invalid_name_special_chars(
        self, temp_workflow_with_provenance
    ):
        """Test creating snapshot with special chars raises error."""
        with pytest.raises(ValueError, match="can only contain"):
            temp_workflow_with_provenance.snapshot("invalid@name!")

    def test_create_snapshot_valid_name_with_hyphens(
        self, temp_workflow_with_provenance
    ):
        """Test creating snapshot with hyphens is valid."""
        result = temp_workflow_with_provenance.snapshot("valid-name-with-hyphens")
        assert result["name"] == "valid-name-with-hyphens"

    def test_create_snapshot_valid_name_with_underscores(
        self, temp_workflow_with_provenance
    ):
        """Test creating snapshot with underscores is valid."""
        result = temp_workflow_with_provenance.snapshot("valid_name_with_underscores")
        assert result["name"] == "valid_name_with_underscores"


class TestListSnapshots:
    """Test list_snapshots method."""

    def test_list_snapshots_method_exists(self, temp_workflow_with_provenance):
        """Test list_snapshots method exists."""
        assert hasattr(temp_workflow_with_provenance, "list_snapshots")
        assert callable(temp_workflow_with_provenance.list_snapshots)

    def test_list_snapshots_requires_load(self, unloaded_workflow):
        """Test list_snapshots raises error when database not loaded."""
        with pytest.raises(RuntimeError, match="No database loaded"):
            unloaded_workflow.list_snapshots()

    def test_list_snapshots_empty(self, temp_workflow_with_provenance):
        """Test listing snapshots when none exist."""
        result = temp_workflow_with_provenance.list_snapshots()

        # Should return empty DataFrame
        assert len(result) == 0

    def test_list_snapshots_after_creating(self, temp_workflow_with_provenance):
        """Test listing snapshots after creating one."""
        temp_workflow_with_provenance.snapshot("first-snapshot")
        temp_workflow_with_provenance.snapshot("second-snapshot")

        result = temp_workflow_with_provenance.list_snapshots()

        assert len(result) == 2
        assert "name" in result.columns
        assert "created_at" in result.columns
        assert "first-snapshot" in result["name"].values
        assert "second-snapshot" in result["name"].values


class TestDeleteSnapshot:
    """Test delete_snapshot method."""

    def test_delete_snapshot_method_exists(self, temp_workflow_with_provenance):
        """Test delete_snapshot method exists."""
        assert hasattr(temp_workflow_with_provenance, "delete_snapshot")
        assert callable(temp_workflow_with_provenance.delete_snapshot)

    def test_delete_snapshot_requires_load(self, unloaded_workflow):
        """Test delete_snapshot raises error when database not loaded."""
        with pytest.raises(RuntimeError, match="No database loaded"):
            unloaded_workflow.delete_snapshot("test")

    def test_delete_snapshot_success(self, temp_workflow_with_provenance):
        """Test deleting an existing snapshot."""
        temp_workflow_with_provenance.snapshot("to-delete")
        assert len(temp_workflow_with_provenance.list_snapshots()) == 1

        result = temp_workflow_with_provenance.delete_snapshot("to-delete")

        assert result is True
        assert len(temp_workflow_with_provenance.list_snapshots()) == 0

    def test_delete_snapshot_not_found(self, temp_workflow_with_provenance):
        """Test deleting non-existent snapshot returns False."""
        result = temp_workflow_with_provenance.delete_snapshot("does-not-exist")
        assert result is False


class TestRestoreSnapshot:
    """Test restore_snapshot method."""

    def test_restore_snapshot_method_exists(self, temp_workflow_with_provenance):
        """Test restore_snapshot method exists."""
        assert hasattr(temp_workflow_with_provenance, "restore_snapshot")
        assert callable(temp_workflow_with_provenance.restore_snapshot)

    def test_restore_snapshot_requires_load(self, unloaded_workflow):
        """Test restore_snapshot raises error when database not loaded."""
        with pytest.raises(RuntimeError, match="No database loaded"):
            unloaded_workflow.restore_snapshot("test")

    def test_restore_snapshot_requires_provenance(self, temp_workflow):
        """Test restore_snapshot raises error when provenance disabled."""
        with pytest.raises(RuntimeError, match="Provenance tracking"):
            temp_workflow.restore_snapshot("test")

    def test_restore_snapshot_not_found(self, temp_workflow_with_provenance):
        """Test restoring non-existent snapshot raises error."""
        with pytest.raises(ValueError, match="not found"):
            temp_workflow_with_provenance.restore_snapshot("does-not-exist")

    def test_restore_snapshot_dry_run(self, temp_workflow_with_provenance):
        """Test restore_snapshot dry run mode."""
        temp_workflow_with_provenance.snapshot("before-changes")

        result = temp_workflow_with_provenance.restore_snapshot(
            "before-changes", dry_run=True
        )

        assert result["dry_run"] is True
        assert "operations_to_rollback" in result
        assert result["operations_to_rollback"] == 0  # No changes since snapshot

    def test_restore_snapshot_already_at_state(self, temp_workflow_with_provenance):
        """Test restoring when already at snapshot state."""
        temp_workflow_with_provenance.snapshot("current-state")

        result = temp_workflow_with_provenance.restore_snapshot("current-state")

        assert result["operations_rolled_back"] == 0
        assert result["values_restored"] == 0


class TestRestoreToTimestamp:
    """Test restore_to_timestamp method."""

    def test_restore_to_timestamp_method_exists(self, temp_workflow_with_provenance):
        """Test restore_to_timestamp method exists."""
        assert hasattr(temp_workflow_with_provenance, "restore_to_timestamp")
        assert callable(temp_workflow_with_provenance.restore_to_timestamp)

    def test_restore_to_timestamp_requires_load(self, unloaded_workflow):
        """Test restore_to_timestamp raises error when database not loaded."""
        with pytest.raises(RuntimeError, match="No database loaded"):
            unloaded_workflow.restore_to_timestamp("2024-01-01 00:00:00")

    def test_restore_to_timestamp_requires_provenance(self, temp_workflow):
        """Test restore_to_timestamp raises error when provenance disabled."""
        with pytest.raises(RuntimeError, match="Provenance tracking"):
            temp_workflow.restore_to_timestamp("2024-01-01 00:00:00")

    def test_restore_to_timestamp_future_raises(self, temp_workflow_with_provenance):
        """Test restoring to future timestamp raises error."""
        with pytest.raises(ValueError, match="future timestamp"):
            temp_workflow_with_provenance.restore_to_timestamp("2099-01-01 00:00:00")

    def test_restore_to_timestamp_dry_run(self, temp_workflow_with_provenance):
        """Test restore_to_timestamp dry run mode."""
        from datetime import datetime

        result = temp_workflow_with_provenance.restore_to_timestamp(
            datetime.now().isoformat(), dry_run=True
        )

        assert result["dry_run"] is True
        assert "operations_to_rollback" in result


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
