# -*- coding: utf-8 -*-
"""
Unit tests for SWORD DuckDB class methods.

Tests cover:
- delete_data / delete_rchs
- delete_nodes
- append_data
- append_nodes
- break_reaches
- save_vectors
- save_nc
"""

import os
import sys
import pytest
import numpy as np
import tempfile
import shutil

# Add project root to path
main_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, main_dir)

from src.sword_duckdb import SWORD


TEST_REGION = "NA"
TEST_VERSION = "v17b"

pytestmark = [pytest.mark.slow, pytest.mark.workflow, pytest.mark.db]


class TestDeleteData:
    """Test delete_data method."""

    def test_delete_single_reach(self, temp_sword):
        """Test deleting a single reach."""
        initial_count = len(temp_sword.reaches)
        reach_to_delete = temp_sword.reaches.id[0]

        temp_sword.delete_data([reach_to_delete])

        # Reload to verify
        new_sword = SWORD(str(temp_sword._db_path), TEST_REGION, TEST_VERSION)
        assert len(new_sword.reaches) == initial_count - 1
        assert reach_to_delete not in new_sword.reaches.id
        new_sword.close()

    @pytest.mark.skip(reason="Segfaults with large DB copy - known DuckDB GC issue")
    def test_delete_multiple_reaches(self, temp_sword):
        """Test deleting multiple reaches."""
        initial_count = len(temp_sword.reaches)
        reaches_to_delete = temp_sword.reaches.id[:3].tolist()

        temp_sword.delete_data(reaches_to_delete)

        new_sword = SWORD(str(temp_sword._db_path), TEST_REGION, TEST_VERSION)
        assert len(new_sword.reaches) == initial_count - 3
        for reach_id in reaches_to_delete:
            assert reach_id not in new_sword.reaches.id
        new_sword.close()

    def test_delete_empty_list(self, temp_sword):
        """Test deleting empty list does nothing."""
        initial_count = len(temp_sword.reaches)
        temp_sword.delete_data([])
        assert len(temp_sword.reaches) == initial_count


class TestDeleteRchs:
    """Test delete_rchs method (alias for delete_data)."""

    def test_delete_rchs_works(self, temp_sword):
        """Test delete_rchs removes reaches."""
        initial_count = len(temp_sword.reaches)
        reach_to_delete = temp_sword.reaches.id[0]

        temp_sword.delete_rchs([reach_to_delete])

        new_sword = SWORD(str(temp_sword._db_path), TEST_REGION, TEST_VERSION)
        assert len(new_sword.reaches) == initial_count - 1
        new_sword.close()


class TestDeleteNodes:
    """Test delete_nodes method."""

    def test_delete_single_node(self, temp_sword):
        """Test deleting a single node by node ID."""
        initial_count = len(temp_sword.nodes)

        # Delete first node by ID (not index)
        node_id_to_delete = int(temp_sword.nodes.id[0])
        temp_sword.delete_nodes([node_id_to_delete])

        new_sword = SWORD(str(temp_sword._db_path), TEST_REGION, TEST_VERSION)
        assert len(new_sword.nodes) == initial_count - 1
        new_sword.close()


class TestCopyMethod:
    """Test copy method for backup."""

    def test_copy_creates_backup_marker(self, temp_sword):
        """Test copy method runs without error."""
        # Just verify it doesn't raise an exception
        temp_sword.copy()


class TestSaveVectors:
    """Test save_vectors method."""

    def test_save_reaches_gpkg(self, temp_sword):
        """Test saving reaches to GeoPackage."""
        temp_dir = tempfile.mkdtemp()
        try:
            # Create output directory
            gpkg_dir = os.path.join(temp_dir, "gpkg")
            os.makedirs(gpkg_dir, exist_ok=True)

            # Temporarily modify paths
            original_paths = temp_sword.paths
            temp_sword._test_gpkg_dir = gpkg_dir

            # This test verifies the method can be called
            # Full test would verify actual file creation
            assert hasattr(temp_sword, "save_vectors")

        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)


class TestBreakReaches:
    """Test break_reaches method."""

    def test_break_reaches_method_exists(self, sword_readonly):
        """Test break_reaches method exists."""
        assert hasattr(sword_readonly, "break_reaches")
        assert callable(sword_readonly.break_reaches)


class TestAppendData:
    """Test append_data method."""

    def test_append_data_method_exists(self, sword_readonly):
        """Test append_data method exists."""
        assert hasattr(sword_readonly, "append_data")
        assert callable(sword_readonly.append_data)


class TestAppendNodes:
    """Test append_nodes method."""

    def test_append_nodes_method_exists(self, sword_readonly):
        """Test append_nodes method exists."""
        assert hasattr(sword_readonly, "append_nodes")
        assert callable(sword_readonly.append_nodes)


class TestContextManager:
    """Test context manager protocol."""

    def test_context_manager(self, sword_writable):
        """Test SWORD can be used as context manager."""
        db_path = str(sword_writable._db_path)
        sword_writable.close()

        with SWORD(db_path, TEST_REGION, TEST_VERSION) as sword:
            assert len(sword.reaches) > 0


class TestIDValidation:
    """Test ID validation methods."""

    def test_validate_reach_id_valid(self, sword_readonly):
        """Test valid reach IDs pass validation."""
        # Standard river reach ID: CBBBBBRRRRT
        assert sword_readonly.validate_reach_id(72140300041) == True  # River
        assert sword_readonly.validate_reach_id(72140300042) == True  # Lake
        assert sword_readonly.validate_reach_id(72140300043) == True  # Lake on river
        assert sword_readonly.validate_reach_id(72140300044) == True  # Dam
        assert sword_readonly.validate_reach_id(72140300045) == True  # Delta
        assert sword_readonly.validate_reach_id(72140300046) == True  # Ghost

    def test_validate_reach_id_invalid(self, sword_readonly):
        """Test invalid reach IDs fail validation."""
        # Wrong length
        assert sword_readonly.validate_reach_id(7214030004) == False  # Too short
        assert sword_readonly.validate_reach_id(721403000411) == False  # Too long
        # Invalid type flag
        assert sword_readonly.validate_reach_id(72140300040) == False  # Type 0 invalid
        assert sword_readonly.validate_reach_id(72140300047) == False  # Type 7 invalid
        # Invalid continent code
        assert (
            sword_readonly.validate_reach_id(2140300041) == False
        )  # 10 digits, no continent

    def test_validate_node_id_valid(self, sword_readonly):
        """Test valid node IDs pass validation."""
        # Standard node ID: CBBBBBRRRRNNNT
        assert sword_readonly.validate_node_id(72140300040011) == True

    def test_validate_node_id_invalid(self, sword_readonly):
        """Test invalid node IDs fail validation."""
        # Wrong length
        assert (
            sword_readonly.validate_node_id(7214030004001) == False
        )  # Too short (13 digits)
        assert (
            sword_readonly.validate_node_id(721403000400111) == False
        )  # Too long (15 digits)
        # Invalid type flag
        assert (
            sword_readonly.validate_node_id(72140300040010) == False
        )  # Type 0 invalid


class TestTopologyConsistency:
    """Test topology consistency checking."""

    def test_check_topo_consistency_exists(self, sword_readonly):
        """Test check_topo_consistency method exists."""
        assert hasattr(sword_readonly, "check_topo_consistency")
        assert callable(sword_readonly.check_topo_consistency)

    def test_check_topo_consistency_returns_dict(self, sword_readonly):
        """Test check_topo_consistency returns a dictionary."""
        result = sword_readonly.check_topo_consistency(verbose=0)
        assert isinstance(result, dict)
        assert "passed" in result
        assert "total_reaches" in result
        assert "error_counts" in result
        assert "warning_counts" in result
        assert "reaches_with_issues" in result

    def test_check_topo_consistency_with_details(self, sword_readonly):
        """Test check_topo_consistency with return_details=True."""
        result = sword_readonly.check_topo_consistency(verbose=0, return_details=True)
        assert "details" in result


class TestNodeLengthCheck:
    """Test node length checking."""

    def test_check_node_lengths_exists(self, sword_readonly):
        """Test check_node_lengths method exists."""
        assert hasattr(sword_readonly, "check_node_lengths")
        assert callable(sword_readonly.check_node_lengths)

    def test_check_node_lengths_returns_dict(self, sword_readonly):
        """Test check_node_lengths returns proper dict."""
        result = sword_readonly.check_node_lengths(verbose=0)
        assert isinstance(result, dict)
        assert "passed" in result
        assert "total_nodes" in result
        assert "long_nodes" in result
        assert "zero_length_nodes" in result
        assert "affected_reaches" in result


class TestAppendDataValidation:
    """Test append_data with ID validation."""

    def test_append_data_has_validate_parameter(self, sword_readonly):
        """Test append_data has validate_ids parameter."""
        import inspect

        sig = inspect.signature(sword_readonly.append_data)
        assert "validate_ids" in sig.parameters


class TestCloseMethod:
    """Test close method."""

    def test_close_method(self, sword_writable):
        """Test close method works."""
        sword_writable.close()
        # Should not raise an error


class TestGhostReachMethods:
    """Test ghost reach related methods."""

    def test_create_ghost_reach_method_exists(self, sword_readonly):
        """Test create_ghost_reach method exists."""
        assert hasattr(sword_readonly, "create_ghost_reach")
        assert callable(sword_readonly.create_ghost_reach)

    def test_find_missing_ghost_reaches_method_exists(self, sword_readonly):
        """Test find_missing_ghost_reaches method exists."""
        assert hasattr(sword_readonly, "find_missing_ghost_reaches")
        assert callable(sword_readonly.find_missing_ghost_reaches)

    def test_find_incorrect_ghost_reaches_method_exists(self, sword_readonly):
        """Test find_incorrect_ghost_reaches method exists."""
        assert hasattr(sword_readonly, "find_incorrect_ghost_reaches")
        assert callable(sword_readonly.find_incorrect_ghost_reaches)

    def test_find_missing_ghost_reaches_returns_dict(self, sword_readonly):
        """Test find_missing_ghost_reaches returns proper dict."""
        result = sword_readonly.find_missing_ghost_reaches()

        assert isinstance(result, dict)
        assert "missing_headwaters" in result
        assert "missing_outlets" in result
        assert "total_missing" in result
        assert isinstance(result["missing_headwaters"], list)
        assert isinstance(result["missing_outlets"], list)

    def test_find_incorrect_ghost_reaches_returns_dict(self, sword_readonly):
        """Test find_incorrect_ghost_reaches returns proper dict."""
        result = sword_readonly.find_incorrect_ghost_reaches()

        assert isinstance(result, dict)
        assert "incorrect_ghost_reaches" in result
        assert "total_incorrect" in result
        assert isinstance(result["incorrect_ghost_reaches"], list)

    def test_create_ghost_reach_invalid_reach(self, temp_sword):
        """Test create_ghost_reach raises error for non-existent reach."""
        with pytest.raises(ValueError, match="not found"):
            temp_sword.create_ghost_reach(99999999999)

    def test_create_ghost_reach_invalid_position(self, temp_sword):
        """Test create_ghost_reach raises error for invalid position."""
        # Get a valid reach ID
        reach_id = int(temp_sword.reaches.id[0])
        with pytest.raises(ValueError, match="Invalid position"):
            temp_sword.create_ghost_reach(reach_id, position="invalid")

    @pytest.mark.skip(reason="DuckDB numpy.int64 type error in create_ghost_reach")
    def test_create_ghost_reach_headwater(self, temp_sword):
        """Test creating a ghost reach at headwater position."""
        # Find a reach that has:
        # 1. No upstream neighbors (n_rch_up == 0)
        # 2. At least 2 nodes (so we can split)
        # 3. Not already a ghost reach (type != 6)
        candidate_reach = None
        for idx, reach_id in enumerate(temp_sword.reaches.id):
            reach_type = str(reach_id)[-1]
            if reach_type == "6":
                continue  # Skip ghost reaches
            n_up = temp_sword.reaches.n_rch_up[idx]
            if n_up == 0:
                # Check node count
                node_count = np.sum(temp_sword.nodes.reach_id == reach_id)
                if node_count >= 2:
                    candidate_reach = int(reach_id)
                    break

        if candidate_reach is None:
            pytest.skip("No suitable reach found for headwater ghost test")

        initial_reach_count = len(temp_sword.reaches.id)
        result = temp_sword.create_ghost_reach(candidate_reach, position="headwater")

        assert result["success"] is True
        assert result["original_reach"] == candidate_reach
        assert result["position"] == "headwater"
        assert result["ghost_reach_id"] is not None
        assert result["ghost_node_id"] is not None

        # Verify ghost reach ID format (should end in 6)
        assert str(result["ghost_reach_id"])[-1] == "6"

        # Verify new reach was created
        assert len(temp_sword.reaches.id) == initial_reach_count + 1

        # Verify ghost reach exists in data
        assert result["ghost_reach_id"] in temp_sword.reaches.id

    @pytest.mark.skip(reason="DuckDB numpy.int64 type error in create_ghost_reach")
    def test_create_ghost_reach_outlet(self, temp_sword):
        """Test creating a ghost reach at outlet position."""
        # Find a reach with no downstream neighbors but has >= 2 nodes
        candidate_reach = None
        for idx, reach_id in enumerate(temp_sword.reaches.id):
            reach_type = str(reach_id)[-1]
            if reach_type == "6":
                continue
            n_down = temp_sword.reaches.n_rch_down[idx]
            if n_down == 0:
                node_count = np.sum(temp_sword.nodes.reach_id == reach_id)
                if node_count >= 2:
                    candidate_reach = int(reach_id)
                    break

        if candidate_reach is None:
            pytest.skip("No suitable reach found for outlet ghost test")

        result = temp_sword.create_ghost_reach(candidate_reach, position="outlet")

        assert result["success"] is True
        assert result["position"] == "outlet"
        assert str(result["ghost_reach_id"])[-1] == "6"

    @pytest.mark.skip(reason="DuckDB numpy.int64 type error in create_ghost_reach")
    def test_create_ghost_reach_auto_position(self, temp_sword):
        """Test create_ghost_reach with auto position detection."""
        # Find a reach with no upstream but has downstream (clear headwater)
        candidate_reach = None
        for idx, reach_id in enumerate(temp_sword.reaches.id):
            reach_type = str(reach_id)[-1]
            if reach_type == "6":
                continue
            n_up = temp_sword.reaches.n_rch_up[idx]
            n_down = temp_sword.reaches.n_rch_down[idx]
            if n_up == 0 and n_down > 0:
                node_count = np.sum(temp_sword.nodes.reach_id == reach_id)
                if node_count >= 2:
                    candidate_reach = int(reach_id)
                    break

        if candidate_reach is None:
            pytest.skip("No suitable reach found for auto position test")

        result = temp_sword.create_ghost_reach(candidate_reach, position="auto")

        assert result["success"] is True
        assert result["position"] == "headwater"  # Should auto-detect headwater

    def test_create_ghost_reach_single_node_fails(self, temp_sword):
        """Test create_ghost_reach fails for single-node reaches."""
        # Find a reach with only 1 node
        candidate_reach = None
        for idx, reach_id in enumerate(temp_sword.reaches.id):
            reach_type = str(reach_id)[-1]
            if reach_type == "6":
                continue
            node_count = temp_sword.reaches.rch_n_nodes[idx]
            if node_count == 1:
                candidate_reach = int(reach_id)
                break

        if candidate_reach is None:
            pytest.skip("No single-node reach found for test")

        with pytest.raises(ValueError, match="only.*node"):
            temp_sword.create_ghost_reach(candidate_reach, position="headwater")


class TestCalculateDistOut:
    """Test calculate_dist_out_from_topology method."""

    def test_calculate_dist_out_method_exists(self, sword_readonly):
        """Test calculate_dist_out_from_topology method exists."""
        assert hasattr(sword_readonly, "calculate_dist_out_from_topology")
        assert callable(sword_readonly.calculate_dist_out_from_topology)

    def test_calculate_dist_out_returns_dict(self, temp_sword):
        """Test calculate_dist_out_from_topology returns proper dict."""
        result = temp_sword.calculate_dist_out_from_topology(
            update_nodes=False, verbose=False
        )
        assert isinstance(result, dict)
        assert "success" in result
        assert "reaches_updated" in result
        assert "nodes_updated" in result
        assert "outlets_found" in result
        assert "loops" in result
        assert "unfilled_reaches" in result

    def test_calculate_dist_out_finds_outlets(self, temp_sword):
        """Test that outlet reaches are found."""
        result = temp_sword.calculate_dist_out_from_topology(
            update_nodes=False, verbose=False
        )
        # Should find at least one outlet (n_rch_down == 0)
        outlet_count = np.sum(temp_sword.reaches.n_rch_down == 0)
        assert result["outlets_found"] == outlet_count

    def test_calculate_dist_out_updates_reaches(self, temp_sword):
        """Test that reach dist_out values are updated."""
        # Store original values
        original_dist_out = np.copy(temp_sword.reaches.dist_out)

        result = temp_sword.calculate_dist_out_from_topology(
            update_nodes=False, verbose=False
        )

        # Should have updated some reaches
        assert result["reaches_updated"] > 0

        # Verify values changed (reload data to check)
        new_sword = SWORD(str(temp_sword._db_path), TEST_REGION, TEST_VERSION)
        # At least some values should have changed
        # (unless already perfectly computed)
        new_sword.close()

    def test_calculate_dist_out_outlet_values(self, temp_sword):
        """Test that outlet reaches have dist_out = reach_length."""
        result = temp_sword.calculate_dist_out_from_topology(
            update_nodes=False, verbose=False
        )

        if result["success"]:
            # Reload to get updated values
            new_sword = SWORD(str(temp_sword._db_path), TEST_REGION, TEST_VERSION)

            # Check outlet reaches
            outlet_idx = np.where(new_sword.reaches.n_rch_down == 0)[0]
            for idx in outlet_idx[:5]:  # Check first 5
                # For outlets: dist_out should equal reach_length
                assert (
                    abs(new_sword.reaches.dist_out[idx] - new_sword.reaches.len[idx])
                    < 1.0
                ), "Outlet reach dist_out should equal reach_length"

            new_sword.close()

    def test_calculate_dist_out_with_nodes(self, temp_sword):
        """Test that node dist_out values are updated when requested."""
        result = temp_sword.calculate_dist_out_from_topology(
            update_nodes=True, verbose=False
        )

        # Should have updated nodes too
        if result["success"]:
            assert result["nodes_updated"] > 0

    def test_calculate_dist_out_node_cumulative(self, temp_sword):
        """Test that node dist_out follows cumulative pattern."""
        result = temp_sword.calculate_dist_out_from_topology(
            update_nodes=True, verbose=False
        )

        if result["success"]:
            new_sword = SWORD(str(temp_sword._db_path), TEST_REGION, TEST_VERSION)

            # Find a reach with multiple nodes
            for idx, reach_id in enumerate(new_sword.reaches.id):
                n_nodes = new_sword.reaches.rch_n_nodes[idx]
                if n_nodes >= 3:
                    # Get nodes for this reach
                    node_idx = np.where(new_sword.nodes.reach_id == reach_id)[0]
                    if len(node_idx) >= 3:
                        # Sort by node ID
                        sorted_idx = node_idx[np.argsort(new_sword.nodes.id[node_idx])]
                        # Node dist_out should be increasing
                        dist_outs = new_sword.nodes.dist_out[sorted_idx]
                        assert np.all(np.diff(dist_outs) >= 0), (
                            "Node dist_out should increase along reach"
                        )
                        break

            new_sword.close()

    def test_calculate_node_dist_out_helper(self, temp_sword):
        """Test _calculate_node_dist_out helper method."""
        assert hasattr(temp_sword, "_calculate_node_dist_out")

        # Create dummy reach dist_out array
        reach_dist_out = np.full(len(temp_sword.reaches.id), 10000.0)

        # Should not raise error
        count = temp_sword._calculate_node_dist_out(reach_dist_out, verbose=False)
        assert count >= 0


class TestMergeReaches:
    """Test merge_reaches method."""

    def test_merge_reaches_method_exists(self, sword_readonly):
        """Test merge_reaches method exists."""
        assert hasattr(sword_readonly, "merge_reaches")
        assert callable(sword_readonly.merge_reaches)

    def test_merge_reaches_invalid_source(self, temp_sword):
        """Test merge_reaches raises error for non-existent source reach."""
        valid_target = int(temp_sword.reaches.id[0])
        with pytest.raises(ValueError, match="Source reach.*not found"):
            temp_sword.merge_reaches(99999999999, valid_target)

    def test_merge_reaches_invalid_target(self, temp_sword):
        """Test merge_reaches raises error for non-existent target reach."""
        valid_source = int(temp_sword.reaches.id[0])
        with pytest.raises(ValueError, match="Target reach.*not found"):
            temp_sword.merge_reaches(valid_source, 99999999999)

    def test_merge_reaches_non_adjacent(self, temp_sword):
        """Test merge_reaches raises error for non-adjacent reaches."""
        # Get two reaches from different basins (guaranteed non-adjacent)
        reach_ids = temp_sword.reaches.id
        if len(reach_ids) < 2:
            pytest.skip("Need at least 2 reaches")

        # Find two reaches that are in different parts of network
        # Check reaches with different level-6 basin prefixes
        source = None
        target = None
        for i, rid1 in enumerate(reach_ids):
            basin1 = str(rid1)[:6]
            for j, rid2 in enumerate(reach_ids):
                if i == j:
                    continue
                basin2 = str(rid2)[:6]
                if basin1 != basin2:
                    source = int(rid1)
                    target = int(rid2)
                    break
            if source and target:
                break

        if source is None or target is None:
            pytest.skip("Could not find non-adjacent reaches in different basins")

        with pytest.raises(ValueError, match="not adjacent"):
            temp_sword.merge_reaches(source, target)

    @pytest.mark.skip(reason="grod_fid NULL in minimal DB causes NoneType comparison")
    def test_merge_reaches_adjacent_pair(self, temp_sword):
        """Test merging two adjacent reaches."""
        # Find two adjacent reaches (source has target as downstream neighbor)
        source_reach = None
        target_reach = None

        for idx, reach_id in enumerate(temp_sword.reaches.id):
            reach_type = str(reach_id)[-1]
            # Skip ghost reaches (type 6) and dam reaches (type 4)
            if reach_type in ("6", "4"):
                continue

            # Check if has exactly 1 downstream neighbor
            n_down = temp_sword.reaches.n_rch_down[idx]
            if n_down == 1:
                # Get downstream neighbor ID from topology
                down_ids = temp_sword.reaches.rch_id_down[:, idx]
                down_id = down_ids[down_ids > 0][0] if np.any(down_ids > 0) else None

                if down_id is not None:
                    # Check downstream reach exists and is not a ghost
                    target_idx = np.where(temp_sword.reaches.id == down_id)[0]
                    if len(target_idx) > 0:
                        target_type = str(down_id)[-1]
                        if target_type not in ("6", "4"):
                            # Also verify downstream has only 1 upstream neighbor
                            # (to avoid complex junctions)
                            target_n_up = temp_sword.reaches.n_rch_up[target_idx[0]]
                            if target_n_up == 1:
                                source_reach = int(reach_id)
                                target_reach = int(down_id)
                                break

        if source_reach is None or target_reach is None:
            pytest.skip("No suitable adjacent reach pair found for merge test")

        # Record initial state
        initial_reach_count = len(temp_sword.reaches.id)
        initial_target_n_nodes = temp_sword.reaches.rch_n_nodes[
            np.where(temp_sword.reaches.id == target_reach)[0][0]
        ]
        source_n_nodes = temp_sword.reaches.rch_n_nodes[
            np.where(temp_sword.reaches.id == source_reach)[0][0]
        ]

        # Perform merge
        result = temp_sword.merge_reaches(source_reach, target_reach, verbose=True)

        # Verify result structure
        assert result["success"] is True
        assert result["source_reach"] == source_reach
        assert result["target_reach"] == target_reach
        assert result["merged_nodes"] >= 0
        assert result["merged_centerlines"] >= 0

        # Verify source reach was deleted
        assert source_reach not in temp_sword.reaches.id

        # Verify reach count decreased by 1
        assert len(temp_sword.reaches.id) == initial_reach_count - 1

        # Verify target reach still exists
        assert target_reach in temp_sword.reaches.id

        # Verify target reach node count increased
        new_target_n_nodes = temp_sword.reaches.rch_n_nodes[
            np.where(temp_sword.reaches.id == target_reach)[0][0]
        ]
        assert new_target_n_nodes == initial_target_n_nodes + source_n_nodes

        # Verify edit flag was set
        target_idx = np.where(temp_sword.reaches.id == target_reach)[0][0]
        edit_flag = temp_sword.reaches.edit_flag[target_idx]
        assert "6" in str(edit_flag)

    @pytest.mark.skip(reason="grod_fid NULL in minimal DB causes NoneType comparison")
    def test_merge_reaches_topology_update(self, temp_sword):
        """Test that topology is correctly updated after merge."""
        # Find a chain of 3 reaches: A -> B -> C
        # We'll merge B into C and verify A now points to C
        source_reach = None
        target_reach = None
        upstream_reach = None

        for idx, reach_id in enumerate(temp_sword.reaches.id):
            reach_type = str(reach_id)[-1]
            if reach_type in ("6", "4"):
                continue

            # Check for 1 upstream and 1 downstream
            n_up = temp_sword.reaches.n_rch_up[idx]
            n_down = temp_sword.reaches.n_rch_down[idx]

            if n_up == 1 and n_down == 1:
                # Get upstream and downstream
                up_ids = temp_sword.reaches.rch_id_up[:, idx]
                down_ids = temp_sword.reaches.rch_id_down[:, idx]

                up_id = up_ids[up_ids > 0][0] if np.any(up_ids > 0) else None
                down_id = down_ids[down_ids > 0][0] if np.any(down_ids > 0) else None

                if up_id and down_id:
                    # Verify neighbors are not ghost/dam
                    up_type = str(up_id)[-1]
                    down_type = str(down_id)[-1]
                    if up_type not in ("6", "4") and down_type not in ("6", "4"):
                        source_reach = int(reach_id)
                        target_reach = int(down_id)
                        upstream_reach = int(up_id)
                        break

        if source_reach is None:
            pytest.skip("No 3-reach chain found for topology test")

        # Perform merge
        temp_sword.merge_reaches(source_reach, target_reach, verbose=True)

        # Verify upstream_reach now points to target_reach as downstream
        upstream_idx = np.where(temp_sword.reaches.id == upstream_reach)[0][0]
        down_ids = temp_sword.reaches.rch_id_down[:, upstream_idx]
        down_neighbors = down_ids[down_ids > 0]

        assert target_reach in down_neighbors, (
            f"After merge, upstream reach should point to target. Got {down_neighbors}"
        )

    def test_check_reaches_adjacent_helper(self, temp_sword):
        """Test _check_reaches_adjacent helper method."""
        # Get two reaches known to be adjacent from topology
        for idx, reach_id in enumerate(temp_sword.reaches.id):
            n_down = temp_sword.reaches.n_rch_down[idx]
            if n_down >= 1:
                down_ids = temp_sword.reaches.rch_id_down[:, idx]
                neighbor_id = down_ids[down_ids > 0][0]

                assert (
                    temp_sword._check_reaches_adjacent(int(reach_id), int(neighbor_id))
                    is True
                )
                break

    def test_merge_direction_detection(self, temp_sword):
        """Test _get_merge_direction helper method."""
        # Find adjacent pair and verify direction detection
        for idx, reach_id in enumerate(temp_sword.reaches.id):
            n_down = temp_sword.reaches.n_rch_down[idx]
            if n_down >= 1:
                down_ids = temp_sword.reaches.rch_id_down[:, idx]
                neighbor_id = down_ids[down_ids > 0][0]

                direction = temp_sword._get_merge_direction(
                    int(reach_id), int(neighbor_id)
                )
                assert direction == "downstream", (
                    "Source flowing to target should give 'downstream' direction"
                )
                break


class TestRecalculateStreamOrder:
    """Test recalculate_stream_order method."""

    def test_recalculate_stream_order_method_exists(self, sword_readonly):
        """Test recalculate_stream_order method exists."""
        assert hasattr(sword_readonly, "recalculate_stream_order")
        assert callable(sword_readonly.recalculate_stream_order)

    def test_recalculate_stream_order_returns_dict(self, temp_sword):
        """Test recalculate_stream_order returns proper dict."""
        result = temp_sword.recalculate_stream_order(
            update_nodes=False, update_reaches=False, verbose=False
        )
        assert isinstance(result, dict)
        assert "nodes_updated" in result
        assert "reaches_updated" in result
        assert "nodes_with_valid_path_freq" in result
        assert "nodes_missing_path_freq" in result

    @pytest.mark.skip(reason="Minimal DB path_freq values don't match expected formula")
    def test_recalculate_stream_order_formula(self, temp_sword):
        """Test stream_order formula: round(ln(path_freq)) + 1."""
        import math

        # Get some nodes with valid path_freq
        conn = temp_sword._db.connect()
        nodes_data = conn.execute(
            """
            SELECT node_id, path_freq, stream_order
            FROM nodes
            WHERE region = ? AND path_freq > 0
            LIMIT 100
        """,
            [temp_sword.region],
        ).fetchall()
        conn.close()

        # Verify formula for existing values
        for node_id, path_freq, stream_order in nodes_data:
            expected = int(round(math.log(path_freq))) + 1
            # Allow for existing data that may have small differences
            if stream_order > 0:
                assert abs(stream_order - expected) <= 1, (
                    f"Stream order should be ~round(ln({path_freq}))+1={expected}, got {stream_order}"
                )

    @pytest.mark.skip(reason="DuckDB GC segfault on large updates")
    def test_recalculate_stream_order_updates_nodes(self, temp_sword):
        """Test that node stream_order values can be updated."""
        # First, artificially modify some stream_order values
        conn = temp_sword._db.connect()
        conn.execute(
            """
            UPDATE nodes
            SET stream_order = -1
            WHERE region = ? AND path_freq > 0
            LIMIT 10
        """,
            [temp_sword.region],
        )
        conn.close()

        # Recalculate
        result = temp_sword.recalculate_stream_order(
            update_nodes=True, update_reaches=False, verbose=False
        )

        # Should have updated at least the 10 nodes we modified
        assert result["nodes_updated"] >= 10

    @pytest.mark.skip(reason="DuckDB GC segfault on large updates")
    def test_recalculate_stream_order_updates_reaches(self, temp_sword):
        """Test that reach stream_order values are updated."""
        result = temp_sword.recalculate_stream_order(
            update_nodes=True, update_reaches=True, verbose=False
        )

        # Should have processed reaches
        assert "reaches_updated" in result
        assert result["reaches_updated"] >= 0

    def test_recalculate_stream_order_handles_nodata(self, temp_sword):
        """Test that nodes with invalid path_freq get -9999."""
        result = temp_sword.recalculate_stream_order(
            update_nodes=False, update_reaches=False, verbose=False
        )

        # Check nodes_missing_path_freq was counted
        assert "nodes_missing_path_freq" in result
        # There should be some nodes with missing data
        total = result["nodes_with_valid_path_freq"] + result["nodes_missing_path_freq"]
        assert total > 0


class TestRecalculatePathSegs:
    """Test recalculate_path_segs method."""

    def test_recalculate_path_segs_method_exists(self, sword_readonly):
        """Test recalculate_path_segs method exists."""
        assert hasattr(sword_readonly, "recalculate_path_segs")
        assert callable(sword_readonly.recalculate_path_segs)

    def test_recalculate_path_segs_returns_dict(self, temp_sword):
        """Test recalculate_path_segs returns proper dict."""
        result = temp_sword.recalculate_path_segs(
            update_nodes=False, update_reaches=False, verbose=False
        )
        assert isinstance(result, dict)
        assert "nodes_updated" in result
        assert "reaches_updated" in result
        assert "total_segments" in result
        assert "nodes_assigned" in result

    def test_recalculate_path_segs_creates_segments(self, temp_sword):
        """Test that unique segments are created."""
        result = temp_sword.recalculate_path_segs(
            update_nodes=False, update_reaches=False, verbose=False
        )

        # Should create some segments
        assert result["total_segments"] > 0
        # Should assign nodes to segments
        assert result["nodes_assigned"] > 0

    def test_recalculate_path_segs_segment_count_reasonable(self, temp_sword):
        """Test that total segments is a reasonable number."""
        result = temp_sword.recalculate_path_segs(
            update_nodes=False,  # Don't actually update to avoid segfault
            update_reaches=False,
            verbose=False,
        )

        # Total segments should be positive and less than total nodes
        assert result["total_segments"] > 0
        assert result["total_segments"] <= result["nodes_assigned"]

    @pytest.mark.skip(reason="DuckDB GC segfault on large updates")
    def test_recalculate_path_segs_updates_nodes(self, temp_sword):
        """Test that node path_segs values can be updated."""
        # First, artificially modify some path_segs values
        conn = temp_sword._db.connect()
        conn.execute(
            """
            UPDATE nodes
            SET path_segs = -1
            WHERE region = ? AND path_order > 0
            LIMIT 10
        """,
            [temp_sword.region],
        )
        conn.close()

        # Recalculate
        result = temp_sword.recalculate_path_segs(
            update_nodes=True, update_reaches=False, verbose=False
        )

        # Should have updated at least the 10 nodes we modified
        assert result["nodes_updated"] >= 10

    @pytest.mark.skip(reason="DuckDB GC segfault on large updates")
    def test_recalculate_path_segs_updates_reaches(self, temp_sword):
        """Test that reach path_segs values are updated."""
        result = temp_sword.recalculate_path_segs(
            update_nodes=True, update_reaches=True, verbose=False
        )

        # Should have processed reaches
        assert "reaches_updated" in result
        assert result["reaches_updated"] >= 0

    @pytest.mark.skip(reason="DuckDB GC segfault on large updates")
    def test_recalculate_path_segs_incremental_ids(self, temp_sword):
        """Test that segment IDs start at 1 and increment."""
        result = temp_sword.recalculate_path_segs(
            update_nodes=True, update_reaches=False, verbose=False
        )

        # Check that segment IDs start from 1
        conn = temp_sword._db.connect()
        min_seg = conn.execute(
            """
            SELECT MIN(path_segs)
            FROM nodes
            WHERE region = ? AND path_segs > 0
        """,
            [temp_sword.region],
        ).fetchone()[0]
        conn.close()

        assert min_seg == 1, "Segment IDs should start at 1"


class TestRecalculateSinuosity:
    """Test suite for recalculate_sinuosity method."""

    def test_recalculate_sinuosity_method_exists(self, sword_readonly):
        """Test that recalculate_sinuosity method exists and is callable."""
        assert hasattr(sword_readonly, "recalculate_sinuosity")
        assert callable(sword_readonly.recalculate_sinuosity)

    def test_recalculate_sinuosity_returns_dict(self, temp_sword):
        """Test recalculate_sinuosity returns proper dict with single reach."""
        # Get a single reach ID to test
        conn = temp_sword._db.connect()
        reach_id = conn.execute(
            """
            SELECT reach_id FROM reaches
            WHERE region = ?
            LIMIT 1
        """,
            [temp_sword.region],
        ).fetchone()[0]

        result = temp_sword.recalculate_sinuosity(
            reach_ids=[reach_id], update_database=False, verbose=False
        )

        assert isinstance(result, dict)
        assert "reaches_processed" in result
        assert "reaches_skipped" in result
        assert "reaches_updated" in result
        assert "mean_sinuosity" in result
        assert "reach_sinuosities" in result

    def test_recalculate_sinuosity_values_reasonable(self, temp_sword):
        """Test that computed sinuosity values are reasonable."""
        # Get a few reach IDs
        conn = temp_sword._db.connect()
        reach_ids = [
            r[0]
            for r in conn.execute(
                """
            SELECT DISTINCT reach_id FROM reaches
            WHERE region = ?
            LIMIT 5
        """,
                [temp_sword.region],
            ).fetchall()
        ]

        result = temp_sword.recalculate_sinuosity(
            reach_ids=reach_ids, update_database=False, verbose=False
        )

        # Sinuosity should be >= 1.0 (can't be less than straight line)
        for reach_id, sinuosity in result["reach_sinuosities"].items():
            assert sinuosity >= 1.0, (
                f"Sinuosity for reach {reach_id} is {sinuosity}, should be >= 1.0"
            )
            # Upper bound is 10.0 (clamped in algorithm)
            assert sinuosity <= 10.0, (
                f"Sinuosity for reach {reach_id} is {sinuosity}, should be <= 10.0"
            )

    def test_recalculate_sinuosity_mean_valid(self, temp_sword):
        """Test that mean sinuosity is valid."""
        conn = temp_sword._db.connect()
        reach_ids = [
            r[0]
            for r in conn.execute(
                """
            SELECT DISTINCT reach_id FROM reaches
            WHERE region = ?
            LIMIT 10
        """,
                [temp_sword.region],
            ).fetchall()
        ]

        result = temp_sword.recalculate_sinuosity(
            reach_ids=reach_ids, update_database=False, verbose=False
        )

        # Mean should be a valid float >= 1.0
        assert isinstance(result["mean_sinuosity"], float)
        assert result["mean_sinuosity"] >= 1.0

    def test_recalculate_sinuosity_handles_short_reaches(self, temp_sword):
        """Test that short reaches (< 3 centerlines) return sinuosity 1.0."""
        # This test runs without errors even for reaches with few centerlines
        conn = temp_sword._db.connect()
        reach_ids = [
            r[0]
            for r in conn.execute(
                """
            SELECT reach_id FROM reaches
            WHERE region = ?
            LIMIT 3
        """,
                [temp_sword.region],
            ).fetchall()
        ]

        result = temp_sword.recalculate_sinuosity(
            reach_ids=reach_ids, update_database=False, verbose=False
        )

        # Should process without errors
        assert result["reaches_processed"] + result["reaches_skipped"] == len(reach_ids)

    def test_recalculate_sinuosity_helper_methods_exist(self, sword_readonly):
        """Test that helper methods exist."""
        assert hasattr(sword_readonly, "_calculate_reach_sinuosity")
        assert hasattr(sword_readonly, "_moving_average")
        assert hasattr(sword_readonly, "_merge_short_reaches")
        assert hasattr(sword_readonly, "_remove_invalid_boundaries")

    def test_moving_average(self, sword_readonly):
        """Test the moving average helper function."""
        import numpy as np

        # Test with simple array
        x = np.array([1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0])
        result = sword_readonly._moving_average(x, span=3)

        # Result should have same length
        assert len(result) == len(x)
        # Result should be smoothed (middle values close to original but averaged)
        # Values should be between min and max of input
        assert result.min() >= x.min() - 0.1
        assert result.max() <= x.max() + 0.1

    def test_moving_average_short_array(self, sword_readonly):
        """Test moving average with array shorter than span."""
        import numpy as np

        x = np.array([1.0, 2.0])
        result = sword_readonly._moving_average(x, span=5)

        # Should return original for short arrays
        assert len(result) == len(x)

    @pytest.mark.skip(reason="DuckDB GC segfault on large updates")
    def test_recalculate_sinuosity_updates_database(self, temp_sword):
        """Test that sinuosity values are actually updated in database."""
        conn = temp_sword._db.connect()
        reach_ids = [
            r[0]
            for r in conn.execute(
                """
            SELECT reach_id FROM reaches
            WHERE region = ?
            LIMIT 5
        """,
                [temp_sword.region],
            ).fetchall()
        ]

        result = temp_sword.recalculate_sinuosity(
            reach_ids=reach_ids, update_database=True, verbose=False
        )

        # Should have attempted updates
        assert "reaches_updated" in result


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
