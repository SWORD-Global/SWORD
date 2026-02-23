# -*- coding: utf-8 -*-
"""
Tests for the SWORD provenance logging system.
"""

import pytest

pytestmark = [pytest.mark.unit, pytest.mark.provenance]


class TestProvenanceImports:
    """Test that provenance module imports correctly."""

    def test_import_provenance_logger(self):
        from src.sword_duckdb import ProvenanceLogger

        assert ProvenanceLogger is not None

    def test_import_operation_type(self):
        from src.sword_duckdb import OperationType

        assert OperationType.CREATE.value == "CREATE"
        assert OperationType.UPDATE.value == "UPDATE"
        assert OperationType.DELETE.value == "DELETE"

    def test_import_operation_status(self):
        from src.sword_duckdb import OperationStatus

        assert OperationStatus.PENDING.value == "PENDING"
        assert OperationStatus.COMPLETED.value == "COMPLETED"
        assert OperationStatus.ROLLED_BACK.value == "ROLLED_BACK"


class TestProvenanceSchema:
    """Test provenance table definitions in schema."""

    def test_operations_table_exists(self):
        from src.sword_duckdb.schema import SWORD_OPERATIONS_TABLE

        assert "sword_operations" in SWORD_OPERATIONS_TABLE
        assert "operation_id" in SWORD_OPERATIONS_TABLE
        assert "operation_type" in SWORD_OPERATIONS_TABLE
        assert "user_id" in SWORD_OPERATIONS_TABLE
        assert "session_id" in SWORD_OPERATIONS_TABLE
        assert "reason" in SWORD_OPERATIONS_TABLE

    def test_snapshots_table_exists(self):
        from src.sword_duckdb.schema import SWORD_VALUE_SNAPSHOTS_TABLE

        assert "sword_value_snapshots" in SWORD_VALUE_SNAPSHOTS_TABLE
        assert "snapshot_id" in SWORD_VALUE_SNAPSHOTS_TABLE
        assert "operation_id" in SWORD_VALUE_SNAPSHOTS_TABLE
        assert "old_value" in SWORD_VALUE_SNAPSHOTS_TABLE
        assert "new_value" in SWORD_VALUE_SNAPSHOTS_TABLE

    def test_lineage_table_exists(self):
        from src.sword_duckdb.schema import SWORD_SOURCE_LINEAGE_TABLE

        assert "sword_source_lineage" in SWORD_SOURCE_LINEAGE_TABLE
        assert "source_dataset" in SWORD_SOURCE_LINEAGE_TABLE
        assert "derivation_method" in SWORD_SOURCE_LINEAGE_TABLE

    def test_recipes_table_exists(self):
        from src.sword_duckdb.schema import SWORD_RECONSTRUCTION_RECIPES_TABLE

        assert "sword_reconstruction_recipes" in SWORD_RECONSTRUCTION_RECIPES_TABLE
        assert "target_attributes" in SWORD_RECONSTRUCTION_RECIPES_TABLE
        assert "required_sources" in SWORD_RECONSTRUCTION_RECIPES_TABLE

    def test_schema_version_updated(self):
        from src.sword_duckdb.schema import SCHEMA_VERSION

        assert (
            SCHEMA_VERSION == "1.7.0"
        )  # Updated for v17c columns (subnetwork_id, facc_quality)


class TestWorkflowProvenance:
    """Test SWORDWorkflow provenance integration."""

    def test_workflow_has_provenance_property(self):
        from src.sword_duckdb import SWORDWorkflow

        workflow = SWORDWorkflow()
        assert hasattr(workflow, "provenance")
        assert hasattr(workflow, "_enable_provenance")

    def test_workflow_init_with_user_id(self):
        from src.sword_duckdb import SWORDWorkflow

        workflow = SWORDWorkflow(user_id="test_user")
        assert workflow._user_id == "test_user"

    def test_workflow_init_provenance_disabled(self):
        from src.sword_duckdb import SWORDWorkflow

        workflow = SWORDWorkflow(enable_provenance=False)
        assert workflow._enable_provenance is False

    def test_workflow_has_transaction_method(self):
        from src.sword_duckdb import SWORDWorkflow

        workflow = SWORDWorkflow()
        assert hasattr(workflow, "transaction")
        assert callable(workflow.transaction)

    def test_workflow_has_rollback_method(self):
        from src.sword_duckdb import SWORDWorkflow

        workflow = SWORDWorkflow()
        assert hasattr(workflow, "rollback")
        assert callable(workflow.rollback)

    def test_workflow_has_get_history_method(self):
        from src.sword_duckdb import SWORDWorkflow

        workflow = SWORDWorkflow()
        assert hasattr(workflow, "get_history")
        assert callable(workflow.get_history)

    def test_workflow_has_get_lineage_method(self):
        from src.sword_duckdb import SWORDWorkflow

        workflow = SWORDWorkflow()
        assert hasattr(workflow, "get_lineage")
        assert callable(workflow.get_lineage)

    def test_workflow_has_modify_reach_method(self):
        from src.sword_duckdb import SWORDWorkflow

        workflow = SWORDWorkflow()
        assert hasattr(workflow, "modify_reach")
        assert callable(workflow.modify_reach)

    def test_workflow_has_modify_node_method(self):
        from src.sword_duckdb import SWORDWorkflow

        workflow = SWORDWorkflow()
        assert hasattr(workflow, "modify_node")
        assert callable(workflow.modify_node)


class TestProvenanceLoggerMethods:
    """Test ProvenanceLogger class methods."""

    def test_logger_has_operation_context_manager(self):
        from src.sword_duckdb import ProvenanceLogger

        assert hasattr(ProvenanceLogger, "operation")

    def test_logger_has_log_value_change_method(self):
        from src.sword_duckdb import ProvenanceLogger

        assert hasattr(ProvenanceLogger, "log_value_change")

    def test_logger_has_get_entity_history_method(self):
        from src.sword_duckdb import ProvenanceLogger

        assert hasattr(ProvenanceLogger, "get_entity_history")

    def test_logger_has_rollback_operation_method(self):
        from src.sword_duckdb import ProvenanceLogger

        assert hasattr(ProvenanceLogger, "rollback_operation")

    def test_logger_has_record_lineage_method(self):
        from src.sword_duckdb import ProvenanceLogger

        assert hasattr(ProvenanceLogger, "record_lineage")


class TestCreateProvenanceTables:
    """Test creating provenance tables in a database."""

    def test_create_provenance_tables_function_exists(self):
        from src.sword_duckdb import create_provenance_tables

        assert callable(create_provenance_tables)

    def test_create_provenance_tables_in_memory(self):
        """Test creating provenance tables in an in-memory database."""
        import duckdb
        from src.sword_duckdb import create_provenance_tables

        conn = duckdb.connect(":memory:")
        create_provenance_tables(conn)

        # Verify tables were created
        tables = conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        ).fetchall()
        table_names = [t[0] for t in tables]

        assert "sword_operations" in table_names
        assert "sword_value_snapshots" in table_names
        assert "sword_source_lineage" in table_names
        assert "sword_reconstruction_recipes" in table_names

        conn.close()


class TestWorkflowStatus:
    """Test workflow status includes provenance info."""

    def test_status_has_provenance_enabled(self):
        from src.sword_duckdb import SWORDWorkflow

        workflow = SWORDWorkflow()
        status = workflow.status()
        assert "provenance_enabled" in status
        assert status["provenance_enabled"] is True

    def test_status_has_in_transaction(self):
        from src.sword_duckdb import SWORDWorkflow

        workflow = SWORDWorkflow()
        status = workflow.status()
        assert "in_transaction" in status
        assert status["in_transaction"] is False
