"""
Tests for SWORD Lint Framework

Tests core functionality, check registration, runners, and formatters.
"""

import json
import pytest
from pathlib import Path

import pandas as pd

from src.sword_duckdb.lint import (
    Severity,
    Category,
    CheckResult,
    LintRunner,
    get_registry,
    get_check,
    get_checks_by_category,
    get_checks_by_severity,
    list_check_ids,
)
from src.sword_duckdb.lint.formatters import (
    ConsoleFormatter,
    JsonFormatter,
    MarkdownFormatter,
)

pytestmark = pytest.mark.lint


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def test_db_path():
    """Path to test database."""
    return Path(__file__).parent / "fixtures" / "sword_test_minimal.duckdb"


@pytest.fixture
def sample_result():
    """Sample CheckResult for testing formatters."""
    return CheckResult(
        check_id="T001",
        name="test_check",
        severity=Severity.ERROR,
        passed=False,
        total_checked=100,
        issues_found=5,
        issue_pct=5.0,
        details=pd.DataFrame(
            {
                "reach_id": [1, 2, 3, 4, 5],
                "region": ["NA"] * 5,
                "issue": ["a", "b", "c", "d", "e"],
            }
        ),
        description="Test check description",
        threshold=10.0,
        elapsed_ms=123.45,
    )


@pytest.fixture
def sample_results(sample_result):
    """List of sample results for testing."""
    return [
        sample_result,
        CheckResult(
            check_id="T002",
            name="passing_check",
            severity=Severity.WARNING,
            passed=True,
            total_checked=50,
            issues_found=0,
            issue_pct=0.0,
            details=pd.DataFrame(),
            description="A passing check",
            elapsed_ms=50.0,
        ),
        CheckResult(
            check_id="A001",
            name="info_check",
            severity=Severity.INFO,
            passed=False,
            total_checked=200,
            issues_found=10,
            issue_pct=5.0,
            details=pd.DataFrame({"reach_id": range(10)}),
            description="An info check",
            elapsed_ms=75.0,
        ),
    ]


# =============================================================================
# Core Types Tests
# =============================================================================


class TestSeverity:
    """Tests for Severity enum."""

    def test_severity_values(self):
        assert Severity.ERROR.value == "error"
        assert Severity.WARNING.value == "warning"
        assert Severity.INFO.value == "info"

    def test_severity_from_string(self):
        assert Severity("error") == Severity.ERROR
        assert Severity("warning") == Severity.WARNING
        assert Severity("info") == Severity.INFO


class TestCategory:
    """Tests for Category enum."""

    def test_category_values(self):
        assert Category.TOPOLOGY.value == "topology"
        assert Category.ATTRIBUTES.value == "attributes"
        assert Category.GEOMETRY.value == "geometry"
        assert Category.CLASSIFICATION.value == "classification"


class TestCheckResult:
    """Tests for CheckResult dataclass."""

    def test_check_result_creation(self, sample_result):
        assert sample_result.check_id == "T001"
        assert sample_result.severity == Severity.ERROR
        assert sample_result.passed is False
        assert sample_result.issues_found == 5

    def test_check_result_repr(self, sample_result):
        repr_str = repr(sample_result)
        assert "T001" in repr_str
        assert "FAIL" in repr_str


# =============================================================================
# Registry Tests
# =============================================================================


class TestRegistry:
    """Tests for check registry."""

    def test_registry_not_empty(self):
        registry = get_registry()
        assert len(registry) > 0

    def test_check_ids_unique(self):
        """All check IDs must be unique."""
        ids = list_check_ids()
        assert len(ids) == len(set(ids))

    def test_check_id_format(self):
        """Check IDs should follow pattern: letter(s) + digits (e.g. T001, FL001)."""
        import re

        for check_id in list_check_ids():
            assert len(check_id) >= 2
            assert re.match(r"^[A-Z]+\d+$", check_id), (
                f"Invalid check ID format: {check_id}"
            )

    def test_get_check(self):
        spec = get_check("T001")
        assert spec is not None
        assert spec.check_id == "T001"
        assert spec.category == Category.TOPOLOGY

    def test_get_nonexistent_check(self):
        assert get_check("Z999") is None

    def test_checks_by_category(self):
        topology_checks = get_checks_by_category(Category.TOPOLOGY)
        assert len(topology_checks) > 0
        for spec in topology_checks:
            assert spec.category == Category.TOPOLOGY

    def test_checks_by_severity(self):
        error_checks = get_checks_by_severity(Severity.ERROR)
        for spec in error_checks:
            assert spec.severity == Severity.ERROR

    def test_required_checks_registered(self):
        """Verify all planned checks are registered."""
        required = [
            "T001",
            "T002",
            "T003",
            "T004",
            "T005",
            "T006",
            "A002",
            "A003",
            "A004",
            "A005",
            "A021",
            "A024",
            "A026",
            "A027",
            "G001",
            "G013",
            "G014",
            "G015",
            "G016",
            "G017",
            "G018",
            "G021",
            "C001",
            "FL001",
            "FL002",
            "FL003",
            "FL004",
            "N001",
            "N002",
            "T013",
            "T014",
            "T015",
            "T017",
            "T018",
            "T019",
            "T020",
            "A030",
            "N003",
            "N004",
            "N005",
            "N006",
            "N008",
            "N010",
            "C005",
            "A031",
            "N011",
            "G022",
            "G023",
        ]
        registry = get_registry()
        for check_id in required:
            assert check_id in registry, f"Missing required check: {check_id}"


# =============================================================================
# Runner Tests
# =============================================================================


@pytest.mark.db
class TestLintRunner:
    """Tests for LintRunner."""

    def test_runner_init_nonexistent_db(self):
        with pytest.raises(FileNotFoundError):
            LintRunner("/nonexistent/path.duckdb")

    @pytest.mark.skipif(
        not (Path(__file__).parent / "fixtures" / "sword_test_minimal.duckdb").exists(),
        reason="Test database not found",
    )
    def test_runner_init_valid_db(self, test_db_path):
        runner = LintRunner(test_db_path)
        assert runner.conn is not None
        runner.close()

    @pytest.mark.skipif(
        not (Path(__file__).parent / "fixtures" / "sword_test_minimal.duckdb").exists(),
        reason="Test database not found",
    )
    def test_runner_context_manager(self, test_db_path):
        with LintRunner(test_db_path) as runner:
            assert runner.conn is not None

    @pytest.mark.skipif(
        not (Path(__file__).parent / "fixtures" / "sword_test_minimal.duckdb").exists(),
        reason="Test database not found",
    )
    def test_runner_run_all_checks(self, test_db_path):
        with LintRunner(test_db_path) as runner:
            results = runner.run()
            assert len(results) > 0
            for result in results:
                assert isinstance(result, CheckResult)

    @pytest.mark.skipif(
        not (Path(__file__).parent / "fixtures" / "sword_test_minimal.duckdb").exists(),
        reason="Test database not found",
    )
    def test_runner_run_specific_checks(self, test_db_path):
        with LintRunner(test_db_path) as runner:
            results = runner.run(checks=["T001", "T002"])
            assert len(results) == 2
            check_ids = {r.check_id for r in results}
            assert check_ids == {"T001", "T002"}

    @pytest.mark.skipif(
        not (Path(__file__).parent / "fixtures" / "sword_test_minimal.duckdb").exists(),
        reason="Test database not found",
    )
    def test_runner_run_category_prefix(self, test_db_path):
        with LintRunner(test_db_path) as runner:
            results = runner.run(checks=["T"])
            for result in results:
                assert result.check_id.startswith("T")

    @pytest.mark.skipif(
        not (Path(__file__).parent / "fixtures" / "sword_test_minimal.duckdb").exists(),
        reason="Test database not found",
    )
    def test_runner_run_severity_filter(self, test_db_path):
        with LintRunner(test_db_path) as runner:
            results = runner.run(severity=Severity.ERROR)
            for result in results:
                assert result.severity == Severity.ERROR

    @pytest.mark.skipif(
        not (Path(__file__).parent / "fixtures" / "sword_test_minimal.duckdb").exists(),
        reason="Test database not found",
    )
    def test_runner_threshold_override(self, test_db_path):
        with LintRunner(test_db_path) as runner:
            runner.set_threshold("A002", 200.0)
            result = runner.run_check("A002")
            assert result.threshold == 200.0

    def test_runner_invalid_check_id(self, test_db_path):
        if not test_db_path.exists():
            pytest.skip("Test database not found")
        with LintRunner(test_db_path) as runner:
            with pytest.raises(ValueError):
                runner.run(checks=["Z999"])

    @pytest.mark.skipif(
        not (Path(__file__).parent / "fixtures" / "sword_test_minimal.duckdb").exists(),
        reason="Test database not found",
    )
    def test_runner_get_summary(self, test_db_path):
        with LintRunner(test_db_path) as runner:
            results = runner.run()
            summary = runner.get_summary(results)
            assert "total_checks" in summary
            assert "passed" in summary
            assert "failed" in summary
            assert "by_severity" in summary


# =============================================================================
# Formatter Tests
# =============================================================================


class TestConsoleFormatter:
    """Tests for ConsoleFormatter."""

    def test_format_with_results(self, sample_results):
        formatter = ConsoleFormatter(use_color=False)
        output = formatter.format(sample_results)
        assert "SWORD LINT REPORT" in output
        assert "T001" in output
        assert "T002" in output
        assert "A001" in output

    def test_format_with_color_disabled(self, sample_results):
        formatter = ConsoleFormatter(use_color=False)
        output = formatter.format(sample_results)
        # No ANSI codes when color disabled
        assert "\033[" not in output

    def test_format_verbose(self, sample_results):
        formatter = ConsoleFormatter(use_color=False, verbose=True)
        output = formatter.format(sample_results)
        assert "Sample issues" in output

    def test_format_summary(self, sample_results):
        formatter = ConsoleFormatter(use_color=False)
        summary = formatter.format_summary(sample_results)
        assert "FAIL" in summary or "WARN" in summary


class TestJsonFormatter:
    """Tests for JsonFormatter."""

    def test_format_valid_json(self, sample_results):
        formatter = JsonFormatter()
        output = formatter.format(sample_results)
        data = json.loads(output)
        assert "metadata" in data
        assert "summary" in data
        assert "results" in data

    def test_format_with_details(self, sample_results):
        formatter = JsonFormatter(include_details=True)
        output = formatter.format(sample_results)
        data = json.loads(output)
        # Should have details for at least one result
        has_details = any("details" in r for r in data["results"])
        assert has_details

    def test_format_summary_counts(self, sample_results):
        formatter = JsonFormatter()
        output = formatter.format(sample_results)
        data = json.loads(output)
        assert data["summary"]["total_checks"] == 3
        assert data["summary"]["passed"] == 1
        assert data["summary"]["failed"] == 2

    def test_format_exit_code(self, sample_results):
        formatter = JsonFormatter()
        output = formatter.format(sample_results)
        data = json.loads(output)
        # Has error, so exit_code should be 2
        assert data["exit_code"] == 2


class TestMarkdownFormatter:
    """Tests for MarkdownFormatter."""

    def test_format_valid_markdown(self, sample_results):
        formatter = MarkdownFormatter()
        output = formatter.format(sample_results)
        assert "# SWORD Lint Report" in output
        assert "## Summary" in output
        assert "## Check Results" in output

    def test_format_has_table(self, sample_results):
        formatter = MarkdownFormatter()
        output = formatter.format(sample_results)
        # Check for markdown table structure
        assert "| Status |" in output
        assert "|:------:|" in output

    def test_format_with_details(self, sample_results):
        formatter = MarkdownFormatter(include_details=True)
        output = formatter.format(sample_results)
        assert "## Issue Details" in output

    def test_format_summary(self, sample_results):
        formatter = MarkdownFormatter()
        summary = formatter.format_summary(sample_results)
        assert "FAIL" in summary or "WARN" in summary


# =============================================================================
# Integration Tests
# =============================================================================


@pytest.mark.db
@pytest.mark.skipif(
    not (Path(__file__).parent / "fixtures" / "sword_test_minimal.duckdb").exists(),
    reason="Test database not found",
)
class TestIntegration:
    """Integration tests running real checks."""

    def test_full_lint_run(self, test_db_path):
        """Run all checks and verify output."""
        with LintRunner(test_db_path) as runner:
            results = runner.run()

            # Should have results
            assert len(results) > 0

            # All results should be CheckResult
            for result in results:
                assert isinstance(result, CheckResult)
                assert result.elapsed_ms >= 0

            # Format with all formatters
            console = ConsoleFormatter(use_color=False).format(results)
            assert len(console) > 0

            json_out = JsonFormatter().format(results)
            json.loads(json_out)  # Should parse

            md_out = MarkdownFormatter().format(results)
            assert "# SWORD Lint Report" in md_out

    def test_region_filter(self, test_db_path):
        """Test region filtering."""
        with LintRunner(test_db_path) as runner:
            results = runner.run(region="NA")
            # Should still get results (test DB has NA data)
            assert len(results) > 0

    def test_check_timing(self, test_db_path):
        """Verify timing is captured."""
        with LintRunner(test_db_path) as runner:
            results = runner.run(checks=["T001"])
            assert len(results) == 1
            assert results[0].elapsed_ms > 0


# =============================================================================
# V011 OSM Name Continuity Tests
# =============================================================================


def _create_v011_test_data(conn, rows):
    """Create minimal reaches table for V011 testing.

    rows: list of (reach_id, region, x, y, river_name_local, rch_id_dn_main, n_rch_down, n_rch_up)
    """

    conn.execute("""
        CREATE TABLE IF NOT EXISTS reaches (
            reach_id BIGINT,
            region VARCHAR,
            x DOUBLE,
            y DOUBLE,
            river_name_local VARCHAR,
            rch_id_dn_main BIGINT,
            n_rch_down INTEGER,
            n_rch_up INTEGER
        )
    """)
    for row in rows:
        conn.execute(
            "INSERT INTO reaches VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            list(row),
        )


@pytest.mark.db
@pytest.mark.skipif(
    not (Path(__file__).parent / "fixtures" / "sword_test_minimal.duckdb").exists(),
    reason="Test database not found",
)
class TestV011OsmNameContinuity:
    """Tests for V011 osm_name_continuity check."""

    def test_v011_registered(self):
        """V011 should be in the registry."""
        check = get_check("V011")
        assert check is not None
        assert check.name == "osm_name_continuity"
        assert check.category == Category.V17C
        assert check.severity == Severity.WARNING

    def test_v011_missing_columns_passes(self, test_db_path):
        """V011 should pass gracefully when columns don't exist."""
        with LintRunner(test_db_path) as runner:
            results = runner.run(checks=["V011"])
            assert len(results) == 1
            r = results[0]
            assert r.check_id == "V011"
            assert r.passed is True
            assert r.total_checked == 0

    def test_v011_same_name_no_flag(self, tmp_path):
        """Same river_name_local on 1:1 link should not be flagged."""
        import duckdb as _duckdb  # noqa: F811

        db_path = tmp_path / "v011_test.duckdb"
        conn = _duckdb.connect(str(db_path))
        _create_v011_test_data(
            conn,
            [
                # reach_id, region, x, y, river_name_local, rch_id_dn_main, n_rch_down, n_rch_up
                (1001, "NA", -75.0, 45.0, "St. Lawrence", 1002, 1, 0),
                (1002, "NA", -75.1, 45.1, "St. Lawrence", None, 0, 1),
            ],
        )
        from src.sword_duckdb.lint.checks.v17c import check_osm_name_continuity

        result = check_osm_name_continuity(conn, region="NA")
        assert result.passed is True
        assert result.issues_found == 0
        conn.close()

    def test_v011_different_name_1to1_flags(self, tmp_path):
        """Different river_name_local on 1:1 link should be flagged."""
        import duckdb as _duckdb  # noqa: F811

        db_path = tmp_path / "v011_test.duckdb"
        conn = _duckdb.connect(str(db_path))
        _create_v011_test_data(
            conn,
            [
                (1001, "NA", -75.0, 45.0, "St. Lawrence", 1002, 1, 0),
                (1002, "NA", -75.1, 45.1, "Ottawa River", None, 0, 1),
            ],
        )
        from src.sword_duckdb.lint.checks.v17c import check_osm_name_continuity

        result = check_osm_name_continuity(conn, region="NA")
        assert result.passed is False
        assert result.issues_found == 1
        conn.close()

    def test_v011_junction_not_flagged(self, tmp_path):
        """Different name at junction (n_rch_up > 1) should NOT be flagged."""
        import duckdb as _duckdb  # noqa: F811

        db_path = tmp_path / "v011_test.duckdb"
        conn = _duckdb.connect(str(db_path))
        _create_v011_test_data(
            conn,
            [
                (1001, "NA", -75.0, 45.0, "St. Lawrence", 1003, 1, 0),
                (1002, "NA", -74.9, 45.0, "Ottawa River", 1003, 1, 0),
                (
                    1003,
                    "NA",
                    -75.1,
                    45.1,
                    "St. Lawrence",
                    None,
                    0,
                    2,
                ),  # junction: n_rch_up=2
            ],
        )
        from src.sword_duckdb.lint.checks.v17c import check_osm_name_continuity

        result = check_osm_name_continuity(conn, region="NA")
        assert result.passed is True
        assert result.issues_found == 0
        conn.close()

    def test_v011_null_name_skipped(self, tmp_path):
        """NULL river_name_local should not be flagged."""
        import duckdb as _duckdb  # noqa: F811

        db_path = tmp_path / "v011_test.duckdb"
        conn = _duckdb.connect(str(db_path))
        _create_v011_test_data(
            conn,
            [
                (1001, "NA", -75.0, 45.0, None, 1002, 1, 0),
                (1002, "NA", -75.1, 45.1, "St. Lawrence", None, 0, 1),
            ],
        )
        from src.sword_duckdb.lint.checks.v17c import check_osm_name_continuity

        result = check_osm_name_continuity(conn, region="NA")
        assert result.passed is True
        assert result.issues_found == 0
        conn.close()


# =============================================================================
# V010/V012/V014/V015 Path Topology Checks
# =============================================================================


def _create_v17c_path_topology_test_data(conn, reaches_rows, topology_rows=None):
    """Create minimal reaches/reach_topology tables for v17c path checks."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS reaches (
            reach_id BIGINT,
            region VARCHAR,
            x DOUBLE,
            y DOUBLE,
            river_name VARCHAR,
            n_rch_up INTEGER,
            n_rch_down INTEGER,
            rch_id_up_main BIGINT,
            rch_id_dn_main BIGINT,
            best_headwater BIGINT,
            best_outlet BIGINT,
            main_path_id BIGINT
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS reach_topology (
            reach_id BIGINT,
            region VARCHAR,
            direction VARCHAR,
            neighbor_rank INTEGER,
            neighbor_reach_id BIGINT
        )
    """)

    for row in reaches_rows:
        conn.execute(
            "INSERT INTO reaches VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            list(row),
        )

    for row in topology_rows or []:
        conn.execute(
            "INSERT INTO reach_topology VALUES (?, ?, ?, ?, ?)",
            list(row),
        )


@pytest.mark.db
class TestV17CPathTopologyChecks:
    """Tests for V010/V012/V014/V015 checks."""

    def test_v010_registered(self):
        check = get_check("V010")
        assert check is not None
        assert check.name == "main_connection_integrity"
        assert check.category == Category.V17C
        assert check.severity == Severity.ERROR

    def test_v012_registered(self):
        check = get_check("V012")
        assert check is not None
        assert check.name == "main_connection_null_semantics"
        assert check.category == Category.V17C
        assert check.severity == Severity.ERROR

    def test_v014_registered(self):
        check = get_check("V014")
        assert check is not None
        assert check.name == "main_path_id_region_consistency"
        assert check.category == Category.V17C
        assert check.severity == Severity.WARNING

    def test_v015_registered(self):
        check = get_check("V015")
        assert check is not None
        assert check.name == "tuple_to_main_path_id_uniqueness"
        assert check.category == Category.V17C
        assert check.severity == Severity.WARNING

    def test_v010_flags_integrity_violations(self, tmp_path):
        import duckdb as _duckdb

        db_path = tmp_path / "v010_test.duckdb"
        conn = _duckdb.connect(str(db_path))
        _create_v17c_path_topology_test_data(
            conn,
            reaches_rows=[
                (1, "NA", -75.0, 45.0, "A", 0, 1, None, 2, 1, 3, 10),
                (2, "NA", -75.1, 45.1, "A", 1, 1, 99, 1, 1, 3, 10),
                (3, "NA", -75.2, 45.2, "A", 1, 0, 2, 3, 1, 3, 10),
            ],
            topology_rows=[
                (1, "NA", "down", 0, 2),
                (2, "NA", "up", 0, 1),
                (2, "NA", "down", 0, 3),
                (3, "NA", "up", 0, 2),
            ],
        )
        from src.sword_duckdb.lint.checks.v17c import check_main_connection_integrity

        result = check_main_connection_integrity(conn, region="NA")
        assert result.passed is False
        assert result.issues_found >= 3
        issue_types = set(result.details["issue_type"].tolist())
        assert "up_main_invalid_fk" in issue_types
        assert "dn_main_not_downstream_neighbor" in issue_types
        assert "dn_main_self_reference" in issue_types
        conn.close()

    def test_v010_ignores_9999_sentinels(self, tmp_path):
        import duckdb as _duckdb

        db_path = tmp_path / "v010_sentinel_test.duckdb"
        conn = _duckdb.connect(str(db_path))
        _create_v17c_path_topology_test_data(
            conn,
            reaches_rows=[
                # rch_id_up_main = -9999 should be ignored
                (1, "NA", -75.0, 45.0, "A", 0, 1, -9999, -9999, 1, 3, 10),
            ],
            topology_rows=[],
        )
        from src.sword_duckdb.lint.checks.v17c import check_main_connection_integrity

        result = check_main_connection_integrity(conn, region="NA")
        assert result.passed is True
        assert result.issues_found == 0
        conn.close()

    def test_v012_flags_null_semantics_violations(self, tmp_path):
        import duckdb as _duckdb

        db_path = tmp_path / "v012_test.duckdb"
        conn = _duckdb.connect(str(db_path))
        _create_v17c_path_topology_test_data(
            conn,
            reaches_rows=[
                (1, "NA", -75.0, 45.0, "A", 0, 1, 2, 2, 1, 3, 10),
                (2, "NA", -75.1, 45.1, "A", 1, 1, None, None, 1, 3, 10),
                (3, "NA", -75.2, 45.2, "A", 1, 0, 2, 2, 1, 3, 10),
            ],
        )
        from src.sword_duckdb.lint.checks.v17c import (
            check_main_connection_null_semantics,
        )

        result = check_main_connection_null_semantics(conn, region="NA")
        assert result.passed is False
        assert result.issues_found == 4
        issue_types = set(result.details["issue_type"].tolist())
        assert "headwater_has_up_main" in issue_types
        assert "non_headwater_missing_up_main" in issue_types
        assert "outlet_has_dn_main" in issue_types
        assert "non_outlet_missing_dn_main" in issue_types
        conn.close()

    def test_v012_passes_for_valid_chain(self, tmp_path):
        import duckdb as _duckdb

        db_path = tmp_path / "v012_valid.duckdb"
        conn = _duckdb.connect(str(db_path))
        _create_v17c_path_topology_test_data(
            conn,
            reaches_rows=[
                (1, "NA", -75.0, 45.0, "A", 0, 1, None, 2, 1, 3, 10),
                (2, "NA", -75.1, 45.1, "A", 1, 1, 1, 3, 1, 3, 10),
                (3, "NA", -75.2, 45.2, "A", 1, 0, 2, None, 1, 3, 10),
            ],
        )
        from src.sword_duckdb.lint.checks.v17c import (
            check_main_connection_null_semantics,
        )

        result = check_main_connection_null_semantics(conn, region="NA")
        assert result.passed is True
        assert result.issues_found == 0
        conn.close()

    def test_v014_v015_detect_main_path_collisions(self, tmp_path):
        import duckdb as _duckdb

        db_path = tmp_path / "v014_v015_test.duckdb"
        conn = _duckdb.connect(str(db_path))
        _create_v17c_path_topology_test_data(
            conn,
            reaches_rows=[
                (1, "NA", -75.0, 45.0, "A", 0, 0, None, None, 100, 900, 10),
                (2, "NA", -75.1, 45.1, "A", 0, 0, None, None, 101, 900, 10),
                (3, "NA", -75.2, 45.2, "A", 0, 0, None, None, 200, 901, 11),
                (4, "NA", -75.3, 45.3, "A", 0, 0, None, None, 200, 901, 12),
            ],
        )
        from src.sword_duckdb.lint.checks.v17c import (
            check_main_path_id_region_consistency,
            check_tuple_to_main_path_id_uniqueness,
        )

        v014 = check_main_path_id_region_consistency(conn, region="NA")
        assert v014.passed is False
        assert v014.issues_found == 1
        assert int(v014.details.iloc[0]["main_path_id"]) == 10

        v015 = check_tuple_to_main_path_id_uniqueness(conn, region="NA")
        assert v015.passed is False
        assert v015.issues_found == 1
        assert int(v015.details.iloc[0]["best_headwater"]) == 200
        assert int(v015.details.iloc[0]["best_outlet"]) == 901
        conn.close()


# =============================================================================
# Geometry Check Tests (G013-G021)
# =============================================================================


def _spatial_conn(tmp_path, name="test.duckdb"):
    """Create a DuckDB connection with spatial extension loaded."""
    import duckdb as _duckdb

    db_path = tmp_path / name
    conn = _duckdb.connect(str(db_path))
    try:
        conn.execute("INSTALL spatial; LOAD spatial")
    except Exception:
        pytest.skip("DuckDB spatial extension unavailable")
    return conn


def _create_reaches_table(conn, rows):
    """Create minimal reaches table.

    rows: list of dicts with at least reach_id, region.
    Other columns have defaults.
    """
    conn.execute("""
        CREATE TABLE IF NOT EXISTS reaches (
            reach_id BIGINT, region VARCHAR, x DOUBLE, y DOUBLE,
            x_min DOUBLE, x_max DOUBLE, y_min DOUBLE, y_max DOUBLE,
            geom GEOMETRY, reach_length DOUBLE, width DOUBLE,
            lakeflag INTEGER, dist_out DOUBLE, river_name VARCHAR,
            n_rch_up INTEGER DEFAULT 0, n_rch_down INTEGER DEFAULT 0,
            sinuosity DOUBLE DEFAULT 1.1, n_nodes INTEGER DEFAULT 5
        )
    """)
    for r in rows:
        conn.execute(
            """INSERT INTO reaches (reach_id, region, x, y, geom,
               reach_length, width, lakeflag, dist_out, river_name,
               n_rch_up, n_rch_down)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            [
                r.get("reach_id"),
                r.get("region", "NA"),
                r.get("x", 0.0),
                r.get("y", 0.0),
                r.get("geom"),
                r.get("reach_length", 5000.0),
                r.get("width", 100.0),
                r.get("lakeflag", 0),
                r.get("dist_out", 50000.0),
                r.get("river_name", "Test"),
                r.get("n_rch_up", 0),
                r.get("n_rch_down", 0),
            ],
        )


def _create_nodes_table(conn, rows):
    """Create minimal nodes table."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS nodes (
            node_id BIGINT, region VARCHAR, x DOUBLE, y DOUBLE,
            geom GEOMETRY, reach_id BIGINT, node_length DOUBLE
        )
    """)
    for n in rows:
        conn.execute(
            "INSERT INTO nodes VALUES (?,?,?,?,?,?,?)",
            [
                n["node_id"],
                n.get("region", "NA"),
                n["x"],
                n["y"],
                n.get("geom"),
                n["reach_id"],
                n.get("node_length", 1000.0),
            ],
        )


def _create_topology_table(conn, rows):
    """Create minimal reach_topology table."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS reach_topology (
            reach_id BIGINT, region VARCHAR, direction VARCHAR,
            neighbor_rank INTEGER, neighbor_reach_id BIGINT
        )
    """)
    for t in rows:
        conn.execute(
            "INSERT INTO reach_topology VALUES (?,?,?,?,?)",
            [
                t["reach_id"],
                t.get("region", "NA"),
                t["direction"],
                t.get("neighbor_rank", 0),
                t["neighbor_reach_id"],
            ],
        )


class TestG013WidthGtLength:
    """Tests for G013 width_gt_length."""

    def test_pass_normal(self, tmp_path):
        conn = _spatial_conn(tmp_path)
        _create_reaches_table(
            conn,
            [
                {"reach_id": 1, "width": 100.0, "reach_length": 5000.0, "lakeflag": 0},
            ],
        )
        from src.sword_duckdb.lint.checks.geometry import check_width_gt_length

        result = check_width_gt_length(conn)
        assert result.passed is True
        conn.close()

    def test_fail_width_exceeds_length(self, tmp_path):
        conn = _spatial_conn(tmp_path)
        _create_reaches_table(
            conn,
            [
                {
                    "reach_id": 1,
                    "width": 10000.0,
                    "reach_length": 5000.0,
                    "lakeflag": 0,
                },
            ],
        )
        from src.sword_duckdb.lint.checks.geometry import check_width_gt_length

        result = check_width_gt_length(conn)
        assert result.passed is False
        assert result.issues_found == 1
        conn.close()

    def test_lake_excluded(self, tmp_path):
        conn = _spatial_conn(tmp_path)
        _create_reaches_table(
            conn,
            [
                {
                    "reach_id": 1,
                    "width": 10000.0,
                    "reach_length": 5000.0,
                    "lakeflag": 1,
                },
            ],
        )
        from src.sword_duckdb.lint.checks.geometry import check_width_gt_length

        result = check_width_gt_length(conn)
        assert result.passed is True
        conn.close()


class TestG014DuplicateGeometry:
    """Tests for G014 duplicate_geometry."""

    def test_pass_unique(self, tmp_path):
        conn = _spatial_conn(tmp_path)
        line1 = "ST_GeomFromText('LINESTRING(0 0, 1 1)')"
        line2 = "ST_GeomFromText('LINESTRING(2 2, 3 3)')"
        conn.execute("""
            CREATE TABLE reaches (
                reach_id BIGINT, region VARCHAR, x DOUBLE, y DOUBLE,
                geom GEOMETRY, reach_length DOUBLE, width DOUBLE,
                lakeflag INTEGER, river_name VARCHAR,
                n_rch_up INTEGER DEFAULT 0, n_rch_down INTEGER DEFAULT 0
            )
        """)
        conn.execute(f"""INSERT INTO reaches VALUES
            (1,'NA',0,0,{line1},5000,100,0,'R1',0,0)""")
        conn.execute(f"""INSERT INTO reaches VALUES
            (2,'NA',2,2,{line2},5000,100,0,'R2',0,0)""")

        from src.sword_duckdb.lint.checks.geometry import (
            check_duplicate_geometry,
        )

        result = check_duplicate_geometry(conn)
        assert result.passed is True
        conn.close()

    def test_fail_duplicates(self, tmp_path):
        conn = _spatial_conn(tmp_path)
        line = "ST_GeomFromText('LINESTRING(0 0, 1 1)')"
        conn.execute("""
            CREATE TABLE reaches (
                reach_id BIGINT, region VARCHAR, x DOUBLE, y DOUBLE,
                geom GEOMETRY, reach_length DOUBLE, width DOUBLE,
                lakeflag INTEGER, river_name VARCHAR,
                n_rch_up INTEGER DEFAULT 0, n_rch_down INTEGER DEFAULT 0
            )
        """)
        conn.execute(f"""INSERT INTO reaches VALUES
            (1,'NA',0,0,{line},5000,100,0,'R1',0,0)""")
        conn.execute(f"""INSERT INTO reaches VALUES
            (2,'NA',0,0,{line},5000,100,0,'R2',0,0)""")

        from src.sword_duckdb.lint.checks.geometry import (
            check_duplicate_geometry,
        )

        result = check_duplicate_geometry(conn)
        assert result.passed is False
        assert result.issues_found >= 1
        conn.close()


class TestG015NodeReachDistance:
    """Tests for G015 node_reach_distance."""

    def test_pass_close_node(self, tmp_path):
        conn = _spatial_conn(tmp_path)
        conn.execute("""
            CREATE TABLE reaches (
                reach_id BIGINT, region VARCHAR, x DOUBLE, y DOUBLE,
                geom GEOMETRY, reach_length DOUBLE, width DOUBLE,
                lakeflag INTEGER, n_rch_up INTEGER DEFAULT 0,
                n_rch_down INTEGER DEFAULT 0
            )
        """)
        conn.execute("""
            CREATE TABLE nodes (
                node_id BIGINT, region VARCHAR, x DOUBLE, y DOUBLE,
                geom GEOMETRY, reach_id BIGINT, node_length DOUBLE
            )
        """)
        # Reach is a line from (0,0) to (0.01,0.01), node is on the line
        conn.execute("""INSERT INTO reaches VALUES
            (1,'NA',0,0,ST_GeomFromText('LINESTRING(0 0, 0.01 0.01)'),
             5000,100,0,0,0)""")
        conn.execute("""INSERT INTO nodes VALUES
            (101,'NA',0.005,0.005,ST_Point(0.005,0.005),1,1000)""")

        from src.sword_duckdb.lint.checks.geometry import (
            check_node_reach_distance,
        )

        result = check_node_reach_distance(conn)
        assert result.passed is True
        conn.close()

    def test_fail_far_node(self, tmp_path):
        conn = _spatial_conn(tmp_path)
        conn.execute("""
            CREATE TABLE reaches (
                reach_id BIGINT, region VARCHAR, x DOUBLE, y DOUBLE,
                geom GEOMETRY, reach_length DOUBLE, width DOUBLE,
                lakeflag INTEGER, n_rch_up INTEGER DEFAULT 0,
                n_rch_down INTEGER DEFAULT 0
            )
        """)
        conn.execute("""
            CREATE TABLE nodes (
                node_id BIGINT, region VARCHAR, x DOUBLE, y DOUBLE,
                geom GEOMETRY, reach_id BIGINT, node_length DOUBLE
            )
        """)
        # Node is ~200km away from its reach
        conn.execute("""INSERT INTO reaches VALUES
            (1,'NA',0,0,ST_GeomFromText('LINESTRING(0 0, 0.01 0.01)'),
             5000,100,0,0,0)""")
        conn.execute("""INSERT INTO nodes VALUES
            (101,'NA',2.0,2.0,ST_Point(2.0,2.0),1,1000)""")

        from src.sword_duckdb.lint.checks.geometry import (
            check_node_reach_distance,
        )

        result = check_node_reach_distance(conn)
        assert result.passed is False
        assert result.issues_found == 1
        conn.close()


class TestG016NodeSpacing:
    """Tests for G016 node_spacing."""

    def test_pass_uniform(self, tmp_path):
        conn = _spatial_conn(tmp_path)
        _create_reaches_table(conn, [{"reach_id": 1}])
        _create_nodes_table(
            conn,
            [
                {"node_id": 1, "reach_id": 1, "x": 0, "y": 0, "node_length": 1000},
                {"node_id": 2, "reach_id": 1, "x": 0.01, "y": 0, "node_length": 1000},
                {"node_id": 3, "reach_id": 1, "x": 0.02, "y": 0, "node_length": 1000},
                {"node_id": 4, "reach_id": 1, "x": 0.03, "y": 0, "node_length": 1100},
            ],
        )
        from src.sword_duckdb.lint.checks.geometry import check_node_spacing

        result = check_node_spacing(conn)
        assert result.passed is True
        conn.close()

    def test_fail_outlier(self, tmp_path):
        conn = _spatial_conn(tmp_path)
        _create_reaches_table(conn, [{"reach_id": 1}])
        _create_nodes_table(
            conn,
            [
                {"node_id": 1, "reach_id": 1, "x": 0, "y": 0, "node_length": 5000},
                {"node_id": 2, "reach_id": 1, "x": 0.01, "y": 0, "node_length": 1000},
                {"node_id": 3, "reach_id": 1, "x": 0.02, "y": 0, "node_length": 1000},
                {"node_id": 4, "reach_id": 1, "x": 0.03, "y": 0, "node_length": 1000},
            ],
        )
        from src.sword_duckdb.lint.checks.geometry import check_node_spacing

        result = check_node_spacing(conn)
        assert result.passed is False
        assert result.issues_found >= 1
        conn.close()


class TestG017CrossReachNodes:
    """Tests for G017 cross_reach_nodes."""

    def test_pass_node_close_to_own_reach(self, tmp_path):
        conn = _spatial_conn(tmp_path)
        conn.execute("""
            CREATE TABLE reaches (
                reach_id BIGINT, region VARCHAR, x DOUBLE, y DOUBLE,
                geom GEOMETRY, reach_length DOUBLE, width DOUBLE,
                lakeflag INTEGER, n_rch_up INTEGER DEFAULT 0,
                n_rch_down INTEGER DEFAULT 0
            )
        """)
        conn.execute("""INSERT INTO reaches VALUES
            (1,'NA',0.005,0,ST_GeomFromText('LINESTRING(0 0, 0.01 0)'),
             5000,100,0,0,0)""")
        conn.execute("""INSERT INTO reaches VALUES
            (2,'NA',1.005,0,ST_GeomFromText('LINESTRING(1 0, 1.01 0)'),
             5000,100,0,0,0)""")
        conn.execute("""
            CREATE TABLE nodes (
                node_id BIGINT, reach_id BIGINT, region VARCHAR,
                x DOUBLE, y DOUBLE, node_length DOUBLE, geom GEOMETRY
            )
        """)
        # Node on top of its own reach — no issue
        conn.execute("""INSERT INTO nodes VALUES
            (1,1,'NA',0.005,0,200,ST_Point(0.005, 0))""")

        from src.sword_duckdb.lint.checks.geometry import (
            check_cross_reach_nodes,
        )

        result = check_cross_reach_nodes(conn)
        assert result.passed is True
        conn.close()

    def test_fail_node_closer_to_other_reach(self, tmp_path):
        conn = _spatial_conn(tmp_path)
        # Reach 1 at x=0, Reach 2 at x=0.001 (very close).
        # Node belongs to reach 1 but is placed at x=0.0009 — closer to reach 2.
        conn.execute("""
            CREATE TABLE reaches (
                reach_id BIGINT, region VARCHAR, x DOUBLE, y DOUBLE,
                geom GEOMETRY, reach_length DOUBLE, width DOUBLE,
                lakeflag INTEGER, n_rch_up INTEGER DEFAULT 0,
                n_rch_down INTEGER DEFAULT 0
            )
        """)
        conn.execute("""INSERT INTO reaches VALUES
            (1,'NA',0,0,ST_GeomFromText('LINESTRING(0 0, 0 0.01)'),
             5000,100,0,0,0)""")
        conn.execute("""INSERT INTO reaches VALUES
            (2,'NA',0.001,0,ST_GeomFromText('LINESTRING(0.001 0, 0.001 0.01)'),
             5000,100,0,0,0)""")
        conn.execute("""
            CREATE TABLE nodes (
                node_id BIGINT, reach_id BIGINT, region VARCHAR,
                x DOUBLE, y DOUBLE, node_length DOUBLE, geom GEOMETRY
            )
        """)
        # Node at x=0.0009 — own reach is at x=0 (dist ~0.0009°≈100m),
        # alt reach is at x=0.001 (dist ~0.0001°≈11m).
        conn.execute("""INSERT INTO nodes VALUES
            (1,1,'NA',0.0009,0.005,200,ST_Point(0.0009, 0.005))""")

        from src.sword_duckdb.lint.checks.geometry import (
            check_cross_reach_nodes,
        )

        # threshold=50m → 0.0009° × 111000 ≈ 100m > 50m, so node is "far"
        result = check_cross_reach_nodes(conn, threshold=50.0)
        assert result.passed is False
        assert result.issues_found >= 1
        conn.close()


class TestG018DistOutVsReachLength:
    """Tests for G018 dist_out_vs_reach_length."""

    def test_pass_consistent(self, tmp_path):
        conn = _spatial_conn(tmp_path)
        # r1.dist_out=10000, r2.dist_out=5000, r1.reach_length=5000 → diff=0
        _create_reaches_table(
            conn,
            [
                {"reach_id": 1, "dist_out": 10000.0, "reach_length": 5000.0},
                {"reach_id": 2, "dist_out": 5000.0, "reach_length": 5000.0},
            ],
        )
        _create_topology_table(
            conn,
            [
                {"reach_id": 1, "direction": "down", "neighbor_reach_id": 2},
            ],
        )
        from src.sword_duckdb.lint.checks.geometry import (
            check_dist_out_vs_reach_length,
        )

        result = check_dist_out_vs_reach_length(conn)
        assert result.passed is True
        conn.close()

    def test_fail_mismatch(self, tmp_path):
        conn = _spatial_conn(tmp_path)
        # r1.dist_out=50000, r2.dist_out=5000, r1.reach_length=5000
        # diff = |50000 - 5000 - 5000| / 5000 = 8.0 >> 0.2
        _create_reaches_table(
            conn,
            [
                {"reach_id": 1, "dist_out": 50000.0, "reach_length": 5000.0},
                {"reach_id": 2, "dist_out": 5000.0, "reach_length": 5000.0},
            ],
        )
        _create_topology_table(
            conn,
            [
                {"reach_id": 1, "direction": "down", "neighbor_reach_id": 2},
            ],
        )
        from src.sword_duckdb.lint.checks.geometry import (
            check_dist_out_vs_reach_length,
        )

        result = check_dist_out_vs_reach_length(conn)
        assert result.passed is False
        assert result.issues_found == 1
        conn.close()


class TestG012ConfluenceAndBifurcation:
    """Tests for G012 endpoint_alignment at confluences and bifurcations.

    G019 (confluence_geometry) and G020 (bifurcation_geometry) were removed
    as strict subsets of G012. These tests verify G012 catches the same
    scenarios.
    """

    def test_confluence_close_endpoints_pass(self, tmp_path):
        """G012 passes when confluence upstream endpoints are near downstream."""
        conn = _spatial_conn(tmp_path)
        conn.execute("""
            CREATE TABLE reaches (
                reach_id BIGINT, region VARCHAR, x DOUBLE, y DOUBLE,
                geom GEOMETRY, reach_length DOUBLE, width DOUBLE,
                lakeflag INTEGER, n_rch_up INTEGER, n_rch_down INTEGER
            )
        """)
        conn.execute("""INSERT INTO reaches VALUES
            (1,'NA',0,0,ST_GeomFromText('LINESTRING(0 0, 0.01 0)'),5000,100,0,0,1)""")
        conn.execute("""INSERT INTO reaches VALUES
            (2,'NA',0,0.01,ST_GeomFromText('LINESTRING(0 0.01, 0.01 0)'),5000,100,0,0,1)""")
        conn.execute("""INSERT INTO reaches VALUES
            (3,'NA',0.01,0,ST_GeomFromText('LINESTRING(0.01 0, 0.02 0)'),5000,100,0,2,0)""")
        # Topology: 1→3 (down), 2→3 (down)
        _create_topology_table(
            conn,
            [
                {"reach_id": 1, "direction": "down", "neighbor_reach_id": 3},
                {"reach_id": 2, "direction": "down", "neighbor_reach_id": 3},
            ],
        )

        from src.sword_duckdb.lint.checks.geometry import check_endpoint_alignment

        result = check_endpoint_alignment(conn)
        assert result.passed is True
        conn.close()

    def test_confluence_far_endpoints_fail(self, tmp_path):
        """G012 flags when a confluence upstream reach is far from downstream."""
        conn = _spatial_conn(tmp_path)
        conn.execute("""
            CREATE TABLE reaches (
                reach_id BIGINT, region VARCHAR, x DOUBLE, y DOUBLE,
                geom GEOMETRY, reach_length DOUBLE, width DOUBLE,
                lakeflag INTEGER, n_rch_up INTEGER, n_rch_down INTEGER
            )
        """)
        conn.execute("""INSERT INTO reaches VALUES
            (1,'NA',10,10,ST_GeomFromText('LINESTRING(10 10, 11 11)'),5000,100,0,0,1)""")
        conn.execute("""INSERT INTO reaches VALUES
            (2,'NA',0,0.01,ST_GeomFromText('LINESTRING(0 0.01, 0.01 0)'),5000,100,0,0,1)""")
        conn.execute("""INSERT INTO reaches VALUES
            (3,'NA',0.01,0,ST_GeomFromText('LINESTRING(0.01 0, 0.02 0)'),5000,100,0,2,0)""")
        _create_topology_table(
            conn,
            [
                {"reach_id": 1, "direction": "down", "neighbor_reach_id": 3},
                {"reach_id": 2, "direction": "down", "neighbor_reach_id": 3},
            ],
        )

        from src.sword_duckdb.lint.checks.geometry import check_endpoint_alignment

        result = check_endpoint_alignment(conn)
        assert result.passed is False
        assert result.issues_found >= 1
        conn.close()

    def test_bifurcation_close_endpoints_pass(self, tmp_path):
        """G012 passes when bifurcation downstream endpoints are near upstream."""
        conn = _spatial_conn(tmp_path)
        conn.execute("""
            CREATE TABLE reaches (
                reach_id BIGINT, region VARCHAR, x DOUBLE, y DOUBLE,
                geom GEOMETRY, reach_length DOUBLE, width DOUBLE,
                lakeflag INTEGER, n_rch_up INTEGER, n_rch_down INTEGER
            )
        """)
        conn.execute("""INSERT INTO reaches VALUES
            (1,'NA',0,0,ST_GeomFromText('LINESTRING(0 0, 0.01 0)'),5000,100,0,0,2)""")
        conn.execute("""INSERT INTO reaches VALUES
            (2,'NA',0.01,0,ST_GeomFromText('LINESTRING(0.01 0, 0.02 0)'),5000,100,0,1,0)""")
        conn.execute("""INSERT INTO reaches VALUES
            (3,'NA',0.01,0,ST_GeomFromText('LINESTRING(0.01 0, 0.02 0.01)'),5000,100,0,1,0)""")
        _create_topology_table(
            conn,
            [
                {"reach_id": 1, "direction": "down", "neighbor_reach_id": 2},
                {
                    "reach_id": 1,
                    "direction": "down",
                    "neighbor_rank": 1,
                    "neighbor_reach_id": 3,
                },
            ],
        )

        from src.sword_duckdb.lint.checks.geometry import check_endpoint_alignment

        result = check_endpoint_alignment(conn)
        assert result.passed is True
        conn.close()

    def test_bifurcation_far_endpoints_fail(self, tmp_path):
        """G012 flags when a bifurcation downstream reach is far from upstream."""
        conn = _spatial_conn(tmp_path)
        conn.execute("""
            CREATE TABLE reaches (
                reach_id BIGINT, region VARCHAR, x DOUBLE, y DOUBLE,
                geom GEOMETRY, reach_length DOUBLE, width DOUBLE,
                lakeflag INTEGER, n_rch_up INTEGER, n_rch_down INTEGER
            )
        """)
        conn.execute("""INSERT INTO reaches VALUES
            (1,'NA',0,0,ST_GeomFromText('LINESTRING(0 0, 0.01 0)'),5000,100,0,0,2)""")
        conn.execute("""INSERT INTO reaches VALUES
            (2,'NA',10,10,ST_GeomFromText('LINESTRING(10 10, 11 11)'),5000,100,0,1,0)""")
        conn.execute("""INSERT INTO reaches VALUES
            (3,'NA',0.01,0,ST_GeomFromText('LINESTRING(0.01 0, 0.02 0.01)'),5000,100,0,1,0)""")
        _create_topology_table(
            conn,
            [
                {"reach_id": 1, "direction": "down", "neighbor_reach_id": 2},
                {
                    "reach_id": 1,
                    "direction": "down",
                    "neighbor_rank": 1,
                    "neighbor_reach_id": 3,
                },
            ],
        )

        from src.sword_duckdb.lint.checks.geometry import check_endpoint_alignment

        result = check_endpoint_alignment(conn)
        assert result.passed is False
        assert result.issues_found >= 1
        conn.close()


class TestG021ReachOverlap:
    """Tests for G021 reach_overlap."""

    def test_pass_no_overlap(self, tmp_path):
        conn = _spatial_conn(tmp_path)
        conn.execute("""
            CREATE TABLE reaches (
                reach_id BIGINT, region VARCHAR, x DOUBLE, y DOUBLE,
                geom GEOMETRY, reach_length DOUBLE, width DOUBLE,
                lakeflag INTEGER, n_rch_up INTEGER DEFAULT 0,
                n_rch_down INTEGER DEFAULT 0
            )
        """)
        conn.execute("""
            CREATE TABLE reach_topology (
                reach_id BIGINT, region VARCHAR, direction VARCHAR,
                neighbor_rank INTEGER, neighbor_reach_id BIGINT
            )
        """)
        # Non-overlapping reaches
        conn.execute("""INSERT INTO reaches VALUES
            (1,'NA',0,0,ST_GeomFromText('LINESTRING(0 0, 1 0)'),5000,100,0,0,0)""")
        conn.execute("""INSERT INTO reaches VALUES
            (2,'NA',5,5,ST_GeomFromText('LINESTRING(5 5, 6 5)'),5000,100,0,0,0)""")

        from src.sword_duckdb.lint.checks.geometry import check_reach_overlap

        result = check_reach_overlap(conn)
        assert result.passed is True
        conn.close()

    def test_fail_overlap_no_connection(self, tmp_path):
        conn = _spatial_conn(tmp_path)
        conn.execute("""
            CREATE TABLE reaches (
                reach_id BIGINT, region VARCHAR, x DOUBLE, y DOUBLE,
                geom GEOMETRY, reach_length DOUBLE, width DOUBLE,
                lakeflag INTEGER, n_rch_up INTEGER DEFAULT 0,
                n_rch_down INTEGER DEFAULT 0
            )
        """)
        conn.execute("""
            CREATE TABLE reach_topology (
                reach_id BIGINT, region VARCHAR, direction VARCHAR,
                neighbor_rank INTEGER, neighbor_reach_id BIGINT
            )
        """)
        # Overlapping reaches with no topology connection
        conn.execute("""INSERT INTO reaches VALUES
            (1,'NA',0,0,ST_GeomFromText('LINESTRING(0 0, 1 1)'),5000,100,0,0,0)""")
        conn.execute("""INSERT INTO reaches VALUES
            (2,'NA',0,0,ST_GeomFromText('LINESTRING(0.5 0, 0.5 1)'),5000,100,0,0,0)""")

        from src.sword_duckdb.lint.checks.geometry import check_reach_overlap

        result = check_reach_overlap(conn)
        assert result.passed is False
        assert result.issues_found >= 1
        conn.close()

    def test_pass_touching_not_crossing(self, tmp_path):
        """Endpoint touches (ST_Touches) should NOT be flagged."""
        conn = _spatial_conn(tmp_path)
        conn.execute("""
            CREATE TABLE reaches (
                reach_id BIGINT, region VARCHAR, x DOUBLE, y DOUBLE,
                geom GEOMETRY, reach_length DOUBLE, width DOUBLE,
                lakeflag INTEGER, n_rch_up INTEGER DEFAULT 0,
                n_rch_down INTEGER DEFAULT 0
            )
        """)
        conn.execute("""
            CREATE TABLE reach_topology (
                reach_id BIGINT, region VARCHAR, direction VARCHAR,
                neighbor_rank INTEGER, neighbor_reach_id BIGINT
            )
        """)
        # Two reaches that share an endpoint but don't cross
        conn.execute("""INSERT INTO reaches VALUES
            (1,'NA',0,0,ST_GeomFromText('LINESTRING(0 0, 1 0)'),5000,100,0,0,0)""")
        conn.execute("""INSERT INTO reaches VALUES
            (2,'NA',0,0,ST_GeomFromText('LINESTRING(1 0, 2 0)'),5000,100,0,0,0)""")

        from src.sword_duckdb.lint.checks.geometry import check_reach_overlap

        result = check_reach_overlap(conn)
        assert result.passed is True
        conn.close()

    def test_pass_crossing_within_two_hops(self, tmp_path):
        """Crossing reaches within 2 topology hops should NOT be flagged."""
        conn = _spatial_conn(tmp_path)
        conn.execute("""
            CREATE TABLE reaches (
                reach_id BIGINT, region VARCHAR, x DOUBLE, y DOUBLE,
                geom GEOMETRY, reach_length DOUBLE, width DOUBLE,
                lakeflag INTEGER, n_rch_up INTEGER DEFAULT 0,
                n_rch_down INTEGER DEFAULT 0
            )
        """)
        conn.execute("""
            CREATE TABLE reach_topology (
                reach_id BIGINT, region VARCHAR, direction VARCHAR,
                neighbor_rank INTEGER, neighbor_reach_id BIGINT
            )
        """)
        # Two crossing reaches connected through intermediate reach 3
        conn.execute("""INSERT INTO reaches VALUES
            (1,'NA',0,0,ST_GeomFromText('LINESTRING(0 0, 1 1)'),5000,100,0,0,0)""")
        conn.execute("""INSERT INTO reaches VALUES
            (2,'NA',0,0,ST_GeomFromText('LINESTRING(0.5 0, 0.5 1)'),5000,100,0,0,0)""")
        conn.execute("""INSERT INTO reaches VALUES
            (3,'NA',0,0,ST_GeomFromText('LINESTRING(1 1, 2 2)'),5000,100,0,0,0)""")
        # Topology: 1 → 3 → 2 (reach 3 bridges 1 and 2)
        conn.execute("""INSERT INTO reach_topology VALUES
            (1,'NA','down',0,3)""")
        conn.execute("""INSERT INTO reach_topology VALUES
            (3,'NA','up',0,1)""")
        conn.execute("""INSERT INTO reach_topology VALUES
            (3,'NA','down',0,2)""")
        conn.execute("""INSERT INTO reach_topology VALUES
            (2,'NA','up',0,3)""")

        from src.sword_duckdb.lint.checks.geometry import check_reach_overlap

        result = check_reach_overlap(conn)
        assert result.passed is True
        conn.close()


# =============================================================================
# T013-T020 Topology Check Tests
# =============================================================================


class TestT013SelfReferential:
    """Tests for T013 self_referential_topology."""

    def test_pass_no_self_ref(self, tmp_path):
        conn = _spatial_conn(tmp_path)
        _create_reaches_table(conn, [{"reach_id": 1}, {"reach_id": 2}])
        _create_topology_table(
            conn,
            [{"reach_id": 1, "direction": "down", "neighbor_reach_id": 2}],
        )
        from src.sword_duckdb.lint.checks.topology import (
            check_self_referential_topology,
        )

        result = check_self_referential_topology(conn)
        assert result.passed is True
        conn.close()

    def test_fail_self_ref(self, tmp_path):
        conn = _spatial_conn(tmp_path)
        _create_reaches_table(conn, [{"reach_id": 1}])
        _create_topology_table(
            conn,
            [{"reach_id": 1, "direction": "down", "neighbor_reach_id": 1}],
        )
        from src.sword_duckdb.lint.checks.topology import (
            check_self_referential_topology,
        )

        result = check_self_referential_topology(conn)
        assert result.passed is False
        assert result.issues_found == 1
        conn.close()


class TestT014Bidirectional:
    """Tests for T014 bidirectional_topology."""

    def test_pass_normal(self, tmp_path):
        conn = _spatial_conn(tmp_path)
        _create_reaches_table(conn, [{"reach_id": 1}, {"reach_id": 2}])
        _create_topology_table(
            conn,
            [
                {"reach_id": 1, "direction": "down", "neighbor_reach_id": 2},
                {"reach_id": 2, "direction": "up", "neighbor_reach_id": 1},
            ],
        )
        from src.sword_duckdb.lint.checks.topology import (
            check_bidirectional_topology,
        )

        result = check_bidirectional_topology(conn)
        assert result.passed is True
        conn.close()

    def test_fail_both_directions(self, tmp_path):
        conn = _spatial_conn(tmp_path)
        _create_reaches_table(conn, [{"reach_id": 1}, {"reach_id": 2}])
        _create_topology_table(
            conn,
            [
                {"reach_id": 1, "direction": "up", "neighbor_reach_id": 2},
                {"reach_id": 1, "direction": "down", "neighbor_reach_id": 2},
            ],
        )
        from src.sword_duckdb.lint.checks.topology import (
            check_bidirectional_topology,
        )

        result = check_bidirectional_topology(conn)
        assert result.passed is False
        assert result.issues_found >= 1
        conn.close()


class TestT015Shortcut:
    """Tests for T015 topology_shortcut."""

    def test_pass_no_shortcut(self, tmp_path):
        conn = _spatial_conn(tmp_path)
        _create_reaches_table(conn, [{"reach_id": 1}, {"reach_id": 2}, {"reach_id": 3}])
        _create_topology_table(
            conn,
            [
                {"reach_id": 1, "direction": "down", "neighbor_reach_id": 2},
                {"reach_id": 2, "direction": "down", "neighbor_reach_id": 3},
            ],
        )
        from src.sword_duckdb.lint.checks.topology import (
            check_topology_shortcut,
        )

        result = check_topology_shortcut(conn)
        assert result.passed is True
        conn.close()

    def test_fail_shortcut(self, tmp_path):
        conn = _spatial_conn(tmp_path)
        _create_reaches_table(conn, [{"reach_id": 1}, {"reach_id": 2}, {"reach_id": 3}])
        _create_topology_table(
            conn,
            [
                {"reach_id": 1, "direction": "down", "neighbor_reach_id": 2},
                {"reach_id": 2, "direction": "down", "neighbor_reach_id": 3},
                {
                    "reach_id": 1,
                    "direction": "down",
                    "neighbor_reach_id": 3,
                    "neighbor_rank": 1,
                },
            ],
        )
        from src.sword_duckdb.lint.checks.topology import (
            check_topology_shortcut,
        )

        result = check_topology_shortcut(conn)
        assert result.passed is False
        assert result.issues_found >= 1
        conn.close()


class TestT017DistOutJump:
    """Tests for T017 dist_out_jump."""

    def test_pass_small_jump(self, tmp_path):
        conn = _spatial_conn(tmp_path)
        _create_reaches_table(
            conn,
            [
                {"reach_id": 1, "dist_out": 10000.0},
                {"reach_id": 2, "dist_out": 5000.0},
            ],
        )
        _create_topology_table(
            conn,
            [{"reach_id": 1, "direction": "down", "neighbor_reach_id": 2}],
        )
        from src.sword_duckdb.lint.checks.topology import (
            check_dist_out_jump,
        )

        result = check_dist_out_jump(conn)
        assert result.passed is True
        conn.close()

    def test_fail_large_jump(self, tmp_path):
        conn = _spatial_conn(tmp_path)
        _create_reaches_table(
            conn,
            [
                {"reach_id": 1, "dist_out": 100000.0},
                {"reach_id": 2, "dist_out": 5000.0},
            ],
        )
        _create_topology_table(
            conn,
            [{"reach_id": 1, "direction": "down", "neighbor_reach_id": 2}],
        )
        from src.sword_duckdb.lint.checks.topology import (
            check_dist_out_jump,
        )

        result = check_dist_out_jump(conn)
        assert result.passed is False
        assert result.issues_found >= 1
        conn.close()


class TestT018IdFormat:
    """Tests for T018 id_format."""

    def test_pass_valid_ids(self, tmp_path):
        conn = _spatial_conn(tmp_path)
        _create_reaches_table(conn, [{"reach_id": 12345678901}])
        _create_nodes_table(
            conn,
            [
                {
                    "node_id": 12345678901001,
                    "reach_id": 12345678901,
                    "x": 0,
                    "y": 0,
                }
            ],
        )
        from src.sword_duckdb.lint.checks.topology import check_id_format

        result = check_id_format(conn)
        assert result.passed is True
        conn.close()

    def test_fail_bad_reach_id(self, tmp_path):
        conn = _spatial_conn(tmp_path)
        # 10-digit reach_id — wrong length
        _create_reaches_table(conn, [{"reach_id": 1234567890}])
        _create_nodes_table(
            conn,
            [
                {
                    "node_id": 12345678901001,
                    "reach_id": 1234567890,
                    "x": 0,
                    "y": 0,
                }
            ],
        )
        from src.sword_duckdb.lint.checks.topology import check_id_format

        result = check_id_format(conn)
        assert result.passed is False
        assert result.issues_found >= 1
        conn.close()


class TestT019NameNodata:
    """Tests for T019 river_name_nodata."""

    def test_reports_nodata(self, tmp_path):
        conn = _spatial_conn(tmp_path)
        _create_reaches_table(
            conn,
            [
                {"reach_id": 1, "river_name": "NODATA"},
                {"reach_id": 2, "river_name": "Rhine"},
            ],
        )
        from src.sword_duckdb.lint.checks.topology import (
            check_river_name_nodata,
        )

        result = check_river_name_nodata(conn)
        assert result.passed is True  # Informational
        assert result.issues_found == 1
        conn.close()

    def test_no_nodata(self, tmp_path):
        conn = _spatial_conn(tmp_path)
        _create_reaches_table(
            conn,
            [{"reach_id": 1, "river_name": "Rhine"}],
        )
        from src.sword_duckdb.lint.checks.topology import (
            check_river_name_nodata,
        )

        result = check_river_name_nodata(conn)
        assert result.issues_found == 0
        conn.close()


class TestT020NameConsensus:
    """Tests for T020 river_name_consensus."""

    def test_pass_same_name(self, tmp_path):
        conn = _spatial_conn(tmp_path)
        _create_reaches_table(
            conn,
            [
                {"reach_id": 1, "river_name": "Rhine"},
                {"reach_id": 2, "river_name": "Rhine"},
                {"reach_id": 3, "river_name": "Rhine"},
            ],
        )
        _create_topology_table(
            conn,
            [
                {"reach_id": 1, "direction": "down", "neighbor_reach_id": 2},
                {"reach_id": 2, "direction": "up", "neighbor_reach_id": 1},
                {"reach_id": 2, "direction": "down", "neighbor_reach_id": 3},
                {"reach_id": 3, "direction": "up", "neighbor_reach_id": 2},
            ],
        )
        from src.sword_duckdb.lint.checks.topology import (
            check_river_name_consensus,
        )

        result = check_river_name_consensus(conn)
        assert result.passed is True
        conn.close()

    def test_fail_disagrees(self, tmp_path):
        conn = _spatial_conn(tmp_path)
        _create_reaches_table(
            conn,
            [
                {"reach_id": 1, "river_name": "Rhine"},
                {"reach_id": 2, "river_name": "Danube"},
                {"reach_id": 3, "river_name": "Rhine"},
            ],
        )
        _create_topology_table(
            conn,
            [
                {"reach_id": 2, "direction": "up", "neighbor_reach_id": 1},
                {"reach_id": 2, "direction": "down", "neighbor_reach_id": 3},
            ],
        )
        from src.sword_duckdb.lint.checks.topology import (
            check_river_name_consensus,
        )

        result = check_river_name_consensus(conn)
        assert result.passed is False
        assert result.issues_found >= 1
        conn.close()


# =============================================================================
# A030 WSE Monotonicity Tests
# =============================================================================


def _create_reaches_with_wse(conn, rows):
    """Create reaches table with wse column."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS reaches (
            reach_id BIGINT, region VARCHAR, x DOUBLE, y DOUBLE,
            geom GEOMETRY, reach_length DOUBLE, width DOUBLE,
            lakeflag INTEGER, dist_out DOUBLE, river_name VARCHAR,
            n_rch_up INTEGER DEFAULT 0, n_rch_down INTEGER DEFAULT 0,
            wse DOUBLE DEFAULT -9999
        )
    """)
    for r in rows:
        conn.execute(
            """INSERT INTO reaches (reach_id, region, x, y, reach_length, width,
               lakeflag, dist_out, river_name, n_rch_up, n_rch_down, wse)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            [
                r.get("reach_id"),
                r.get("region", "NA"),
                r.get("x", 0.0),
                r.get("y", 0.0),
                r.get("reach_length", 5000.0),
                r.get("width", 100.0),
                r.get("lakeflag", 0),
                r.get("dist_out", 50000.0),
                r.get("river_name", "Test"),
                r.get("n_rch_up", 0),
                r.get("n_rch_down", 0),
                r.get("wse", -9999),
            ],
        )


class TestA030WseMonotonicity:
    """Tests for A030 wse_monotonicity."""

    def test_pass_decreasing(self, tmp_path):
        conn = _spatial_conn(tmp_path)
        _create_reaches_with_wse(
            conn,
            [
                {"reach_id": 1, "wse": 100.0},
                {"reach_id": 2, "wse": 50.0},
            ],
        )
        _create_topology_table(
            conn,
            [{"reach_id": 1, "direction": "down", "neighbor_reach_id": 2}],
        )
        from src.sword_duckdb.lint.checks.attributes import (
            check_wse_monotonicity,
        )

        result = check_wse_monotonicity(conn)
        assert result.passed is True
        conn.close()

    def test_fail_increasing(self, tmp_path):
        conn = _spatial_conn(tmp_path)
        _create_reaches_with_wse(
            conn,
            [
                {"reach_id": 1, "wse": 50.0},
                {"reach_id": 2, "wse": 100.0},
            ],
        )
        _create_topology_table(
            conn,
            [{"reach_id": 1, "direction": "down", "neighbor_reach_id": 2}],
        )
        from src.sword_duckdb.lint.checks.attributes import (
            check_wse_monotonicity,
        )

        result = check_wse_monotonicity(conn)
        assert result.passed is False
        assert result.issues_found == 1
        conn.close()

    def test_sentinel_skipped(self, tmp_path):
        conn = _spatial_conn(tmp_path)
        _create_reaches_with_wse(
            conn,
            [
                {"reach_id": 1, "wse": -9999},
                {"reach_id": 2, "wse": -9999},
            ],
        )
        _create_topology_table(
            conn,
            [{"reach_id": 1, "direction": "down", "neighbor_reach_id": 2}],
        )
        from src.sword_duckdb.lint.checks.attributes import (
            check_wse_monotonicity,
        )

        result = check_wse_monotonicity(conn)
        assert result.passed is True
        assert result.total_checked == 0
        conn.close()


# =============================================================================
# N003-N010 Node Check Tests
# =============================================================================


def _create_nodes_with_dist_out(conn, rows):
    """Create nodes table with dist_out column."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS nodes (
            node_id BIGINT, region VARCHAR, x DOUBLE, y DOUBLE,
            geom GEOMETRY, reach_id BIGINT, node_length DOUBLE,
            dist_out DOUBLE DEFAULT -9999,
            wse DOUBLE DEFAULT -9999
        )
    """)
    for n in rows:
        conn.execute(
            "INSERT INTO nodes VALUES (?,?,?,?,?,?,?,?,?)",
            [
                n["node_id"],
                n.get("region", "NA"),
                n["x"],
                n["y"],
                n.get("geom"),
                n["reach_id"],
                n.get("node_length", 200.0),
                n.get("dist_out", -9999),
                n.get("wse", -9999),
            ],
        )


class TestN003NodeSpacingGap:
    """Tests for N003 node_spacing_gap."""

    def test_pass_close_nodes(self, tmp_path):
        conn = _spatial_conn(tmp_path)
        _create_nodes_with_dist_out(
            conn,
            [
                {"node_id": 1001, "reach_id": 1, "x": 0.0, "y": 0.0},
                {"node_id": 1002, "reach_id": 1, "x": 0.001, "y": 0.0},
            ],
        )
        from src.sword_duckdb.lint.checks.node import check_node_spacing_gap

        result = check_node_spacing_gap(conn)
        assert result.passed is True
        conn.close()

    def test_fail_far_nodes(self, tmp_path):
        conn = _spatial_conn(tmp_path)
        _create_nodes_with_dist_out(
            conn,
            [
                {"node_id": 1001, "reach_id": 1, "x": 0.0, "y": 0.0},
                {"node_id": 1002, "reach_id": 1, "x": 1.0, "y": 0.0},
            ],
        )
        from src.sword_duckdb.lint.checks.node import check_node_spacing_gap

        result = check_node_spacing_gap(conn)
        assert result.passed is False
        assert result.issues_found >= 1
        conn.close()


class TestN004NodeDistOutMonotonicity:
    """Tests for N004 node_dist_out_monotonicity.

    SWORD convention: node_id increases upstream, so dist_out INCREASES with node_id.
    """

    def test_pass_increasing(self, tmp_path):
        """dist_out increases with node_id -- correct SWORD convention."""
        conn = _spatial_conn(tmp_path)
        _create_nodes_with_dist_out(
            conn,
            [
                {
                    "node_id": 1001,
                    "reach_id": 1,
                    "x": 0.0,
                    "y": 0.0,
                    "dist_out": 4600.0,
                },
                {
                    "node_id": 1002,
                    "reach_id": 1,
                    "x": 0.001,
                    "y": 0.0,
                    "dist_out": 4800.0,
                },
                {
                    "node_id": 1003,
                    "reach_id": 1,
                    "x": 0.002,
                    "y": 0.0,
                    "dist_out": 5000.0,
                },
            ],
        )
        from src.sword_duckdb.lint.checks.node import (
            check_node_dist_out_monotonicity,
        )

        result = check_node_dist_out_monotonicity(conn)
        assert result.passed is True
        conn.close()

    def test_fail_decreasing(self, tmp_path):
        """dist_out decreases with node_id -- violates SWORD convention."""
        conn = _spatial_conn(tmp_path)
        _create_nodes_with_dist_out(
            conn,
            [
                {
                    "node_id": 1001,
                    "reach_id": 1,
                    "x": 0.0,
                    "y": 0.0,
                    "dist_out": 5500.0,
                },
                {
                    "node_id": 1002,
                    "reach_id": 1,
                    "x": 0.001,
                    "y": 0.0,
                    "dist_out": 5000.0,
                },
            ],
        )
        from src.sword_duckdb.lint.checks.node import (
            check_node_dist_out_monotonicity,
        )

        result = check_node_dist_out_monotonicity(conn)
        assert result.passed is False
        assert result.issues_found >= 1
        conn.close()


class TestN005NodeDistOutJump:
    """Tests for N005 node_dist_out_jump.

    SWORD convention: node_id increases upstream, so dist_out increases with node_id.
    """

    def test_pass_small_jump(self, tmp_path):
        """Small dist_out step (200m) -- below 600m threshold."""
        conn = _spatial_conn(tmp_path)
        _create_nodes_with_dist_out(
            conn,
            [
                {
                    "node_id": 1001,
                    "reach_id": 1,
                    "x": 0.0,
                    "y": 0.0,
                    "dist_out": 4800.0,
                },
                {
                    "node_id": 1002,
                    "reach_id": 1,
                    "x": 0.001,
                    "y": 0.0,
                    "dist_out": 5000.0,
                },
            ],
        )
        from src.sword_duckdb.lint.checks.node import check_node_dist_out_jump

        result = check_node_dist_out_jump(conn)
        assert result.passed is True
        conn.close()

    def test_fail_large_jump(self, tmp_path):
        """Large dist_out step (2000m) -- above 600m threshold."""
        conn = _spatial_conn(tmp_path)
        _create_nodes_with_dist_out(
            conn,
            [
                {
                    "node_id": 1001,
                    "reach_id": 1,
                    "x": 0.0,
                    "y": 0.0,
                    "dist_out": 3000.0,
                },
                {
                    "node_id": 1002,
                    "reach_id": 1,
                    "x": 0.001,
                    "y": 0.0,
                    "dist_out": 5000.0,
                },
            ],
        )
        from src.sword_duckdb.lint.checks.node import check_node_dist_out_jump

        result = check_node_dist_out_jump(conn)
        assert result.passed is False
        assert result.issues_found >= 1
        conn.close()


class TestN006BoundaryDistOut:
    """Tests for N006 boundary_dist_out.

    SWORD convention: node_id increases upstream (higher node_id = higher dist_out).
    For upstream reach A -> downstream reach B:
      A's downstream boundary = MIN(node_id) in A
      B's upstream boundary   = MAX(node_id) in B
    Gap = |A_min_dist_out - B_max_dist_out|.
    """

    def test_pass_close_dist_out(self, tmp_path):
        """Boundary gap = |5000 - 4900| = 100m < 1000m threshold. PASS."""
        conn = _spatial_conn(tmp_path)
        _create_reaches_table(conn, [{"reach_id": 1}, {"reach_id": 2}])
        _create_topology_table(
            conn,
            [{"reach_id": 1, "direction": "down", "neighbor_reach_id": 2}],
        )
        _create_nodes_with_dist_out(
            conn,
            [
                {
                    "node_id": 1001,
                    "reach_id": 1,
                    "x": 0.0,
                    "y": 0.0,
                    "dist_out": 5000.0,
                },
                {
                    "node_id": 1002,
                    "reach_id": 1,
                    "x": 0.001,
                    "y": 0.0,
                    "dist_out": 5500.0,
                },
                {
                    "node_id": 2001,
                    "reach_id": 2,
                    "x": 0.002,
                    "y": 0.0,
                    "dist_out": 4500.0,
                },
                {
                    "node_id": 2002,
                    "reach_id": 2,
                    "x": 0.003,
                    "y": 0.0,
                    "dist_out": 4900.0,
                },
            ],
        )
        from src.sword_duckdb.lint.checks.node import check_boundary_dist_out

        result = check_boundary_dist_out(conn)
        assert result.passed is True
        conn.close()

    def test_fail_large_gap(self, tmp_path):
        """Boundary gap = |9500 - 5000| = 4500m > 1000m threshold. FAIL."""
        conn = _spatial_conn(tmp_path)
        _create_reaches_table(conn, [{"reach_id": 1}, {"reach_id": 2}])
        _create_topology_table(
            conn,
            [{"reach_id": 1, "direction": "down", "neighbor_reach_id": 2}],
        )
        _create_nodes_with_dist_out(
            conn,
            [
                {
                    "node_id": 1001,
                    "reach_id": 1,
                    "x": 0.0,
                    "y": 0.0,
                    "dist_out": 9500.0,
                },
                {
                    "node_id": 1002,
                    "reach_id": 1,
                    "x": 0.001,
                    "y": 0.0,
                    "dist_out": 10000.0,
                },
                {
                    "node_id": 2001,
                    "reach_id": 2,
                    "x": 0.002,
                    "y": 0.0,
                    "dist_out": 4500.0,
                },
                {
                    "node_id": 2002,
                    "reach_id": 2,
                    "x": 0.003,
                    "y": 0.0,
                    "dist_out": 5000.0,
                },
            ],
        )
        from src.sword_duckdb.lint.checks.node import check_boundary_dist_out

        result = check_boundary_dist_out(conn)
        assert result.passed is False
        assert result.issues_found >= 1
        conn.close()


class TestN008NodeCountVsNNodes:
    """Tests for N008 node_count_vs_n_nodes."""

    def test_pass_matching_count(self, tmp_path):
        conn = _spatial_conn(tmp_path)
        conn.execute("""
            CREATE TABLE reaches (
                reach_id BIGINT, region VARCHAR, x DOUBLE, y DOUBLE,
                geom GEOMETRY, reach_length DOUBLE, width DOUBLE,
                lakeflag INTEGER, dist_out DOUBLE, river_name VARCHAR,
                n_rch_up INTEGER DEFAULT 0, n_rch_down INTEGER DEFAULT 0,
                n_nodes INTEGER DEFAULT 3
            )
        """)
        conn.execute(
            "INSERT INTO reaches (reach_id, region, river_name, n_nodes) "
            "VALUES (1, 'NA', 'Test', 3)"
        )
        _create_nodes_table(
            conn,
            [
                {"node_id": 1001, "reach_id": 1, "x": 0.0, "y": 0.0},
                {"node_id": 1002, "reach_id": 1, "x": 0.001, "y": 0.0},
                {"node_id": 1003, "reach_id": 1, "x": 0.002, "y": 0.0},
            ],
        )
        from src.sword_duckdb.lint.checks.node import (
            check_node_count_vs_n_nodes,
        )

        result = check_node_count_vs_n_nodes(conn)
        assert result.passed is True
        conn.close()

    def test_fail_mismatch(self, tmp_path):
        conn = _spatial_conn(tmp_path)
        conn.execute("""
            CREATE TABLE reaches (
                reach_id BIGINT, region VARCHAR, x DOUBLE, y DOUBLE,
                geom GEOMETRY, reach_length DOUBLE, width DOUBLE,
                lakeflag INTEGER, dist_out DOUBLE, river_name VARCHAR,
                n_rch_up INTEGER DEFAULT 0, n_rch_down INTEGER DEFAULT 0,
                n_nodes INTEGER DEFAULT 5
            )
        """)
        conn.execute(
            "INSERT INTO reaches (reach_id, region, river_name, n_nodes) "
            "VALUES (1, 'NA', 'Test', 5)"
        )
        _create_nodes_table(
            conn,
            [
                {"node_id": 1001, "reach_id": 1, "x": 0.0, "y": 0.0},
                {"node_id": 1002, "reach_id": 1, "x": 0.001, "y": 0.0},
            ],
        )
        from src.sword_duckdb.lint.checks.node import (
            check_node_count_vs_n_nodes,
        )

        result = check_node_count_vs_n_nodes(conn)
        assert result.passed is False
        assert result.issues_found >= 1
        conn.close()


class TestN010NodeIndexContiguity:
    """Tests for N010 node_index_contiguity.

    SWORD uses step-10 node suffixes: 001, 011, 021, ..., 991.
    Expected count = (max_suffix - min_suffix) / 10 + 1.
    """

    def test_pass_contiguous(self, tmp_path):
        """Suffixes 001, 011, 021 -- step-10 contiguous."""
        conn = _spatial_conn(tmp_path)
        _create_nodes_with_dist_out(
            conn,
            [
                {"node_id": 10001001, "reach_id": 1, "x": 0.0, "y": 0.0},
                {"node_id": 10001011, "reach_id": 1, "x": 0.001, "y": 0.0},
                {"node_id": 10001021, "reach_id": 1, "x": 0.002, "y": 0.0},
            ],
        )
        from src.sword_duckdb.lint.checks.node import (
            check_node_index_contiguity,
        )

        result = check_node_index_contiguity(conn)
        assert result.passed is True
        conn.close()

    def test_fail_gap(self, tmp_path):
        """Suffixes 001, 031 -- missing 011, 021 (gap of 2)."""
        conn = _spatial_conn(tmp_path)
        _create_nodes_with_dist_out(
            conn,
            [
                {"node_id": 10001001, "reach_id": 1, "x": 0.0, "y": 0.0},
                {"node_id": 10001031, "reach_id": 1, "x": 0.002, "y": 0.0},
            ],
        )
        from src.sword_duckdb.lint.checks.node import (
            check_node_index_contiguity,
        )

        result = check_node_index_contiguity(conn)
        assert result.passed is False
        assert result.issues_found >= 1
        conn.close()


# =============================================================================
# C005 Centerline Distance Tests
# =============================================================================


def _create_centerlines_table(conn, rows):
    """Create minimal centerlines table."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS centerlines (
            cl_id BIGINT, region VARCHAR, x DOUBLE, y DOUBLE,
            reach_id BIGINT
        )
    """)
    for cl in rows:
        conn.execute(
            "INSERT INTO centerlines VALUES (?,?,?,?,?)",
            [
                cl["cl_id"],
                cl.get("region", "NA"),
                cl["x"],
                cl["y"],
                cl["reach_id"],
            ],
        )


class TestC005CenterlineReachDistance:
    """Tests for C005 centerline_reach_distance."""

    def test_pass_close(self, tmp_path):
        conn = _spatial_conn(tmp_path)
        _create_reaches_table(conn, [{"reach_id": 1, "x": 0.0, "y": 0.0}])
        _create_centerlines_table(
            conn,
            [
                {"cl_id": 101, "reach_id": 1, "x": 0.001, "y": 0.001},
                {"cl_id": 102, "reach_id": 1, "x": -0.001, "y": -0.001},
            ],
        )
        from src.sword_duckdb.lint.checks.classification import (
            check_centerline_reach_distance,
        )

        result = check_centerline_reach_distance(conn)
        assert result.passed is True
        conn.close()

    def test_fail_far(self, tmp_path):
        conn = _spatial_conn(tmp_path)
        _create_reaches_table(conn, [{"reach_id": 1, "x": 0.0, "y": 0.0}])
        _create_centerlines_table(
            conn,
            [
                {"cl_id": 101, "reach_id": 1, "x": 5.0, "y": 5.0},
                {"cl_id": 102, "reach_id": 1, "x": 6.0, "y": 6.0},
            ],
        )
        from src.sword_duckdb.lint.checks.classification import (
            check_centerline_reach_distance,
        )

        result = check_centerline_reach_distance(conn)
        assert result.passed is False
        assert result.issues_found >= 1
        conn.close()

    def test_skip_no_centerlines(self, tmp_path):
        conn = _spatial_conn(tmp_path)
        _create_reaches_table(conn, [{"reach_id": 1}])
        from src.sword_duckdb.lint.checks.classification import (
            check_centerline_reach_distance,
        )

        result = check_centerline_reach_distance(conn)
        assert result.passed is True
        assert result.total_checked == 0
        conn.close()


# =============================================================================
# A031 WSE Variance Tests
# =============================================================================


class TestA031WseVariance:
    """Tests for A031 wse_variance."""

    def test_a031_registered(self):
        check = get_check("A031")
        assert check is not None
        assert check.name == "wse_variance"
        assert check.category == Category.ATTRIBUTES
        assert check.severity == Severity.WARNING

    def test_a031_runs(self, tmp_path):
        conn = _spatial_conn(tmp_path)
        _create_nodes_with_dist_out(
            conn,
            [
                {"node_id": 1, "reach_id": 1, "x": 0, "y": 0, "node_length": 200, "wse": 10.0},
                {"node_id": 2, "reach_id": 1, "x": 1, "y": 1, "node_length": 200, "wse": 10.1},
            ],
        )
        from src.sword_duckdb.lint.checks.attributes import check_wse_variance

        result = check_wse_variance(conn)
        assert result.check_id == "A031"
        assert result.passed is True
        conn.close()

    def test_a031_finds_high_variance(self, tmp_path):
        conn = _spatial_conn(tmp_path)
        _create_nodes_with_dist_out(
            conn,
            [
                {"node_id": 1, "reach_id": 1, "x": 0, "y": 0, "node_length": 200, "wse": 10.0},
                {"node_id": 2, "reach_id": 1, "x": 1, "y": 1, "node_length": 200, "wse": 30.0},
            ],
        )
        from src.sword_duckdb.lint.checks.attributes import check_wse_variance

        result = check_wse_variance(conn, threshold=5.0)
        assert result.passed is False
        assert result.issues_found == 1
        conn.close()


# =============================================================================
# N011 Node Ordering Problems Tests
# =============================================================================


class TestN011NodeOrdering:
    """Tests for N011 node_ordering_problems."""

    def test_n011_registered(self):
        check = get_check("N011")
        assert check is not None
        assert check.name == "node_ordering_problems"
        assert check.category == Category.NETWORK
        assert check.severity == Severity.WARNING

    def test_n011_runs(self, tmp_path):
        conn = _spatial_conn(tmp_path)
        _create_nodes_table(
            conn,
            [
                {"node_id": 1, "reach_id": 1, "x": 0, "y": 0, "node_length": 200},
            ],
        )
        from src.sword_duckdb.lint.checks.node import check_node_ordering_problems

        result = check_node_ordering_problems(conn)
        assert result.check_id == "N011"
        assert result.passed is True
        conn.close()

    def test_n011_finds_zero_length(self, tmp_path):
        conn = _spatial_conn(tmp_path)
        _create_nodes_table(
            conn,
            [
                {"node_id": 1, "reach_id": 1, "x": 0, "y": 0, "node_length": 0},
            ],
        )
        from src.sword_duckdb.lint.checks.node import check_node_ordering_problems

        result = check_node_ordering_problems(conn)
        assert result.passed is False
        assert result.issues_found == 1
        conn.close()

    def test_n011_finds_excessive_length(self, tmp_path):
        conn = _spatial_conn(tmp_path)
        _create_nodes_table(
            conn,
            [
                {"node_id": 1, "reach_id": 1, "x": 0, "y": 0, "node_length": 5000},
            ],
        )
        from src.sword_duckdb.lint.checks.node import check_node_ordering_problems

        result = check_node_ordering_problems(conn)
        assert result.passed is False
        assert result.issues_found == 1
        conn.close()


# =============================================================================
# G022 Single Node Reaches Tests
# =============================================================================


class TestG022SingleNodeReaches:
    """Tests for G022 single_node_reaches."""

    def test_g022_registered(self):
        check = get_check("G022")
        assert check is not None
        assert check.name == "single_node_reaches"
        assert check.category == Category.GEOMETRY
        assert check.severity == Severity.INFO

    def test_g022_runs(self, tmp_path):
        conn = _spatial_conn(tmp_path)
        _create_reaches_table(
            conn,
            [{"reach_id": 1, "n_rch_up": 0, "n_rch_down": 0}],
        )
        from src.sword_duckdb.lint.checks.geometry import check_single_node_reaches

        result = check_single_node_reaches(conn)
        assert result.check_id == "G022"
        conn.close()

    def test_g022_finds_single_node(self, tmp_path):
        conn = _spatial_conn(tmp_path)
        conn.execute("""
            CREATE TABLE reaches (
                reach_id BIGINT, region VARCHAR, x DOUBLE, y DOUBLE,
                geom GEOMETRY, reach_length DOUBLE, width DOUBLE,
                lakeflag INTEGER, n_rch_up INTEGER DEFAULT 0,
                n_rch_down INTEGER DEFAULT 0, n_nodes INTEGER DEFAULT 5,
                type INTEGER DEFAULT 1
            )
        """)
        conn.execute("""INSERT INTO reaches
            (reach_id, region, n_nodes, type)
            VALUES (1, 'NA', 1, 1)""")
        conn.execute("""INSERT INTO reaches
            (reach_id, region, n_nodes, type)
            VALUES (2, 'NA', 5, 1)""")

        from src.sword_duckdb.lint.checks.geometry import check_single_node_reaches

        result = check_single_node_reaches(conn)
        assert result.issues_found == 1
        conn.close()

    def test_g022_excludes_ghost_and_dam(self, tmp_path):
        conn = _spatial_conn(tmp_path)
        conn.execute("""
            CREATE TABLE reaches (
                reach_id BIGINT, region VARCHAR, x DOUBLE, y DOUBLE,
                geom GEOMETRY, reach_length DOUBLE, width DOUBLE,
                lakeflag INTEGER, n_rch_up INTEGER DEFAULT 0,
                n_rch_down INTEGER DEFAULT 0, n_nodes INTEGER DEFAULT 5,
                type INTEGER DEFAULT 1
            )
        """)
        # ghost (type=6) and dam (type=4) with 1 node should be excluded
        conn.execute("""INSERT INTO reaches
            (reach_id, region, n_nodes, type) VALUES (1, 'NA', 1, 6)""")
        conn.execute("""INSERT INTO reaches
            (reach_id, region, n_nodes, type) VALUES (2, 'NA', 1, 4)""")

        from src.sword_duckdb.lint.checks.geometry import check_single_node_reaches

        result = check_single_node_reaches(conn)
        assert result.issues_found == 0
        conn.close()


# =============================================================================
# G023 Duplicate Centerline Points Tests
# =============================================================================


class TestG023DuplicateCenterlines:
    """Tests for G023 duplicate_centerline_points."""

    def test_g023_registered(self):
        check = get_check("G023")
        assert check is not None
        assert check.name == "duplicate_centerline_points"
        assert check.category == Category.GEOMETRY
        assert check.severity == Severity.INFO

    def test_g023_runs(self, tmp_path):
        conn = _spatial_conn(tmp_path)
        _create_centerlines_table(
            conn,
            [
                {"cl_id": 1, "reach_id": 1, "x": 0.0, "y": 0.0},
                {"cl_id": 2, "reach_id": 1, "x": 0.001, "y": 0.001},
            ],
        )
        from src.sword_duckdb.lint.checks.geometry import (
            check_duplicate_centerline_points,
        )

        result = check_duplicate_centerline_points(conn)
        assert result.check_id == "G023"
        assert result.issues_found == 0
        conn.close()

    def test_g023_finds_duplicate(self, tmp_path):
        conn = _spatial_conn(tmp_path)
        _create_centerlines_table(
            conn,
            [
                {"cl_id": 1, "reach_id": 1, "x": 0.0, "y": 0.0},
                {"cl_id": 2, "reach_id": 1, "x": 0.0, "y": 0.0},
                {"cl_id": 3, "reach_id": 1, "x": 0.001, "y": 0.001},
            ],
        )
        from src.sword_duckdb.lint.checks.geometry import (
            check_duplicate_centerline_points,
        )

        result = check_duplicate_centerline_points(conn)
        assert result.issues_found == 1
        conn.close()

    def test_g023_no_centerlines_table(self, tmp_path):
        conn = _spatial_conn(tmp_path)
        from src.sword_duckdb.lint.checks.geometry import (
            check_duplicate_centerline_points,
        )

        result = check_duplicate_centerline_points(conn)
        assert result.passed is True
        assert result.total_checked == 0
        conn.close()


# =============================================================================
# V026-V031 subnetwork_id Checks
# =============================================================================


def _create_subnetwork_test_data(conn, reaches_rows, topology_rows=None):
    """Create minimal reaches/reach_topology tables for subnetwork_id checks.

    reaches_rows: list of tuples
        (reach_id, region, x, y, river_name, n_rch_up, n_rch_down, subnetwork_id, network)
    """
    conn.execute("""
        CREATE TABLE IF NOT EXISTS reaches (
            reach_id BIGINT,
            region VARCHAR,
            x DOUBLE,
            y DOUBLE,
            river_name VARCHAR,
            n_rch_up INTEGER,
            n_rch_down INTEGER,
            subnetwork_id INTEGER,
            network INTEGER
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS reach_topology (
            reach_id BIGINT,
            region VARCHAR,
            direction VARCHAR,
            neighbor_rank INTEGER,
            neighbor_reach_id BIGINT
        )
    """)

    for row in reaches_rows:
        conn.execute(
            "INSERT INTO reaches VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            list(row),
        )
    for row in topology_rows or []:
        conn.execute(
            "INSERT INTO reach_topology VALUES (?, ?, ?, ?, ?)",
            list(row),
        )


@pytest.mark.db
class TestV17CSubnetworkIdChecks:
    """Tests for V026-V031 subnetwork_id checks."""

    def test_v026_registered(self):
        check = get_check("V026")
        assert check is not None
        assert check.name == "subnetwork_id_coverage"
        assert check.category == Category.V17C
        assert check.severity == Severity.ERROR

    def test_v027_registered(self):
        check = get_check("V027")
        assert check is not None
        assert check.name == "subnetwork_id_pfafstetter_range"

    def test_v028_registered(self):
        check = get_check("V028")
        assert check is not None
        assert check.name == "subnetwork_id_topology_consistency"

    def test_v029_registered(self):
        check = get_check("V029")
        assert check is not None
        assert check.name == "subnetwork_id_cross_region_uniqueness"

    def test_v030_registered(self):
        check = get_check("V030")
        assert check is not None
        assert check.name == "subnetwork_id_singleton_consistency"

    def test_v031_registered(self):
        check = get_check("V031")
        assert check is not None
        assert check.name == "subnetwork_id_distribution"

    def test_v026_missing_column_passes(self, tmp_path):
        import duckdb as _duckdb

        db_path = tmp_path / "v026_nocol.duckdb"
        conn = _duckdb.connect(str(db_path))
        conn.execute("CREATE TABLE reaches (reach_id BIGINT, region VARCHAR)")
        from src.sword_duckdb.lint.checks.v17c import check_subnetwork_id_coverage

        result = check_subnetwork_id_coverage(conn)
        assert result.passed is True
        assert result.total_checked == 0
        conn.close()

    def test_v026_flags_null_subnetwork(self, tmp_path):
        """Connected reaches with NULL subnetwork_id should be flagged."""
        import duckdb as _duckdb

        db_path = tmp_path / "v026_null.duckdb"
        conn = _duckdb.connect(str(db_path))
        _create_subnetwork_test_data(
            conn,
            reaches_rows=[
                (1, "NA", -75.0, 45.0, "R", 0, 1, None, 1),
                (2, "NA", -75.1, 45.1, "R", 1, 0, 7_000_001, 1),
            ],
            topology_rows=[
                (1, "NA", "down", 0, 2),
                (2, "NA", "up", 0, 1),
            ],
        )
        from src.sword_duckdb.lint.checks.v17c import check_subnetwork_id_coverage

        result = check_subnetwork_id_coverage(conn, region="NA")
        assert result.passed is False
        assert result.issues_found == 1
        conn.close()

    def test_v026_passes_when_all_populated(self, tmp_path):
        import duckdb as _duckdb

        db_path = tmp_path / "v026_ok.duckdb"
        conn = _duckdb.connect(str(db_path))
        _create_subnetwork_test_data(
            conn,
            reaches_rows=[
                (1, "NA", -75.0, 45.0, "R", 0, 1, 7_000_001, 1),
                (2, "NA", -75.1, 45.1, "R", 1, 0, 7_000_001, 1),
            ],
            topology_rows=[
                (1, "NA", "down", 0, 2),
                (2, "NA", "up", 0, 1),
            ],
        )
        from src.sword_duckdb.lint.checks.v17c import check_subnetwork_id_coverage

        result = check_subnetwork_id_coverage(conn, region="NA")
        assert result.passed is True
        assert result.issues_found == 0
        conn.close()

    def test_v027_flags_out_of_band(self, tmp_path):
        """subnetwork_id outside Pfafstetter band should be flagged."""
        import duckdb as _duckdb

        db_path = tmp_path / "v027_bad.duckdb"
        conn = _duckdb.connect(str(db_path))
        _create_subnetwork_test_data(
            conn,
            reaches_rows=[
                # NA reach with AF-band subnetwork_id -> wrong
                (1, "NA", -75.0, 45.0, "R", 0, 1, 1_000_001, 1),
                (2, "NA", -75.1, 45.1, "R", 1, 0, 7_000_001, 1),
            ],
        )
        from src.sword_duckdb.lint.checks.v17c import (
            check_subnetwork_id_pfafstetter_range,
        )

        result = check_subnetwork_id_pfafstetter_range(conn, region="NA")
        assert result.passed is False
        assert result.issues_found == 1
        conn.close()

    def test_v027_passes_correct_bands(self, tmp_path):
        import duckdb as _duckdb

        db_path = tmp_path / "v027_ok.duckdb"
        conn = _duckdb.connect(str(db_path))
        _create_subnetwork_test_data(
            conn,
            reaches_rows=[
                (1, "NA", -75.0, 45.0, "R", 0, 1, 7_000_001, 1),
                (2, "NA", -75.1, 45.1, "R", 1, 0, 7_000_002, 1),
                (3, "AF", 30.0, -5.0, "R", 0, 0, 1_000_001, 2),
            ],
        )
        from src.sword_duckdb.lint.checks.v17c import (
            check_subnetwork_id_pfafstetter_range,
        )

        result = check_subnetwork_id_pfafstetter_range(conn)
        assert result.passed is True
        assert result.issues_found == 0
        conn.close()

    def test_v028_flags_mismatched_neighbors(self, tmp_path):
        """Connected reaches with different subnetwork_id should be flagged."""
        import duckdb as _duckdb

        db_path = tmp_path / "v028_bad.duckdb"
        conn = _duckdb.connect(str(db_path))
        _create_subnetwork_test_data(
            conn,
            reaches_rows=[
                (1, "NA", -75.0, 45.0, "R", 0, 1, 7_000_001, 1),
                (2, "NA", -75.1, 45.1, "R", 1, 0, 7_000_002, 1),
            ],
            topology_rows=[
                (1, "NA", "down", 0, 2),
                (2, "NA", "up", 0, 1),
            ],
        )
        from src.sword_duckdb.lint.checks.v17c import (
            check_subnetwork_id_topology_consistency,
        )

        result = check_subnetwork_id_topology_consistency(conn, region="NA")
        assert result.passed is False
        assert result.issues_found == 2  # Both directions
        conn.close()

    def test_v028_passes_matching_neighbors(self, tmp_path):
        import duckdb as _duckdb

        db_path = tmp_path / "v028_ok.duckdb"
        conn = _duckdb.connect(str(db_path))
        _create_subnetwork_test_data(
            conn,
            reaches_rows=[
                (1, "NA", -75.0, 45.0, "R", 0, 1, 7_000_001, 1),
                (2, "NA", -75.1, 45.1, "R", 1, 0, 7_000_001, 1),
            ],
            topology_rows=[
                (1, "NA", "down", 0, 2),
                (2, "NA", "up", 0, 1),
            ],
        )
        from src.sword_duckdb.lint.checks.v17c import (
            check_subnetwork_id_topology_consistency,
        )

        result = check_subnetwork_id_topology_consistency(conn, region="NA")
        assert result.passed is True
        assert result.issues_found == 0
        conn.close()

    def test_v029_flags_cross_region_collision(self, tmp_path):
        """Same subnetwork_id in two regions should be flagged."""
        import duckdb as _duckdb

        db_path = tmp_path / "v029_bad.duckdb"
        conn = _duckdb.connect(str(db_path))
        _create_subnetwork_test_data(
            conn,
            reaches_rows=[
                (1, "NA", -75.0, 45.0, "R", 0, 0, 42, 1),
                (2, "AF", 30.0, -5.0, "R", 0, 0, 42, 2),
            ],
        )
        from src.sword_duckdb.lint.checks.v17c import (
            check_subnetwork_id_cross_region_uniqueness,
        )

        result = check_subnetwork_id_cross_region_uniqueness(conn)
        assert result.passed is False
        assert result.issues_found == 1
        conn.close()

    def test_v029_passes_unique_ids(self, tmp_path):
        import duckdb as _duckdb

        db_path = tmp_path / "v029_ok.duckdb"
        conn = _duckdb.connect(str(db_path))
        _create_subnetwork_test_data(
            conn,
            reaches_rows=[
                (1, "NA", -75.0, 45.0, "R", 0, 0, 7_000_001, 1),
                (2, "AF", 30.0, -5.0, "R", 0, 0, 1_000_001, 2),
            ],
        )
        from src.sword_duckdb.lint.checks.v17c import (
            check_subnetwork_id_cross_region_uniqueness,
        )

        result = check_subnetwork_id_cross_region_uniqueness(conn)
        assert result.passed is True
        assert result.issues_found == 0
        conn.close()

    def test_v030_flags_isolated_in_large_component(self, tmp_path):
        """Isolated reach sharing subnetwork_id with connected reaches."""
        import duckdb as _duckdb

        db_path = tmp_path / "v030_bad.duckdb"
        conn = _duckdb.connect(str(db_path))
        _create_subnetwork_test_data(
            conn,
            reaches_rows=[
                # Isolated reach with same subnetwork_id as connected pair
                (1, "NA", -75.0, 45.0, "R", 0, 0, 7_000_001, 1),
                (2, "NA", -75.1, 45.1, "R", 0, 1, 7_000_001, 1),
                (3, "NA", -75.2, 45.2, "R", 1, 0, 7_000_001, 1),
            ],
            topology_rows=[
                (2, "NA", "down", 0, 3),
                (3, "NA", "up", 0, 2),
            ],
        )
        from src.sword_duckdb.lint.checks.v17c import (
            check_subnetwork_id_singleton_consistency,
        )

        result = check_subnetwork_id_singleton_consistency(conn, region="NA")
        assert result.issues_found == 1
        assert int(result.details.iloc[0]["reach_id"]) == 1
        conn.close()

    def test_v031_reports_distribution(self, tmp_path):
        import duckdb as _duckdb

        db_path = tmp_path / "v031_stats.duckdb"
        conn = _duckdb.connect(str(db_path))
        _create_subnetwork_test_data(
            conn,
            reaches_rows=[
                (1, "NA", -75.0, 45.0, "R", 0, 1, 7_000_001, 1),
                (2, "NA", -75.1, 45.1, "R", 1, 0, 7_000_001, 1),
                (3, "NA", -75.2, 45.2, "R", 0, 0, 7_000_002, 2),
            ],
        )
        from src.sword_duckdb.lint.checks.v17c import (
            check_subnetwork_id_distribution,
        )

        result = check_subnetwork_id_distribution(conn, region="NA")
        assert result.passed is True
        assert result.total_checked == 2  # 2 distinct subnetwork_ids
        assert len(result.details) == 1  # 1 region
        assert int(result.details.iloc[0]["n_components"]) == 2
        assert int(result.details.iloc[0]["singletons"]) == 1
        conn.close()
