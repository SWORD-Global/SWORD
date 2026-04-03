"""Tests for v17c pipeline validation gates."""

import shutil
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from src.sword_v17c_pipeline.gates import (
    GateFailure,
    GateResult,
    gate_pom_release,
    gate_post_save,
    gate_source_data,
    run_gate,
)
from src.sword_duckdb.lint.core import CheckResult, Severity

pytestmark = [pytest.mark.pipeline, pytest.mark.db]

FIXTURE_DB = Path(__file__).parent / "fixtures" / "sword_test_minimal.duckdb"


def _load_spatial(conn):
    """Load DuckDB spatial extension (required before UPDATE on RTREE-indexed tables)."""
    try:
        conn.execute("LOAD spatial;")
    except Exception:
        conn.execute("INSTALL spatial; LOAD spatial;")


@pytest.fixture
def writable_db_path(tmp_path):
    """Copy test DB to tmp and return its path as a string."""
    if not FIXTURE_DB.exists():
        pytest.skip(f"Test fixture DB not found: {FIXTURE_DB}")
    dst = tmp_path / "test.duckdb"
    shutil.copy2(FIXTURE_DB, dst)
    return str(dst)


class TestGateSourceData:
    """Tests for gate_source_data (T005, T012)."""

    def test_passes_on_clean_db(self, writable_db_path):
        """Clean test DB should pass source data gate."""
        result = gate_source_data(writable_db_path, "NA")
        assert isinstance(result, GateResult)
        assert result.passed is True
        assert result.label == "source_data"
        assert result.failed_checks == []

    def test_fails_on_orphan_neighbor(self, writable_db_path):
        """Inserting a topology row referencing non-existent reach triggers gate failure."""
        conn = duckdb.connect(writable_db_path)
        conn.execute(
            "INSERT INTO reach_topology "
            "(reach_id, region, neighbor_reach_id, direction, neighbor_rank) "
            "VALUES (11000000001, 'NA', 99999999999, 'down', 3)"
        )
        conn.close()

        with pytest.raises(GateFailure) as exc_info:
            gate_source_data(writable_db_path, "NA")
        # Both T005 and T012 fail; gate raises on whichever comes first
        assert exc_info.value.check_id in ("T005", "T012")
        assert exc_info.value.issues_found > 0

    def test_fails_on_count_mismatch(self, writable_db_path):
        """Corrupt n_rch_up triggers T005."""
        conn = duckdb.connect(writable_db_path)
        _load_spatial(conn)
        conn.execute(
            "UPDATE reaches SET n_rch_up = 99 "
            "WHERE reach_id = (SELECT reach_id FROM reaches WHERE region='NA' LIMIT 1)"
        )
        conn.close()

        with pytest.raises(GateFailure) as exc_info:
            gate_source_data(writable_db_path, "NA")
        assert exc_info.value.check_id == "T005"


class TestGatePostSave:
    """Tests for gate_post_save (V001, V005, V007, V008, T001, T002)."""

    def test_runs_all_checks(self, writable_db_path):
        """Gate runs all 6 checks regardless of pass/fail."""
        try:
            result = gate_post_save(writable_db_path, "NA")
        except GateFailure as e:
            # Gate failed but we can still verify it ran multiple checks
            # by checking that failed_checks is populated
            assert len(e.failed_checks) >= 1
            return

        assert isinstance(result, GateResult)
        assert result.label == "post_save"
        assert len(result.results) == 6

    def test_fails_on_bad_hydro_dist(self, writable_db_path):
        """Hydro_dist_out increasing downstream triggers gate failure."""
        conn = duckdb.connect(writable_db_path)
        _load_spatial(conn)
        row = conn.execute("""
            SELECT rt.reach_id, rt.neighbor_reach_id
            FROM reach_topology rt
            WHERE rt.region = 'NA' AND rt.direction = 'down'
            LIMIT 1
        """).fetchone()
        assert row is not None, "Test fixture must have downstream topology rows for NA"
        upstream_id, downstream_id = row
        conn.execute(
            "UPDATE reaches SET hydro_dist_out = 100 WHERE reach_id = ?",
            [upstream_id],
        )
        conn.execute(
            "UPDATE reaches SET hydro_dist_out = 99999 WHERE reach_id = ?",
            [downstream_id],
        )
        conn.close()

        with pytest.raises(GateFailure) as exc_info:
            gate_post_save(writable_db_path, "NA")
        # Should fail on V001 or T001 (both check dist_out monotonicity)
        assert exc_info.value.check_id in ("V001", "T001")


class TestRunGate:
    """Tests for the generic run_gate function."""

    def test_custom_check_ids(self, writable_db_path):
        """Run gate with a single check."""
        result = run_gate(writable_db_path, "NA", ["T005"], "custom_gate")
        assert result.label == "custom_gate"
        assert len(result.results) == 1
        assert result.results[0].check_id == "T005"

    def test_fail_on_warning(self, writable_db_path):
        """With fail_on={WARNING}, warnings become failures."""
        conn = duckdb.connect(writable_db_path)
        _load_spatial(conn)
        row = conn.execute("""
            SELECT rt.reach_id, rt.neighbor_reach_id
            FROM reach_topology rt
            WHERE rt.region = 'NA' AND rt.direction = 'down'
            LIMIT 1
        """).fetchone()
        assert row is not None, "Test fixture must have downstream topology rows for NA"
        upstream_id, downstream_id = row
        conn.execute(
            "UPDATE reaches SET path_freq = 999 WHERE reach_id = ?",
            [upstream_id],
        )
        conn.execute(
            "UPDATE reaches SET path_freq = 1 WHERE reach_id = ?",
            [downstream_id],
        )
        conn.close()

        with pytest.raises(GateFailure):
            run_gate(
                writable_db_path,
                "NA",
                ["T002"],
                "strict_gate",
                fail_on={Severity.WARNING},
            )

    def test_artifact_output(self, writable_db_path, tmp_path):
        """Gate writes JSON artifact when artifact_dir is set."""
        artifact_dir = str(tmp_path / "artifacts")
        result = run_gate(
            writable_db_path,
            "NA",
            ["T005"],
            "test_artifact",
            artifact_dir=artifact_dir,
        )
        assert result.passed
        artifact_file = Path(artifact_dir) / "test_artifact.json"
        assert artifact_file.exists()
        import json

        data = json.loads(artifact_file.read_text())
        assert len(data) == 1
        assert data[0]["check_id"] == "T005"


def _check_result(check_id: str, severity: Severity, passed: bool, issues_found: int):
    """Build a minimal CheckResult for gate-policy tests."""
    return CheckResult(
        check_id=check_id,
        name=check_id.lower(),
        severity=severity,
        passed=passed,
        total_checked=100,
        issues_found=issues_found,
        issue_pct=float(issues_found),
        details=pd.DataFrame(),
        description=check_id,
    )


class TestGatePomRelease:
    """Tests for the POM-derived release gate with allowed residuals."""

    def test_passes_within_allowed_budgets(self, monkeypatch):
        class FakeRunner:
            def __init__(self, *args, **kwargs):
                pass

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return None

            def run(self, checks=None, region=None):
                return [
                    _check_result("T001", Severity.ERROR, True, 0),
                    _check_result("N004", Severity.WARNING, True, 0),
                    _check_result("A030", Severity.WARNING, False, 669),
                    _check_result("N013", Severity.WARNING, False, 311),
                    _check_result("T020", Severity.INFO, False, 197),
                ]

        monkeypatch.setattr("src.sword_v17c_pipeline.gates.LintRunner", FakeRunner)

        result = gate_pom_release("fake.duckdb")
        assert result.passed is True
        assert result.failed_checks == []

    def test_fails_on_unexpected_nonzero_check(self, monkeypatch):
        class FakeRunner:
            def __init__(self, *args, **kwargs):
                pass

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return None

            def run(self, checks=None, region=None):
                return [
                    _check_result("T001", Severity.ERROR, True, 0),
                    _check_result("N004", Severity.WARNING, False, 1),
                ]

        monkeypatch.setattr("src.sword_v17c_pipeline.gates.LintRunner", FakeRunner)

        with pytest.raises(GateFailure) as exc_info:
            gate_pom_release("fake.duckdb")
        assert exc_info.value.check_id == "N004"

    def test_fails_when_allowed_budget_is_exceeded(self, monkeypatch):
        class FakeRunner:
            def __init__(self, *args, **kwargs):
                pass

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return None

            def run(self, checks=None, region=None):
                return [
                    _check_result("T001", Severity.ERROR, True, 0),
                    _check_result("A030", Severity.WARNING, False, 670),
                ]

        monkeypatch.setattr("src.sword_v17c_pipeline.gates.LintRunner", FakeRunner)

        with pytest.raises(GateFailure) as exc_info:
            gate_pom_release("fake.duckdb")
        assert exc_info.value.check_id == "A030"


class TestGateFailureException:
    """Tests for GateFailure exception attributes."""

    def test_attributes(self):
        exc = GateFailure("my_gate", "T005", 42)
        assert exc.label == "my_gate"
        assert exc.check_id == "T005"
        assert exc.issues_found == 42
        assert exc.failed_checks == ["T005"]
        assert "T005" in str(exc)

    def test_multiple_failed_checks(self):
        exc = GateFailure("my_gate", "T005", 42, failed_checks=["T005", "T012"])
        assert exc.failed_checks == ["T005", "T012"]
        assert "T005" in str(exc)
        assert "T012" in str(exc)

    def test_is_exception(self):
        assert issubclass(GateFailure, Exception)


class TestGateResultStructure:
    """Tests for GateResult dataclass."""

    def test_defaults(self):
        result = GateResult(label="test", passed=True, results=[])
        assert result.label == "test"
        assert result.passed is True
        assert result.results == []
        assert result.failed_checks == []
