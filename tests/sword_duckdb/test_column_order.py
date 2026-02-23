"""Tests for canonical column ordering definitions."""

import shutil

import duckdb
import pandas as pd
import pytest

from src.sword_duckdb.column_order import (
    CENTERLINES_COLUMN_ORDER,
    NODES_COLUMN_ORDER,
    REACHES_COLUMN_ORDER,
    get_column_order,
    reorder_columns,
)


@pytest.mark.unit
class TestCanonicalLists:
    def test_reaches_starts_with_identity(self):
        assert REACHES_COLUMN_ORDER[:2] == ("reach_id", "region")

    def test_reaches_ends_with_version(self):
        assert REACHES_COLUMN_ORDER[-1] == "version"

    def test_nodes_starts_with_identity(self):
        assert NODES_COLUMN_ORDER[:2] == ("node_id", "region")

    def test_nodes_ends_with_version(self):
        assert NODES_COLUMN_ORDER[-1] == "version"

    def test_centerlines_starts_with_identity(self):
        assert CENTERLINES_COLUMN_ORDER[:2] == ("cl_id", "region")

    def test_no_duplicates_reaches(self):
        assert len(REACHES_COLUMN_ORDER) == len(set(REACHES_COLUMN_ORDER))

    def test_no_duplicates_nodes(self):
        assert len(NODES_COLUMN_ORDER) == len(set(NODES_COLUMN_ORDER))

    def test_no_duplicates_centerlines(self):
        assert len(CENTERLINES_COLUMN_ORDER) == len(set(CENTERLINES_COLUMN_ORDER))

    def test_reaches_has_subnetwork_id(self):
        assert "subnetwork_id" in REACHES_COLUMN_ORDER

    def test_reaches_has_facc_quality(self):
        assert "facc_quality" in REACHES_COLUMN_ORDER

    def test_nodes_has_subnetwork_id(self):
        assert "subnetwork_id" in NODES_COLUMN_ORDER

    def test_nodes_has_facc_quality(self):
        assert "facc_quality" in NODES_COLUMN_ORDER

    def test_facc_quality_after_facc_reaches(self):
        idx_facc = REACHES_COLUMN_ORDER.index("facc")
        idx_fq = REACHES_COLUMN_ORDER.index("facc_quality")
        assert idx_fq == idx_facc + 1, (
            "facc_quality should immediately follow facc in reaches"
        )

    def test_subnetwork_id_after_network_reaches(self):
        idx_net = REACHES_COLUMN_ORDER.index("network")
        idx_sub = REACHES_COLUMN_ORDER.index("subnetwork_id")
        assert idx_sub == idx_net + 1, (
            "subnetwork_id should immediately follow network in reaches"
        )

    def test_facc_quality_after_facc_nodes(self):
        idx_facc = NODES_COLUMN_ORDER.index("facc")
        idx_fq = NODES_COLUMN_ORDER.index("facc_quality")
        assert idx_fq == idx_facc + 1, (
            "facc_quality should immediately follow facc in nodes"
        )

    def test_subnetwork_id_after_network_nodes(self):
        idx_net = NODES_COLUMN_ORDER.index("network")
        idx_sub = NODES_COLUMN_ORDER.index("subnetwork_id")
        assert idx_sub == idx_net + 1, (
            "subnetwork_id should immediately follow network in nodes"
        )


@pytest.mark.unit
class TestGetColumnOrder:
    def test_reaches(self):
        assert get_column_order("reaches") is REACHES_COLUMN_ORDER

    def test_nodes(self):
        assert get_column_order("nodes") is NODES_COLUMN_ORDER

    def test_centerlines(self):
        assert get_column_order("centerlines") is CENTERLINES_COLUMN_ORDER

    def test_unknown_table_raises(self):
        with pytest.raises(ValueError, match="Unknown table"):
            get_column_order("nonexistent")


@pytest.mark.unit
class TestReorderColumns:
    def test_reorders_to_canonical(self):
        df = pd.DataFrame({"version": [1], "reach_id": [100], "region": ["NA"]})
        result = reorder_columns(df, "reaches")
        assert result.columns[0] == "reach_id"
        assert result.columns[1] == "region"

    def test_missing_columns_skipped(self):
        df = pd.DataFrame({"reach_id": [100], "region": ["NA"]})
        result = reorder_columns(df, "reaches")
        assert list(result.columns) == ["reach_id", "region"]

    def test_extra_columns_appended(self):
        df = pd.DataFrame({"reach_id": [1], "region": ["NA"], "extra_col": [42]})
        result = reorder_columns(df, "reaches")
        assert result.columns[0] == "reach_id"
        assert result.columns[-1] == "extra_col"

    def test_data_preserved(self):
        df = pd.DataFrame({"version": ["v17c"], "reach_id": [100], "region": ["NA"]})
        result = reorder_columns(df, "reaches")
        assert result["reach_id"].iloc[0] == 100
        assert result["version"].iloc[0] == "v17c"


@pytest.mark.db
class TestCanonicalMatchesDB:
    """Verify canonical lists cover all columns in the test DB."""

    def test_reaches_covers_db_columns(self, ensure_test_db, tmp_path):
        db = tmp_path / "test.duckdb"
        shutil.copy2(ensure_test_db, db)
        con = duckdb.connect(str(db), read_only=True)
        try:
            db_cols = {
                r[0]
                for r in con.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_name='reaches'"
                ).fetchall()
            }
            canonical = set(REACHES_COLUMN_ORDER)
            missing_from_canonical = db_cols - canonical
            assert not missing_from_canonical, (
                f"DB has columns not in REACHES_COLUMN_ORDER: {missing_from_canonical}"
            )
        finally:
            con.close()

    def test_nodes_covers_db_columns(self, ensure_test_db, tmp_path):
        db = tmp_path / "test.duckdb"
        shutil.copy2(ensure_test_db, db)
        con = duckdb.connect(str(db), read_only=True)
        try:
            db_cols = {
                r[0]
                for r in con.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_name='nodes'"
                ).fetchall()
            }
            canonical = set(NODES_COLUMN_ORDER)
            missing_from_canonical = db_cols - canonical
            assert not missing_from_canonical, (
                f"DB has columns not in NODES_COLUMN_ORDER: {missing_from_canonical}"
            )
        finally:
            con.close()

    def test_centerlines_covers_db_columns(self, ensure_test_db, tmp_path):
        db = tmp_path / "test.duckdb"
        shutil.copy2(ensure_test_db, db)
        con = duckdb.connect(str(db), read_only=True)
        try:
            db_cols = {
                r[0]
                for r in con.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_name='centerlines'"
                ).fetchall()
            }
            canonical = set(CENTERLINES_COLUMN_ORDER)
            missing_from_canonical = db_cols - canonical
            assert not missing_from_canonical, (
                f"DB has columns not in CENTERLINES_COLUMN_ORDER: {missing_from_canonical}"
            )
        finally:
            con.close()


def _extract_ddl_columns(ddl: str) -> list[str]:
    """Extract column names from CREATE TABLE DDL, ignoring PK/constraints/comments."""
    import re

    cols = []
    for line in ddl.strip().split("\n"):
        line = line.strip()
        if (
            not line
            or line.startswith("--")
            or line.startswith("CREATE")
            or line.startswith(")")
        ):
            continue
        if line.startswith("PRIMARY"):
            continue
        match = re.match(r"(\w+)\s+", line)
        if match:
            cols.append(match.group(1))
    return cols


@pytest.mark.unit
class TestSchemaDDLOrder:
    """Verify schema DDL column order matches canonical order."""

    def test_reaches_ddl_matches_canonical(self):
        from src.sword_duckdb.schema import REACHES_TABLE

        ddl_cols = _extract_ddl_columns(REACHES_TABLE)
        canonical = list(REACHES_COLUMN_ORDER)
        assert ddl_cols == canonical, (
            f"DDL order != canonical.\nDDL: {ddl_cols}\nCanonical: {canonical}"
        )

    def test_nodes_ddl_matches_canonical(self):
        from src.sword_duckdb.schema import NODES_TABLE

        ddl_cols = _extract_ddl_columns(NODES_TABLE)
        canonical = list(NODES_COLUMN_ORDER)
        assert ddl_cols == canonical, (
            f"DDL order != canonical.\nDDL: {ddl_cols}\nCanonical: {canonical}"
        )

    def test_centerlines_ddl_matches_canonical(self):
        from src.sword_duckdb.schema import CENTERLINES_TABLE

        ddl_cols = _extract_ddl_columns(CENTERLINES_TABLE)
        canonical = list(CENTERLINES_COLUMN_ORDER)
        assert ddl_cols == canonical, (
            f"DDL order != canonical.\nDDL: {ddl_cols}\nCanonical: {canonical}"
        )
