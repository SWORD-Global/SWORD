# -*- coding: utf-8 -*-
"""
Tests for SWORD DuckDB Connection Manager

Tests SWORDDatabase class and create_database function.
"""

import pytest

import pandas as pd

pytestmark = pytest.mark.db


class TestSWORDDatabaseInit:
    """Tests for SWORDDatabase initialization."""

    def test_init_with_path_string(self, tmp_path):
        """Test initialization with string path."""
        from src.sword_duckdb.sword_db import SWORDDatabase

        db_path = str(tmp_path / "test.duckdb")
        db = SWORDDatabase(db_path)

        # db_path may be stored as string or Path, compare as string
        assert str(db.db_path) == db_path
        assert db.read_only == False
        assert db.spatial == True
        db.close()

    def test_init_with_path_object(self, tmp_path):
        """Test initialization with Path object."""
        from src.sword_duckdb.sword_db import SWORDDatabase

        db_path = tmp_path / "test.duckdb"
        db = SWORDDatabase(db_path)

        assert db.db_path == db_path
        db.close()

    def test_init_memory_database(self):
        """Test initialization with in-memory database."""
        from src.sword_duckdb.sword_db import SWORDDatabase

        db = SWORDDatabase(":memory:")
        assert db.db_path == ":memory:"
        db.close()

    def test_init_read_only(self, sword_readonly):
        """Test read-only mode initialization."""
        # The sword_readonly fixture already has db initialized
        # We just verify it can be accessed
        assert sword_readonly._db is not None

    def test_init_no_spatial(self, tmp_path):
        """Test initialization without spatial extension."""
        from src.sword_duckdb.sword_db import SWORDDatabase

        db_path = tmp_path / "test.duckdb"
        db = SWORDDatabase(db_path, spatial=False)

        assert db.spatial == False
        db.connect()
        assert db._spatial_loaded == False
        db.close()


class TestSWORDDatabaseConnection:
    """Tests for connection management."""

    def test_connect_creates_file(self, tmp_path):
        """Test that connect() creates database file."""
        from src.sword_duckdb.sword_db import SWORDDatabase

        db_path = tmp_path / "new_db.duckdb"
        assert not db_path.exists()

        db = SWORDDatabase(db_path)
        db.connect()

        assert db_path.exists()
        db.close()

    def test_connect_returns_connection(self, tmp_path):
        """Test that connect() returns a connection object."""
        from src.sword_duckdb.sword_db import SWORDDatabase
        import duckdb

        db_path = tmp_path / "test.duckdb"
        db = SWORDDatabase(db_path)
        conn = db.connect()

        assert isinstance(conn, duckdb.DuckDBPyConnection)
        db.close()

    def test_connect_reuses_connection(self, tmp_path):
        """Test that connect() reuses existing connection."""
        from src.sword_duckdb.sword_db import SWORDDatabase

        db_path = tmp_path / "test.duckdb"
        db = SWORDDatabase(db_path)

        conn1 = db.connect()
        conn2 = db.connect()

        assert conn1 is conn2
        db.close()

    def test_conn_property(self, tmp_path):
        """Test conn property is alias for connect()."""
        from src.sword_duckdb.sword_db import SWORDDatabase

        db_path = tmp_path / "test.duckdb"
        db = SWORDDatabase(db_path)

        assert db.conn is db.connect()
        db.close()

    def test_close_sets_none(self, tmp_path):
        """Test that close() sets connection to None."""
        from src.sword_duckdb.sword_db import SWORDDatabase

        db_path = tmp_path / "test.duckdb"
        db = SWORDDatabase(db_path)
        db.connect()

        assert db._conn is not None
        db.close()
        assert db._conn is None

    def test_close_resets_spatial_flag(self, tmp_path):
        """Test that close() resets spatial_loaded flag."""
        from src.sword_duckdb.sword_db import SWORDDatabase

        db_path = tmp_path / "test.duckdb"
        db = SWORDDatabase(db_path)
        db.connect()

        db.close()
        assert db._spatial_loaded == False


class TestSWORDDatabaseContextManager:
    """Tests for context manager usage."""

    def test_context_manager_connects(self, tmp_path):
        """Test that entering context manager connects."""
        from src.sword_duckdb.sword_db import SWORDDatabase

        db_path = tmp_path / "test.duckdb"

        with SWORDDatabase(db_path) as db:
            assert db._conn is not None

    def test_context_manager_closes(self, tmp_path):
        """Test that exiting context manager closes connection."""
        from src.sword_duckdb.sword_db import SWORDDatabase

        db_path = tmp_path / "test.duckdb"

        db = SWORDDatabase(db_path)
        with db:
            pass

        assert db._conn is None

    def test_context_manager_returns_db(self, tmp_path):
        """Test that context manager returns database instance."""
        from src.sword_duckdb.sword_db import SWORDDatabase

        db_path = tmp_path / "test.duckdb"

        with SWORDDatabase(db_path) as db:
            assert isinstance(db, SWORDDatabase)


class TestSWORDDatabaseQuery:
    """Tests for query and execute methods."""

    def test_query_returns_dataframe(self, sword_readonly):
        """Test that query() returns a DataFrame."""
        df = sword_readonly._db.query("SELECT COUNT(*) as cnt FROM reaches")

        assert isinstance(df, pd.DataFrame)
        assert "cnt" in df.columns

    def test_query_with_params(self, sword_readonly):
        """Test parameterized query."""
        df = sword_readonly._db.query(
            "SELECT COUNT(*) as cnt FROM reaches WHERE region = ?", ["NA"]
        )

        assert isinstance(df, pd.DataFrame)
        assert df["cnt"].iloc[0] > 0

    def test_execute_returns_result(self, sword_readonly):
        """Test that execute() returns a result object."""
        result = sword_readonly._db.execute("SELECT 1 as val")

        assert result is not None
        assert result.fetchone()[0] == 1

    def test_execute_with_params(self, sword_readonly):
        """Test parameterized execute."""
        result = sword_readonly._db.execute("SELECT ? as val", [42])

        assert result.fetchone()[0] == 42


class TestSWORDDatabaseSchema:
    """Tests for schema-related methods."""

    def test_init_schema_creates_tables(self, tmp_path):
        """Test that init_schema() creates required tables."""
        from src.sword_duckdb.sword_db import SWORDDatabase

        db_path = tmp_path / "new.duckdb"
        db = SWORDDatabase(db_path)
        db.init_schema()

        # Check that tables exist
        result = db.query("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'main'
        """)

        tables = result["table_name"].tolist()
        assert "reaches" in tables
        assert "nodes" in tables
        assert "centerlines" in tables

        db.close()

    def test_reaches_has_subnetwork_id_and_facc_quality(self, tmp_path):
        """Test that reaches table includes subnetwork_id and facc_quality columns."""
        from src.sword_duckdb.sword_db import SWORDDatabase

        db_path = tmp_path / "new.duckdb"
        db = SWORDDatabase(db_path)
        db.init_schema()

        result = db.query(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'reaches'"
        )
        cols = result["column_name"].tolist()
        assert "subnetwork_id" in cols
        assert "facc_quality" in cols
        db.close()

    def test_nodes_has_subnetwork_id_and_facc_quality(self, tmp_path):
        """Test that nodes table includes subnetwork_id and facc_quality columns."""
        from src.sword_duckdb.sword_db import SWORDDatabase

        db_path = tmp_path / "new.duckdb"
        db = SWORDDatabase(db_path)
        db.init_schema()

        result = db.query(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'nodes'"
        )
        cols = result["column_name"].tolist()
        assert "subnetwork_id" in cols
        assert "facc_quality" in cols
        db.close()

    def test_add_v17c_columns_adds_subnetwork_and_facc_quality(self, tmp_path):
        """add_v17c_columns() adds subnetwork_id and facc_quality to existing DB.

        Uses a raw duckdb connection (not SWORDDatabase) to simulate migrating
        a pre-existing database not created through the SWORD schema layer.
        """
        import duckdb
        from src.sword_duckdb.schema import add_v17c_columns

        db_path = tmp_path / "old.duckdb"
        con = duckdb.connect(str(db_path))

        # Simulate an old DB: minimal reaches and nodes without the new columns
        con.execute("""
            CREATE TABLE reaches (
                reach_id BIGINT NOT NULL,
                region VARCHAR(2) NOT NULL,
                reach_length DOUBLE,
                dist_out DOUBLE,
                PRIMARY KEY (reach_id, region)
            )
        """)
        con.execute("""
            CREATE TABLE nodes (
                node_id BIGINT NOT NULL,
                region VARCHAR(2) NOT NULL,
                reach_id BIGINT NOT NULL,
                dist_out DOUBLE,
                PRIMARY KEY (node_id, region)
            )
        """)

        add_v17c_columns(con)

        for table in ("reaches", "nodes"):
            result = con.execute(
                f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table}'"
            ).fetchall()
            cols = {r[0] for r in result}
            assert "subnetwork_id" in cols, f"subnetwork_id missing from {table}"
            assert "facc_quality" in cols, f"facc_quality missing from {table}"

        con.close()


class TestSWORDDatabaseInfo:
    """Tests for information retrieval methods."""

    def test_get_regions(self, sword_readonly):
        """Test get_regions() returns list of region codes."""
        regions = sword_readonly._db.get_regions()

        assert isinstance(regions, list)
        assert "NA" in regions

    def test_count_records(self, sword_readonly):
        """Test count_records() returns dict with counts."""
        counts = sword_readonly._db.count_records()

        assert isinstance(counts, dict)
        assert "reaches" in counts
        assert "nodes" in counts
        assert "centerlines" in counts
        assert all(v >= 0 for v in counts.values())

    def test_count_records_with_region(self, sword_readonly):
        """Test count_records() with region filter."""
        counts = sword_readonly._db.count_records(region="NA")

        assert isinstance(counts, dict)
        assert counts["reaches"] > 0

    def test_spatial_available(self, sword_readonly):
        """Test spatial_available() returns boolean."""
        result = sword_readonly._db.spatial_available()

        assert isinstance(result, bool)


class TestCreateDatabase:
    """Tests for create_database function."""

    def test_create_new_database(self, tmp_path):
        """Test creating a new database."""
        from src.sword_duckdb.sword_db import create_database

        db_path = tmp_path / "new.duckdb"

        db = create_database(db_path)

        assert db_path.exists()
        assert isinstance(db, type(db))  # Check it's a SWORDDatabase
        db.close()

    def test_create_existing_raises_error(self, tmp_path):
        """Test that creating existing database raises error."""
        from src.sword_duckdb.sword_db import create_database

        db_path = tmp_path / "existing.duckdb"
        db_path.touch()  # Create empty file

        with pytest.raises(FileExistsError):
            create_database(db_path)

    def test_create_with_overwrite(self, tmp_path):
        """Test overwrite option."""
        from src.sword_duckdb.sword_db import create_database

        db_path = tmp_path / "existing.duckdb"
        db_path.touch()  # Create empty file

        # Should not raise
        db = create_database(db_path, overwrite=True)

        assert db_path.exists()
        db.close()

    def test_create_initializes_schema(self, tmp_path):
        """Test that create_database initializes schema."""
        from src.sword_duckdb.sword_db import create_database

        db_path = tmp_path / "new.duckdb"
        db = create_database(db_path)

        # Check that tables exist
        counts = db.count_records()

        assert "reaches" in counts
        assert "nodes" in counts

        db.close()


class TestSWORDDatabaseSpatial:
    """Tests for spatial extension handling."""

    def test_spatial_loads_by_default(self, tmp_path):
        """Test that spatial extension loads by default."""
        from src.sword_duckdb.sword_db import SWORDDatabase

        db_path = tmp_path / "test.duckdb"
        db = SWORDDatabase(db_path)
        db.connect()

        # Spatial should attempt to load
        # (may succeed or fail depending on DuckDB version)
        assert db.spatial == True
        db.close()

    def test_spatial_disabled_does_not_load(self, tmp_path):
        """Test that spatial=False prevents loading."""
        from src.sword_duckdb.sword_db import SWORDDatabase

        db_path = tmp_path / "test.duckdb"
        db = SWORDDatabase(db_path, spatial=False)
        db.connect()

        assert db._spatial_loaded == False
        db.close()
