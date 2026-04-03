"""
Unit tests for v17c_pipeline.py DuckDB output functions.

Tests the following functions:
- create_v17c_tables(conn) - creates v17c_sections and v17c_section_slope_validation tables
- save_to_duckdb(conn, region, hydro_dist, hw_out, is_mainstem) - updates reaches table
- save_sections_to_duckdb(conn, region, sections_df, validation_df) - saves section data
"""

import pytest
import duckdb
import shutil
import pandas as pd
from pathlib import Path

from src.sword_v17c_pipeline.v17c_pipeline import (
    create_v17c_tables,
    save_to_duckdb,
    save_sections_to_duckdb,
)

pytestmark = [pytest.mark.pipeline, pytest.mark.db]


@pytest.fixture
def writable_db(tmp_path):
    """Create a writable copy of the test database with v17c columns."""
    from sword_duckdb.schema import add_v17c_columns

    src = Path(__file__).parent / "fixtures" / "sword_test_minimal.duckdb"
    dst = tmp_path / "test.duckdb"
    shutil.copy2(src, dst)
    conn = duckdb.connect(str(dst))
    add_v17c_columns(conn)
    yield conn
    conn.close()


@pytest.fixture
def sample_reach_ids(writable_db):
    """Get sample reach IDs from the test database."""
    result = writable_db.execute(
        "SELECT reach_id FROM reaches WHERE region='NA' LIMIT 10"
    ).fetchall()
    return [row[0] for row in result]


class TestCreateV17cTables:
    """Tests for create_v17c_tables function."""

    def test_creates_sections_table(self, writable_db):
        """Test that create_v17c_tables creates v17c_sections table."""
        create_v17c_tables(writable_db)

        # Check table exists
        tables = writable_db.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_name = 'v17c_sections'"
        ).fetchall()
        assert len(tables) == 1

    def test_creates_validation_table(self, writable_db):
        """Test that create_v17c_tables creates v17c_section_slope_validation table."""
        create_v17c_tables(writable_db)

        # Check table exists
        tables = writable_db.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_name = 'v17c_section_slope_validation'"
        ).fetchall()
        assert len(tables) == 1

    def test_sections_table_schema(self, writable_db):
        """Test that v17c_sections table has correct schema."""
        create_v17c_tables(writable_db)

        columns = writable_db.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_name = 'v17c_sections' ORDER BY ordinal_position"
        ).fetchall()

        expected_columns = [
            ("section_id", "INTEGER"),
            ("region", "VARCHAR"),
            ("upstream_junction", "BIGINT"),
            ("downstream_junction", "BIGINT"),
            ("reach_ids", "VARCHAR"),
            ("distance", "DOUBLE"),
            ("n_reaches", "INTEGER"),
        ]

        for (col_name, col_type), (exp_name, exp_type) in zip(
            columns, expected_columns
        ):
            assert col_name == exp_name
            assert col_type == exp_type

    def test_validation_table_schema(self, writable_db):
        """Test that v17c_section_slope_validation table has correct schema."""
        create_v17c_tables(writable_db)

        columns = writable_db.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_name = 'v17c_section_slope_validation' ORDER BY ordinal_position"
        ).fetchall()

        expected_columns = [
            ("section_id", "INTEGER"),
            ("region", "VARCHAR"),
            ("slope_from_upstream", "DOUBLE"),
            ("slope_from_downstream", "DOUBLE"),
            ("direction_valid", "BOOLEAN"),
            ("likely_cause", "VARCHAR"),
        ]

        for (col_name, col_type), (exp_name, exp_type) in zip(
            columns, expected_columns
        ):
            assert col_name == exp_name
            assert col_type == exp_type

    def test_idempotent_can_run_twice(self, writable_db):
        """Test that create_v17c_tables is idempotent (can run twice without error)."""
        # First call
        create_v17c_tables(writable_db)

        # Second call should not raise
        create_v17c_tables(writable_db)

        # Tables should still exist
        tables = writable_db.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_name IN ('v17c_sections', 'v17c_section_slope_validation')"
        ).fetchall()
        assert len(tables) == 2


class TestSaveToDuckDB:
    """Tests for save_to_duckdb function."""

    @pytest.fixture
    def db_with_v17c_columns(self, writable_db):
        """Add v17c columns to reaches table."""
        # Add the columns that save_to_duckdb expects to update
        v17c_columns = [
            ("dist_out_dijkstra", "DOUBLE"),
            ("hydro_dist_out", "DOUBLE"),
            ("best_headwater", "BIGINT"),
            ("best_outlet", "BIGINT"),
            ("pathlen_hw", "DOUBLE"),
            ("pathlen_out", "DOUBLE"),
            ("is_mainstem", "BOOLEAN"),
            ("rch_id_up_main", "BIGINT"),
            ("rch_id_dn_main", "BIGINT"),
        ]
        for col_name, col_type in v17c_columns:
            try:
                writable_db.execute(
                    f"ALTER TABLE reaches ADD COLUMN {col_name} {col_type}"
                )
            except duckdb.CatalogException:
                # Column already exists
                pass
        return writable_db

    def test_updates_reach_columns(self, db_with_v17c_columns, sample_reach_ids):
        """Test that save_to_duckdb updates reach columns correctly."""
        conn = db_with_v17c_columns
        reach_id = sample_reach_ids[0]

        hydro_dist = {reach_id: {"hydro_dist_out": 1000.5}}
        dijkstra_dist = {reach_id: {"dist_out_dijkstra": 900.0}}
        hw_out = {
            reach_id: {
                "best_headwater": 11000000099,
                "best_outlet": 11000000001,
                "pathlen_hw": 2000.0,
                "pathlen_out": 3000.0,
            }
        }
        is_mainstem = {reach_id: True}

        n_updated = save_to_duckdb(
            conn, "NA", hydro_dist, hw_out, is_mainstem, dijkstra_dist=dijkstra_dist
        )

        assert n_updated == 1

        # Verify the values were written
        row = conn.execute(
            "SELECT hydro_dist_out, dist_out_dijkstra, best_headwater, best_outlet, "
            "pathlen_hw, pathlen_out, is_mainstem "
            "FROM reaches WHERE reach_id = ?",
            [reach_id],
        ).fetchone()

        assert row[0] == pytest.approx(1000.5)
        assert row[1] == pytest.approx(900.0)
        assert row[2] == 11000000099
        assert row[3] == 11000000001
        assert row[4] == pytest.approx(2000.0)
        assert row[5] == pytest.approx(3000.0)
        assert row[6] is True

    def test_handles_empty_dict_gracefully(self, db_with_v17c_columns):
        """Test that save_to_duckdb handles empty dict gracefully."""
        conn = db_with_v17c_columns

        n_updated = save_to_duckdb(conn, "NA", {}, {}, {})

        assert n_updated == 0

    def test_updates_multiple_reaches(self, db_with_v17c_columns, sample_reach_ids):
        """Test that save_to_duckdb can update multiple reaches."""
        conn = db_with_v17c_columns

        hydro_dist = {}
        dijkstra_dist = {}
        hw_out = {}
        is_mainstem = {}

        for i, reach_id in enumerate(sample_reach_ids[:5]):
            hydro_dist[reach_id] = {
                "hydro_dist_out": 1000.0 * i,
            }
            dijkstra_dist[reach_id] = {
                "dist_out_dijkstra": 500.0 * i,
            }
            hw_out[reach_id] = {
                "best_headwater": sample_reach_ids[-1],
                "best_outlet": sample_reach_ids[0],
                "pathlen_hw": 100.0 * i,
                "pathlen_out": 200.0 * i,
            }
            is_mainstem[reach_id] = i % 2 == 0

        n_updated = save_to_duckdb(
            conn, "NA", hydro_dist, hw_out, is_mainstem, dijkstra_dist=dijkstra_dist
        )

        assert n_updated == 5

    def test_handles_infinity_values(self, db_with_v17c_columns, sample_reach_ids):
        """Test that save_to_duckdb converts infinity values to NULL."""
        conn = db_with_v17c_columns
        reach_id = sample_reach_ids[0]

        hydro_dist = {reach_id: {"hydro_dist_out": float("inf")}}
        hw_out = {
            reach_id: {
                "best_headwater": None,
                "best_outlet": None,
                "pathlen_hw": 0,
                "pathlen_out": 0,
            }
        }
        is_mainstem = {reach_id: False}

        n_updated = save_to_duckdb(conn, "NA", hydro_dist, hw_out, is_mainstem)

        assert n_updated == 1

        # Verify infinity was converted to NULL
        row = conn.execute(
            "SELECT hydro_dist_out FROM reaches WHERE reach_id = ?",
            [reach_id],
        ).fetchone()

        assert row[0] is None

    def test_region_case_insensitive(self, db_with_v17c_columns, sample_reach_ids):
        """Test that save_to_duckdb normalizes region to uppercase."""
        conn = db_with_v17c_columns
        reach_id = sample_reach_ids[0]

        hydro_dist = {reach_id: {"hydro_dist_out": 999.0}}
        hw_out = {
            reach_id: {
                "best_headwater": None,
                "best_outlet": None,
                "pathlen_hw": 0,
                "pathlen_out": 0,
            }
        }
        is_mainstem = {reach_id: False}

        # Use lowercase region
        n_updated = save_to_duckdb(conn, "na", hydro_dist, hw_out, is_mainstem)

        assert n_updated == 1


class TestSaveSectionsToDuckDB:
    """Tests for save_sections_to_duckdb function."""

    @pytest.fixture
    def db_with_tables(self, writable_db):
        """Create v17c tables before testing."""
        create_v17c_tables(writable_db)
        return writable_db

    def test_inserts_sections(self, db_with_tables, sample_reach_ids):
        """Test that save_sections_to_duckdb inserts section rows."""
        conn = db_with_tables

        sections_df = pd.DataFrame(
            [
                {
                    "section_id": 0,
                    "upstream_junction": sample_reach_ids[0],
                    "downstream_junction": sample_reach_ids[5],
                    "reach_ids": sample_reach_ids[0:6],
                    "distance": 5000.0,
                    "n_reaches": 6,
                },
                {
                    "section_id": 1,
                    "upstream_junction": sample_reach_ids[5],
                    "downstream_junction": sample_reach_ids[9],
                    "reach_ids": sample_reach_ids[5:10],
                    "distance": 3000.0,
                    "n_reaches": 5,
                },
            ]
        )

        validation_df = pd.DataFrame(
            [
                {
                    "section_id": 0,
                    "slope_from_upstream": -0.001,
                    "slope_from_downstream": 0.001,
                    "direction_valid": True,
                    "likely_cause": None,
                },
                {
                    "section_id": 1,
                    "slope_from_upstream": 0.002,
                    "slope_from_downstream": -0.002,
                    "direction_valid": False,
                    "likely_cause": "potential_topology_error",
                },
            ]
        )

        save_sections_to_duckdb(conn, "NA", sections_df, validation_df)

        # Verify sections were inserted
        sections_count = conn.execute(
            "SELECT COUNT(*) FROM v17c_sections WHERE region = 'NA'"
        ).fetchone()[0]
        assert sections_count == 2

        # Verify validation records were inserted
        validation_count = conn.execute(
            "SELECT COUNT(*) FROM v17c_section_slope_validation WHERE region = 'NA'"
        ).fetchone()[0]
        assert validation_count == 2

    def test_handles_empty_sections_df(self, db_with_tables):
        """Test that save_sections_to_duckdb handles empty DataFrame gracefully."""
        conn = db_with_tables

        empty_sections = pd.DataFrame()
        empty_validation = pd.DataFrame()

        # Should not raise
        save_sections_to_duckdb(conn, "NA", empty_sections, empty_validation)

        # Verify no rows inserted
        count = conn.execute(
            "SELECT COUNT(*) FROM v17c_sections WHERE region = 'NA'"
        ).fetchone()[0]
        assert count == 0

    def test_handles_empty_validation_df(self, db_with_tables, sample_reach_ids):
        """Test that save_sections_to_duckdb handles empty validation DataFrame."""
        conn = db_with_tables

        sections_df = pd.DataFrame(
            [
                {
                    "section_id": 0,
                    "upstream_junction": sample_reach_ids[0],
                    "downstream_junction": sample_reach_ids[5],
                    "reach_ids": sample_reach_ids[0:6],
                    "distance": 5000.0,
                    "n_reaches": 6,
                }
            ]
        )

        empty_validation = pd.DataFrame()

        save_sections_to_duckdb(conn, "NA", sections_df, empty_validation)

        # Sections should be inserted
        sections_count = conn.execute(
            "SELECT COUNT(*) FROM v17c_sections WHERE region = 'NA'"
        ).fetchone()[0]
        assert sections_count == 1

        # No validation records
        validation_count = conn.execute(
            "SELECT COUNT(*) FROM v17c_section_slope_validation WHERE region = 'NA'"
        ).fetchone()[0]
        assert validation_count == 0

    def test_reach_ids_stored_as_json(self, db_with_tables, sample_reach_ids):
        """Test that reach_ids list is stored as JSON string."""
        conn = db_with_tables

        sections_df = pd.DataFrame(
            [
                {
                    "section_id": 0,
                    "upstream_junction": sample_reach_ids[0],
                    "downstream_junction": sample_reach_ids[2],
                    "reach_ids": [
                        sample_reach_ids[0],
                        sample_reach_ids[1],
                        sample_reach_ids[2],
                    ],
                    "distance": 1000.0,
                    "n_reaches": 3,
                }
            ]
        )

        empty_validation = pd.DataFrame()

        save_sections_to_duckdb(conn, "NA", sections_df, empty_validation)

        # Verify reach_ids is stored as JSON string
        reach_ids_str = conn.execute(
            "SELECT reach_ids FROM v17c_sections WHERE section_id = 0 AND region = 'NA'"
        ).fetchone()[0]

        import json

        reach_ids = json.loads(reach_ids_str)
        assert reach_ids == [
            sample_reach_ids[0],
            sample_reach_ids[1],
            sample_reach_ids[2],
        ]

    def test_region_stored_uppercase(self, db_with_tables, sample_reach_ids):
        """Test that region is stored in uppercase."""
        conn = db_with_tables

        sections_df = pd.DataFrame(
            [
                {
                    "section_id": 0,
                    "upstream_junction": sample_reach_ids[0],
                    "downstream_junction": sample_reach_ids[2],
                    "reach_ids": sample_reach_ids[0:3],
                    "distance": 1000.0,
                    "n_reaches": 3,
                }
            ]
        )

        empty_validation = pd.DataFrame()

        # Use lowercase region
        save_sections_to_duckdb(conn, "na", sections_df, empty_validation)

        # Region should be stored as uppercase
        region = conn.execute(
            "SELECT region FROM v17c_sections WHERE section_id = 0"
        ).fetchone()[0]
        assert region == "NA"

    def test_validation_columns_correct(self, db_with_tables, sample_reach_ids):
        """Test that validation columns are stored correctly."""
        conn = db_with_tables

        sections_df = pd.DataFrame(
            [
                {
                    "section_id": 0,
                    "upstream_junction": sample_reach_ids[0],
                    "downstream_junction": sample_reach_ids[5],
                    "reach_ids": sample_reach_ids[0:6],
                    "distance": 5000.0,
                    "n_reaches": 6,
                }
            ]
        )

        validation_df = pd.DataFrame(
            [
                {
                    "section_id": 0,
                    "slope_from_upstream": -0.00123,
                    "slope_from_downstream": 0.00456,
                    "direction_valid": True,
                    "likely_cause": None,
                }
            ]
        )

        save_sections_to_duckdb(conn, "NA", sections_df, validation_df)

        row = conn.execute(
            "SELECT slope_from_upstream, slope_from_downstream, direction_valid, likely_cause "
            "FROM v17c_section_slope_validation WHERE section_id = 0 AND region = 'NA'"
        ).fetchone()

        assert row[0] == pytest.approx(-0.00123)
        assert row[1] == pytest.approx(0.00456)
        assert row[2] is True
        assert row[3] is None


class TestSaveToDuckdbWithPathVars:
    """Tests for save_to_duckdb with path_vars parameter."""

    @pytest.fixture
    def db_with_v17c_columns(self, writable_db):
        """Add v17c columns to reaches table."""
        v17c_columns = [
            ("dist_out_dijkstra", "DOUBLE"),
            ("hydro_dist_out", "DOUBLE"),
            ("best_headwater", "BIGINT"),
            ("best_outlet", "BIGINT"),
            ("pathlen_hw", "DOUBLE"),
            ("pathlen_out", "DOUBLE"),
            ("is_mainstem", "BOOLEAN"),
            ("rch_id_up_main", "BIGINT"),
            ("rch_id_dn_main", "BIGINT"),
        ]
        for col_name, col_type in v17c_columns:
            try:
                writable_db.execute(
                    f"ALTER TABLE reaches ADD COLUMN {col_name} {col_type}"
                )
            except duckdb.CatalogException:
                pass
        return writable_db

    def test_path_vars_written_to_db(self, db_with_v17c_columns, sample_reach_ids):
        """Test that path_freq, stream_order, path_segs, path_order are written."""
        conn = db_with_v17c_columns
        rid = sample_reach_ids[0]

        hydro_dist = {rid: {"hydro_dist_out": 1000.0}}
        hw_out = {
            rid: {
                "best_headwater": None,
                "best_outlet": None,
                "pathlen_hw": 0,
                "pathlen_out": 0,
            }
        }
        is_mainstem = {rid: True}
        path_vars = {
            rid: {
                "path_freq": 5,
                "stream_order": 3,
                "path_segs": 42,
                "path_order": 7,
            }
        }

        n_updated = save_to_duckdb(
            conn, "NA", hydro_dist, hw_out, is_mainstem, path_vars=path_vars
        )
        assert n_updated == 1

        row = conn.execute(
            "SELECT path_freq, stream_order, path_segs, path_order "
            "FROM reaches WHERE reach_id = ?",
            [rid],
        ).fetchone()
        assert row[0] == 5
        assert row[1] == 3
        assert row[2] == 42
        assert row[3] == 7

    def test_no_path_vars_leaves_existing(self, db_with_v17c_columns, sample_reach_ids):
        """Test that omitting path_vars doesn't overwrite existing values."""
        conn = db_with_v17c_columns
        rid = sample_reach_ids[0]

        # Get original path_freq
        orig = conn.execute(
            "SELECT path_freq FROM reaches WHERE reach_id = ?", [rid]
        ).fetchone()[0]

        hydro_dist = {rid: {"hydro_dist_out": 999.0}}
        hw_out = {
            rid: {
                "best_headwater": None,
                "best_outlet": None,
                "pathlen_hw": 0,
                "pathlen_out": 0,
            }
        }
        is_mainstem = {rid: False}

        save_to_duckdb(conn, "NA", hydro_dist, hw_out, is_mainstem)

        after = conn.execute(
            "SELECT path_freq FROM reaches WHERE reach_id = ?", [rid]
        ).fetchone()[0]
        assert after == orig


class TestPropagateReachToNodes:
    """Tests for propagate_reach_to_nodes with normal, flipped, and single-node reaches."""

    @pytest.fixture
    def interpolation_db(self, tmp_path):
        """Create an in-memory DB with 3 reaches and their nodes for interpolation tests.

        Reach 100 (normal, 5 nodes): dist_out=10000, reach_length=1000
        Reach 200 (flipped, 5 nodes): dist_out=8000, reach_length=800
        Reach 300 (single-node, 1 node): dist_out=5000, reach_length=400
        """
        db_path = tmp_path / "interp_test.duckdb"
        conn = duckdb.connect(str(db_path))

        conn.execute("""
            CREATE TABLE reaches (
                reach_id BIGINT, region VARCHAR, dist_out DOUBLE,
                reach_length DOUBLE, n_nodes INTEGER,
                hydro_dist_out DOUBLE, hydro_dist_hw DOUBLE,
                dist_out_dijkstra DOUBLE, pathlen_hw DOUBLE, pathlen_out DOUBLE,
                best_headwater BIGINT, best_outlet BIGINT, subnetwork_id INTEGER
            )
        """)
        conn.execute("""
            CREATE TABLE nodes (
                node_id BIGINT, region VARCHAR, reach_id BIGINT, dist_out DOUBLE,
                node_order INTEGER, node_length DOUBLE,
                hydro_dist_out DOUBLE, hydro_dist_hw DOUBLE,
                dist_out_dijkstra DOUBLE, pathlen_hw DOUBLE, pathlen_out DOUBLE,
                best_headwater BIGINT, best_outlet BIGINT, subnetwork_id INTEGER
            )
        """)

        # Reach 100: normal, 5 nodes, each 200m long (total=1000m)
        conn.execute(
            "INSERT INTO reaches VALUES (100,'NA',10000,1000,5, 5000,3000,4500,2000,4000, 99,1,10)"
        )
        for i in range(5):
            conn.execute(
                "INSERT INTO nodes (node_id,region,reach_id,dist_out,node_order,node_length) "
                f"VALUES ({1000 + i},'NA',100,{9000 + 250 * i},{i + 1},200)"
            )

        # Reach 200: 5 nodes, each 160m long (total=800m)
        conn.execute(
            "INSERT INTO reaches VALUES (200,'NA',8000,800,5, 6000,3000,5500,2500,3500, 98,2,20)"
        )
        for i in range(5):
            conn.execute(
                "INSERT INTO nodes (node_id,region,reach_id,dist_out,node_order,node_length) "
                f"VALUES ({2000 + i},'NA',200,{7200 + 160 * i},{i + 1},160)"
            )

        # Reach 300: single-node, node_length=400 (=reach_length)
        conn.execute(
            "INSERT INTO reaches VALUES (300,'NA',5000,400,1, 2000,1500,1800,1200,800, 97,3,30)"
        )
        conn.execute(
            "INSERT INTO nodes (node_id,region,reach_id,dist_out,node_order,node_length) "
            "VALUES (3000,'NA',300,5000,1,400)"
        )

        yield conn
        conn.close()

    def _get_node(self, conn, node_id):
        """Fetch interpolated columns for a single node."""
        return conn.execute(
            "SELECT dist_out, hydro_dist_out, hydro_dist_hw, dist_out_dijkstra, "
            "pathlen_hw, pathlen_out, best_headwater, best_outlet, subnetwork_id "
            "FROM nodes WHERE node_id = ?",
            [node_id],
        ).fetchone()

    def test_normal_reach_interpolation(self, interpolation_db):
        """Node-length midpoint offset: cumsum(node_length) - 0.5*node_length."""
        from src.sword_v17c_pipeline.stages.output import propagate_reach_to_nodes

        propagate_reach_to_nodes(interpolation_db, "NA")

        # Reach 100: 5 nodes, each 200m. Midpoints: 100, 300, 500, 700, 900
        # Downstream boundary = reach_value - reach_length

        # Upstream node (order=5, midpoint=900)
        up = self._get_node(interpolation_db, 1004)
        assert up[0] == pytest.approx(10000.0)  # dist_out unchanged (v17b)
        assert up[1] == pytest.approx(4900.0)  # hdo = 5000 - 1000 + 900
        assert up[2] == pytest.approx(3100.0)  # hdw = 3000 + 1000 - 900
        assert up[3] == pytest.approx(4400.0)  # dij = 4500 - 1000 + 900
        assert up[4] == pytest.approx(1900.0)  # plhw = 2000 - 1000 + 900
        assert up[5] == pytest.approx(4100.0)  # plout = 4000 + 1000 - 900

        # Downstream node (order=1, midpoint=100)
        dn = self._get_node(interpolation_db, 1000)
        assert dn[0] == pytest.approx(9000.0)  # dist_out unchanged (v17b)
        assert dn[1] == pytest.approx(4100.0)  # hdo = 5000 - 1000 + 100
        assert dn[2] == pytest.approx(3900.0)  # hdw = 3000 + 1000 - 100
        assert dn[3] == pytest.approx(3600.0)  # dij = 4500 - 1000 + 100
        assert dn[4] == pytest.approx(1100.0)  # plhw = 2000 - 1000 + 100
        assert dn[5] == pytest.approx(4900.0)  # plout = 4000 + 1000 - 100

        # Flat-copy columns
        assert up[6] == 99  # best_headwater
        assert up[7] == 1  # best_outlet
        assert up[8] == 10  # subnetwork_id

    def test_midpoint_same_for_all_reaches(self, interpolation_db):
        """Midpoint offsets use node_order — no special flipped handling needed."""
        from src.sword_v17c_pipeline.stages.output import propagate_reach_to_nodes

        propagate_reach_to_nodes(interpolation_db, "NA")

        # Reach 200: 5 nodes, each 160m. Midpoints: 80, 240, 400, 560, 720
        # dist_out values: 7200, 7360, 7520, 7680, 7840 (from fixture: 7200+160*i)

        # Upstream node (order=5, midpoint=720)
        up = self._get_node(interpolation_db, 2004)
        assert up[0] == pytest.approx(7840.0)  # dist_out unchanged (v17b: 7200+160*4)
        assert up[1] == pytest.approx(5920.0)  # hdo = 6000 - 800 + 720
        assert up[2] == pytest.approx(3080.0)  # hdw = 3000 + 800 - 720

        # Downstream node (order=1, midpoint=80)
        dn = self._get_node(interpolation_db, 2000)
        assert dn[0] == pytest.approx(7200.0)  # dist_out unchanged (v17b)
        assert dn[1] == pytest.approx(5280.0)  # hdo = 6000 - 800 + 80
        assert dn[2] == pytest.approx(3720.0)  # hdw = 3000 + 800 - 80

        # Middle node (order=3, midpoint=400)
        mid = self._get_node(interpolation_db, 2002)
        assert mid[0] == pytest.approx(7520.0)  # dist_out unchanged (v17b: 7200+160*2)
        assert mid[1] == pytest.approx(5600.0)  # hdo = 6000 - 800 + 400
        assert mid[2] == pytest.approx(3400.0)  # hdw = 3000 + 800 - 400

    def test_single_node_centroid(self, interpolation_db):
        """Single-node: midpoint = 0.5 * node_length = 0.5 * reach_length."""
        from src.sword_v17c_pipeline.stages.output import propagate_reach_to_nodes

        propagate_reach_to_nodes(interpolation_db, "NA")

        # midpoint = 0.5 * 400 = 200
        node = self._get_node(interpolation_db, 3000)
        assert node[0] == pytest.approx(4800.0)  # dist_out = 5000 - 200 (single-node)
        assert node[1] == pytest.approx(1800.0)  # hdo = 2000 - 400 + 200
        assert node[2] == pytest.approx(1700.0)  # hdw = 1500 + 400 - 200
        assert node[3] == pytest.approx(1600.0)  # dij = 1800 - 400 + 200
        assert node[4] == pytest.approx(1000.0)  # plhw = 1200 - 400 + 200
        assert node[5] == pytest.approx(1000.0)  # plout = 800 + 400 - 200

    def test_no_negative_values(self, interpolation_db):
        """GREATEST(0, ...) clamp prevents negative node distances."""
        from src.sword_v17c_pipeline.stages.output import propagate_reach_to_nodes

        propagate_reach_to_nodes(interpolation_db, "NA")

        rows = interpolation_db.execute(
            "SELECT MIN(hydro_dist_out), MIN(hydro_dist_hw), "
            "MIN(dist_out_dijkstra), MIN(pathlen_hw), MIN(pathlen_out) "
            "FROM nodes WHERE region = 'NA'"
        ).fetchone()
        for val in rows:
            assert val >= 0, f"Negative distance found: {val}"
