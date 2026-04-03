"""Regression tests for scripts/maintenance/export_sword.py."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import duckdb


def _load_export_sword_module():
    module_path = Path(
        "/Users/jakegearon/projects/SWORD/scripts/maintenance/export_sword.py"
    )
    spec = importlib.util.spec_from_file_location("export_sword", module_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_query_region_orders_nodes_by_reach_then_node_order(tmp_path):
    module = _load_export_sword_module()

    db_path = tmp_path / "export_order.duckdb"
    con = duckdb.connect(str(db_path))
    try:
        con.execute(
            """
            CREATE TABLE nodes (
                node_id BIGINT,
                reach_id BIGINT,
                region VARCHAR,
                node_order INTEGER,
                x DOUBLE,
                y DOUBLE
            )
            """
        )
        con.executemany(
            "INSERT INTO nodes VALUES (?, ?, ?, ?, ?, ?)",
            [
                (2002, 20, "AF", 2, 2.0, 2.0),
                (1003, 10, "AF", 3, 1.3, 1.3),
                (2001, 20, "AF", 1, 2.1, 2.1),
                (1001, 10, "AF", 1, 1.1, 1.1),
                (1002, 10, "AF", 2, 1.2, 1.2),
            ],
        )

        df = module._query_region(con, "nodes", "AF", geom_as_wkb=False)

        assert df["reach_id"].tolist() == [10, 10, 10, 20, 20]
        assert df["node_order"].tolist() == [1, 2, 3, 1, 2]
        assert df["node_id"].tolist() == [1001, 1002, 1003, 2001, 2002]
    finally:
        con.close()


def test_query_region_orders_topology_by_rank(tmp_path):
    module = _load_export_sword_module()

    db_path = tmp_path / "export_topology.duckdb"
    con = duckdb.connect(str(db_path))
    try:
        con.execute(
            """
            CREATE TABLE reach_topology (
                reach_id BIGINT,
                region VARCHAR,
                direction VARCHAR,
                neighbor_rank INTEGER,
                neighbor_reach_id BIGINT
            )
            """
        )
        con.executemany(
            "INSERT INTO reach_topology VALUES (?, ?, ?, ?, ?)",
            [
                (10, "AF", "down", 1, 30),
                (10, "AF", "down", 0, 20),
                (10, "AF", "up", 1, 2),
                (10, "AF", "up", 0, 1),
            ],
        )

        df = module._query_region(con, "reach_topology", "AF", geom_as_wkb=False)

        assert df["direction"].tolist() == ["down", "down", "up", "up"]
        assert df["neighbor_rank"].tolist() == [0, 1, 0, 1]
    finally:
        con.close()
