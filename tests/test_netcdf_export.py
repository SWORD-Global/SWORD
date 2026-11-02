"""NetCDF consumer regression tests; no production data or network required."""

import importlib.util
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

import duckdb
import netCDF4 as nc
import numpy as np
import pytest

from sword_duckdb import netcdf as writer
from scripts.maintenance import export_sword as maintenance
from scripts.export import assemble_zenodo_bundle as bundle
from sword_duckdb.netcdf_schema import (
    NetCDFCompatibilityError,
    validate_file,
    SETFINDER_SOURCE_SHA256,
)
from sword_duckdb.sword_class import SWORD
from sword_duckdb.workflow import SWORDWorkflow


pytestmark = pytest.mark.export
ROOT = Path(__file__).resolve().parents[1]
CONTRACT = json.loads((ROOT / "src/sword_duckdb/netcdf_v17c_schema.json").read_text())
REGIONS = ["AF", "AS", "EU", "NA", "OC", "SA"]
R1, R2 = 21111000011, 21111000021


def _template_group(group, spec):
    sizes = {"num_reaches": 2, "num_nodes": 4, "num_points": 4}
    for name, size in spec.get("template_dimensions", spec["dimensions"]).items():
        group.createDimension(name, sizes[name] if size is None else size)
    for name, var_spec in spec["variables"].items():
        if var_spec["introduced"] != "v17b":
            continue
        dtype = var_spec["dtype"]
        fill = var_spec["fill_value"]
        if dtype != "str" and fill is not None:
            fill = np.array(fill, dtype=dtype).item()
        var = group.createVariable(
            name,
            str if dtype == "str" else dtype,
            var_spec["dimensions"],
            fill_value=fill,
        )
        if dtype == "str":
            var[:] = np.full(var.shape, "", dtype=object)
    for name, child in spec["groups"].items():
        _template_group(group.createGroup(name), child)


@pytest.fixture
def netcdf_inputs(tmp_path):
    """Real tiny DuckDB + full-schema templates with deliberately different ordering."""
    templates = tmp_path / "templates"
    templates.mkdir()
    for region in REGIONS:
        with nc.Dataset(templates / f"{region.lower()}_sword_v17b.nc", "w") as ds:
            _template_group(ds, CONTRACT["schema"])
            ds["reaches/reach_id"][:] = [R2, R1]
            ds["nodes/node_id"][:] = [202, 201, 102, 101]
            ds["nodes/reach_id"][:] = [R2, R2, R1, R1]
            ds["centerlines/cl_id"][:] = [21, 22, 11, 12]
            for name in ["x", "x_min", "x_max", "y", "y_min", "y_max"]:
                ds[f"reaches/{name}"][:] = [2.0, 1.0]
            ds["reaches/iceflag"][:] = np.tile(np.arange(366)[:, None] % 2, (1, 2))
            ds["centerlines/reach_id"][:] = [[R2, R2, R1, R1]] * 4
            ds["centerlines/node_id"][:] = [[201, 202, 101, 102]] * 4

    db_path = tmp_path / "source.duckdb"
    con = duckdb.connect(str(db_path))
    for table, specs in [
        (
            "reaches",
            writer._v17b_reach_scalar_specs() + writer._v17c_reach_scalar_specs(),
        ),
        ("nodes", writer._v17b_node_scalar_specs() + writer._v17c_node_scalar_specs()),
    ]:
        columns = {spec[3]: spec[1] for spec in specs}
        columns.update(
            {name: "str" for name in writer.VARCHAR_INT_COLS if name in columns}
        )
        columns.update(
            {
                "cl_id_min": "i8",
                "cl_id_max": "i8",
                "river_name": "str",
                "edit_flag": "str",
            }
        )
        ddl = ", ".join(
            f'"{name}" {"VARCHAR" if dtype == "str" else "DOUBLE" if dtype == "f8" else "BIGINT"}'
            for name, dtype in columns.items()
        )
        con.execute(f"CREATE TABLE {table} (region VARCHAR, {ddl})")
        for region in REGIONS:
            for i in range(2 if table == "reaches" else 4):
                r = i if table == "reaches" else i // 2
                values = {
                    name: ("fixture" if dtype == "str" else 1)
                    for name, dtype in columns.items()
                }
                values.update(
                    reach_id=[R1, R2][r],
                    cl_id_min=[11, 21][r],
                    cl_id_max=[12, 22][r],
                    facc=[100.0, 200.0][r],
                    facc_total=300.0,
                    facc_quality="anchor",
                    facc_flag="ok",
                )
                if "slope_obs_quality" in values:
                    values["slope_obs_quality"] = "reliable"
                if table == "reaches":
                    values.update(n_rch_up=r, n_rch_down=1 - r, swot_obs=2, n_nodes=2)
                else:
                    values.update(node_id=[101, 102, 201, 202][i], node_order=i % 2 + 1)
                params = [region] + [values[name] for name in columns]
                con.execute(
                    f"INSERT INTO {table} VALUES ({', '.join('?' for _ in params)})",
                    params,
                )
    con.execute(
        "CREATE TABLE centerlines (region VARCHAR, cl_id BIGINT, x DOUBLE, y DOUBLE, reach_id BIGINT, node_id BIGINT)"
    )
    con.execute(
        "CREATE TABLE reach_topology (region VARCHAR, reach_id BIGINT, direction VARCHAR, neighbor_rank INTEGER, neighbor_reach_id BIGINT)"
    )
    con.execute(
        "CREATE TABLE reach_swot_orbits (region VARCHAR, reach_id BIGINT, orbit_rank INTEGER, orbit_id BIGINT)"
    )
    for region in REGIONS:
        con.executemany(
            "INSERT INTO centerlines VALUES (?, ?, ?, ?, ?, ?)",
            [
                (region, cid, 1.0, 2.0, R1 if cid < 20 else R2, nid)
                for cid, nid in zip([11, 12, 21, 22], [101, 102, 201, 202])
            ],
        )
        con.executemany(
            "INSERT INTO reach_topology VALUES (?, ?, ?, ?, ?)",
            [(region, R1, "down", 0, R2), (region, R2, "up", 0, R1)],
        )
        con.executemany(
            "INSERT INTO reach_swot_orbits VALUES (?, ?, ?, ?)",
            [
                (region, rid, rank, orbit)
                for rid in [R1, R2]
                for rank, orbit in enumerate([42, 43])
            ],
        )
    yield con, templates, tmp_path / "output", db_path
    con.close()


@pytest.mark.parametrize("region", REGIONS)
def test_maintenance_export_is_setfinder_compatible(netcdf_inputs, region):
    con, templates, output, _ = netcdf_inputs
    path = maintenance.export_netcdf(con, region, output, v17b_nc_dir=templates)
    with nc.Dataset(path) as ds:
        reaches = ds["reaches"]
        assert {
            name: len(reaches.dimensions[name])
            for name in ["orbits", "num_domains", "num_reaches"]
        } == {"orbits": 75, "num_domains": 4, "num_reaches": 2}
        assert reaches["rch_id_up"].dimensions == ("num_domains", "num_reaches")
        assert reaches["iceflag"].shape == (366, 2)
        assert ds["centerlines/reach_id"].shape == (4, 4)
        assert ds["nodes/cl_ids"].shape == (2, 4)
        assert set(reaches.groups) == {"area_fits", "discharge_models"}
        assert reaches["reach_id"][:].tolist() == [R2, R1]
        assert reaches["facc"][:].tolist() == [200.0, 100.0]
        assert ds["nodes/node_id"][:].tolist() == [201, 202, 101, 102]


def _sword(con, region):
    # The real class entry point writes from its database connection; avoid
    # unrelated geometry loaders/migrations in this controlled boundary fixture.
    sword = SimpleNamespace(
        db=SimpleNamespace(conn=con, is_postgres=False), region=region
    )
    sword.save_nc = lambda *args, **kwargs: SWORD.save_nc(sword, *args, **kwargs)
    return sword


@pytest.mark.parametrize("entrypoint", ["canonical", "save_nc", "workflow"])
@pytest.mark.parametrize("region", REGIONS)
def test_python_entrypoints_share_the_complete_layout(
    netcdf_inputs, entrypoint, region
):
    con, templates, output, _ = netcdf_inputs
    if entrypoint == "canonical":
        path = writer.export_region(con, region, templates, output)
    elif entrypoint == "save_nc":
        path = _sword(con, region).save_nc(
            str(output / "custom.nc"), v17b_nc_dir=templates
        )
    else:
        workflow = SWORDWorkflow(enable_provenance=False)
        workflow._sword = _sword(con, region)
        workflow._region = region
        path = workflow.export(["netcdf"], output, v17b_nc_dir=templates)["netcdf"]
    validate_file(path)
    with nc.Dataset(path) as ds:
        assert ds["reaches/facc"][:].tolist() == [200.0, 100.0]
        assert ds["nodes/node_id"][:].tolist() == [201, 202, 101, 102]
        assert ds.netcdf_schema_id == CONTRACT["schema_id"]


@pytest.mark.parametrize(
    "regression,expected",
    [
        ("orbits", "/dimensions/orbits"),
        ("neighbors", "/dimensions/num_domains"),
        ("node_cl_ids", "/nodes/cl_ids"),
        ("reach_cl_ids", "/reaches/cl_ids"),
        ("iceflag", "/reaches/iceflag: expected dimensions"),
        ("centerline_reach_id", "/centerlines/reach_id: expected dimensions"),
        ("area_fits", "/reaches/area_fits: missing group"),
        ("discharge_models", "/reaches/discharge_models: missing group"),
        ("padding", "_FillValue"),
        ("dtype", "expected dtype int32"),
    ],
)
def test_rejects_october_layout_regressions(netcdf_inputs, regression, expected):
    con, templates, output, _ = netcdf_inputs
    path = writer.export_region(con, "AF", templates, output)
    with nc.Dataset(path, "r+") as ds:
        reaches = ds["reaches"]
        if regression == "orbits":
            reaches.renameDimension("orbits", "num_orbits")
        elif regression == "neighbors":
            reaches.renameDimension("num_domains", "num_rch_neighbors")
        elif regression in ["node_cl_ids", "reach_cl_ids"]:
            ds["nodes" if regression == "node_cl_ids" else "reaches"].renameVariable(
                "cl_ids", "lost_cl_ids"
            )
        elif regression == "iceflag":
            reaches.renameVariable("iceflag", "old_daily_iceflag")
            reaches.createVariable("iceflag", "i4", ("num_reaches",), fill_value=-9999)
        elif regression == "centerline_reach_id":
            ds["centerlines"].renameVariable("reach_id", "old_reach_id")
            ds["centerlines"].createVariable(
                "reach_id", "i8", ("num_points",), fill_value=-9999
            )
        elif regression in ["area_fits", "discharge_models"]:
            reaches.renameGroup(regression, f"lost_{regression}")
        elif regression == "padding":
            reaches.renameVariable("swot_orbits", "old_swot_orbits")
            reaches.createVariable(
                "swot_orbits", "i8", ("orbits", "num_reaches"), fill_value=0
            )
        elif regression == "dtype":
            reaches.renameVariable("swot_obs", "old_swot_obs")
            reaches.createVariable("swot_obs", "i8", ("num_reaches",), fill_value=-9999)
    with pytest.raises(NetCDFCompatibilityError, match=expected):
        validate_file(path)


def test_nested_dimension_cannot_hide_a_wrong_shape(tmp_path):
    """Matching dimension names must still resolve to the canonical reach count."""
    path = tmp_path / "shadowed_dimension.nc"
    with nc.Dataset(path, "w") as ds:
        reaches = ds.createGroup("reaches")
        reaches.createDimension("num_reaches", 2)
        area_fits = reaches.createGroup("area_fits")
        area_fits.createDimension("num_reaches", 1)
        area_fits.createVariable(
            "h_variance", "f8", ("num_reaches",), fill_value=-9999.0
        )
    with pytest.raises(
        NetCDFCompatibilityError,
        match="/reaches/area_fits/h_variance: expected shape",
    ):
        validate_file(path)


@pytest.mark.parametrize("region", ["AF", "AS", "SA"])
def test_incomplete_template_fails_before_any_output(netcdf_inputs, region):
    con, templates, output, _ = netcdf_inputs
    with nc.Dataset(templates / f"{region.lower()}_sword_v17b.nc", "r+") as ds:
        ds["reaches"].renameGroup("area_fits", "lost_area_fits")
    with pytest.raises(NetCDFCompatibilityError, match="area_fits: missing group"):
        maintenance.export_netcdf(con, region, output, v17b_nc_dir=templates)
    assert not output.exists()


def test_missing_template_fails_before_any_output(netcdf_inputs):
    con, templates, output, _ = netcdf_inputs
    (templates / "af_sword_v17b.nc").unlink()
    with pytest.raises(FileNotFoundError, match="Required v17b"):
        maintenance.export_netcdf(con, "AF", output, v17b_nc_dir=templates)
    assert not output.exists()


@pytest.mark.parametrize("failure", ["write", "validation"])
def test_failed_export_preserves_previous_file_and_removes_partial(
    netcdf_inputs, monkeypatch, failure
):
    con, templates, output, _ = netcdf_inputs
    path = writer.export_region(con, "AF", templates, output)
    before = path.read_bytes()
    if failure == "write":
        con.execute(
            "UPDATE nodes SET facc_quality = 'unmapped_source' WHERE region='AF'"
        )
        expected = ValueError
    else:
        # Simulate a writer regression that silently drops original subgroups.
        monkeypatch.setattr(writer, "_passthrough_v17b_subgroups", lambda *args: None)
        expected = NetCDFCompatibilityError
    with pytest.raises(expected):
        writer.export_region(con, "AF", templates, output, overwrite=True)
    assert path.read_bytes() == before
    assert not list(output.glob("*.partial"))


def test_original_template_cannot_be_overwritten(netcdf_inputs):
    con, templates, output, _ = netcdf_inputs
    path = templates / "af_sword_v17b.nc"
    before = path.read_bytes()
    with pytest.raises(ValueError, match="Cannot overwrite"):
        writer.export_region(
            con, "AF", templates, output, output_path=path, overwrite=True
        )
    assert path.read_bytes() == before


def test_save_nc_requires_original_template(netcdf_inputs):
    con, _, output, _ = netcdf_inputs
    with pytest.raises(ValueError, match="v17b_nc_dir is required"):
        _sword(con, "AF").save_nc(str(output / "partial.nc"))
    assert not output.exists()


def test_node_ids_must_match_original_template(netcdf_inputs):
    con, templates, output, _ = netcdf_inputs
    con.execute("UPDATE nodes SET node_id=999 WHERE region='AF' AND node_id=101")
    with pytest.raises(ValueError, match="Node ID set mismatch"):
        writer.export_region(con, "AF", templates, output)
    assert not list(output.glob("*.nc"))
    assert not list(output.glob("*.partial"))


def test_packaging_rejects_an_incompatible_external_overlay(netcdf_inputs):
    con, templates, output, _ = netcdf_inputs
    for region in REGIONS:
        writer.export_region(con, region, templates, output / "netcdf")
    bundle.validate_netcdf_exports(output)
    with nc.Dataset(output / "netcdf/as_sword_v17c.nc", "r+") as ds:
        ds["reaches"].renameDimension("orbits", "num_orbits")
    with pytest.raises(NetCDFCompatibilityError, match="/dimensions/orbits"):
        bundle.validate_netcdf_exports(output)


def test_packaging_fails_before_cloning_invalid_staging(netcdf_inputs):
    con, templates, output, _ = netcdf_inputs
    for region in REGIONS:
        writer.export_region(con, region, templates, output / "netcdf")
    with nc.Dataset(output / "netcdf/af_sword_v17c.nc", "r+") as ds:
        ds["reaches"].renameGroup("area_fits", "lost_area_fits")
    destination = output.parent / "bundle"
    result = subprocess.run(
        [
            sys.executable,
            "scripts/export/assemble_zenodo_bundle.py",
            "--src",
            str(output),
            "--dst",
            str(destination),
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
    )
    assert result.returncode != 0
    assert "area_fits: missing group" in result.stderr
    assert not destination.exists()


def _cli(entrypoint, db_path, templates, output, *scope):
    if entrypoint == "canonical":
        args = [
            "scripts/export/export_netcdf.py",
            "--db",
            str(db_path),
            "--v17b-dir",
            str(templates),
        ]
    else:
        args = [
            "scripts/maintenance/export_sword.py",
            "--source",
            str(db_path),
            "--format",
            "netcdf",
            "--v17b-nc-dir",
            str(templates),
        ]
    return subprocess.run(
        [sys.executable, *args, "--output-dir", str(output), *scope],
        cwd=ROOT,
        text=True,
        capture_output=True,
    )


@pytest.mark.parametrize("entrypoint", ["canonical", "maintenance"])
def test_cli_writes_only_complete_files(netcdf_inputs, entrypoint):
    con, templates, output, db_path = netcdf_inputs
    con.close()
    result = _cli(entrypoint, db_path, templates, output, "--all")
    assert result.returncode == 0, result.stderr
    paths = sorted(output.glob("*.nc"))
    assert len(paths) == 6
    for path in paths:
        validate_file(path)


@pytest.mark.parametrize("entrypoint", ["canonical", "maintenance"])
def test_cli_preflights_all_sources_before_batch(netcdf_inputs, entrypoint):
    con, templates, output, db_path = netcdf_inputs
    con.close()
    (templates / "as_sword_v17b.nc").unlink()
    result = _cli(entrypoint, db_path, templates, output, "--all")
    assert result.returncode != 0
    assert "as_sword_v17b.nc" in result.stderr
    assert not list(output.glob("*.nc"))


def test_validation_cli_reports_failure_with_nonzero_exit(netcdf_inputs):
    con, templates, output, _ = netcdf_inputs
    path = writer.export_region(con, "AF", templates, output)
    with nc.Dataset(path, "r+") as ds:
        ds["reaches"].renameDimension("orbits", "num_orbits")
    result = subprocess.run(
        [sys.executable, "scripts/export/validate_netcdf.py", str(path)],
        cwd=ROOT,
        text=True,
        capture_output=True,
    )
    assert result.returncode == 1
    report = json.loads(result.stdout)
    assert not report["files"][0]["passed"]
    assert "/dimensions/orbits" in report["files"][0]["error"]


@pytest.mark.parametrize("region", REGIONS)
def test_actual_pinned_setfinder_extraction(netcdf_inputs, region):
    source = os.environ.get("SWORD_SETFINDER_SOURCE")
    if source is None:
        pytest.skip(
            "Set SWORD_SETFINDER_SOURCE to the pinned upstream source; mandatory in CI"
        )
    assert (
        hashlib.sha256(Path(source).read_bytes()).hexdigest() == SETFINDER_SOURCE_SHA256
    )
    spec = importlib.util.spec_from_file_location("upstream_setfinder", source)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    con, templates, output, _ = netcdf_inputs
    path = maintenance.export_netcdf(con, region, output, v17b_nc_dir=templates)
    with nc.Dataset(path) as ds:
        ids, data = module.Sets({}, [], ds).extract_data_sword_continent_file()
        assert ids.tolist() == [R2, R1]
        assert data["orbits"] == 75 and data["num_domains"] == 4
        assert data["facc"].tolist() == [200.0, 100.0]
