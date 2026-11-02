"""Independent published NetCDF contract and setfinder consumer checks.

The contract was captured from the checksum-verified November 2026 release,
not generated from writer specifications. Added variables are allowed; existing
names, dimensions, shapes, types, fill values and subgroups must survive.
"""

import json
from pathlib import Path

import netCDF4 as nc
import numpy as np


_CONTRACT = json.loads((Path(__file__).parent / "netcdf_v17c_schema.json").read_text())
SCHEMA_ID = _CONTRACT["schema_id"]
SETFINDER_COMMIT = "d0b30ca7276ebf01137521828152f4632ec0c822"
SETFINDER_SOURCE_SHA256 = (
    "114bda32952aac68a605f805a9445cd9c8464eceebce17601322699c406f932e"
)
SETFINDER_DIMENSIONS = ("orbits", "num_domains", "num_reaches")
SETFINDER_FIELDS = (
    "reach_id",
    "facc",
    "n_rch_up",
    "n_rch_down",
    "rch_id_up",
    "rch_id_dn",
    "swot_obs",
    "swot_orbits",
)


class NetCDFCompatibilityError(ValueError):
    """A file cannot be accepted as a published-layout SWORD NetCDF."""


def _check_group(group, spec, errors, *, template, dimension_sizes=None):
    prefix = group.path.rstrip("/")
    sizes = dict(dimension_sizes or {})
    dimensions = (
        spec.get("template_dimensions", spec["dimensions"])
        if template
        else spec["dimensions"]
    )
    for name, expected in dimensions.items():
        path = f"{prefix}/dimensions/{name}"
        if name not in group.dimensions:
            errors.append(f"{path}: missing dimension")
            continue
        size = len(group.dimensions[name])
        sizes[name] = size if expected is None else expected
        if expected is not None and size != expected:
            errors.append(f"{path}: expected size {expected}, got {size}")
        elif expected is None and size == 0:
            errors.append(f"{path}: empty entity dimension")

    for name, expected in spec["variables"].items():
        if template and expected["introduced"] != "v17b":
            continue
        path = f"{prefix}/{name}"
        if name not in group.variables:
            errors.append(f"{path}: missing variable")
            continue
        var = group.variables[name]
        dimensions = tuple(expected["dimensions"])
        if var.dimensions != dimensions:
            errors.append(
                f"{path}: expected dimensions {dimensions}, got {var.dimensions}"
            )
        elif all(name in sizes for name in dimensions):
            shape = tuple(sizes[name] for name in dimensions)
            if var.shape != shape:
                errors.append(f"{path}: expected shape {shape}, got {var.shape}")
        dtype = "str" if var.dtype is str else np.dtype(var.dtype).name
        if dtype != expected["dtype"]:
            errors.append(f"{path}: expected dtype {expected['dtype']}, got {dtype}")
        fill = getattr(var, "_FillValue", None)
        fill = None if fill is None else str(fill)
        if fill != expected["fill_value"]:
            errors.append(
                f"{path}: expected _FillValue {expected['fill_value']!r}, got {fill!r}"
            )

    for name, child in spec["groups"].items():
        if name not in group.groups:
            errors.append(f"{prefix}/{name}: missing group")
        else:
            _check_group(
                group.groups[name],
                child,
                errors,
                template=template,
                dimension_sizes=sizes,
            )


def _raise_errors(errors, label):
    if errors:
        raise NetCDFCompatibilityError(
            f"{label} violates {SCHEMA_ID}:\n" + "\n".join(errors)
        )


def validate_template(dataset):
    """Require every original field before querying or writing the export."""
    errors = []
    _check_group(dataset, _CONTRACT["schema"], errors, template=True)
    _raise_errors(errors, f"Original template {dataset.filepath()}")


def validate_templates(directory, regions):
    """Preflight every requested original source before a batch writes files."""
    if directory is None:
        raise ValueError("A complete v17b NetCDF directory is required")
    for region in regions:
        path = Path(directory) / f"{region.lower()}_sword_v17b.nc"
        if not path.is_file():
            raise FileNotFoundError(f"Required v17b NetCDF template not found: {path}")
        with nc.Dataset(path, "r") as dataset:
            validate_template(dataset)


def check_setfinder_reader(dataset):
    """Exercise the dimension/array access used by the pinned setfinder reader.

    CI also loads the unchanged upstream method at SETFINDER_COMMIT. Keeping
    the production access check local avoids a network dependency on export.
    """
    reaches = dataset["reaches"]
    for name in SETFINDER_DIMENSIONS:
        reaches.dimensions[name].size
    for name in SETFINDER_FIELDS:
        reaches[name][:]


def validate_file(path):
    """Validate an existing NetCDF; raise on any schema or consumer failure."""
    path = Path(path)
    with nc.Dataset(path, "r") as dataset:
        errors = []
        _check_group(dataset, _CONTRACT["schema"], errors, template=False)
        _raise_errors(errors, str(path))
        try:
            check_setfinder_reader(dataset)
        except (KeyError, IndexError, TypeError, ValueError, RuntimeError) as error:
            raise NetCDFCompatibilityError(
                f"{path}: setfinder extraction failed: {error}"
            ) from error
