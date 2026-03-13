#!/usr/bin/env python3
"""Export SWORD v17c DuckDB to NetCDF files matching v17b structure + new v17c columns.

Produces one file per region: {region_lower}_sword_v17c.nc

Multi-dim arrays that DuckDB normalizes are reconstructed:
  - rch_id_up/dn [4,N] from reach_topology
  - swot_orbits [75,N] from reach_swot_orbits
  - cl_ids [2,N] from cl_id_min/cl_id_max
  - iceflag [366,N] copied from v17b (DuckDB only stores scalar summary)
  - centerlines reach_id/node_id [4,N] copied from v17b (unchanged)

Usage:
    python scripts/export/export_netcdf.py --region NA
    python scripts/export/export_netcdf.py --all
    python scripts/export/export_netcdf.py --all --v17b-dir data/netcdf --output-dir data/exports
"""

from __future__ import annotations

import argparse
import hashlib
import sys
import time
from pathlib import Path

import duckdb
import netCDF4 as nc
import numpy as np

REGIONS = ["NA", "SA", "EU", "AF", "AS", "OC"]
FILL_I4 = np.int32(-9999)
FILL_I8 = np.int64(-9999)
FILL_F8 = np.float64(-9999.0)

# -- Quality/flag string-to-int mappings ------------------------------------
# CF-convention: flag_values + flag_meanings attributes document the encoding.

FACC_QUALITY_MAP = {"denoise_v3": 1}
FACC_QUALITY_FLAG_ATTRS = {
    "flag_values": np.array([1], dtype=np.int32),
    "flag_meanings": "denoise_v3",
    "comment": "1=corrected by facc denoising v3; fill_value=not flagged",
}

SLOPE_OBS_QUALITY_MAP = {
    "reliable": 0,
    "small_negative": 1,
    "moderate_negative": 2,
    "large_negative": 3,
    "negative": 4,
    "below_ref_uncertainty": 5,
    "high_uncertainty": 6,
    "noise_high_nobs": 7,
    "flat_water_noise": 8,
}
SLOPE_OBS_QUALITY_FLAG_ATTRS = {
    "flag_values": np.array([0, 1, 2, 3, 4, 5, 6, 7, 8], dtype=np.int32),
    "flag_meanings": (
        "reliable small_negative moderate_negative large_negative "
        "negative below_ref_uncertainty high_uncertainty "
        "noise_high_nobs flat_water_noise"
    ),
}

SLOPE_OBS_RELIABLE_FLAG_ATTRS = {
    "flag_values": np.array([0, 1], dtype=np.int32),
    "flag_meanings": "unreliable reliable",
    "comment": "fill_value=not computed (no SWOT slope observations)",
}

IS_MAINSTEM_EDGE_FLAG_ATTRS = {
    "flag_values": np.array([0, 1], dtype=np.int32),
    "flag_meanings": "not_mainstem mainstem",
}

SLOPE_OBS_Q_FLAG_ATTRS = {
    "flag_values": np.array([1, 2, 4, 8, 16], dtype=np.int32),
    "flag_meanings": "negative low_passes high_var extreme clipped",
    "comment": (
        "Bitfield: combine flags by addition. "
        "0=no issues. Example: 5 = negative(1) + high_var(4). "
        "fill_value=not computed (no SWOT slope observations)"
    ),
}

# Map nc_name → extra attributes for flag/quality vars
FLAG_VAR_ATTRS: dict[str, dict] = {
    "facc_quality": FACC_QUALITY_FLAG_ATTRS,
    "slope_obs_quality": SLOPE_OBS_QUALITY_FLAG_ATTRS,
    "slope_obs_reliable": SLOPE_OBS_RELIABLE_FLAG_ATTRS,
    "is_mainstem_edge": IS_MAINSTEM_EDGE_FLAG_ATTRS,
    "slope_obs_q": SLOPE_OBS_Q_FLAG_ATTRS,
}

# -- Variable spec helpers --------------------------------------------------
# Each spec: (nc_name, nc_type, fill_value, duckdb_col, attrs_dict)
# For multi-dim vars, duckdb_col is None (handled separately).


def _v17b_reach_scalar_specs():
    """Reach variables matching v17b structure (scalar, from DuckDB)."""
    return [
        ("reach_id", "i8", FILL_I8, "reach_id", {"format": "CBBBBBRRRRT"}),
        # cl_ids handled separately (multi-dim)
        ("x", "f8", FILL_F8, "x", {"units": "degrees east"}),
        ("x_min", "f8", FILL_F8, "x_min", {"units": "degrees east"}),
        ("x_max", "f8", FILL_F8, "x_max", {"units": "degrees east"}),
        ("y", "f8", FILL_F8, "y", {"units": "degrees north"}),
        ("y_min", "f8", FILL_F8, "y_min", {"units": "degrees north"}),
        ("y_max", "f8", FILL_F8, "y_max", {"units": "degrees north"}),
        ("reach_length", "f8", FILL_F8, "reach_length", {"units": "meters"}),
        ("n_nodes", "i4", FILL_I4, "n_nodes", {}),
        ("wse", "f8", FILL_F8, "wse", {"units": "meters"}),
        ("wse_var", "f8", FILL_F8, "wse_var", {"units": "meters^2"}),
        ("width", "f8", FILL_F8, "width", {"units": "meters"}),
        ("width_var", "f8", FILL_F8, "width_var", {"units": "meters^2"}),
        ("facc", "f8", FILL_F8, "facc", {"units": "km^2"}),
        ("n_chan_max", "i4", FILL_I4, "n_chan_max", {}),
        ("n_chan_mod", "i4", FILL_I4, "n_chan_mod", {}),
        ("obstr_type", "i4", FILL_I4, "obstr_type", {}),
        ("grod_id", "i8", FILL_I8, "grod_id", {}),
        ("hfalls_id", "i8", FILL_I8, "hfalls_id", {}),
        ("slope", "f8", FILL_F8, "slope", {"units": "meters/kilometers"}),
        ("dist_out", "f8", FILL_F8, "dist_out", {"units": "meters"}),
        ("n_rch_up", "i4", FILL_I4, "n_rch_up", {}),
        ("n_rch_down", "i4", FILL_I4, "n_rch_down", {}),
        # rch_id_up/dn handled separately (multi-dim from topology)
        ("lakeflag", "i4", FILL_I4, "lakeflag", {}),
        ("type", "i4", FILL_I4, "type", {}),
        # iceflag handled separately (multi-dim from v17b)
        ("swot_obs", "i4", FILL_I4, "swot_obs", {}),
        # swot_orbits handled separately (multi-dim from orbit table)
        ("max_width", "f8", FILL_F8, "max_width", {"units": "meters"}),
        ("low_slope_flag", "i4", FILL_I4, "low_slope_flag", {}),
        ("trib_flag", "i4", FILL_I4, "trib_flag", {}),
        ("path_freq", "i8", FILL_I8, "path_freq", {}),
        ("path_order", "i8", FILL_I8, "path_order", {}),
        ("path_segs", "i8", FILL_I8, "path_segs", {}),
        ("stream_order", "i4", FILL_I4, "stream_order", {}),
        ("main_side", "i4", FILL_I4, "main_side", {}),
        ("end_reach", "i4", FILL_I4, "end_reach", {}),
        ("network", "i4", FILL_I4, "network", {}),
    ]


def _v17c_reach_scalar_specs():
    """NEW v17c reach variables (scalar)."""
    return [
        ("dist_out_dijkstra", "f8", FILL_F8, "dist_out_dijkstra", {"units": "meters"}),
        ("hydro_dist_out", "f8", FILL_F8, "hydro_dist_out", {"units": "meters"}),
        ("hydro_dist_hw", "f8", FILL_F8, "hydro_dist_hw", {"units": "meters"}),
        ("rch_id_up_main", "i8", FILL_I8, "rch_id_up_main", {}),
        ("rch_id_dn_main", "i8", FILL_I8, "rch_id_dn_main", {}),
        ("subnetwork_id", "i4", FILL_I4, "subnetwork_id", {}),
        ("main_path_id", "i8", FILL_I8, "main_path_id", {}),
        ("is_mainstem_edge", "i4", FILL_I4, "is_mainstem_edge", {}),  # BOOL→i4
        ("best_headwater", "i8", FILL_I8, "best_headwater", {}),
        ("best_outlet", "i8", FILL_I8, "best_outlet", {}),
        ("pathlen_hw", "f8", FILL_F8, "pathlen_hw", {"units": "meters"}),
        ("pathlen_out", "f8", FILL_F8, "pathlen_out", {"units": "meters"}),
        ("facc_quality", "i4", FILL_I4, "facc_quality", {}),  # VARCHAR→i4
        ("dl_grod_id", "i8", FILL_I8, "dl_grod_id", {}),
        # WSE obs percentiles
        ("wse_obs_p10", "f8", FILL_F8, "wse_obs_p10", {"units": "meters"}),
        ("wse_obs_p20", "f8", FILL_F8, "wse_obs_p20", {"units": "meters"}),
        ("wse_obs_p30", "f8", FILL_F8, "wse_obs_p30", {"units": "meters"}),
        ("wse_obs_p40", "f8", FILL_F8, "wse_obs_p40", {"units": "meters"}),
        ("wse_obs_p50", "f8", FILL_F8, "wse_obs_p50", {"units": "meters"}),
        ("wse_obs_p60", "f8", FILL_F8, "wse_obs_p60", {"units": "meters"}),
        ("wse_obs_p70", "f8", FILL_F8, "wse_obs_p70", {"units": "meters"}),
        ("wse_obs_p80", "f8", FILL_F8, "wse_obs_p80", {"units": "meters"}),
        ("wse_obs_p90", "f8", FILL_F8, "wse_obs_p90", {"units": "meters"}),
        ("wse_obs_range", "f8", FILL_F8, "wse_obs_range", {"units": "meters"}),
        ("wse_obs_mad", "f8", FILL_F8, "wse_obs_mad", {"units": "meters"}),
        # Width obs percentiles
        ("width_obs_p10", "f8", FILL_F8, "width_obs_p10", {"units": "meters"}),
        ("width_obs_p20", "f8", FILL_F8, "width_obs_p20", {"units": "meters"}),
        ("width_obs_p30", "f8", FILL_F8, "width_obs_p30", {"units": "meters"}),
        ("width_obs_p40", "f8", FILL_F8, "width_obs_p40", {"units": "meters"}),
        ("width_obs_p50", "f8", FILL_F8, "width_obs_p50", {"units": "meters"}),
        ("width_obs_p60", "f8", FILL_F8, "width_obs_p60", {"units": "meters"}),
        ("width_obs_p70", "f8", FILL_F8, "width_obs_p70", {"units": "meters"}),
        ("width_obs_p80", "f8", FILL_F8, "width_obs_p80", {"units": "meters"}),
        ("width_obs_p90", "f8", FILL_F8, "width_obs_p90", {"units": "meters"}),
        ("width_obs_range", "f8", FILL_F8, "width_obs_range", {"units": "meters"}),
        ("width_obs_mad", "f8", FILL_F8, "width_obs_mad", {"units": "meters"}),
        # Slope obs percentiles + derived
        (
            "slope_obs_p10",
            "f8",
            FILL_F8,
            "slope_obs_p10",
            {"units": "meters/kilometers"},
        ),
        (
            "slope_obs_p20",
            "f8",
            FILL_F8,
            "slope_obs_p20",
            {"units": "meters/kilometers"},
        ),
        (
            "slope_obs_p30",
            "f8",
            FILL_F8,
            "slope_obs_p30",
            {"units": "meters/kilometers"},
        ),
        (
            "slope_obs_p40",
            "f8",
            FILL_F8,
            "slope_obs_p40",
            {"units": "meters/kilometers"},
        ),
        (
            "slope_obs_p50",
            "f8",
            FILL_F8,
            "slope_obs_p50",
            {"units": "meters/kilometers"},
        ),
        (
            "slope_obs_p60",
            "f8",
            FILL_F8,
            "slope_obs_p60",
            {"units": "meters/kilometers"},
        ),
        (
            "slope_obs_p70",
            "f8",
            FILL_F8,
            "slope_obs_p70",
            {"units": "meters/kilometers"},
        ),
        (
            "slope_obs_p80",
            "f8",
            FILL_F8,
            "slope_obs_p80",
            {"units": "meters/kilometers"},
        ),
        (
            "slope_obs_p90",
            "f8",
            FILL_F8,
            "slope_obs_p90",
            {"units": "meters/kilometers"},
        ),
        (
            "slope_obs_range",
            "f8",
            FILL_F8,
            "slope_obs_range",
            {"units": "meters/kilometers"},
        ),
        (
            "slope_obs_mad",
            "f8",
            FILL_F8,
            "slope_obs_mad",
            {"units": "meters/kilometers"},
        ),
        (
            "slope_obs_adj",
            "f8",
            FILL_F8,
            "slope_obs_adj",
            {"units": "meters/kilometers"},
        ),
        ("slope_obs_slopeF", "f8", FILL_F8, "slope_obs_slopeF", {}),
        ("slope_obs_reliable", "i4", FILL_I4, "slope_obs_reliable", {}),  # BOOL→i4
        ("slope_obs_quality", "i4", FILL_I4, "slope_obs_quality", {}),  # VARCHAR→i4
        ("slope_obs_n", "i4", FILL_I4, "slope_obs_n", {}),
        ("slope_obs_n_passes", "i4", FILL_I4, "slope_obs_n_passes", {}),
        ("slope_obs_q", "i4", FILL_I4, "slope_obs_q", {}),
        ("n_obs", "i4", FILL_I4, "n_obs", {}),
    ]


def _v17b_node_scalar_specs():
    """Node variables matching v17b structure (scalar, from DuckDB)."""
    return [
        ("node_id", "i8", FILL_I8, "node_id", {"format": "CBBBBBRRRRNNNT"}),
        # cl_ids handled separately
        ("x", "f8", FILL_F8, "x", {"units": "degrees east"}),
        ("y", "f8", FILL_F8, "y", {"units": "degrees north"}),
        ("node_length", "f8", FILL_F8, "node_length", {"units": "meters"}),
        ("reach_id", "i8", FILL_I8, "reach_id", {"format": "CBBBBBRRRRT"}),
        ("wse", "f8", FILL_F8, "wse", {"units": "meters"}),
        ("wse_var", "f8", FILL_F8, "wse_var", {"units": "meters^2"}),
        ("width", "f8", FILL_F8, "width", {"units": "meters"}),
        ("width_var", "f8", FILL_F8, "width_var", {"units": "meters^2"}),
        ("n_chan_max", "i4", FILL_I4, "n_chan_max", {}),
        ("n_chan_mod", "i4", FILL_I4, "n_chan_mod", {}),
        ("obstr_type", "i4", FILL_I4, "obstr_type", {}),
        ("grod_id", "i8", FILL_I8, "grod_id", {}),
        ("hfalls_id", "i8", FILL_I8, "hfalls_id", {}),
        ("dist_out", "f8", FILL_F8, "dist_out", {"units": "meters"}),
        ("wth_coef", "f8", FILL_F8, "wth_coef", {}),
        ("ext_dist_coef", "f8", FILL_F8, "ext_dist_coef", {}),
        ("facc", "f8", FILL_F8, "facc", {"units": "km^2"}),
        ("lakeflag", "i8", FILL_I8, "lakeflag", {}),  # i8 in v17b nodes
        ("max_width", "f8", FILL_F8, "max_width", {"units": "meters"}),
        ("meander_length", "f8", FILL_F8, "meander_length", {}),
        ("sinuosity", "f8", FILL_F8, "sinuosity", {}),
        ("manual_add", "i4", FILL_I4, "manual_add", {}),
        ("trib_flag", "i4", FILL_I4, "trib_flag", {}),
        ("path_freq", "i8", FILL_I8, "path_freq", {}),
        ("path_order", "i8", FILL_I8, "path_order", {}),
        ("path_segs", "i8", FILL_I8, "path_segs", {}),
        ("stream_order", "i4", FILL_I4, "stream_order", {}),
        ("main_side", "i4", FILL_I4, "main_side", {}),
        ("end_reach", "i4", FILL_I4, "end_reach", {}),
        ("network", "i4", FILL_I4, "network", {}),
    ]


def _v17c_node_scalar_specs():
    """NEW v17c node variables (scalar)."""
    return [
        ("subnetwork_id", "i4", FILL_I4, "subnetwork_id", {}),
        ("best_headwater", "i8", FILL_I8, "best_headwater", {}),
        ("best_outlet", "i8", FILL_I8, "best_outlet", {}),
        ("pathlen_hw", "f8", FILL_F8, "pathlen_hw", {"units": "meters"}),
        ("pathlen_out", "f8", FILL_F8, "pathlen_out", {"units": "meters"}),
        # WSE obs percentiles
        ("wse_obs_p10", "f8", FILL_F8, "wse_obs_p10", {"units": "meters"}),
        ("wse_obs_p20", "f8", FILL_F8, "wse_obs_p20", {"units": "meters"}),
        ("wse_obs_p30", "f8", FILL_F8, "wse_obs_p30", {"units": "meters"}),
        ("wse_obs_p40", "f8", FILL_F8, "wse_obs_p40", {"units": "meters"}),
        ("wse_obs_p50", "f8", FILL_F8, "wse_obs_p50", {"units": "meters"}),
        ("wse_obs_p60", "f8", FILL_F8, "wse_obs_p60", {"units": "meters"}),
        ("wse_obs_p70", "f8", FILL_F8, "wse_obs_p70", {"units": "meters"}),
        ("wse_obs_p80", "f8", FILL_F8, "wse_obs_p80", {"units": "meters"}),
        ("wse_obs_p90", "f8", FILL_F8, "wse_obs_p90", {"units": "meters"}),
        ("wse_obs_range", "f8", FILL_F8, "wse_obs_range", {"units": "meters"}),
        ("wse_obs_mad", "f8", FILL_F8, "wse_obs_mad", {"units": "meters"}),
        # Width obs percentiles
        ("width_obs_p10", "f8", FILL_F8, "width_obs_p10", {"units": "meters"}),
        ("width_obs_p20", "f8", FILL_F8, "width_obs_p20", {"units": "meters"}),
        ("width_obs_p30", "f8", FILL_F8, "width_obs_p30", {"units": "meters"}),
        ("width_obs_p40", "f8", FILL_F8, "width_obs_p40", {"units": "meters"}),
        ("width_obs_p50", "f8", FILL_F8, "width_obs_p50", {"units": "meters"}),
        ("width_obs_p60", "f8", FILL_F8, "width_obs_p60", {"units": "meters"}),
        ("width_obs_p70", "f8", FILL_F8, "width_obs_p70", {"units": "meters"}),
        ("width_obs_p80", "f8", FILL_F8, "width_obs_p80", {"units": "meters"}),
        ("width_obs_p90", "f8", FILL_F8, "width_obs_p90", {"units": "meters"}),
        ("width_obs_range", "f8", FILL_F8, "width_obs_range", {"units": "meters"}),
        ("width_obs_mad", "f8", FILL_F8, "width_obs_mad", {"units": "meters"}),
        ("n_obs", "i4", FILL_I4, "n_obs", {}),
        ("facc_quality", "i4", FILL_I4, "facc_quality", {}),  # VARCHAR→i4
    ]


# ---------------------------------------------------------------------------
# Data conversion helpers
# ---------------------------------------------------------------------------


def _null_to_fill(arr: np.ndarray, fill) -> np.ndarray:
    """Replace NaN/None/masked with fill value, cast to target dtype."""
    target_dtype = type(fill)
    # Handle masked arrays from DuckDB fetchnumpy()
    if isinstance(arr, np.ma.MaskedArray):
        arr = np.where(arr.mask, fill, arr.data)
    if np.issubdtype(arr.dtype, np.floating):
        mask = np.isnan(arr)
        if mask.any():
            arr = arr.copy()
            arr[mask] = fill
        return arr.astype(target_dtype)
    # For object arrays (from DuckDB NULL → None)
    if arr.dtype == object:
        out = np.full(len(arr), fill, dtype=target_dtype)
        for i, v in enumerate(arr):
            if _is_valid(v):
                out[i] = target_dtype(v)
        return out
    return arr.astype(target_dtype)


def _convert_bool_col(arr: np.ndarray) -> np.ndarray:
    """Convert boolean/object column to i4: True→1, False→0, None→-9999."""
    out = np.full(len(arr), FILL_I4, dtype=np.int32)
    for i, v in enumerate(arr):
        if not _is_valid(v):
            continue
        if v is True or v == 1:
            out[i] = 1
        elif v is False or v == 0:
            out[i] = 0
    return out


def _is_valid(v) -> bool:
    """Check if a value is valid (not None, not masked, not NaN)."""
    if v is None:
        return False
    if isinstance(v, np.ma.core.MaskedConstant):
        return False
    try:
        if np.isnan(v):
            return False
    except (TypeError, ValueError):
        pass
    return True


def _convert_varchar_col(arr: np.ndarray, mapping: dict) -> np.ndarray:
    """Convert VARCHAR column to i4 using mapping dict. Unmapped → -9999."""
    out = np.full(len(arr), FILL_I4, dtype=np.int32)
    for i, v in enumerate(arr):
        if _is_valid(v) and v in mapping:
            out[i] = mapping[v]
    return out


def _convert_string_col(arr: np.ndarray) -> np.ndarray:
    """Convert string column: None/masked → empty string."""
    out = np.empty(len(arr), dtype=object)
    for i, v in enumerate(arr):
        out[i] = str(v) if _is_valid(v) else ""
    return out


def _reorder(arr: np.ndarray, idx: np.ndarray) -> np.ndarray:
    """Reorder array using index mapping. Handles masked/object arrays."""
    if isinstance(arr, np.ma.MaskedArray):
        return arr[idx]
    if arr.dtype == object:
        return np.array([arr[i] for i in idx], dtype=object)
    return arr[idx]


# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------


def _query_table(
    con, table: str, columns: list[str], region: str, order_by: str
) -> dict:
    """Query DuckDB table and return dict of column_name → numpy array."""
    col_str = ", ".join(columns)
    rows = con.execute(
        f"SELECT {col_str} FROM {table} WHERE region = ? ORDER BY {order_by}",
        [region],
    ).fetchnumpy()
    return rows


def _build_topology_array(
    con, region: str, direction: str, num_reaches: int, reach_id_to_idx: dict
) -> np.ndarray:
    """Build [4, num_reaches] topology array from reach_topology table."""
    arr = np.full((4, num_reaches), FILL_I8, dtype=np.int64)
    rows = con.execute(
        "SELECT reach_id, neighbor_rank, neighbor_reach_id "
        "FROM reach_topology WHERE region = ? AND direction = ?",
        [region, direction],
    ).fetchall()
    for reach_id, rank, neighbor_id in rows:
        idx = reach_id_to_idx.get(reach_id)
        if idx is not None and 0 <= rank < 4:
            arr[rank, idx] = neighbor_id
    return arr


def _build_swot_orbits_array(
    con, region: str, num_reaches: int, reach_id_to_idx: dict
) -> np.ndarray:
    """Build [75, num_reaches] swot_orbits array from reach_swot_orbits table."""
    arr = np.full((75, num_reaches), FILL_I8, dtype=np.int64)
    rows = con.execute(
        "SELECT reach_id, orbit_rank, orbit_id FROM reach_swot_orbits WHERE region = ?",
        [region],
    ).fetchall()
    for reach_id, rank, orbit_id in rows:
        idx = reach_id_to_idx.get(reach_id)
        if idx is not None and 0 <= rank < 75:
            arr[rank, idx] = orbit_id
    return arr


def _build_cl_ids_array(cl_id_min: np.ndarray, cl_id_max: np.ndarray) -> np.ndarray:
    """Build [2, N] cl_ids array from cl_id_min and cl_id_max."""
    n = len(cl_id_min)
    arr = np.full((2, n), FILL_I8, dtype=np.int64)
    arr[0, :] = _null_to_fill(cl_id_min, FILL_I8)
    arr[1, :] = _null_to_fill(cl_id_max, FILL_I8)
    return arr


def _copy_nc_variable(src_var, dst_grp, dims, idx_map):
    """Copy a single NetCDF variable, optionally reordering along last axis."""
    fill = getattr(src_var, "_FillValue", None)
    dst_var = dst_grp.createVariable(src_var.name, src_var.dtype, dims, fill_value=fill)
    data = src_var[:]
    if idx_map is not None:
        data = data[..., idx_map]
    dst_var[:] = data


def _passthrough_v17b_subgroups(ds_out, ds_v17b, reorder_reach):
    """Copy area_fits and discharge_models from v17b into ds_out reaches group."""
    src_rch = ds_v17b.groups["reaches"]
    rch_grp = ds_out.groups["reaches"]

    # Build inverse index: reorder_reach[i] = DuckDB idx for output position i
    # Since we already reordered DuckDB data to v17b order, idx_map is identity
    idx_map = None  # v17b and output share the same reach order

    # --- area_fits ---
    if "area_fits" in src_rch.groups:
        src_af = src_rch.groups["area_fits"]
        dst_af = rch_grp.createGroup("area_fits")

        if "nCoeffs" not in rch_grp.dimensions:
            rch_grp.createDimension("nCoeffs", 2)
        if "nReg" not in rch_grp.dimensions:
            rch_grp.createDimension("nReg", 3)

        for vname in src_af.variables:
            src_var = src_af.variables[vname]
            out_dims = tuple(
                "num_reaches" if d == "num_reaches" else d for d in src_var.dimensions
            )
            _copy_nc_variable(src_var, dst_af, out_dims, idx_map)

        print(f"  Copied area_fits ({len(src_af.variables)} vars) from v17b")

    # --- discharge_models ---
    if "discharge_models" in src_rch.groups:
        src_dm = src_rch.groups["discharge_models"]
        dst_dm = rch_grp.createGroup("discharge_models")

        for constraint_name in src_dm.groups:
            src_cg = src_dm.groups[constraint_name]
            dst_cg = dst_dm.createGroup(constraint_name)

            for model_name in src_cg.groups:
                src_mg = src_cg.groups[model_name]
                dst_mg = dst_cg.createGroup(model_name)

                for vname in src_mg.variables:
                    src_var = src_mg.variables[vname]
                    out_dims = tuple(
                        "num_reaches" if d == "num_reaches" else d
                        for d in src_var.dimensions
                    )
                    _copy_nc_variable(src_var, dst_mg, out_dims, idx_map)

        n_models = sum(len(src_dm.groups[c].groups) for c in src_dm.groups)
        print(f"  Copied discharge_models ({n_models} models) from v17b")


# ---------------------------------------------------------------------------
# Write a single region
# ---------------------------------------------------------------------------

# Columns needing special conversion (not straight numeric cast)
BOOL_COLS = {"is_mainstem_edge", "slope_obs_reliable"}
VARCHAR_INT_COLS = {
    "facc_quality": FACC_QUALITY_MAP,
    "slope_obs_quality": SLOPE_OBS_QUALITY_MAP,
}
STRING_COLS = {"river_name", "edit_flag"}


def _write_scalar_var(grp, nc_name, nc_type, fill_value, data, attrs, dim_name):
    """Create and write a scalar variable to a NetCDF group."""
    if nc_name in STRING_COLS:
        fv = (
            "-9999.0"
            if (nc_name == "edit_flag" and dim_name == "num_reaches")
            else None
        )
        var = grp.createVariable(nc_name, str, (dim_name,), fill_value=fv)
        var._Encoding = "ascii"
        var[:] = _convert_string_col(data)
    else:
        var = grp.createVariable(nc_name, nc_type, (dim_name,), fill_value=fill_value)
        if nc_name in BOOL_COLS:
            var[:] = _convert_bool_col(data)
        elif nc_name in VARCHAR_INT_COLS:
            var[:] = _convert_varchar_col(data, VARCHAR_INT_COLS[nc_name])
        else:
            var[:] = _null_to_fill(data, fill_value)
    for attr_name, attr_val in attrs.items():
        var.setncattr(attr_name, attr_val)
    # Add flag_values/flag_meanings for encoded variables
    if nc_name in FLAG_VAR_ATTRS:
        for attr_name, attr_val in FLAG_VAR_ATTRS[nc_name].items():
            var.setncattr(attr_name, attr_val)


def export_region(
    con: duckdb.DuckDBPyConnection,
    region: str,
    v17b_dir: Path,
    output_dir: Path,
) -> Path:
    """Export a single region to NetCDF."""
    region_lower = region.lower()
    v17b_path = v17b_dir / f"{region_lower}_sword_v17b.nc"
    out_path = output_dir / f"{region_lower}_sword_v17c.nc"

    if not v17b_path.exists():
        raise FileNotFoundError(f"v17b NetCDF not found: {v17b_path}")

    print(f"\n{'=' * 60}")
    print(f"Exporting {region}...")
    print(f"  v17b source: {v17b_path}")
    print(f"  output: {out_path}")
    t0 = time.time()

    v17b = nc.Dataset(str(v17b_path), "r")

    # -- Query DuckDB reach data -------------------------------------------
    reach_scalar_specs = _v17b_reach_scalar_specs() + _v17c_reach_scalar_specs()
    reach_duckdb_cols = [s[3] for s in reach_scalar_specs]
    # Add cl_id_min/max for cl_ids reconstruction, plus string cols handled separately
    reach_query_cols = list(
        dict.fromkeys(
            reach_duckdb_cols + ["cl_id_min", "cl_id_max", "river_name", "edit_flag"]
        )
    )
    reach_data = _query_table(con, "reaches", reach_query_cols, region, "reach_id")
    num_reaches = len(reach_data["reach_id"])
    print(f"  reaches: {num_reaches}")

    # Validate reach count matches v17b
    v17b_reach_ids = v17b.groups["reaches"].variables["reach_id"][:]
    if num_reaches != len(v17b_reach_ids):
        v17b.close()
        raise ValueError(
            f"Reach count mismatch for {region}: "
            f"v17c={num_reaches}, v17b={len(v17b_reach_ids)}"
        )
    if set(reach_data["reach_id"].tolist()) != set(v17b_reach_ids.tolist()):
        v17b.close()
        raise ValueError(f"Reach ID set mismatch for {region}")

    # Build reorder index: v17b ordering is canonical.
    # DuckDB data is sorted by reach_id; v17b has its own order.
    # reorder_reach[i] = DuckDB index for v17b position i
    db_reach_idx = {int(rid): i for i, rid in enumerate(reach_data["reach_id"])}
    reorder_reach = np.array([db_reach_idx[int(rid)] for rid in v17b_reach_ids])

    # Reorder all reach data arrays to match v17b order
    reach_data = {k: _reorder(v, reorder_reach) for k, v in reach_data.items()}

    # reach_id_to_idx now maps reach_id → position in OUTPUT (v17b) order
    reach_id_to_idx = {int(rid): i for i, rid in enumerate(reach_data["reach_id"])}

    # -- Query DuckDB node data --------------------------------------------
    node_scalar_specs = _v17b_node_scalar_specs() + _v17c_node_scalar_specs()
    node_duckdb_cols = [s[3] for s in node_scalar_specs]
    node_query_cols = list(
        dict.fromkeys(
            node_duckdb_cols + ["cl_id_min", "cl_id_max", "river_name", "edit_flag"]
        )
    )
    node_data = _query_table(con, "nodes", node_query_cols, region, "node_id")
    num_nodes = len(node_data["node_id"])
    print(f"  nodes: {num_nodes}")

    # Validate node count and reorder to match v17b
    v17b_node_ids = v17b.groups["nodes"].variables["node_id"][:]
    if num_nodes != len(v17b_node_ids):
        v17b.close()
        raise ValueError(
            f"Node count mismatch for {region}: v17c={num_nodes}, v17b={len(v17b_node_ids)}"
        )
    db_node_idx = {int(nid): i for i, nid in enumerate(node_data["node_id"])}
    reorder_node = np.array([db_node_idx[int(nid)] for nid in v17b_node_ids])
    node_data = {k: _reorder(v, reorder_node) for k, v in node_data.items()}

    # -- Query DuckDB centerline data --------------------------------------
    cl_data = _query_table(con, "centerlines", ["cl_id", "x", "y"], region, "cl_id")
    num_points = len(cl_data["cl_id"])
    print(f"  centerlines: {num_points}")

    v17b_cl_ids = v17b.groups["centerlines"].variables["cl_id"][:]
    if num_points != len(v17b_cl_ids):
        v17b.close()
        raise ValueError(
            f"Centerline count mismatch for {region}: "
            f"v17c={num_points}, v17b={len(v17b_cl_ids)}"
        )
    db_cl_idx = {int(cid): i for i, cid in enumerate(cl_data["cl_id"])}
    reorder_cl = np.array([db_cl_idx[int(cid)] for cid in v17b_cl_ids])
    cl_data = {k: _reorder(v, reorder_cl) for k, v in cl_data.items()}

    # -- Build multi-dim arrays --------------------------------------------
    print("  Building multi-dim arrays...")
    rch_id_up = _build_topology_array(con, region, "up", num_reaches, reach_id_to_idx)
    rch_id_dn = _build_topology_array(con, region, "down", num_reaches, reach_id_to_idx)
    swot_orbits = _build_swot_orbits_array(con, region, num_reaches, reach_id_to_idx)
    reach_cl_ids = _build_cl_ids_array(reach_data["cl_id_min"], reach_data["cl_id_max"])
    node_cl_ids = _build_cl_ids_array(node_data["cl_id_min"], node_data["cl_id_max"])

    # Copy iceflag [366, N] from v17b
    iceflag = v17b.groups["reaches"].variables["iceflag"][:]

    # Copy centerline multi-dim arrays from v17b
    cl_reach_id = v17b.groups["centerlines"].variables["reach_id"][:]
    cl_node_id = v17b.groups["centerlines"].variables["node_id"][:]

    # -- Create NetCDF file ------------------------------------------------
    print("  Writing NetCDF...")
    root = nc.Dataset(str(out_path), "w", format="NETCDF4")

    # Root attributes - compute from centerline data
    cl_x = cl_data["x"]
    cl_y = cl_data["y"]
    valid_x = cl_x[~np.isnan(cl_x)] if np.issubdtype(cl_x.dtype, np.floating) else cl_x
    valid_y = cl_y[~np.isnan(cl_y)] if np.issubdtype(cl_y.dtype, np.floating) else cl_y
    root.x_min = float(np.min(valid_x))
    root.x_max = float(np.max(valid_x))
    root.y_min = float(np.min(valid_y))
    root.y_max = float(np.max(valid_y))
    root.Name = region
    root.production_date = time.strftime("%d-%b-%Y %H:%M:%S", time.gmtime())

    # -- Centerlines group -------------------------------------------------
    cl_grp = root.createGroup("centerlines")
    cl_grp.createDimension("num_points", num_points)
    cl_grp.createDimension("num_domains", 4)

    v = cl_grp.createVariable("cl_id", "i8", ("num_points",), fill_value=FILL_I8)
    v[:] = cl_data["cl_id"]

    v = cl_grp.createVariable("x", "f8", ("num_points",), fill_value=FILL_F8)
    v.units = "degrees east"
    v[:] = _null_to_fill(cl_data["x"], FILL_F8)

    v = cl_grp.createVariable("y", "f8", ("num_points",), fill_value=FILL_F8)
    v.units = "degrees north"
    v[:] = _null_to_fill(cl_data["y"], FILL_F8)

    # Copy [4, num_points] arrays from v17b
    v = cl_grp.createVariable(
        "reach_id", "i8", ("num_domains", "num_points"), fill_value=FILL_I8
    )
    v.format = "CBBBBBRRRRT"
    v[:] = cl_reach_id

    v = cl_grp.createVariable(
        "node_id", "i8", ("num_domains", "num_points"), fill_value=FILL_I8
    )
    v.format = "CBBBBBRRRRNNNT"
    v[:] = cl_node_id

    # -- Nodes group -------------------------------------------------------
    node_grp = root.createGroup("nodes")
    node_grp.createDimension("num_nodes", num_nodes)
    node_grp.createDimension("num_ids", 2)

    # Write all scalar node vars (v17b + v17c)
    for nc_name, nc_type, fill_val, db_col, attrs in node_scalar_specs:
        data = node_data[db_col]
        _write_scalar_var(
            node_grp, nc_name, nc_type, fill_val, data, attrs, "num_nodes"
        )

    # String vars: river_name, edit_flag (write after scalars if not already written)
    # river_name
    v = node_grp.createVariable("river_name", str, ("num_nodes",))
    v._Encoding = "ascii"
    v[:] = _convert_string_col(node_data["river_name"])
    # edit_flag
    v = node_grp.createVariable("edit_flag", str, ("num_nodes",))
    v._Encoding = "ascii"
    v[:] = _convert_string_col(node_data["edit_flag"])

    # cl_ids [2, num_nodes]
    v = node_grp.createVariable(
        "cl_ids", "i8", ("num_ids", "num_nodes"), fill_value=FILL_I8
    )
    v[:] = node_cl_ids

    # -- Reaches group -----------------------------------------------------
    rch_grp = root.createGroup("reaches")
    rch_grp.createDimension("num_reaches", num_reaches)
    rch_grp.createDimension("num_ids", 2)
    rch_grp.createDimension("num_domains", 4)
    rch_grp.createDimension("julian_day", 366)
    rch_grp.createDimension("orbits", 75)

    # Write all scalar reach vars (v17b + v17c)
    for nc_name, nc_type, fill_val, db_col, attrs in reach_scalar_specs:
        data = reach_data[db_col]
        _write_scalar_var(
            rch_grp, nc_name, nc_type, fill_val, data, attrs, "num_reaches"
        )

    # String vars: river_name, edit_flag
    v = rch_grp.createVariable("river_name", str, ("num_reaches",))
    v._Encoding = "ascii"
    v[:] = _convert_string_col(reach_data["river_name"])

    v = rch_grp.createVariable("edit_flag", str, ("num_reaches",), fill_value="-9999.0")
    v._Encoding = "ascii"
    v[:] = _convert_string_col(reach_data["edit_flag"])

    # Multi-dim arrays
    v = rch_grp.createVariable(
        "cl_ids", "i8", ("num_ids", "num_reaches"), fill_value=FILL_I8
    )
    v[:] = reach_cl_ids

    v = rch_grp.createVariable(
        "rch_id_up", "i8", ("num_domains", "num_reaches"), fill_value=FILL_I8
    )
    v[:] = rch_id_up

    v = rch_grp.createVariable(
        "rch_id_dn", "i8", ("num_domains", "num_reaches"), fill_value=FILL_I8
    )
    v[:] = rch_id_dn

    v = rch_grp.createVariable(
        "iceflag", "i4", ("julian_day", "num_reaches"), fill_value=FILL_I4
    )
    v[:] = iceflag

    v = rch_grp.createVariable(
        "swot_orbits", "i8", ("orbits", "num_reaches"), fill_value=FILL_I8
    )
    v[:] = swot_orbits

    # -- Passthrough area_fits / discharge_models from v17b ----------------
    _passthrough_v17b_subgroups(root, v17b, reorder_reach)

    root.close()
    v17b.close()

    elapsed = time.time() - t0
    print(f"  Done in {elapsed:.1f}s: {out_path}")
    return out_path


# ---------------------------------------------------------------------------
# Checksum
# ---------------------------------------------------------------------------


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(description="Export SWORD v17c DuckDB to NetCDF")
    parser.add_argument(
        "--db", default="data/duckdb/sword_v17c.duckdb", help="Path to v17c DuckDB"
    )
    parser.add_argument(
        "--v17b-dir", default="data/netcdf", help="Directory with v17b NetCDF files"
    )
    parser.add_argument("--output-dir", default="data/exports", help="Output directory")
    parser.add_argument("--region", type=str, help="Single region to export (e.g. NA)")
    parser.add_argument("--all", action="store_true", help="Export all 6 regions")
    parser.add_argument(
        "--checksums",
        action="store_true",
        default=True,
        help="Generate SHA256 checksums",
    )
    args = parser.parse_args()

    if not args.region and not args.all:
        parser.error("Specify --region or --all")

    db_path = Path(args.db)
    v17b_dir = Path(args.v17b_dir)
    output_dir = Path(args.output_dir)

    if not db_path.exists():
        print(f"Database not found: {db_path}", file=sys.stderr)
        sys.exit(1)
    if not v17b_dir.exists():
        print(f"v17b directory not found: {v17b_dir}", file=sys.stderr)
        sys.exit(1)

    output_dir.mkdir(parents=True, exist_ok=True)

    regions = REGIONS if args.all else [args.region.upper()]

    con = duckdb.connect(str(db_path), read_only=True)

    output_files: list[Path] = []
    for region in regions:
        out_path = export_region(con, region, v17b_dir, output_dir)
        output_files.append(out_path)

    con.close()

    # Generate checksums
    if args.checksums and output_files:
        checksum_path = output_dir / "SHA256SUMS.txt"
        print(f"\nGenerating checksums → {checksum_path}")
        with open(checksum_path, "w") as f:
            for p in output_files:
                digest = sha256_file(p)
                f.write(f"{digest}  {p.name}\n")
                print(f"  {digest}  {p.name}")

    print(f"\nExport complete. {len(output_files)} files in {output_dir}")


if __name__ == "__main__":
    main()
