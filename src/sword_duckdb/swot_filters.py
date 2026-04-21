"""
SWOT observation filter constants and SQL builders.

Single source of truth for all SWOT L2 RiverSP quality filters.
Used by workflow.py (aggregation) and reach_swot_obs.py (OLS slope pipeline).
"""

# ---------------------------------------------------------------------------
# Value range filters
# ---------------------------------------------------------------------------
WSE_MIN: float = -1000  # m (Dead Sea ~-430 m)
WSE_MAX: float = 10000  # m (highest navigable ~5000 m)
WIDTH_MIN: float = 0  # m (exclusive — width > 0)
WIDTH_MAX: float = 100000  # m (100 km, generous)
SLOPE_MIN: float = -1  # m/m (= -1000 m/km, garbage filter)
SLOPE_MAX: float = 1  # m/m (= 1000 m/km, garbage filter)

# ---------------------------------------------------------------------------
# Sentinel / quality thresholds
# ---------------------------------------------------------------------------
SENTINEL: float = -999_999_999_999  # SWOT fill value
DARK_FRAC_MAX: float = 0.5
XTRACK_MIN: float = 10000  # m
XTRACK_MAX: float = 60000  # m
QUALITY_MAX: int = 1  # 0=good, 1=suspect

# ---------------------------------------------------------------------------
# Slope reference uncertainty
# ---------------------------------------------------------------------------
SLOPE_REF_UNCERTAINTY: float = 0.000017  # m/m (~17 mm/km)
# Mission-spec random error scale for reach-averaged slope.
# NOT a hard detectability floor — actual uncertainty scales with reach
# length, width, node count, and cross-track position.  Used as a pragmatic
# first-pass SNR~1 threshold.


def build_node_filter_sql(colnames: set[str]) -> tuple[str, str]:
    """Build WHERE clause and WSE column name for node-level SWOT filtering.

    Parameters
    ----------
    colnames : set[str]
        Lowercase column names available in the parquet files.

    Returns
    -------
    tuple[str, str]
        (where_clause, wse_col) — ready for SQL interpolation.
    """
    wse_col = "wse" if "wse" in colnames else "wse_sm"
    conditions: list[str] = []

    # WSE filters
    conditions.append(f"{wse_col} IS NOT NULL")
    conditions.append(f"NULLIF({wse_col}, {SENTINEL}) IS NOT NULL")
    conditions.append(f"{wse_col} > {WSE_MIN} AND {wse_col} < {WSE_MAX}")
    conditions.append(f"isfinite({wse_col})")

    # Width filters
    conditions.append("width IS NOT NULL")
    conditions.append(f"NULLIF(width, {SENTINEL}) IS NOT NULL")
    conditions.append(f"width > {WIDTH_MIN} AND width < {WIDTH_MAX}")
    conditions.append("isfinite(width)")

    # WSE quality filter
    if "wse_q" in colnames:
        conditions.append(f"COALESCE(wse_q, 3) <= {QUALITY_MAX}")
    elif "wse_sm_q" in colnames:
        conditions.append(f"COALESCE(wse_sm_q, 3) <= {QUALITY_MAX}")

    # Dark water fraction
    _append_dark_frac(conditions, colnames)

    # Cross-track distance
    _append_xtrack(conditions, colnames)

    # Crossover calibration quality
    if "xovr_cal_q" in colnames:
        conditions.append(f"(xovr_cal_q <= {QUALITY_MAX} OR xovr_cal_q IS NULL)")

    # Ice climatology
    if "ice_clim_f" in colnames:
        conditions.append("ice_clim_f = 0")

    # Valid time
    if "time_str" in colnames:
        conditions.append("time_str IS NOT NULL AND time_str != ''")

    return " AND ".join(conditions), wse_col


def build_reach_filter_sql(colnames: set[str]) -> str:
    """Build WHERE clause for reach-level SWOT filtering.

    Parameters
    ----------
    colnames : set[str]
        Lowercase column names available in the parquet files.

    Returns
    -------
    str
        WHERE clause (without leading WHERE keyword).
    """
    conditions: list[str] = []

    # Basic value filters
    conditions.append("wse IS NOT NULL")
    conditions.append(f"NULLIF(wse, {SENTINEL}) IS NOT NULL")
    conditions.append(f"wse > {WSE_MIN} AND wse < {WSE_MAX}")
    conditions.append("width IS NOT NULL")
    conditions.append(f"NULLIF(width, {SENTINEL}) IS NOT NULL")
    conditions.append(f"width > {WIDTH_MIN} AND width < {WIDTH_MAX}")
    conditions.append("slope IS NOT NULL")
    conditions.append(f"NULLIF(slope, {SENTINEL}) IS NOT NULL")
    conditions.append(f"slope > {SLOPE_MIN} AND slope < {SLOPE_MAX}")
    conditions.append("isfinite(wse) AND isfinite(width) AND isfinite(slope)")

    # Reach quality filter
    if "reach_q" in colnames:
        conditions.append(f"(reach_q IS NULL OR reach_q <= {QUALITY_MAX})")

    # Dark water fraction
    _append_dark_frac(conditions, colnames)

    # Cross-track distance
    _append_xtrack(conditions, colnames)

    # Crossover calibration quality
    if "xovr_cal_q" in colnames:
        conditions.append(f"(xovr_cal_q <= {QUALITY_MAX} OR xovr_cal_q IS NULL)")

    # Ice climatology
    if "ice_clim_f" in colnames:
        conditions.append("ice_clim_f = 0")

    # Valid time
    if "time_str" in colnames:
        conditions.append("time_str IS NOT NULL AND time_str != ''")

    return " AND ".join(conditions)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _append_dark_frac(conditions: list[str], colnames: set[str]) -> None:
    if "dark_frac" in colnames and "dark_water_frac" in colnames:
        conditions.append(
            f"(COALESCE(dark_frac, dark_water_frac) <= {DARK_FRAC_MAX}"
            " OR (dark_frac IS NULL AND dark_water_frac IS NULL))"
        )
    elif "dark_frac" in colnames:
        conditions.append(f"(dark_frac <= {DARK_FRAC_MAX} OR dark_frac IS NULL)")
    elif "dark_water_frac" in colnames:
        conditions.append(
            f"(dark_water_frac <= {DARK_FRAC_MAX} OR dark_water_frac IS NULL)"
        )


def _append_xtrack(conditions: list[str], colnames: set[str]) -> None:
    xmin, xmax = XTRACK_MIN, XTRACK_MAX
    if "xtrk_dist" in colnames and "cross_track_dist" in colnames:
        conditions.append(
            f"(ABS(COALESCE(xtrk_dist, cross_track_dist)) BETWEEN {xmin} AND {xmax}"
            " OR (xtrk_dist IS NULL AND cross_track_dist IS NULL))"
        )
    elif "xtrk_dist" in colnames:
        conditions.append(
            f"(ABS(xtrk_dist) BETWEEN {xmin} AND {xmax} OR xtrk_dist IS NULL)"
        )
    elif "cross_track_dist" in colnames:
        conditions.append(
            f"(ABS(cross_track_dist) BETWEEN {xmin} AND {xmax}"
            " OR cross_track_dist IS NULL)"
        )
