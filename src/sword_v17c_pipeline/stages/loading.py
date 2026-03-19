"""Data loading stage for v17c pipeline."""

import duckdb
import pandas as pd

from ._logging import log


def load_topology(conn: duckdb.DuckDBPyConnection, region: str) -> pd.DataFrame:
    """Load reach_topology from DuckDB."""
    log(f"Loading topology for {region}...")
    df = conn.execute(
        """
        SELECT reach_id, direction, neighbor_rank, neighbor_reach_id
        FROM reach_topology
        WHERE region = ?
    """,
        [region.upper()],
    ).fetchdf()
    log(f"Loaded {len(df):,} topology rows")
    return df


def load_reaches(conn: duckdb.DuckDBPyConnection, region: str) -> pd.DataFrame:
    """Load reaches with attributes."""
    log(f"Loading reaches for {region}...")

    # Get available columns (handles older DBs without v17c columns)
    cols_result = conn.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name = 'reaches'"
    ).fetchall()
    available_cols = {row[0].lower() for row in cols_result}

    # Core columns (required)
    core_cols = [
        "reach_id",
        "region",
        "reach_length",
        "width",
        "slope",
        "facc",
        "n_rch_up",
        "n_rch_down",
        "dist_out",
        "path_freq",
        "stream_order",
        "lakeflag",
        "trib_flag",
    ]

    # Optional columns (v17c additions)
    optional_cols = [
        "wse",
        "wse_obs_p50",
        "width_obs_p50",
        "n_obs",
        "main_side",
        "type",
        "end_reach",
        "path_order",
        "path_segs",
    ]

    # Build column list
    select_cols = [c for c in core_cols if c.lower() in available_cols]
    select_cols += [c for c in optional_cols if c.lower() in available_cols]

    df = conn.execute(
        f"""
        SELECT {", ".join(select_cols)}
        FROM reaches
        WHERE region = ?
    """,
        [region.upper()],
    ).fetchdf()
    log(f"Loaded {len(df):,} reaches")
    return df


def run_facc_corrections(db_path: str, v17b_path: str, region: str) -> int:
    """
    Detect and correct facc anomalies using the biphase denoise pipeline.

    This opens its own DuckDB connection internally, so the caller must
    close any existing write connection before calling this function.

    Parameters
    ----------
    db_path : str
        Path to sword_v17c.duckdb.
    v17b_path : str
        Path to sword_v17b.duckdb (read-only baseline).
    region : str
        Region code (e.g. 'NA').

    Returns
    -------
    int
        Number of corrections applied.
    """
    from sword_duckdb.facc_detection.correct_facc_denoise import correct_facc_denoise

    log(f"Running biphase facc denoise for {region}...")
    corrections_df = correct_facc_denoise(
        db_path=db_path,
        v17b_path=v17b_path,
        region=region,
        dry_run=False,
    )
    n = len(corrections_df)
    log(f"Facc corrections applied: {n:,}")
    return n
