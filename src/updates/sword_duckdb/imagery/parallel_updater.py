"""
Parallel SWORD Update Pipeline

Scales centerline analysis across cluster using Dask distributed.
Each reach processes independently → embarrassingly parallel.
"""

import numpy as np
import duckdb
import logging
from pathlib import Path
from typing import Optional, Iterator
from dataclasses import dataclass
import json

logger = logging.getLogger(__name__)


@dataclass
class ReachTask:
    """Single reach to process."""
    reach_id: int
    sword_wgs84: np.ndarray
    sword_ids: np.ndarray
    width_m: float
    slope: float
    n_chan: int


@dataclass
class ReachResult:
    """Result from processing one reach."""
    reach_id: int
    status: str  # 'success', 'failed', 'skipped'
    mean_drift_m: Optional[float] = None
    max_drift_m: Optional[float] = None
    pct_ok: Optional[float] = None
    pct_minor: Optional[float] = None
    pct_major: Optional[float] = None
    n_nodes: Optional[int] = None
    proposed_wgs84: Optional[np.ndarray] = None
    error: Optional[str] = None


def get_reach_tasks(
    sword_db_path: str,
    continent_id: Optional[int] = None,
    basin_id: Optional[int] = None,
    min_width_m: float = 50.0,
    min_slope: float = 1e-5,
    limit: Optional[int] = None,
) -> Iterator[ReachTask]:
    """
    Generate reach tasks from SWORD database.

    Args:
        sword_db_path: Path to sword_v17b.duckdb
        continent_id: Filter by continent (1-9)
        basin_id: Filter by basin
        min_width_m: Skip narrow reaches
        min_slope: Skip ultra-flat reaches (RivGraph fails on these)
        limit: Max reaches to process
    """
    db = duckdb.connect(sword_db_path, read_only=True)

    # Build query - filter by width and slope
    where_clauses = [
        f"r.width >= {min_width_m}",
        f"r.slope >= {min_slope}",  # Skip ultra-flat reaches
    ]
    if continent_id:
        where_clauses.append(f"CAST(r.reach_id / 10000000000 AS INT) = {continent_id}")
    if basin_id:
        where_clauses.append(f"CAST((r.reach_id / 100000000) % 100 AS INT) = {basin_id}")

    where_sql = " AND ".join(where_clauses)
    limit_sql = f"LIMIT {limit}" if limit else ""

    # Get reaches with slope and n_chan
    reaches = db.execute(f"""
        SELECT r.reach_id, r.width, r.slope, COALESCE(r.n_chan_max, 1) as n_chan
        FROM reaches r
        WHERE {where_sql}
        ORDER BY r.reach_id
        {limit_sql}
    """).fetchall()

    logger.info(f"Found {len(reaches)} reaches to process (slope >= {min_slope:.0e})")

    for reach_id, width, slope, n_chan in reaches:
        # Get centerline nodes
        nodes = db.execute("""
            SELECT cl_id, x, y FROM centerlines
            WHERE reach_id = ? ORDER BY cl_id
        """, [reach_id]).fetchall()

        if len(nodes) < 5:
            continue

        sword_nodes = np.array([(cl_id, x, y) for cl_id, x, y in nodes])

        yield ReachTask(
            reach_id=reach_id,
            sword_wgs84=sword_nodes[:, 1:3],
            sword_ids=sword_nodes[:, 0].astype(int),
            width_m=width,
            slope=slope,
            n_chan=n_chan,
        )

    db.close()


def process_reach(task: ReachTask) -> ReachResult:
    """
    Process single reach - runs on worker.

    This function must be self-contained (imports inside).
    """
    try:
        # Lazy imports - only load on worker
        from sword_duckdb.imagery import SWORDUpdater

        updater = SWORDUpdater(
            minor_drift_threshold_m=30.0,
            major_drift_threshold_m=100.0,
            corridor_buffer_factor=5.0,
            min_corridor_buffer_m=400.0,
            min_slope=1e-5,
            max_centerline_escape_pct=0.1,
            braided_corridor_factor=3.0,
        )

        result = updater.analyze_reach(
            sword_wgs84=task.sword_wgs84,
            sword_ids=task.sword_ids,
            reach_width_m=task.width_m,
            reach_id=task.reach_id,
            start_date="2023-01-01",
            end_date="2024-12-31",
            slope=task.slope,
            n_chan=task.n_chan,
        )

        return ReachResult(
            reach_id=task.reach_id,
            status='success',
            mean_drift_m=result.mean_drift,
            max_drift_m=result.max_drift,
            pct_ok=result.pct_ok,
            pct_minor=result.pct_minor,
            pct_major=result.pct_major,
            n_nodes=len(task.sword_ids),
            proposed_wgs84=result.proposed_wgs84,
        )

    except Exception as e:
        logger.error(f"Reach {task.reach_id} failed: {e}")
        return ReachResult(
            reach_id=task.reach_id,
            status='failed',
            error=str(e),
        )


def run_parallel_dask(
    sword_db_path: str,
    output_dir: str,
    scheduler: str = "processes",  # or "distributed" for cluster
    n_workers: int = 4,
    continent_id: Optional[int] = None,
    basin_id: Optional[int] = None,
    min_width_m: float = 50.0,
    limit: Optional[int] = None,
):
    """
    Run parallel processing with Dask.

    Args:
        sword_db_path: Path to SWORD database
        output_dir: Directory for results (parquet files)
        scheduler: 'processes' (local), 'distributed' (cluster)
        n_workers: Number of parallel workers
        continent_id: Optional filter
        basin_id: Optional filter
        min_width_m: Skip narrow reaches
        limit: Max reaches
    """
    import dask
    from dask import delayed
    import dask.dataframe as dd
    import pandas as pd

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # Get tasks
    tasks = list(get_reach_tasks(
        sword_db_path, continent_id, basin_id, min_width_m, limit
    ))
    logger.info(f"Processing {len(tasks)} reaches with {n_workers} workers")

    # Create delayed tasks
    delayed_results = [delayed(process_reach)(task) for task in tasks]

    # Configure scheduler
    if scheduler == "distributed":
        from dask.distributed import Client
        client = Client()  # Connect to existing cluster
        logger.info(f"Connected to cluster: {client.dashboard_link}")
    else:
        dask.config.set(scheduler=scheduler, num_workers=n_workers)

    # Execute
    results = dask.compute(*delayed_results)

    # Convert to DataFrame
    rows = []
    proposed_coords = {}

    for r in results:
        rows.append({
            'reach_id': r.reach_id,
            'status': r.status,
            'mean_drift_m': r.mean_drift_m,
            'max_drift_m': r.max_drift_m,
            'pct_ok': r.pct_ok,
            'pct_minor': r.pct_minor,
            'pct_major': r.pct_major,
            'n_nodes': r.n_nodes,
            'error': r.error,
        })

        if r.proposed_wgs84 is not None:
            proposed_coords[r.reach_id] = r.proposed_wgs84

    df = pd.DataFrame(rows)

    # Save results
    df.to_parquet(output_path / "drift_summary.parquet")
    np.savez_compressed(
        output_path / "proposed_coords.npz",
        **{str(k): v for k, v in proposed_coords.items()}
    )

    # Summary
    n_success = (df['status'] == 'success').sum()
    n_failed = (df['status'] == 'failed').sum()

    logger.info(f"Complete: {n_success} success, {n_failed} failed")
    logger.info(f"Results saved to {output_path}")

    if n_success > 0:
        success_df = df[df['status'] == 'success']
        logger.info(f"Mean drift across reaches: {success_df['mean_drift_m'].mean():.1f}m")
        logger.info(f"Reaches with >50% major drift: {(success_df['pct_major'] > 50).sum()}")

    return df


def run_parallel_slurm(
    sword_db_path: str,
    output_dir: str,
    partition: str = "normal",
    time: str = "04:00:00",
    mem_per_cpu: str = "4G",
    array_size: int = 100,
    continent_id: Optional[int] = None,
):
    """
    Generate SLURM job array script for HPC cluster.

    Creates sbatch script that processes reaches in chunks.
    """
    script = f'''#!/bin/bash
#SBATCH --job-name=sword_update
#SBATCH --partition={partition}
#SBATCH --time={time}
#SBATCH --mem-per-cpu={mem_per_cpu}
#SBATCH --array=0-{array_size - 1}
#SBATCH --output={output_dir}/logs/slurm_%A_%a.out

# Load environment
source ~/.bashrc
conda activate sword

# Calculate reach range for this task
TOTAL_REACHES=$(python -c "
import duckdb
db = duckdb.connect('{sword_db_path}', read_only=True)
count = db.execute('SELECT COUNT(*) FROM reaches WHERE width >= 50').fetchone()[0]
print(count)
")

CHUNK_SIZE=$((TOTAL_REACHES / {array_size} + 1))
START=$((SLURM_ARRAY_TASK_ID * CHUNK_SIZE))

# Run processing
python -c "
import sys
sys.path.insert(0, '/path/to/sword/src/updates')
from sword_duckdb.imagery.parallel_updater import process_chunk

process_chunk(
    sword_db_path='{sword_db_path}',
    output_dir='{output_dir}',
    start_idx=$START,
    chunk_size=$CHUNK_SIZE,
    continent_id={continent_id or 'None'},
)
"
'''

    script_path = Path(output_dir) / "submit_sword_update.sh"
    script_path.parent.mkdir(parents=True, exist_ok=True)
    script_path.write_text(script)

    logger.info(f"SLURM script written to {script_path}")
    logger.info(f"Submit with: sbatch {script_path}")

    return script_path


def process_chunk(
    sword_db_path: str,
    output_dir: str,
    start_idx: int,
    chunk_size: int,
    continent_id: Optional[int] = None,
):
    """Process a chunk of reaches (for SLURM array jobs)."""
    import pandas as pd

    output_path = Path(output_dir)

    # Get tasks for this chunk
    all_tasks = list(get_reach_tasks(sword_db_path, continent_id=continent_id))
    chunk_tasks = all_tasks[start_idx:start_idx + chunk_size]

    if not chunk_tasks:
        return

    logger.info(f"Processing chunk: {len(chunk_tasks)} reaches starting at {start_idx}")

    # Process sequentially within chunk
    results = [process_reach(task) for task in chunk_tasks]

    # Save chunk results
    rows = [{
        'reach_id': r.reach_id,
        'status': r.status,
        'mean_drift_m': r.mean_drift_m,
        'max_drift_m': r.max_drift_m,
        'pct_ok': r.pct_ok,
        'pct_minor': r.pct_minor,
        'pct_major': r.pct_major,
        'n_nodes': r.n_nodes,
        'error': r.error,
    } for r in results]

    df = pd.DataFrame(rows)
    chunk_file = output_path / f"chunk_{start_idx:08d}.parquet"
    df.to_parquet(chunk_file)

    logger.info(f"Saved {len(results)} results to {chunk_file}")


# CLI interface
if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description="Parallel SWORD Update")
    parser.add_argument("--sword-db", required=True, help="Path to SWORD DuckDB")
    parser.add_argument("--output", required=True, help="Output directory")
    parser.add_argument("--scheduler", default="processes", choices=["processes", "distributed"])
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--continent", type=int, help="Filter by continent ID (1-9)")
    parser.add_argument("--basin", type=int, help="Filter by basin ID")
    parser.add_argument("--min-width", type=float, default=50.0)
    parser.add_argument("--limit", type=int, help="Max reaches to process")
    parser.add_argument("--slurm", action="store_true", help="Generate SLURM script instead")

    args = parser.parse_args()

    if args.slurm:
        run_parallel_slurm(
            args.sword_db, args.output,
            continent_id=args.continent,
        )
    else:
        run_parallel_dask(
            args.sword_db, args.output,
            scheduler=args.scheduler,
            n_workers=args.workers,
            continent_id=args.continent,
            basin_id=args.basin,
            min_width_m=args.min_width,
            limit=args.limit,
        )
