# SWORD v17c Pipeline

Simplified pipeline for generating SWORD v17c from v17b with topology attributes.

## Overview

The v17c pipeline:
1. **Uses original v17b topology** - No MILP optimization (archived in `_archived/`)
2. **Computes hydrologic attributes** - Distance metrics, headwater/outlet assignments
3. **Validates with SWOT data** - Junction-level slope validation
4. **Writes to DuckDB** - Direct output via SWORDWorkflow with provenance

## Pipeline Steps

```
v17c_pipeline.py
    ├── Load v17b topology from DuckDB
    ├── Build reach-level directed graph (NetworkX)
    ├── Identify junctions (confluences, bifurcations, HW, outlets)
    ├── Compute:
    │   ├── hydro_dist_out / hydro_dist_hw
    │   ├── best_headwater / best_outlet
    │   ├── pathlen_hw / pathlen_out
    │   └── is_mainstem
    ├── Build section graph (junction-to-junction)
    ├── Validate slopes at junctions (if WSE data available)
    ├── **Automated Flow Direction Correction** (Heals legacy v17b errors)
    └── Write to sword_v17c.duckdb

## Automated Flow Direction Correction

The v17c pipeline now includes a methodical stage to detect and "heal" physically reversed reaches inherited from v17b.

### Methodology: The "Hydrological Snap"
To ensure high confidence before flipping any topology, the pipeline uses a multi-evidence scoring system:
1. **Slope Signal:** Checks if SWOT WSE data indicates uphill flow in the current direction.
2. **Hydrological Snap:** Performs a virtual flip and re-accumulates Flow Accumulation (fACC) from headwaters. If the flipped chain "snaps" perfectly into the downstream neighbor's drainage area (<20% error), confidence is boosted.
3. **DAG Validation:** Ensures flips do not introduce cycles or illegal topological structures.

Reaches with a confidence score > 80 are automatically flipped in the `reach_topology` table.
```

## New Columns Added

| Column | Type | Description |
|--------|------|-------------|
| hydro_dist_out | DOUBLE | Hydrologic distance to outlet (m) |
| hydro_dist_hw | DOUBLE | Hydrologic distance from headwater (m) |
| best_headwater | BIGINT | Upstream headwater reach ID |
| best_outlet | BIGINT | Downstream outlet reach ID |
| pathlen_hw | DOUBLE | Path length to headwater (m) |
| pathlen_out | DOUBLE | Path length to outlet (m) |
| is_mainstem | BOOLEAN | True if on mainstem path |
| swot_slope | DOUBLE | SWOT-derived slope (m/km) |
| swot_slope_se | DOUBLE | Standard error of SWOT slope |
| swot_slope_confidence | VARCHAR | R=reliable, U=unreliable |

## New Tables

### v17c_sections
Junction-to-junction sections with reach lists.

| Column | Type | Description |
|--------|------|-------------|
| section_id | INTEGER | Section identifier |
| region | VARCHAR(2) | Region code |
| upstream_junction | BIGINT | Upstream junction reach ID |
| downstream_junction | BIGINT | Downstream junction reach ID |
| reach_ids | VARCHAR | JSON array of reach IDs |
| distance | DOUBLE | Section length (m) |
| n_reaches | INTEGER | Number of reaches |

### v17c_section_slope_validation
SWOT slope validation results per section.

| Column | Type | Description |
|--------|------|-------------|
| section_id | INTEGER | Section identifier |
| region | VARCHAR(2) | Region code |
| slope_from_upstream | DOUBLE | Slope from upstream junction |
| slope_from_downstream | DOUBLE | Slope from downstream junction |
| direction_valid | BOOLEAN | True if slopes match expected |
| likely_cause | VARCHAR | lake_section, extreme_slope_data_error, potential_topology_error |

## Usage

### Command Line

```bash
# Process all regions
python -m src.sword_v17c_pipeline.v17c_pipeline \
    --db data/duckdb/sword_v17c.duckdb --all

# Process single region
python -m src.sword_v17c_pipeline.v17c_pipeline \
    --db data/duckdb/sword_v17c.duckdb --region NA

# Skip SWOT integration (faster)
python -m src.sword_v17c_pipeline.v17c_pipeline \
    --db data/duckdb/sword_v17c.duckdb --all --skip-swot

# Custom SWOT path
python -m src.sword_v17c_pipeline.v17c_pipeline \
    --db data/duckdb/sword_v17c.duckdb --region NA \
    --swot-path /path/to/swot/data
```

### Shell Script

```bash
cd src/sword_v17c_pipeline

# All regions
./run_pipeline.sh

# Single region
REGION=NA ./run_pipeline.sh

# Skip SWOT
SKIP_SWOT=1 ./run_pipeline.sh

# Custom database
DB=/path/to/sword_v17c.duckdb ./run_pipeline.sh
```

### Python API

```python
from src.sword_v17c_pipeline import run_pipeline, process_region

# Process all regions
stats = run_pipeline(
    db_path="data/duckdb/sword_v17c.duckdb",
    regions=["NA", "SA", "EU", "AF", "AS", "OC"],
    skip_swot=True,
)

# Process single region
stats = process_region(
    db_path="data/duckdb/sword_v17c.duckdb",
    region="NA",
    user_id="my_user",
    skip_swot=False,
    swot_path="/Volumes/SWORD_DATA/data/swot/RiverSP_D_parq/node",
)
```

## Pipeline Results (2026-01-27)

All 6 regions processed successfully:

| Region | Reaches | Sections | Mainstem | Direction Valid |
|--------|---------|----------|----------|-----------------|
| NA | 38,696 | 6,363 | 38,057 (98.3%) | 91.6% |
| SA | 42,159 | 7,272 | 41,342 (98.1%) | 93.2% |
| EU | 31,103 | 4,222 | 30,240 (97.2%) | 92.1% |
| AF | 21,441 | 3,137 | 20,746 (96.8%) | 93.4% |
| AS | 100,185 | 18,634 | 96,671 (96.5%) | 94.3% |
| OC | 15,090 | 2,979 | 14,899 (98.7%) | 90.3% |
| **Total** | **248,674** | **42,607** | **241,955** | **93.2%** |

## SWOT Observation Aggregation

The `reach_swot_obs.py` script computes reach-level SWOT observations from node data:

```bash
# Process all regions
python -m src.sword_v17c_pipeline.reach_swot_obs \
    --db data/duckdb/sword_v17c.duckdb --all

# Process single region
python -m src.sword_v17c_pipeline.reach_swot_obs \
    --db data/duckdb/sword_v17c.duckdb --region NA

# Dry run (compute but don't update DB)
python -m src.sword_v17c_pipeline.reach_swot_obs \
    --db data/duckdb/sword_v17c.duckdb --region NA --dry-run
```

### Outputs

| Column | Description |
|--------|-------------|
| slope_obs_mean/std/median/range | Slope statistics (m/km) from OLS regression |
| wse_obs_mean/std/median/range | WSE statistics (m) |
| width_obs_mean/std/median/range | Width statistics (m) |
| n_obs | Total node observations |
| slope_obs_n_passes | Number of cycle/pass groups for slope |
| slope_obs_q | Quality flag (bit flags for issues) |
| swot_obs_source | 'node' (indicates node-derived) |

### Coverage (2026-02-03)

| Region | Slope | WSE/Width |
|--------|-------|-----------|
| NA | 82.4% | 67.8% |
| SA | 86.2% | 82.8% |
| EU | 76.2% | 78.4% |
| AF | 87.0% | 84.6% |
| AS | 79.6% | 83.1% |
| OC | 77.3% | 78.6% |

## Verification

```bash
# Query results
duckdb data/duckdb/sword_v17c.duckdb -c "
SELECT reach_id, hydro_dist_out, best_headwater, is_mainstem
FROM reaches WHERE region='NA' AND hydro_dist_out IS NOT NULL LIMIT 10"

# Check provenance
duckdb data/duckdb/sword_v17c.duckdb -c "
SELECT * FROM sword_operations ORDER BY started_at DESC LIMIT 5"

# Run lint
python -m src.sword_duckdb.lint.cli --db data/duckdb/sword_v17c.duckdb --region NA
```

## Known Issues

### T001 dist_out Monotonicity (Issue #74)

The v17c database has 16 T001 dist_out monotonicity violations in NA region. These are **pre-existing** from previous dist_out recalculation operations, NOT caused by this pipeline.

**Symptoms:**
- Lint check `T001 dist_out_monotonicity` shows 16 errors (0.04%) in NA
- All violations are at junction reaches (bifurcations)
- v17b has 0 errors; issue was introduced during previous FIX_FACC_VIOLATIONS

**Root Cause:**
When facc was fixed, reactive recalculation modified dist_out but didn't propagate correctly through all bifurcation branches.

**Impact:**
- The v17c pipeline uses `hydro_dist_out` (newly computed, correct) instead of legacy `dist_out`
- Legacy `dist_out` inconsistencies don't affect new v17c attributes

**Fix Options:**
1. Recalculate dist_out via `workflow.calculate_dist_out(region='NA')`
2. Revert dist_out to v17b values for affected reaches

See: https://github.com/ealtenau/SWORD/issues/74

## Archived Files

The original MILP-based phi optimization has been archived in `_archived/`:
- `phi_only_global.py` - MILP flow direction optimization
- `phi_r_global_refine.py` - Phi refinement pass
- `ocn.py` - Optimal Channel Network research
- `orient_global_edges.py` - Edge orientation utilities

These were archived because v17c now uses the original v17b topology.

## Supporting Files

| File | Purpose |
|------|---------|
| `v17c_pipeline.py` | Main pipeline script |
| `run_pipeline.sh` | Shell wrapper |
| `reach_swot_obs.py` | SWOT observation aggregation (slope, wse, width from node data) |
| `SWOT_slopes.py` | SWOT section-level slope computation (standalone) |
| `SWORD_graph.py` | Graph utilities |
| `validate_topology.py` | Topology validation |
| `validate_edges.py` | Edge validation |
| `check_dag.py` | DAG property checks |
