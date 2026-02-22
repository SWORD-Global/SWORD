# SWORD Project Instructions

## Project Overview

**SWORD (SWOT River Database)** - Global hydrological river network database re-engineered from NetCDF to DuckDB.

- **66.9M centerlines**, **11.1M nodes**, **248.7K reaches**
- **6 regions**: NA, SA, EU, AF, AS, OC
- **Database**: ~10 GB (v17b), ~11 GB (v17c)
- **Website**: https://www.swordexplorer.com/

## Architecture

```
SWORDWorkflow (ALWAYS use this - main entry point)
    ├── SWORD class (DuckDB-backed data access)
    ├── SWORDReactive (auto-recalculation of derived attrs)
    ├── ProvenanceLogger (audit trail + rollback)
    ├── ReconstructionEngine (rebuild from source data)
    └── ImageryPipeline (satellite water mask integration)
```

## PostgreSQL Backend

**Prerequisites:** PostgreSQL 12+, PostGIS extension

**Connection String Format:**
```
postgresql://user:password@host:port/database
```

**Environment Variables (set in `.env`, never commit):**
| Variable | Description |
|----------|-------------|
| `SWORD_PRIMARY_BACKEND` | `duckdb` or `postgres` |
| `SWORD_POSTGRES_URL` | Full connection string |
| `SWORD_DUCKDB_PATH` | Path to DuckDB file (when using duckdb) |

**Quick Start - Load DuckDB to PostgreSQL:**
```bash
# Copy .env.example to .env and set SWORD_POSTGRES_URL
cp .env.example .env

# Load all regions from DuckDB to PostgreSQL
# (auto-overwrites reach geom with v17b originals for endpoint connectivity)
python scripts/maintenance/load_from_duckdb.py --source data/duckdb/sword_v17c.duckdb --all

# Load single region
python scripts/maintenance/load_from_duckdb.py --source data/duckdb/sword_v17c.duckdb --region NA

# Skip v17b geometry overwrite (if v17b PG table not available)
python scripts/maintenance/load_from_duckdb.py --source data/duckdb/sword_v17c.duckdb --all --skip-v17b-geom

# Verify load
python scripts/maintenance/load_from_duckdb.py --verify
```

**Notes:**
- PostgreSQL enables multi-user access, web APIs, and spatial indexing via PostGIS
- DuckDB remains the primary development backend (faster local queries)
- Keep connection strings in `.env` - never commit credentials
- Reach geometries in PostgreSQL come from v17b (`postgres.sword_reaches_v17b`) — they include endpoint overlap vertices so adjacent reaches connect visually. DuckDB geometries (from NetCDF) lack these overlap points.

## ⚠️ CRITICAL: Database Handling Rules

| Database | Purpose | Editable? |
|----------|---------|-----------|
| `sword_v17b.duckdb` | **READ-ONLY reference baseline** | **NEVER modify** |
| `sword_v17c.duckdb` | Working database for all edits | Yes |

**v17b is the pristine reference for comparison.** If v17b gets corrupted, rebuild from NetCDF:
```bash
python scripts/maintenance/rebuild_v17b.py  # Rebuilds from data/netcdf/*.nc
```

**All topology fixes, facc corrections, and experimental changes go to v17c only.**

## Key Directories

```
src/
  sword_duckdb/           # Core module - workflow, schema, validation
    imagery/              # Satellite water detection (NDWI, ML4Floods, OPERA)
    lint/                 # Lint framework (61 checks)
  sword_v17c_pipeline/    # v17b→v17c topology enhancement (phi algorithm)
  _legacy/                # Archived pre-DuckDB code (see _legacy/README.md)
    updates/              # Old updates module (delta_updates, mhv_sword)
    development/          # Original development scripts

scripts/
  topology/               # Topology recalculation scripts
  visualization/          # Visualization and presentation scripts
  analysis/               # Comparison and analysis scripts
  maintenance/            # Database rebuild, import, and setup scripts
  sql/                    # SQL utility scripts

deploy/
  reviewer/               # Streamlit topology/lake reviewer app

data/
  duckdb/
    sword_v17b.duckdb     # ⚠️ READ-ONLY REFERENCE - never modify! (9.9 GB)
    sword_v17c.duckdb     # Working database for edits (11 GB)
  netcdf/                 # Legacy source files (rebuild v17b from these)

tests/sword_duckdb/
  fixtures/sword_test_minimal.duckdb  # Test DB (8.26 MB, 100 reaches)
```

## Usage

**ALWAYS use SWORDWorkflow:**

```python
from sword_duckdb import SWORDWorkflow

workflow = SWORDWorkflow(user_id="jake")
# IMPORTANT: Use v17c for modifications, v17b is READ-ONLY reference
sword = workflow.load('data/duckdb/sword_v17c.duckdb', 'NA')

# Modify with provenance
workflow.modify_reach(reach_id, wse=45.5, reason="field correction")

# Recalculate topology
workflow.calculate_dist_out()
workflow.recalculate_stream_order()
workflow.recalculate_path_segs()

# Query history
workflow.get_history(entity_type='reach', entity_id=123)

# Export
workflow.export(formats=['geopackage'], output_dir='outputs/')
workflow.close()
```

## Database Schema

**Core Tables:**
- **centerlines** - PK: (cl_id, region) - river path points
- **nodes** - PK: (node_id, region) - measurement points at ~200m intervals
  - `node_order`: 1-based position within reach (1=downstream, n=upstream, by dist_out)
- **reaches** - PK: reach_id - river segments between junctions
  - `dn_node_id`, `up_node_id`: downstream/upstream boundary node IDs (by dist_out, not node_id)

**Topology:**
- **reach_topology** - upstream/downstream neighbors, normalized from NetCDF [4,N] arrays
  - `direction`: 'up' or 'down'
  - `neighbor_rank`: 0-3 (up to 4 neighbors per direction)
  - `neighbor_reach_id`: the neighboring reach
  - `topology_suspect`, `topology_approved`: manual review workflow flags
- **reach_swot_orbits** - SWOT satellite coverage
- **reach_ice_flags** - daily ice presence (366 days)

**Note:** In NetCDF, `rch_id_up`/`rch_id_dn` are [4, num_reaches] arrays. In DuckDB, these are normalized into the `reach_topology` table. The `rch_id_up_1-4` columns seen in some contexts are reconstructed on-demand, not stored.

**Provenance:**
- **sword_operations** - audit trail
- **sword_value_snapshots** - old/new values for rollback

## Version History

### v17 (October 2024) - UNC Official Release
**New topology variables added:**
- `path_freq`, `path_order`, `path_segs` - traversal-based path analysis
- `main_side` - 0=main, 1=side channel, 2=secondary outlet
- `stream_order` - log scale of path_freq
- `end_reach` - 0=middle, 1=headwater, 2=outlet, 3=junction
- `network` - connected component ID
- `rch_id_up`, `rch_id_dn` - [4,N] arrays in NetCDF → normalized to `reach_topology` table

### v17b (March 2025) - Bug Fixes
- Type changes for 1662 reaches
- Node length corrections (<2% globally)

### v17c (Our Additions) - Computed by Us
**New variables we compute via `v17c_pipeline.py`:**
- `hydro_dist_out`, `hydro_dist_hw` - Dijkstra-based distances
- `best_headwater`, `best_outlet` - width-prioritized endpoints
- `pathlen_hw`, `pathlen_out` - cumulative path lengths
- `is_mainstem_edge`, `main_path_id` - mainstem identification
- `dist_out_short` - shortest-path distance to any outlet
- `rch_id_up_main`, `rch_id_dn_main` - main neighbor selection
- `subnetwork_id` - connected component (matches `network`)
- `*_obs_mean/median/std/range`, `n_obs` - SWOT observation aggregations

**New tables:**
- `v17c_sections` - junction-to-junction segments
- `v17c_section_slope_validation` - slope direction validation

## Key Attributes

### v17b Attributes (from UNC)

| Attribute | Description |
|-----------|-------------|
| dist_out | Distance to outlet (m) - decreases downstream |
| facc | Flow accumulation (km²) from MERIT Hydro |
| stream_order | Log scale of path_freq: `round(log(path_freq)) + 1` |
| path_freq | Traversal count - increases toward outlets |
| path_segs | Unique ID for (path_order, path_freq) combo |
| lakeflag | 0=river, 1=lake, 2=canal, 3=tidal (physical water body from GRWL) |
| type | 1=river, 3=lake_on_river, 4=dam, 5=unreliable, 6=ghost (**NO type=2; type=3 is NOT tidal**) |
| trib_flag | 0=no tributary, 1=MHV tributary enters (spatial proximity) |
| n_rch_up/down | Count of upstream/downstream neighbors |
| main_side | 0=main (95%), 1=side (3%), 2=secondary outlet (2%) |
| end_reach | 0=middle, 1=headwater, 2=outlet, 3=junction |
| network | Connected component ID |

### v17c Attributes (computed by us)

| Attribute | Description |
|-----------|-------------|
| hydro_dist_out | Dijkstra distance to nearest outlet (m) |
| hydro_dist_hw | Max distance from any headwater (m) |
| best_headwater | Width-prioritized upstream headwater reach_id |
| best_outlet | Width-prioritized downstream outlet reach_id |
| is_mainstem_edge | TRUE if on mainstem path |
| rch_id_up_main | Main upstream neighbor (mainstem-preferred) |
| rch_id_dn_main | Main downstream neighbor (mainstem-preferred) |

## ⚠️ CRITICAL: Reconstruction Rules

**NEVER assume variable semantics from names.** Past bugs from guessing:

| Variable | Wrong assumption | Actual meaning |
|----------|------------------|----------------|
| trib_flag | "1 if n_rch_up > 1" (junction) | External MHV tributary enters (spatial proximity) |
| main_side | "1=main, 2=side" | **0**=main (95%), 1=side (3%), 2=secondary outlet (2%) |
| type=3 | "tidal_river" | **lake_on_river** (tidal is lakeflag=3, not type=3). lakeflag=1+type=3 is the primary lake combo (21k reaches). type=2 does NOT exist in SWORD. |

**Before implementing ANY reconstruction:**

1. **Query v17b first** - see actual value distribution:
   ```sql
   SELECT variable, COUNT(*) FROM reaches GROUP BY 1 ORDER BY 2 DESC;
   ```

2. **Check if your logic matches** - if 95% have value X, your code better produce X mostly

3. **Find original source code** - check `src/_legacy/development/` for original algorithms

4. **Check validation specs** - `docs/validation_specs/` has deep documentation

5. **When in doubt, make it a STUB** - preserve existing values rather than corrupt data

**Validation specs (28 total in `docs/validation_specs/`):**

| Spec | Variables Covered |
|------|-------------------|
| dist_out | dist_out algorithm, failure modes |
| facc | facc, MERIT Hydro source, D8 limitations |
| wse | wse, wse_var, elevation data |
| width_slope | width, width_var, slope, max_width |
| path_freq | path_freq, path_order algorithm |
| stream_order_path_segs | stream_order, path_segs derivation |
| end_reach_trib_flag | end_reach, trib_flag (MHV-based) |
| main_side_network | main_side, network |
| lakeflag_type | lakeflag, type classification |
| reach_length_neighbor_count | reach_length, n_rch_up, n_rch_down |
| obstruction | obstr_type, grod_id, hfalls_id |
| flags | iceflag, low_slope_flag, add_flag, edit_flag |
| channel_count | n_chan_max, n_chan_mod |
| swot_observations | swot_obs, *_obs_mean/median/std/range, n_obs |
| grades_discharge | ~~h_variance, w_variance~~ (removed in v17c) |
| v17c_mainstem | hydro_dist_out/hw, best_headwater/outlet, pathlen_*, is_mainstem_edge |
| v17c_path_topology | main_path_id, dist_out_short, rch_id_up/dn_main |
| v17c_sections | v17c_sections table, section_slope_validation |
| reach_neighbor_ids | rch_id_up_1-4, rch_id_dn_1-4 (reconstructed from topology) |
| topology_review_flags | topology_suspect, topology_approved |
| facc_quality | facc_quality flag |
| geometry_metadata | x, y, x_min/max, y_min/max, cl_id_min/max |
| n_nodes | n_nodes (node count per reach) |
| river_name | river_name (GRWL source) |
| subnetwork_id | subnetwork_id (connected components) |
| identifier_metadata | reach_id, region, version |
| geom | geom (LINESTRING geometry) |
| swot_slope | (REMOVED - documented for history) |

## v17c Pipeline

**Location:** `src/sword_v17c_pipeline/`

**Steps:**
1. Load v17b topology from DuckDB
2. Build reach-level directed graph (NetworkX DiGraph)
3. Compute v17c attributes (hydro_dist_out, best_headwater, is_mainstem_edge, etc.)
4. Create junction-to-junction sections
5. Save to DuckDB

**New columns added to reaches:**
- `hydro_dist_out`, `hydro_dist_hw` - Dijkstra distances
- `best_headwater`, `best_outlet` - endpoint selection
- `pathlen_hw`, `pathlen_out` - cumulative path lengths
- `is_mainstem_edge`, `main_path_id` - mainstem identification
- `dist_out_short` - shortest-path distance
- `rch_id_up_main`, `rch_id_dn_main` - main neighbor IDs

**New tables:** `v17c_sections`, `v17c_section_slope_validation`

**Input:** sword_v17c.duckdb (reads topology, writes attributes)

**Run:**
```bash
# All regions
python -m src.sword_v17c_pipeline.v17c_pipeline --db data/duckdb/sword_v17c.duckdb --all

# Single region
python -m src.sword_v17c_pipeline.v17c_pipeline --db data/duckdb/sword_v17c.duckdb --region NA

# Skip SWOT (faster)
python -m src.sword_v17c_pipeline.v17c_pipeline --db data/duckdb/sword_v17c.duckdb --all --skip-swot
```

**Note:** MILP optimization files archived in `_archived/` - v17c uses original v17b topology.

## Known Issues

| Issue | Workaround |
|-------|------------|
| **RTREE index segfault** | Drop index → UPDATE → Recreate index. See RTREE Update Pattern below. |
| **Region case sensitivity** | DuckDB=uppercase (NA), pipeline=lowercase (na) |
| **Lake sandwiches** | 1,252 corrected (wide + shorter than ≥1 lake neighbor) → lakeflag=1, tagged `edit_flag='lake_sandwich'`. ~1,755 remaining (narrow connecting channels, chains). See issues #18/#19 |
| **DuckDB lock contention** | Only one write connection at a time. Kill Streamlit/other processes before UPDATE. |
| **end_reach divergence from v17b** | v17c recomputed end_reach from topology: junction=3 when n_up>1 OR n_down>1. ~30k v17b "phantom junctions" (n_up=1, n_dn=1, end_reach=3) relabeled to 0. UNC's original junction criterion is unknown. See `docs/validation_specs/end_reach_trib_flag_validation_spec.md` Section 1.8. |
| **reconstruction.py end_reach bug** | `_reconstruct_reach_end_reach` uses `n_up > 1` only (misses bifurcations). `reactive.py` has the correct logic (`n_up > 1 OR n_down > 1`). Don't use the reconstruction function without fixing it. |
| **DuckDB reach geometry missing endpoint overlap** | DuckDB geometries (rebuilt from NetCDF) lack the overlap vertices at endpoints that make adjacent reaches visually connect. The v17b PostgreSQL table (`postgres.sword_reaches_v17b`) has the full-fidelity geometries. `scripts/maintenance/load_from_duckdb.py` auto-copies v17b geometries to v17c PostgreSQL via dblink (`--skip-v17b-geom` to disable). |
| **path_freq=0/-9999 on connected reaches** | 4,952 connected non-ghost reaches globally have invalid path_freq (34 with 0, 4,918 with -9999). 91% are 1:1 links (fixable by propagation), 9% are junctions (need full traversal). AS has 2,478. See issue #16. |

## Column Name Gotchas

DuckDB column names that are easy to get wrong:

| Wrong | Correct | Table |
|-------|---------|-------|
| `n_rch_dn` | `n_rch_down` | reaches |
| `timestamp` | `started_at` / `completed_at` | sword_operations |
| `description` | `reason` | sword_operations |

**`sword_operations` schema:** `operation_id` (NOT auto-increment — must provide), `operation_type`, `table_name`, `entity_ids` (BIGINT[]), `region`, `user_id`, `session_id`, `started_at`, `completed_at`, `operation_details` (JSON), `affected_columns` (VARCHAR[]), `reason`, `source_operation_id`, `status` (default 'PENDING'), `error_message`, `before_checksum`, `after_checksum`

## RTREE Update Pattern

DuckDB cannot UPDATE tables with RTREE indexes without loading the spatial extension first and dropping/recreating indexes:

```python
con.execute('INSTALL spatial; LOAD spatial;')
# 1. Find RTREE indexes
indexes = con.execute("SELECT index_name, table_name, sql FROM duckdb_indexes() WHERE sql LIKE '%RTREE%'").fetchall()
# 2. Drop them
for idx_name, tbl, sql in indexes:
    con.execute(f'DROP INDEX "{idx_name}"')
# 3. Do your UPDATEs
con.execute('UPDATE reaches SET ...')
# 4. Recreate indexes
for idx_name, tbl, sql in indexes:
    con.execute(sql)
```

## Reactive Recalculation

Dependency graph auto-recalculates derived attributes:
- Geometry changes → reach_length, sinuosity
- Topology changes → dist_out, stream_order, path_freq, path_segs
- Node changes → reach aggregates (wse, width)

## Validation Checks

`src/sword_duckdb/validation.py`:
- dist_out decreasing downstream
- path_freq increasing toward outlets
- lake sandwich detection
- topology consistency

## Lint Framework

**Location:** `src/sword_duckdb/lint/`

Comprehensive linting framework with 61 checks across 8 categories (T=Topology, A=Attributes, F=Facc, G=Geometry, C=Classification, V=v17c, FL=Flags, N=Network).

**CLI Usage:**
```bash
# Run all checks
python -m src.sword_duckdb.lint.cli --db sword_v17c.duckdb

# Filter by region
python -m src.sword_duckdb.lint.cli --db sword_v17c.duckdb --region NA

# Specific checks or category
python -m src.sword_duckdb.lint.cli --db sword_v17c.duckdb --checks T001 T002
python -m src.sword_duckdb.lint.cli --db sword_v17c.duckdb --checks T  # all topology

# Output formats
python -m src.sword_duckdb.lint.cli --db sword_v17c.duckdb --format json -o report.json
python -m src.sword_duckdb.lint.cli --db sword_v17c.duckdb --format markdown -o report.md

# CI mode (exit codes)
python -m src.sword_duckdb.lint.cli --db sword_v17c.duckdb --fail-on-error   # exit 2 on errors
python -m src.sword_duckdb.lint.cli --db sword_v17c.duckdb --fail-on-warning  # exit 1 on warnings

# List all checks
python -m src.sword_duckdb.lint.cli --list-checks
```

**Python API:**
```python
from sword_duckdb.lint import LintRunner, Severity

with LintRunner("sword_v17c.duckdb") as runner:
    results = runner.run()  # all checks
    results = runner.run(checks=["T"])  # topology only
    results = runner.run(region="NA", severity=Severity.ERROR)
```

**Check IDs (58 total):**

| ID | Name | Severity | Description |
|----|------|----------|-------------|
| T001 | dist_out_monotonicity | ERROR | dist_out decreases downstream |
| T002 | path_freq_monotonicity | WARNING | path_freq increases to outlets |
| T003 | facc_monotonicity | WARNING | facc increases downstream |
| T004 | orphan_reaches | WARNING | No neighbors |
| T005 | neighbor_count_consistency | ERROR | n_rch_up/down matches topology |
| T006 | connected_components | INFO | Network connectivity |
| T007 | topology_reciprocity | WARNING | A→B implies B→A |
| T008 | dist_out_negative | ERROR | No negative dist_out |
| T009 | dist_out_zero_at_nonoutlet | ERROR | dist_out=0 only at outlets |
| T010 | headwater_path_freq | ERROR | Headwaters have path_freq >= 1 |
| T011 | path_freq_zero | WARNING | path_freq=0 only for disconnected |
| T012 | topology_referential_integrity | ERROR | All neighbor_reach_ids exist in reaches |
| A002 | slope_reasonableness | WARNING | No negative, <100 m/km |
| A003 | width_trend | INFO | Width increases downstream |
| A004 | attribute_completeness | INFO | Required attrs present |
| A005 | trib_flag_distribution | INFO | Unmapped tributary stats |
| A006 | attribute_outliers | INFO | Extreme values |
| A007 | headwater_facc | WARNING | Headwaters have low facc |
| A008 | headwater_width | WARNING | Headwaters have narrow width |
| A009 | outlet_facc | INFO | Outlets have high facc |
| A010 | end_reach_consistency | WARNING | end_reach matches topology |
| F001 | facc_width_ratio_anomaly | WARNING | facc/width > 50000 (extreme outliers) |
| F002 | facc_jump_ratio | WARNING | facc >> sum(upstream), entry points |
| F006 | facc_junction_conservation | ERROR | facc < sum(upstream) at junctions (incl. incremental area) |
| F009 | facc_quality_coverage | INFO | facc_quality tag distribution |
| F010 | junction_raise_drop | INFO | facc drop downstream of raised junction |
| F011 | facc_link_monotonicity | INFO | 1:1 link facc drop (D8 artifact) |
| G001 | reach_length_bounds | INFO | 100m-50km, excl end_reach |
| G002 | node_length_consistency | WARNING | Node sum ≈ reach length |
| G003 | zero_length_reaches | INFO | Zero/negative length |
| G004 | self_intersection | WARNING | ST_IsSimple = FALSE |
| G005 | reach_length_vs_geom_length | WARNING | reach_length vs ST_Length >20% diff |
| G006 | excessive_sinuosity | INFO | sinuosity > 10 |
| G008 | geom_not_null | ERROR | NULL geometry |
| G009 | geom_is_valid | ERROR | ST_IsValid = FALSE |
| G010 | geom_min_points | ERROR | ST_NPoints < 2 |
| G011 | bbox_consistency | WARNING | Centroid outside bbox or inverted min/max |
| G012 | endpoint_alignment | INFO | Connected reach endpoints >500m apart (incl. confluences/bifurcations) |
| C001 | lake_sandwich | WARNING | River between lakes |
| C002 | lakeflag_distribution | INFO | Lakeflag values |
| C003 | type_distribution | INFO | Type field values |
| C004 | lakeflag_type_consistency | INFO | Lakeflag/type cross-tab (needs investigation) |
| V001 | hydro_dist_out_monotonicity | ERROR | hydro_dist_out decreases downstream |
| V002 | hydro_dist_vs_pathlen | INFO | hydro_dist_out vs pathlen_out diff |
| V004 | mainstem_continuity | WARNING | is_mainstem_edge forms continuous path |
| V005 | hydro_dist_out_coverage | ERROR | All connected reaches have hydro_dist_out |
| V006 | mainstem_coverage | INFO | is_mainstem_edge coverage stats |
| V007 | best_headwater_validity | WARNING | best_headwater is actual headwater |
| V008 | best_outlet_validity | WARNING | best_outlet is actual outlet |
| FL001 | swot_obs_coverage | INFO | SWOT observation coverage statistics |
| FL002 | iceflag_values | WARNING | iceflag in {-9999, 0, 1, 2} |
| FL003 | low_slope_flag_consistency | WARNING | low_slope_flag consistent with slope |
| FL004 | edit_flag_format | INFO | edit_flag distribution |
| N001 | main_side_values | ERROR | main_side in {0, 1, 2} |
| N002 | main_side_stream_order | ERROR | main_side=0 implies valid stream_order |
| A021 | wse_obs_vs_wse | WARNING | wse_obs_median close to wse |
| A024 | width_obs_vs_width | INFO | width_obs_median reasonable vs width |
| A026 | slope_obs_nonneg | ERROR | slope_obs_mean >= 0 |
| A027 | slope_obs_extreme | WARNING | slope_obs_mean < 50 m/km |

**Validation Specs:** 23 deep-dive documents in `docs/validation_specs/` covering every variable. Each spec includes:
- Official definition (from PDD)
- Algorithm/code path
- Valid ranges and distributions
- Failure modes
- Proposed lint checks
- Reconstruction rules

## Testing

```bash
cd /Users/jakegearon/projects/SWORD
python -m pytest tests/sword_duckdb/ -v
```

Test DB: `tests/sword_duckdb/fixtures/sword_test_minimal.duckdb` (100 reaches, 500 nodes)

## Important Files

| File | Purpose |
|------|---------|
| `src/sword_duckdb/workflow.py` | Main entry point (3,511 lines) |
| `src/sword_duckdb/sword_class.py` | SWORD data class (4,623 lines) |
| `src/sword_duckdb/schema.py` | Table definitions |
| `src/sword_duckdb/reactive.py` | Dependency graph |
| `src/sword_duckdb/reconstruction.py` | 35+ attribute reconstructors |
| `src/sword_duckdb/lint/` | Lint framework (50 checks) |
| `scripts/topology/run_v17c_topology.py` | Topology recalculation script |
| `scripts/maintenance/rebuild_v17b.py` | Rebuild v17b from NetCDF (if corrupted) |
| `deploy/reviewer/` | Streamlit GUI for topology/lake review |

## Topology Reviewer (deploy/reviewer/)

Streamlit app for manual QA review of SWORD reaches. Located in `deploy/reviewer/`.

- `deploy/reviewer/app.py` - topology reviewer (main app)
- `deploy/reviewer/lake_app.py` - lake classification reviewer

**Key gotchas:**
- `check_lakeflag_type_consistency()` returns cross-tab summary (lakeflag, type, count), NOT individual reaches. Use direct SQL for per-reach review.
- Streamlit tabs must ALL be created in every code path — no conditional `None` tabs
- `render_reach_map_satellite()` supports `color_by_type=True` for lakeflag-colored connected reaches (used in C004 tab)
- Beginner mode (default ON) reorders tabs: C004, A010, T004, A002, Suspect, Fix History first
- All review actions logged to `lint_fix_log` table with check_id, action, old/new values
- `requirements-reviewer.txt` has minimal deps for reviewer-only usage (no psycopg2/aiohttp)

## Git

- **Main branch:** main (release-only — never commit directly)
- **Working branch:** v17c-updates (all active work happens here)
- **v18 branch:** v18-planning (future planning)
- Never force push to main
- **NEVER merge to main** until v17c is fully validated — PRs go to v17c-updates
- Feature branches branch off v17c-updates and PR back into v17c-updates

## GitHub Issue Tracking

**All v17c/v18 work is tracked via GitHub Issues.** See: https://github.com/ealtenau/SWORD/issues

### Milestones

| Milestone | Description | Deadline |
|-----------|-------------|----------|
| v17c-verify | Verify pipeline outputs before use | FIRST |
| v17c-topology | Keep dist_out, add hydro_dist_out | 1-2 months |
| v17c-lake-type | Fix lake/type classification | 1-2 months |
| v17c-pipeline | Import 20+ new attrs | 1-2 months |
| v17c-swot | WSE/width/slope stats | 1-2 months |
| v17c-schema | New columns only | 1-2 months |
| v17c-export | DuckDB, GPKG, NetCDF, Parquet | 1-2 months |
| v17c-docs | Release notes, data dict | 1-2 months |
| v18-planning | Scope, ID mapping | 6+ months |
| v18-sources | MERIT Hydro, GROD | 6+ months |
| v18-imagery | Sentinel-2 centerlines | 6+ months |
| v18-reach-mod | Merge/add reaches | 6+ months |
| v18-export | v18 exports | 6+ months |

### Labels

- **Priority:** P0-critical, P1-high, P2-medium, P3-low
- **Type:** type:bug, type:feature, type:docs, type:verify
- **Region:** region:NA/SA/EU/AF/AS/OC, region:all
- **Component:** comp:topology, comp:pipeline, comp:swot, comp:export, comp:lake-type, comp:schema, comp:verify

### Key Issues (v17c)

| # | Title | Milestone |
|---|-------|-----------|
| 4 | Inventory pipeline output files | v17c-verify |
| 14 | Fix facc using MERIT Hydro | v17c-topology |
| 17 | Fix island-in-lake misclassification | v17c-lake-type |
| 31 | Run aggregate_swot_observations | v17c-swot |
| 34 | Export DuckDB (v17c final) | v17c-export |

### Workflow

1. Pick issue from milestone (priority order: v17c-verify → topology → lake-type → pipeline → swot → export → docs)
2. Create branch from v17c-updates: `git checkout -b issue-N-short-desc`
3. Work on issue, reference it in commits: `git commit -m "Fix #N: description"`
4. PR to v17c-updates (NOT main)

## Source Datasets

- **GRWL** - Global River Widths from Landsat
- **MERIT Hydro** - Elevation, flow accumulation
- **HydroBASINS** - Drainage areas
- **GRanD/GROD** - Dams and obstructions
- **SWOT** - Satellite water surface elevation

## Imagery Pipeline

**Location:** `src/sword_duckdb/imagery/`

**Water Detection Ensemble (6 methods):**
- NDWI, MNDWI, AWEI_nsh, AWEI_sh (spectral indices)
- ML4Floods, DeepWaterMap (ML models)
- Voting threshold: ≥4/6 methods agree
- Post-processing: morphological closing, blob removal (200px), relative threshold

**Key Classes:**
- `SentinelSTACClient` - Sentinel-2 imagery search
- `COGReader` - Cloud Optimized GeoTIFF reads
- `WaterEnsemble` - Multi-method water detection
- `RiverTracer` - Patch-based water mask + RivGraph centerline

## Centerline Update Approach

**Goal:** Update SWORD geometries using satellite-derived water masks

**Algorithm (skeleton + SWORD-guided pathfinding):**
1. Get water mask from ensemble
2. Skeletonize → true water center
3. Find start/end on skeleton nearest SWORD start/end
4. Pathfind with cost = `1 + dist_to_sword * 0.1`
5. At junctions, cost naturally picks SWORD's branch
6. Result: true center following SWORD's path

**Key insight:** SWORD defines PATH (which channel), skeleton defines POSITION (center)

**Test results:**
| River | Mean Drift | Notes |
|-------|------------|-------|
| Rhine | 61.5m | Clean single channel |
| Missouri | 85.6m | Correct branch selection |

**Limitations:**
- Narrow rivers (<50m) fail with 4/6 vote threshold
- Braided/anastomosing rivers need manual review
- Wide lake-like sections have noisy skeletons

**Note:** The centerline update approach is experimental. See `src/sword_duckdb/imagery/` for implementation.
