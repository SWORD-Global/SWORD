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
    lint/                 # Lint framework (70+ checks)
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

**Validation specs:** 27 deep-dive docs in `docs/validation_specs/` covering every variable (algorithm, valid ranges, failure modes, reconstruction rules). `ls docs/validation_specs/` to browse.

## v17c Pipeline

**Location:** `src/sword_v17c_pipeline/`

Computes v17c attributes (see "v17c Attributes" table above) and creates `v17c_sections` / `v17c_section_slope_validation` tables. Reads v17b topology from DuckDB, builds NetworkX DiGraph, writes results back.

```bash
# All regions
python -m src.sword_v17c_pipeline.v17c_pipeline --db data/duckdb/sword_v17c.duckdb --all

# Single region
python -m src.sword_v17c_pipeline.v17c_pipeline --db data/duckdb/sword_v17c.duckdb --region NA

# Skip SWOT (faster)
python -m src.sword_v17c_pipeline.v17c_pipeline --db data/duckdb/sword_v17c.duckdb --all --skip-swot
```

## Known Issues

| Issue | Workaround |
|-------|------------|
| **RTREE index segfault** | Drop index → UPDATE → Recreate index. See RTREE Update Pattern below. |
| **Region case sensitivity** | DuckDB=uppercase (NA), pipeline=lowercase (na) |
| **Lake sandwiches** | 1,252 corrected (wide + shorter than ≥1 lake neighbor) → lakeflag=1, tagged `edit_flag='lake_sandwich'`. ~1,755 remaining (narrow connecting channels, chains). See issues #18/#19 |
| **DuckDB lock contention** | Only one write connection at a time. Kill Streamlit/other processes before UPDATE. |
| **end_reach divergence from v17b** | v17c recomputed end_reach from topology: junction=3 when n_up>1 OR n_down>1. ~30k v17b "phantom junctions" (n_up=1, n_dn=1, end_reach=3) relabeled to 0. UNC's original junction criterion is unknown. See `docs/validation_specs/end_reach_trib_flag_validation_spec.md` Section 1.8. |
| **reconstruction.py end_reach bug** | `_reconstruct_reach_end_reach` uses `n_up > 1` only (misses bifurcations). `reactive.py` has the correct logic (`n_up > 1 OR n_down > 1`). Don't use the reconstruction function without fixing it. |
| **DuckDB reach geometry missing endpoint overlap** | DuckDB geometries (rebuilt from NetCDF) lack the overlap vertices at endpoints that make adjacent reaches visually connect. The v17b PostgreSQL table (`postgres.sword_reaches_v17b`) has the full-fidelity geometries. `scripts/maintenance/load_from_duckdb.py` auto-copies v17b geometries to v17c PostgreSQL via dblink (`--skip-v17b-geom` to disable). See issue #187. |
| **path_freq=0/-9999 on connected reaches** | 4,952 connected non-ghost reaches globally have invalid path_freq (34 with 0, 4,918 with -9999). 91% are 1:1 links (fixable by propagation), 9% are junctions (need full traversal). AS has 2,478. See issue #16. |
| **POM lint findings (10 open investigation issues)** | POM validation checks found issues in v17c: 89K CL-node misallocations (#194), 4.8K WSE inversions (#195), 3.5K node spacing gaps (#193), 2.6K boundary dist_out gaps (#192), 553 dist_out jumps (#191), 467 boundary geo gaps (#188–#190), 197 name disagreements (#196). All are diagnose-first — see `docs/technical/pom_requests_summary.md` for full tracker. |

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

## Lint Framework

**Location:** `src/sword_duckdb/lint/`

70+ checks across 9 categories:

| Category | Prefix | Examples |
|----------|--------|---------|
| Topology | T | dist_out monotonicity, neighbor consistency, reciprocity, ID format, river names |
| Attributes | A | slope, width, WSE, observation stats, end_reach consistency |
| Facc | F | junction conservation, jump ratios, quality coverage |
| Geometry | G | null/invalid geom, length bounds, sinuosity, endpoint alignment |
| Classification | C | lake sandwich, lakeflag/type consistency |
| v17c | V | hydro_dist_out monotonicity, mainstem continuity, headwater/outlet validity |
| Flags | FL | SWOT obs coverage, iceflag values, low_slope_flag |
| Network | N | main_side values, stream_order consistency |
| Node | N0xx | node spacing, dist_out ordering, boundary alignment, node count, geolocation |

Run `python -m src.sword_duckdb.lint.cli --list-checks` for the full list.

**CLI:**
```bash
python -m src.sword_duckdb.lint.cli --db sword_v17c.duckdb                    # all checks
python -m src.sword_duckdb.lint.cli --db sword_v17c.duckdb --region NA        # one region
python -m src.sword_duckdb.lint.cli --db sword_v17c.duckdb --checks T         # category
python -m src.sword_duckdb.lint.cli --db sword_v17c.duckdb --checks T001 T002 # specific
python -m src.sword_duckdb.lint.cli --db sword_v17c.duckdb --format json -o report.json
python -m src.sword_duckdb.lint.cli --db sword_v17c.duckdb --fail-on-error    # CI mode
```

**Python API:**
```python
from sword_duckdb.lint import LintRunner, Severity

with LintRunner("sword_v17c.duckdb") as runner:
    results = runner.run(checks=["T"], region="NA", severity=Severity.ERROR)
```

**POM checks:** See `docs/technical/pom_requests_summary.md` for the 19 checks derived from Pierre-Olivier Malaterre's `sword_validity.m`, production results, and open investigation issues.

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
| `src/sword_duckdb/lint/` | Lint framework (70+ checks) |
| `docs/technical/pom_requests_summary.md` | **Living doc**: POM (Pierre-Olivier Malaterre) lint checks, columns, production results, and investigation issues. Update when POM-related work changes. |
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

**All work tracked via GitHub Issues.** See: https://github.com/SWORD-Global/SWORD/issues

**Active milestone:** `v17c-april-2026` — all current v17c work. Future: `v18-planning`.

**Labels:** P0–P3 priority, type:{bug,feature,docs,verify}, region:{NA,SA,EU,AF,AS,OC,all}, comp:{topology,pipeline,swot,export,lake-type,schema,lint,verify}

**Workflow:**
1. Pick issue from milestone by priority
2. Branch from v17c-updates: `git checkout -b issue-N-short-desc`
3. Reference in commits: `git commit -m "Fix #N: description"`
4. PR to v17c-updates (NOT main)

## Source Datasets

- **GRWL** - Global River Widths from Landsat
- **MERIT Hydro** - Elevation, flow accumulation
- **HydroBASINS** - Drainage areas
- **GRanD/GROD** - Dams and obstructions
- **SWOT** - Satellite water surface elevation

## Imagery Pipeline (experimental)

**Location:** `src/sword_duckdb/imagery/` — Sentinel-2 water detection (6-method ensemble) + skeleton-based centerline updates. Not active for v17c. See source files for details.
