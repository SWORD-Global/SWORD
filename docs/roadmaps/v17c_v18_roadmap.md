# SWORD v17c/v18 Roadmap

## Overview

| Version | Deadline | Scope | Issues |
|---------|----------|-------|--------|
| **v17c** | 1-2 months (JPL) | Attributes only, no geometry changes | 20 open / 64 closed (`v17c-april-2026`) |
| **v18** | 6+ months | Geometry updates, new reaches, ID changes | 25 open (`label:v18-deferred`) |

---

## v17c Scope (JPL Delivery)

### What's IN v17c
- **Topology fixes** - facc corrections from MERIT Hydro, new hydro_dist_out
- **Lake/type fixes** - Island reclassification, lakeflag updates
- **SWOT observation stats** - WSE, width, slope (mean, median, std, range, n_obs)
- **20+ new pipeline columns** - hydro_dist_out, hydro_dist_hw, is_mainstem_edge, subnetwork_id, etc.

### What's NOT in v17c
- No geometry changes
- No reach ID changes
- No dtype optimization

### v17c Milestones (Current Tracking)

| Milestone | State | Open | Closed | Description |
|-----------|-------|------|--------|-------------|
| v17c-april-2026 | Open | 20 | 64 | Active consolidated delivery milestone (due 2026-03-31) |
| v17c-verify/topology/lake-type/pipeline/swot/schema/export/docs | Closed | 0 | n/a | Legacy split milestones; retained for history |

### v17c New Columns

| Column | Description |
|--------|-------------|
| hydro_dist_out | Hydrological distance to outlet via main channel |
| hydro_dist_hw | Hydrological distance to headwater |
| is_mainstem_edge | Boolean mainstem flag |
| rch_id_up_main | Main upstream reach ID |
| rch_id_dn_main | Main downstream reach ID |
| subnetwork_id | Subnetwork identifier |
| main_path_id | Main path identifier |
| best_headwater | Chosen headwater reach |
| best_outlet | Chosen outlet reach |
| dist_out_suspect | Flag for 241 monotonicity violations |
| wse_mean/median/std/range | SWOT WSE statistics |
| width_mean/median/std/range | SWOT width statistics |
| slope_mean/median/std/range | SWOT slope statistics |
| n_obs | Number of SWOT observations |

### v17c Deliverables

| File | Format |
|------|--------|
| sword_v17c.duckdb | DuckDB |
| {region}_v17c.gpkg × 6 | GeoPackage |
| {region}_v17c.nc × 6 | NetCDF |
| {region}_v17c.parquet × 6 | Parquet (new) |
| CHECKSUMS.txt | SHA256 hashes |
| v17c_release_notes.md | Documentation |
| data_dictionary_v17c.md | Column definitions |

---

## v18 Scope (Future)

### What's IN v18
- **Centerline updates** - Sentinel-2 derived water masks
- **Reach modifications** - Merge short reaches, add MERIT rivers (~30m width)
- **Source data refresh** - Latest MERIT Hydro, updated GROD
- **ID mapping** - old_reach_id → new_reach_id tracking

### v18 Planned Workstreams (Deferred)

| Workstream | Planned Issues | Description |
|-----------|----------------|-------------|
| v18-planning | 5 | Scope, ID mapping design, legacy code |
| v18-sources | 5 | MERIT Hydro, GRWL v2, GROD updates |
| v18-imagery | 6 | Sentinel-2 centerline pipeline |
| v18-reach-mod | 6 | Merge/add reaches |
| v18-export | 3 | Exports + ID mapping file |

### v18 Key Challenges

| Challenge | Approach |
|-----------|----------|
| Narrow rivers (<50m) | Lower vote threshold from 4/6 |
| Braided/anastomosing | Multi-thread handling TBD |
| River migration | Skeleton + SWORD-guided pathfinding |
| Backward compatibility | reach_id_mapping.csv (M:N support) |

---

## Tracking

- **Issues**: https://github.com/ealtenau/SWORD/issues
- **v17c Filter**: `milestone:"v17c-april-2026" is:open`
- **v18 Filter**: `label:v18-deferred is:open`
- **Project Board**: https://github.com/users/jameshgrn/projects/1 (verified 2026-02-21)
- **Project Board Scope Recovery**: `gh auth refresh -s read:project -s project` (required for `gh project` commands)
- **Roadmap Sync Check**: `scripts/maintenance/check_roadmap_sync.sh`
- **v17c Branch**: `main` (merged from v17c-updates, March 2026)
- **v18 Branch**: `v18-planning`

---

## Key Decisions

| Topic | Decision |
|-------|----------|
| dist_out | Keep legacy + add hydro_dist_out (both) |
| 241 violations | Flag, don't fix |
| facc source | MERIT Hydro as ground truth |
| Pipeline | Verify outputs BEFORE import |
| Exports | Legacy (NetCDF, GPKG) + new (Parquet) |
| v18 imagery | Sentinel-2 |
| v18 new reaches | MERIT Hydro source |
| v18 ID changes | Mapping table for backward compat |

---

## Timeline

```
NOW ─────────────────────────────────────────────────────> 6+ months
 │
 ├── v17c-verify (FIRST)
 ├── v17c-topology
 ├── v17c-lake-type
 ├── v17c-pipeline
 ├── v17c-swot
 ├── v17c-export
 └── v17c-docs
      │
      └── JPL DELIVERY (1-2 months)
                │
                ├── v18-planning
                ├── v18-sources
                ├── v18-imagery
                ├── v18-reach-mod
                └── v18-export
                     │
                     └── v18 RELEASE (6+ months)
```

---

## Unresolved Questions

1. **Estuary/tidal approach** - How to fix rivers extending too far into estuaries?
2. **facc threshold** - What defines "suspect" facc needing correction?

## Resolved: Legacy Reach Code Location

**Found in `src/_legacy/development/reach_definition/`**

| File | Lines | Purpose |
|------|-------|---------|
| `Reach_Definition_Tools_v11.py` | 6,368 | Core reach generation functions |
| `Reach_Node_Definition_v11.py` | - | Master orchestration script |
| `Write_Database_Files.py` | - | NetCDF output writer |

**Key functions for v18 reuse:**
- `find_swot_bounds()` - SWOT orbit-based reach breaking
- `cut_reaches()` - Split reaches >20km
- `aggregate_rivers()` - Merge reaches <10km
- `reach_topology()` - Final ID assignment with Pfafstetter codes

**Modern equivalent:** `SWORD.break_reaches()` in `src/sword_duckdb/sword_class.py:1496`
