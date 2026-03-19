# SWORD v17c/v18 Roadmap

**Last updated:** March 2026

---

## v17c Status

### Completed

- Mainstem topology: 11 new variables (dist_out_dijkstra, hydro_dist_out,
  rch_id_up_main, rch_id_dn_main, best_headwater, best_outlet, pathlen_hw,
  pathlen_out, is_mainstem, main_path_id, subnetwork_id)
- SWOT observation statistics: percentile-based summaries (p10-p90, MAD,
  range) for WSE, width, slope at reach and node level. 72.6% coverage.
- Facc corrections: 95,913/248,673 reaches (38.6%) corrected via
  two-stage denoise pipeline (34/39 seeds detected)
- Lake typology: 1,252 lake sandwich reaches reclassified
- PostgreSQL backend: v17c schema matches DuckDB
- NetCDF beta export: all 6 regions, GeoPackage + GeoParquet available
- 64 GitHub issues closed

### Open Items

| Priority | Item | Status |
|----------|------|--------|
| P1 | main_path_id consistency (3,134 issues) | Needs recompute |
| P1 | 80 best_headwater -> non-headwater reaches (NA) | Needs investigation |
| P2 | 1,755 remaining lake sandwich candidates | Narrow channels, chains |
| P2 | 4,952 path_freq=0/-9999 reaches | 91% fixable by propagation |
| P2 | 51.2% unnamed reaches (NODATA) | Naming sources limited |
| P2 | POM validation findings (10 open issues) | Diagnose-first |
| P3 | Persistent flow correction oscillation guard | Cross-run tracking |
| P3 | Estuary/tidal approach | Deferred |

### Delivery

- **JPL deadline:** April 15, 2026 (SWOT percentiles required by JPL)
- **Stakeholder priorities:** validated flow directions, SWOT percentiles
  (p10/p50/p90), improved type 1 vs type 3 classification

---

## v18 Planning

Deferred 6+ months. Key changes requiring reach ID or geometry modifications:

- Reach splitting/merging (geometry changes)
- New reach ID scheme
- Full path_freq recomputation from scratch
- Estuary/tidal handling
- Facc correction threshold refinement

v17c remains the active release. v18 planning tracked on the `v18-planning`
branch and GitHub milestone.
