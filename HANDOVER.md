# Handover: v17c Beta 0.0.4 Released - Niek's Issues Fixed

**Date:** 2026-04-01T14:50:00Z
**Branch:** `main` @ `d8225ab`
**Context:** Fixed two critical issues Niek reported (node dist_out and ghost coastal outlets). All exports regenerated and synced to Google Drive. Waiting for Niek's validation.

---

## In Progress

| Task | Status | Location | Notes |
|------|--------|----------|-------|
| Node dist_out reactive recalc fix | ✅ Complete | `src/sword_duckdb/reactive.py:836` | Changed sorting from `node_id` to `node_order` |
| Ghost coastal outlet dist_out_dijkstra | ✅ Complete | `src/sword_v17c_pipeline/stages/distances.py:100` | Isolated ghosts now get `reach_length` |
| Re-export all formats | ✅ Complete | `data/exports/v17c_beta/` | NetCDF, GeoPackage, Parquet updated |
| Google Drive sync | ✅ Complete | `~/Google Drive/SWORD_V17c_beta/` | All files synced with _0.0.4 naming |
| Release notes updated | ✅ Complete | `docs/v17c_release_notes.md` | Date updated to April 2026 |

## Niek's Issues - Resolution Status

| Issue | Status | Details |
|-------|--------|---------|
| `hydro_dist_hw` fixed | ✅ Verified | Previously stale, now computed correctly |
| `dist_out` node values wrong | ✅ Fixed | Reactive recalc now uses `node_order` for geometric sorting |
| Ghosts excluded from Dijkstra | ✅ Fixed | Ghost coastal outlets now get `dist_out_dijkstra = reach_length` (703 in NA) |

## Files Modified (All Committed)

```
M  src/sword_duckdb/reactive.py          # Line 836: sort by node_order
M  src/sword_duckdb/views.py              # Added node_order property
M  src/sword_v17c_pipeline/stages/distances.py  # Ghost outlet fix
M  docs/v17c_release_notes.md             # Updated for 0.0.4
M  docs/v17c_release_notes.pdf             # Regenerated
```

## Local Exports Structure

```
data/exports/v17c_beta/
├── netcdf/
│   ├── af_sword_v17c_beta_0.0.4.nc
│   ├── as_sword_v17c_beta_0.0.4.nc
│   ├── eu_sword_v17c_beta_0.0.4.nc
│   ├── na_sword_v17c_beta_0.0.4.nc
│   ├── oc_sword_v17c_beta_0.0.4.nc
│   └── sa_sword_v17c_beta_0.0.4.nc
├── gpkg/
│   └── sword_{AF,AS,EU,NA,OC,SA}_v17c_beta_0.0.4.gpkg
├── parquet/
│   └── sword_{AF,AS,EU,NA,OC,SA}_v17c_beta_0.0.4_{nodes,reaches}.parquet
├── v17c_release_notes_0.0.4.md
├── v17c_release_notes_0.0.4.pdf
└── SHA256SUMS_0.0.4.txt
```

## Google Drive Status

All files synced to `~/Google Drive/My Drive/SWORD_V17c_beta/`:
- `netcdf/` - 6 NetCDF files
- `gpkg/` - 6 GeoPackage files
- `parquet/` - 12 Parquet files
- Root: release notes (md+pdf), SHA256SUMS

## Next Steps (Priority Order)

1. **Wait for Niek's validation** — he's checking the fixes, may have more feedback
2. **Address any additional issues** — if Niek finds more problems
3. **Re-export if needed** — if database changes required
4. **Final 0.0.4 release** — once Niek confirms all issues resolved

## Key Decisions

- **Node dist_out:** Changed from `node_id` to `node_order` sorting in reactive recalc. This is the correct semantic — `node_order` reflects geometric position (1=downstream), while `node_id` is arbitrary ID that doesn't change when reaches are flow-corrected.

- **Ghost coastal outlets:** These are isolated ghost sinks (no path to real hydrologic outlets). They should get `dist_out_dijkstra = reach_length` to match v17b `dist_out` behavior, not NULL. The 703 ghost coastal outlets in NA now have proper values.

- **Export naming:** Standardized on `_0.0.4` suffix for all exported files for clarity.

## References

- Issue: Niek Netherlands feedback (WhatsApp, April 1 2026)
- Previous fix: Commit `e322542` (node ordering in pipeline, but not reactive)
- Validation spec: `docs/validation_specs/validation_spec_dist_out.md`
