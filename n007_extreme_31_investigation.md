# N007 Extreme Boundary Gap Investigation (Issue #189)

## Summary

31 reach pairs have boundary nodes >5 km apart at **all four** boundary pair combinations (min/max node_id of each reach). These are not threshold artifacts or wrong-boundary-end errors — the reaches are genuinely far apart.

## Method

1. For each downstream topology link (`direction='down'`), compute distances between all 4 boundary node pairings:
   - `up_min ↔ dn_max` (expected combo per SWORD convention)
   - `up_min ↔ dn_min`
   - `up_max ↔ dn_max`
   - `up_max ↔ dn_min`
2. Take the **minimum** across all 4 combos
3. Filter to cases where minimum > 5 km, excluding antimeridian wrapping

## Key Findings

| Metric | Value |
|--------|-------|
| Total extreme pairs | 31 |
| Inherited from v17b | 28 (90%) |
| Added in v17c | 3 (all NA, ghost/unreliable downstream reaches) |
| All reciprocal | Yes (both sides agree on the link) |
| Wrong dist_out direction | 1 (71224300023 → 71224200036) |

### Category Breakdown

| Category | Count | Description |
|----------|-------|-------------|
| river_river | 17 | Both sides are type=1 river, lakeflag=0 |
| unreliable_or_ghost | 6 | At least one side is type=5 or type=6 |
| lake_or_tidal | 4 | At least one side has lakeflag ≥ 1 |
| v17c_added | 3 | Link does not exist in v17b |
| width_mismatch | 1 | Width ratio > 10x (Bandama River: 11m → 184m, 14.8 km gap) |

### Region Breakdown

| Region | Count |
|--------|-------|
| AS | 14 |
| NA | 5 |
| AF | 4 |
| SA | 4 |
| EU | 4 |

## Notable Cases

### Worst: Bandama River (AF) — 14.8 km gap, 11m → 184m width
- `14540300041 → 14540300031` (min gap 14,821 m)
- Width jumps from 11.3 m to 184 m — strong misconnection signal
- Both type=1 river, facc nearly identical (186k km²)
- **Likely explanation:** GRWL centerline skips a section; topology bridged it anyway

### Largest river: Angara River (AS) — 11.5 km gap, lake-to-lake
- `32273900073 → 32273900063` (min gap 11,456 m)
- Both type=3 lake_on_river, lakeflag=1
- Width 1,925 m → 10,503 m — reservoir to river transition
- **Likely explanation:** Lake segments with sparse centerlines

### v17c-added: Tennessee River (NA) — 6.2 km gap
- `74262300481 → 74262300956` (min gap 6,194 m)
- river (434m) → ghost (214m, lakeflag=1)
- v17b had no link between these reaches; v17c pipeline added it as `neighbor_rank=1`
- Ghost reach was a headwater in v17b
- **Action needed:** Investigate whether v17c pipeline correctly added this link

### v17c-added: Bear Creek → Big Briar Creek (NA) — 8.0 km gap
- `73270900155 → 73281000086` (min gap 8,035 m)
- unreliable (tidal) → ghost (tidal)
- Different river names, different Pfafstetter basins (7327 vs 7328)
- **Action needed:** Almost certainly a bad link — cross-basin tidal connection

### v17c-added: Lake reach (NA) — 12.7 km gap
- `71224300023 → 71224200036` (min gap 12,700 m)
- lake_on_river → ghost, both lakeflag=1
- Different Pfafstetter basins (7122430 vs 7122420)
- dist_out goes WRONG direction (up_reach has lower dist_out)
- **Action needed:** Bad link — wrong direction + cross-basin

## Recommendations

### Immediate (v17c)
1. **Mark 3 v17c-added links as `topology_suspect=true`** — these were not in UNC's original data and appear to be pipeline artifacts
2. **Do NOT auto-remove any v17b-inherited links** — they may need upstream reporting to UNC

### For QGIS/Reviewer Verification
The diagnostic CSV (`n007_extreme_31_diagnostic.csv`) includes lat/lon coordinates for each boundary node pair. Load in QGIS to visually confirm:
- Whether the reaches are truly disconnected or just have sparse centerline coverage
- Whether the topology link makes physical sense (same river, same drainage)
- Whether width/facc continuity suggests a real connection

### Triage Priority
1. **3 v17c-added links** → Fix immediately (mark suspect or remove)
2. **1 width_mismatch** (Bandama) → Strong misconnection signal, flag for UNC
3. **6 unreliable/ghost** → Lower priority, expected to have topology issues
4. **4 lake/tidal** → May be legitimate sparse-centerline gaps in lakes
5. **17 river-river** → Need visual verification, many may be legitimate long-distance reach connections

## Files

- `n007_extreme_31_diagnostic.csv` — Full diagnostic data with coordinates, metadata, categories
- `MassConservationCheck/` — POM's original analysis (different scope)
