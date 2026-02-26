# v17c Status Report

*Prepared: February 2026 | Deadline: March 31, 2026*

## Summary

| Component | Status | Details |
|-----------|--------|---------|
| Lake Typology | 🟡 In Progress | 3,167 sandwiches identified, 165 fixed |
| facc Fixes | 🟡 In Progress | 7,355 T003 violations, fix exists |
| SWOT Integration | 🟢 Working | 72.6% coverage, slopes WIP |
| v17c Topology | 🟢 Working | hydro_dist, mainstem complete |
| PostgreSQL Backend | 🟢 Complete | Ready for testing |

---

## Lake Typology

### Current State
- **C001 lake_sandwich**: 3,167 identified, 165 reclassified (high-confidence)
- **Remaining**: 393 medium-confidence + 1,142 low-confidence need review
- **C004 lakeflag/type**: 240 questionable cases (semi-independent fields)
- **Island detection**: Architecture exists (type=3), no dedicated lint check

### Type Column Status ✅ RESOLVED

| Database | type column | Status |
|----------|-------------|--------|
| v17b DuckDB | ✅ Present | 248,673 reaches (copied from GPKGs 2026-02-03) |
| v17c DuckDB | ✅ Present | 248,674 reaches |
| PostgreSQL v17c | ✅ Present | 248,674 reaches (synced 2026-02-03) |

**Distribution:** type=1 (river): 65.8%, type=3 (lake): 10.3%, type=4 (canal): 8.6%, type=5 (tidal): 6.0%, type=6 (reservoir): 9.3%

**TODO (later):** Add type to validation pipeline lint checks.

### Routing Issue Example

**Reach 62270000143** (Amazon, SA):
- lakeflag=1, type=3 (classified as lake-on-river)
- width=4,560m
- Upstream/downstream: both lakeflag=0, type=1 (normal river)

**Problem**: Mid-channel bar creates false lake classification, disrupts routing algorithms.

**Question for meeting**: How many similar cases exist? Systematic fix approach?

---

## facc Fixes

### Current State
- **Detection pipeline**: `src/sword_duckdb/facc_detection/` ✅ Complete
- **GeoJSON exports**: `output/facc_detection/` for QGIS review
- **1,725 anomalies detected** globally (run on v17b pristine reference)
- **34/39 seeds detected** (87.2% recall), 5 missed are propagation reaches
- **Known FPs**: 12 (Ob multi-channel, narrow width, Nile delta, Indus/Irrawaddy)
- **Correction pipeline**: Exists, awaiting visual validation before applying to v17c

### Detection Pipeline (2026-02-04)

**Run command:**
```bash
python -m src.sword_duckdb.facc_detection.cli \
    --db data/duckdb/sword_v17b.duckdb \
    --all \
    --export-geojson \
    --output-dir output/facc_detection/
```

**Detection rules in `detect_hybrid()` (11 rules):**

| Rule | Criteria | Count | Description |
|------|----------|-------|-------------|
| fwr_drop | FWR drops >5x downstream | 815 | FWR inconsistent with downstream |
| entry_point | facc_jump > 10 AND ratio_to_median > 40 | 466 | Bad facc enters network |
| extreme_fwr | FWR > 15,000 | 200 | Extremely high facc/width ratio |
| jump_entry | path_freq invalid AND facc_jump > 20 AND FWR > 500 | 99 | D8 error with missing metadata |
| upstream_fwr_spike | Upstream FWR >10x this reach | 40 | Bad facc from upstream |
| impossible_headwater | path_freq ≤ 2 AND facc > 1M AND fwr_drop > 2 | 30 | Mainstem facc on tributary |
| invalid_side_channel | path_freq=-9999 AND main_side=1 AND facc>200K AND fwr_drop>3 | 27 | Side channel with invalid metadata |
| high_ratio | ratio_to_median > 500 (with FWR drop) | 17 | Very high facc per path_freq |
| side_channel_misroute | main_side=1 AND fwr_drop>20 AND facc>100K | 15 | Side channel with mainstem facc |
| facc_sum_inflation | facc > 3x sum(upstream) at confluence | 12 | D8 error inflates facc at junction |
| headwater_extreme | n_rch_up = 0 AND facc > 500K AND FWR > 5000 | 4 | Impossible headwater facc |

**Key discriminators:**
- FWR consistency through network (legitimate rivers have consistent FWR up/down)
- main_side=1 + dramatic FWR drop = misrouted facc on side channel
- path_freq=-9999 often indicates D8 routing errors
- Width minimum 15m to avoid FWR inflation from narrow reaches

**Output files:** `output/facc_detection/` contains GeoJSON per rule + `all_anomalies.geojson` + `detection_summary.json`

### Seed Reaches (39 confirmed bad)

| Region | Count | Example reach_ids |
|--------|-------|-------------------|
| SA | 15 | 64231000301, 62236100011, 62255000451, 62210000705 |
| EU | 7 | 28315000523, 28315000751, 28311300405, 28311700191, 22513000171 |
| AF | 5 | 31251000111, 31248100141, 32257000231, 14279001411, 14631000181 |
| AS | 12 | 45670300691, 31241700301, 44240100011, 34211700241, 34211101775, etc. |

**Seed detection:** 34/39 detected (87.2%). 5 missed are propagation reaches:
- 22513000171: Upstream of detected fwr_drop at 22513000161
- 44581100665, 44581100675: Downstream of detected fwr_drop at 44581100715
- 34211700241, 34211101775: Downstream of detected extreme_fwr at 34211700251

These will be corrected when their adjacent detected reaches are corrected.

**FWR capping:** Width capped at 15m for FWR calculation (avoids inflation from narrow reaches while still detecting them).

**Known FPs excluded (12):** Ob River multi-channel (3), narrow width inflation (4), complex tidal/delta (3), moderate FWR consistent (2)

### Correction Safeguards

The correction pipeline uses v17c routing (`is_mainstem_edge`) to preserve mainstem facc series:

| Correction Type | Trigger | Method |
|-----------------|---------|--------|
| side_to_mainstem | `is_mainstem=False` + high facc | Set to local catchment only |
| mainstem_reextract | `is_mainstem=True` + FWR > 10K | MERIT re-extraction (preserves network) |
| headwater_reset | n_rch_up=0 + facc > 500K | Regional typical value |
| side_channel_high_facc | main_side=1 + high facc | Local catchment estimate |
| regression | fallback | log(facc) ~ log(width,slope,path_freq) |

**Key insight:** Mainstem reaches use MERIT re-extraction, not regression. This preserves legitimate facc series while correcting D8 misroutes.

### Anomaly Patterns (informing correction strategy)

| Factor | Anomaly Rate | Insight |
|--------|--------------|---------|
| Side channels | 6.2% flagged | 12x higher than main channels (0.5%) |
| Junctions | 2.35% flagged | 73% of all anomalies occur here |
| AF, SA regions | 0.9% flagged | Highest rates; EU, OC lowest (0.4%) |

**Root cause:** D8 routing picks ONE downstream cell at bifurcations. The other branch gets facc from a different (often wrong) watershed.

### Next Steps

1. ✅ **Detection complete** - 1,725 anomalies in `output/facc_detection/`
2. 🔄 **Visual review** in QGIS using exported GeoJSONs
3. 🔄 **Investigate RF classifier** to improve detection accuracy
4. ⏳ **Implement correction propagation** - trace downstream from entry points
5. ⏳ **Apply corrections** to v17c
6. ⏳ **Re-run T003 lint** to verify monotonicity restored

---

## SWOT Integration

### Coverage
- **Reaches with observations**: 180,266 / 248,373 (72.6%)
- **WSE/width obs**: Working, excellent agreement with v17b

### Slopes
- **reach_swot_obs.py**: Provides reach-level slope/wse/width via OLS ✅ (renamed from reach_slope_obs.py)
- **Negative slopes**: 24,129 reaches (documented, physically plausible in some cases)
- **Decision**: Using reach-level observations only (reach_swot_obs.py), section-level not needed

### Updated Coverage (2026-02-03)
| Region | Slope | WSE/Width |
|--------|-------|-----------|
| NA | 82.4% | 67.8% |
| SA | 86.2% | 82.8% |
| EU | 76.2% | 78.4% |
| AF | 87.0% | 84.6% |
| AS | 79.6% | 83.1% |
| OC | 77.3% | 78.6% |

---

## v17c Topology Additions

### Working
| Attribute | Status | Notes |
|-----------|--------|-------|
| hydro_dist_out | ✅ | Dijkstra-based |
| hydro_dist_hw | ✅ | Max distance from headwater |
| best_headwater | ✅ | Width-prioritized |
| best_outlet | ✅ | Width-prioritized |
| is_mainstem_edge | ✅ | Continuous paths |
| v17c_sections | ✅ | 42,607 junction-to-junction segments |

### Known Issues
- **#69**: Mississippi/Ohio mainstem algorithm bug at Cairo confluence ✅ FIXED (2026-02-26)
- **#111**: 1,210 hydro_dist_out monotonicity violations (closed, documented as expected)

---

## PostgreSQL/DuckDB Architecture

**Status**: Complete, production-ready, awaiting testing

### Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                    SWORD Data Workflow                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────────┐                                           │
│  │   PostgreSQL     │  ← Master DB (multi-user QGIS editing)    │
│  │   + PostGIS      │                                           │
│  │                  │                                           │
│  │  • Advisory locks│  ← Region-level locking                   │
│  │  • sword_ops log │  ← All changes tracked                    │
│  │  • synced flag   │  ← Marks what's pushed to DuckDB          │
│  └────────┬─────────┘                                           │
│           │                                                     │
│           │ workflow.sync_to_duckdb()                           │
│           │ (incremental: only changed rows)                    │
│           ▼                                                     │
│  ┌──────────────────┐                                           │
│  │   DuckDB Copy    │  ← Local analysis/processing              │
│  │  sword_v17c.db   │                                           │
│  │                  │                                           │
│  │  • Fast queries  │                                           │
│  │  • Pipeline runs │                                           │
│  │  • Export source │                                           │
│  └──────────────────┘                                           │
│           │                                                     │
│           │ workflow.export()                                   │
│           ▼                                                     │
│  ┌──────────────────┐                                           │
│  │   Exports        │                                           │
│  │  • NetCDF        │  ← For UNC/JPL distribution               │
│  │  • GeoPackage    │  ← For GIS users                          │
│  │  • Parquet       │  ← For cloud analytics                    │
│  └──────────────────┘                                           │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Components
- Backend abstraction layer (`backends/base.py`, `duckdb.py`, `postgres.py`)
- Connection manager (`sword_db.py`) - auto-detects backend
- PostgreSQL schema (`scripts/create_postgres_schema.sql`, 900 lines)
- DuckDB → PostgreSQL export
- PostgreSQL → DuckDB incremental sync
- Region-level advisory locking

### Related Issues
- #77, #78, #80, #120: CLOSED (PostgreSQL backend, schema, and sync delivered)
- #121: OPEN (QGIS integration guide with PostGIS)
- #33: CLOSED (essential schema columns added 2026-02-23)

**Question for meeting**: Ready for undergrad QGIS editing? Timeline?

---

## GitHub Milestone: v17c-april-2026 (as of 2026-02-21)

| Status | Count |
|--------|-------|
| Closed | 64 |
| Open | 20 |
| Due date | 2026-03-31 |

### Priority Open Issues

| Priority | Issue | Title |
|----------|-------|-------|
| P1 | #19 | Update lakeflag for misclassified reaches |
| P1 | #121 | QGIS integration guide with PostGIS |
| P2 | #164 | Port MERIT Hydro raster ingestion from legacy code |
| P2 | #18 | Document lake sandwich reaches (3,167) |
| P2 | #42 | Generate validation report |

### Recently Closed Priority Work

- #17: Island-in-lake lint check C007 (Closed 2026-02-26)
- #127: DL-GROD ingestion and O001-O003 lint checks (Closed 2026-02-26)
- #15: Recalculate stream_order from path_freq (Closed 2026-02-26)
- #33: Add essential new columns only (Closed 2026-02-23)
- #178: Globally unique main_path_id via region offsets (Closed 2026-02-24)
- #87, #101, #83: Topology and lint closure set complete
- #78, #77, #80, #120: PostgreSQL infrastructure closure set complete
- #72, #71, #70: Validation closure set complete

---

## Meeting Discussion Points

1. **Lake routing**: How many cases like 62270000143? Systematic approach?
2. **PostgreSQL timeline**: When to deploy for undergrad QGIS editing?
3. **April deadline**: What's must-have vs nice-to-have?
