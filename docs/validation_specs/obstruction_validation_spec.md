# Validation Spec: Obstruction Variables (obstr_type, grod_id, dl_grod_id, hfalls_id)

## Summary

| Property | obstr_type | grod_id | dl_grod_id | hfalls_id |
|----------|------------|---------|------------|-----------|
| **Source** | GROD + DL-GROD + HydroFALLS | GROD | DL-GROD | HydroFALLS |
| **Units** | Categorical (0-5) | Database ID | Database ID | Database ID |
| **Applied to** | Nodes, Reaches | Nodes, Reaches | Reaches | Nodes, Reaches |

**Official definitions (v17b PDD, Tables 3-5):**

- **obstr_type**: "Type of obstruction for each [node/reach] based on GROD and HydroFALLS databases. Obstr_type values: 0 - No Dam, 1 - Dam, 2 - Lock, 3 - Low Permeable Dam, 4 - Waterfall."
- **grod_id**: "The unique GROD ID for each [node/reach] with obstr_type values 1-3."
- **hfalls_id**: "The unique HydroFALLS ID for each [node/reach] with obstr_type value 4."

**v17c encoding (extends v17b — semantic changes to types 2 and 3):**

- **obstr_type**: 0=none, 1=dam, 2=low-head dam (was lock in v17b), 3=lock (was low-perm in v17b), 4=waterfall, 5=partial dam
- **dl_grod_id**: DL-GROD database ID (He et al. 2025). Set for obstr_type in {1, 2, 3, 5}. NULL for waterfalls.
- **grod_id**: Retained from v17b; sunset in v18. Cleared when DL-GROD reclassifies a reach to waterfall.

---

## Source Datasets

### GROD (Global River Obstruction Database)

**Reference:** Whittemore et al. (2020). "A Participatory Science Approach to Expanding Instream Infrastructure Inventories." Earth's Future, 8(11), e2020EF001558.

**Description:** GROD provides global locations of anthropogenic river obstructions (dams, locks, low-permeable barriers) along the GRWL river network.

**File:** `data/inputs/GROD/GROD_ALL.csv`

**Key fields:**
- `lat`, `lon` - Location coordinates
- `grod_fid` - Unique GROD feature ID (mapped to `grod_id` in SWORD)
- Obstruction type classification (mapped to `obstr_type` 1-3)

### HydroFALLS

**Reference:** http://wp.geog.mcgill.ca/hydrolab/hydrofalls/

**Description:** HydroFALLS provides global locations of waterfalls and natural river obstructions.

**File:** `data/inputs/HydroFalls/hydrofalls.csv` (filtered version: `hydrofalls_filt.csv`)

**Key fields:**
- `FALLS_ID` - Unique waterfall ID (mapped to `hfalls_id` in SWORD)
- `LAT_HYSD`, `LONG_HYSD` - Location coordinates
- `CONFIDENCE` - Confidence rating
- `CONTINENT` - Continental region

**Filtering:** HydroFALLS points within 500m of GROD locations are removed to avoid double-counting (see `hydrofalls_filtering.py`).

### DL-GROD (Deep Learning Global River Obstruction Database)

**Reference:** He et al. (2025). DL-GROD deep-learning extension of GROD.

**File:** `barrier_reach_mapping.csv` from swot_obstructions project

**Key fields:**
- `ID` - Unique DL-GROD feature ID (mapped to `dl_grod_id` in SWORD)
- `Type` - Obstruction type: dam, low-head dam, lock, partial dam, waterfall
- `reach_id` - SWORD reach assignment
- `distance_m` - Distance from obstruction to reach

**Ingestion:** `scripts/maintenance/ingest_dl_grod.py` resolves multi-obstruction reaches by priority (dam > lock > low-head dam > partial dam > waterfall), updates 27,071 reaches. DL-GROD supersedes GROD (GROD is a subset of DL-GROD).

---

## Code Path

### Primary Implementation

**Original Production:**
- **File:** `src/_legacy/development/reach_definition/Reach_Definition_Tools_v11.py`
- **Node computation:** `basin_node_attributes()` lines 4534-4556
- **Reach computation:** `reach_attributes()` lines 4927-4937

### Algorithm (Original Production)

```python
# From Reach_Definition_Tools_v11.py, basin_node_attributes()

# 1. Get GROD values for centerlines in this node
GROD = np.copy(grod_id[nodes])

# 2. Reset values > 4 to 0 (invalid obstruction codes)
GROD[np.where(GROD > 4)] = 0

# 3. Node obstr_type = MAX of centerline GROD values
node_grod_id[ind] = np.max(GROD)

# 4. Assign corresponding grod_id or hfalls_id based on obstr_type
ID = np.where(GROD == np.max(GROD))[0][0]  # First matching index
if np.max(GROD) == 0:
    node_grod_fid[ind] = 0           # No obstruction
elif np.max(GROD) == 4:
    node_hfalls_fid[ind] = hfalls_fid[nodes[ID]]  # Waterfall -> HydroFALLS ID
else:
    node_grod_fid[ind] = grod_fid[nodes[ID]]      # Dam/Lock/Low-perm -> GROD ID
```

### Current Reconstruction (DuckDB)

**File:** `src/sword_duckdb/reconstruction.py`

**AttributeSpec definitions (lines 301-326):**
```python
"reach.obstr_type": AttributeSpec(
    name="reach.obstr_type",
    source=SourceDataset.GROD,
    method=DerivationMethod.MAX,
    source_columns=["obstruction_type"],
    dependencies=["centerline.grod"],
    description="Obstruction type: np.max(GROD[reach]), values >5 reset to 0. 0=none, 1=dam, 2=low-head dam, 3=lock, 4=waterfall, 5=partial dam"
),

"reach.grod_id": AttributeSpec(
    name="reach.grod_id",
    source=SourceDataset.GROD,
    method=DerivationMethod.SPATIAL_JOIN,
    source_columns=["grod_fid"],
    dependencies=["reach.obstr_type"],
    description="GROD database ID at max obstruction point"
),

"reach.hfalls_id": AttributeSpec(
    name="reach.hfalls_id",
    source=SourceDataset.HYDROFALLS,
    method=DerivationMethod.SPATIAL_JOIN,
    source_columns=["hfalls_fid"],
    dependencies=["reach.obstr_type"],
    description="HydroFALLS ID (only if obstr_type == 4)"
),
```

**Reconstruction methods:**
- `_reconstruct_reach_obstr_type()` (lines 3661-3694): MAX of node obstr_type
- `_reconstruct_node_obstr_type()` (lines 3067-3105): Inherits from reach (fallback)
- `_reconstruct_reach_grod_id()` (lines 3912-3939): **STUB** - preserves existing values
- `_reconstruct_reach_hfalls_id()` (lines 3941-3968): **STUB** - preserves existing values

**Note:** `grod_id` and `hfalls_id` reconstruction are stubs requiring external GROD/HydroFALLS spatial data not currently integrated into DuckDB.

---

## Valid Values

### obstr_type

**v17c encoding** (types 2 and 3 redefined from v17b):

| Value | v17c Meaning | v17b Meaning | Source | ID Field |
|-------|-------------|-------------|--------|----------|
| 0 | No obstruction | No obstruction | N/A | Neither (both = 0) |
| 1 | Dam | Dam | GROD / DL-GROD | grod_id, dl_grod_id |
| 2 | Low-head dam | Lock | DL-GROD | dl_grod_id |
| 3 | Lock | Low permeable dam | GROD / DL-GROD | grod_id, dl_grod_id |
| 4 | Waterfall | Waterfall | HydroFALLS | hfalls_id |
| 5 | Partial dam | Undocumented (5 buggy reaches) | DL-GROD | dl_grod_id |

### grod_id, dl_grod_id, and hfalls_id

| Field | Valid Range | Null/Zero Meaning |
|-------|-------------|-------------------|
| grod_id | 1 - ~30,350 | No GROD obstruction (sunset in v18) |
| dl_grod_id | 1+ | No DL-GROD obstruction |
| hfalls_id | 1 - ~3,934 | No waterfall |

---

## Database Statistics (v17b)

### Reach obstr_type Distribution

| obstr_type | Count | Percentage | Description |
|------------|-------|------------|-------------|
| 0 | 227,194 | 91.36% | No obstruction |
| 1 | 8,104 | 3.26% | Dam |
| 2 | 1,622 | 0.65% | Lock |
| 3 | 10,500 | 4.22% | Low permeable dam |
| 4 | 1,248 | 0.50% | Waterfall |
| 5 | 5 | 0.00% | **UNDOCUMENTED** |

### Node obstr_type Distribution

| obstr_type | Count | Percentage |
|------------|-------|------------|
| 0 | 11,090,464 | 99.80% |
| 1 | 8,260 | 0.07% |
| 2 | 1,670 | 0.02% |
| 3 | 10,804 | 0.10% |
| 4 | 1,251 | 0.01% |
| 5 | 5 | 0.00% |

### Regional Breakdown (Reaches)

| Region | Dams | Locks | Low-Perm | Waterfalls | Total |
|--------|------|-------|----------|------------|-------|
| AF | 199 | 23 | 267 | 155 | 644 |
| AS | 5,045 | 264 | 6,169 | 109 | 11,587 |
| EU | 1,473 | 1,036 | 2,664 | 150 | 5,323 |
| NA | 814 | 276 | 776 | 264 | 2,130 |
| OC | 173 | 11 | 253 | 33 | 470 |
| SA | 400 | 12 | 371 | 537 | 1,320 |

### ID Statistics

| Metric | grod_id | hfalls_id |
|--------|---------|-----------|
| Unique IDs (reaches) | 20,129 | 1,230 |
| Non-zero count (reaches) | 20,247 | 1,232 |
| Min value | 1 | 1 |
| Max value | 30,350 | 3,934 |

---

## Failure Modes

### 1. v17b obstr_type=5 bug (SUPERSEDED by DL-GROD)

**Description:** In v17b, 5 reaches had `obstr_type=5`, which was undocumented in the PDD. All 5 had `hfalls_id != 0`, suggesting they should have been waterfalls (type=4).

**Affected reaches (v17b):**
| reach_id | region | hfalls_id | lakeflag |
|----------|--------|-----------|----------|
| 82291000364 | NA | 18 | 0 (river) |
| 72510000035 | NA | 514 | 3 (tidal) |
| 78220000044 | NA | 628 | 0 (river) |
| 73120000244 | NA | 862 | 1 (lake) |
| 73120000424 | NA | 783 | 0 (river) |

**v17c status:** `obstr_type=5` is now a valid value meaning "partial dam" (DL-GROD, He et al. 2025). These 5 v17b reaches may have been re-assigned by DL-GROD ingestion. The original v17b bug (5 should have been 4) is superseded by the new encoding.

**Severity:** RESOLVED in v17c

### 2. obstr_type/grod_id Mismatch

**Description:** 21 reaches have `grod_id != 0` but `obstr_type = 4` (waterfall). These should have `grod_id = 0` since waterfalls use HydroFALLS.

**Investigation:** All 21 are in region NA and have reach IDs ending in 4 (dam/waterfall type). Sample:
- 72510000854, 72510000864, 72510000874, etc.

**Root cause:** These appear to be reaches where GROD and HydroFALLS locations overlap. The GROD 500m filtering may have been incomplete.

**Impact:** grod_id contains valid GROD IDs, but obstr_type indicates waterfall. The obstruction is likely a dam co-located with a waterfall.

**Severity:** LOW (21 reaches)

### 3. obstr_type/hfalls_id Mismatch

**Description:** 5 reaches have `hfalls_id != 0` but `obstr_type != 4`. All 5 have `obstr_type = 5` (undocumented).

**This is the same as Failure Mode 1.** The `obstr_type=5` bug causes these reaches to show hfalls_id without proper obstr_type classification.

### 4. Missing IDs for Obstructed Reaches

**Check:** Are there reaches with `obstr_type in (1,2,3)` but `grod_id = 0`?

**Result:** 0 reaches - **No issues found.** All GROD-sourced obstruction types have valid grod_id.

**Check:** Are there reaches with `obstr_type = 4` but `hfalls_id = 0`?

**Result:** 21 reaches have this condition. This is because these 21 reaches have both GROD and HydroFALLS locations (see Failure Mode 2), and the algorithm assigned `hfalls_id` from the first matching waterfall point which may be 0.

### 5. Spatial Join Ambiguity

**Description:** When multiple GROD or HydroFALLS points fall within a reach, the algorithm takes the first matching point at the MAX obstruction type location.

**Impact:** If multiple obstructions exist, only one grod_id/hfalls_id is recorded. The choice of which ID is recorded depends on centerline point ordering, not necessarily the most significant obstruction.

**Mitigation:** Consider storing all obstruction IDs in an array column for v18.

### 6. Centerline-Node-Reach Aggregation

**Description:** obstr_type is computed as MAX from centerlines to nodes, then MAX from nodes to reaches. This means a single obstructed centerline point propagates to the entire reach.

**Impact:** A short dam section influences the entire reach's obstr_type.

**Acceptable behavior:** This is intentional - reaches containing ANY obstruction should be flagged.

---

## Lint Checks (Implemented)

**Category:** `O` (Obstruction) — `src/sword_duckdb/lint/checks/classification.py`

### O001: obstr_type_values (ERROR)

**Description:** Check that obstr_type is in valid range {0, 1, 2, 3, 4, 5}.

**Note:** v17b numeric range was 0-4; v17c extends to 0-5. Semantic meanings of 2 and 3 differ between versions (see Valid Values table above).

### O002: grod_id_consistency (WARNING)

**Description:** Check that grod_id and dl_grod_id are non-zero only when obstr_type in {1, 2, 3, 5}. Waterfall reaches (obstr_type=4) should use hfalls_id instead.

**Note:** `ingest_dl_grod.py` clears stale `grod_id` when reclassifying a reach to waterfall.

### O003: hfalls_id_consistency (WARNING)

**Description:** Check that hfalls_id is non-zero only when obstr_type = 4. Uses NULL-safe logic: `obstr_type IS NULL OR obstr_type != 4`.

---

## Proposed Checks (Not Yet Implemented)

### O004: obstruction_mutual_exclusivity (INFO)

**Description:** Check that reaches don't have both grod_id and hfalls_id non-zero (should be mutually exclusive).

**SQL:**
```sql
SELECT reach_id, grod_id, hfalls_id, obstr_type
FROM reaches
WHERE grod_id > 0 AND hfalls_id > 0
```

**Expected failures:** 0 (confirmed)

### O005: node_reach_obstr_consistency (WARNING)

**Description:** Check that reach obstr_type equals MAX of its node obstr_types.

**SQL:**
```sql
SELECT r.reach_id, r.obstr_type as reach_obstr, MAX(n.obstr_type) as max_node_obstr
FROM reaches r
JOIN nodes n ON r.reach_id = n.reach_id AND r.region = n.region
GROUP BY r.reach_id, r.obstr_type
HAVING r.obstr_type != MAX(n.obstr_type)
```

### O006: reach_type_obstr_consistency (INFO)

**Description:** Check that reaches with obstr_type=4 (waterfall) or obstr_type in (1,2,3) (dams) have reach type=4 (dam/waterfall type).

**SQL:**
```sql
SELECT reach_id, obstr_type, reach_id % 10 as reach_type
FROM reaches
WHERE obstr_type IN (1, 2, 3, 4) AND reach_id % 10 != 4
```

**Note:** Most obstructed reaches have type=4 (dam_type), but some have type=1 (river) or type=3 (lake_on_river). This may be acceptable but worth reporting.

---

## Edge Cases

### Co-located GROD and HydroFALLS

Some locations have both anthropogenic obstructions (dams) and natural obstructions (waterfalls) nearby. The HydroFALLS filtering (500m buffer) removes most but not all overlaps.

**Recommendation:** For v18, consider allowing both grod_id and hfalls_id to be non-zero, or add a flag for "co-located obstructions."

### Multiple Obstructions per Reach

A reach may contain multiple dams or waterfalls. Only one ID is stored (the first at the MAX obstruction type location).

**Recommendation:** For v18, consider storing obstruction IDs as arrays: `grod_ids BIGINT[]`, `hfalls_ids BIGINT[]`.

### Reach Type Encoding

Reach IDs encode type in the last digit. Type=4 indicates "dam or waterfall." However, some obstructed reaches have type=1 (river) or type=3 (lake_on_river) based on original GRWL classification.

**Current behavior:** obstr_type is computed independently from reach ID type.

**Consideration:** The reach ID type may not always match obstr_type. For example:
- Reach type=4 with obstr_type=0: Reach was originally classified as dam but later found to have no obstruction
- Reach type=1 with obstr_type=1: River reach with a dam not in original classification

---

## Recommendations

### Short-term (v17c) — DONE

1. ~~**Fix obstr_type=5 bug:**~~ Superseded — obstr_type=5 is now "partial dam" (DL-GROD)
2. **Lint checks O001-O003:** Implemented in `classification.py` (PR #184)
3. **DL-GROD ingestion:** 27,071 reaches updated via `ingest_dl_grod.py` (PR #184)
4. **Document co-located obstructions:** 21 v17b reaches with both GROD and HydroFALLS IDs noted

### Medium-term (v18)

1. **Re-run HydroFALLS filtering:** Increase buffer or use point-in-polygon instead of distance
2. **Support multiple obstruction IDs per reach:** Use array columns
3. **Add obstruction metadata:** Include obstruction name, height, year built from source datasets
4. **Full reconstruction:** Integrate GROD/HydroFALLS spatial data into DuckDB pipeline

---

## References

1. **SWORD v17b PDD** - Tables 3-5 (variable definitions)
2. **Whittemore et al., 2020** - GROD dataset documentation
3. **He et al., 2025** - DL-GROD deep-learning extension of GROD
4. **HydroFALLS** - http://wp.geog.mcgill.ca/hydrolab/hydrofalls/
5. **reconstruction.py** - AttributeSpecs and reconstruction methods
6. **Reach_Definition_Tools_v11.py** - Original production algorithm
7. **hydrofalls_filtering.py** - HydroFALLS preprocessing
8. **ingest_dl_grod.py** - DL-GROD ingestion script (PR #184)

---

## Appendix: SQL Validation Queries

```sql
-- Complete obstruction audit query (v17c)
SELECT
    obstr_type,
    COUNT(*) as count,
    SUM(CASE WHEN grod_id IS NOT NULL AND grod_id != 0 THEN 1 ELSE 0 END) as has_grod_id,
    SUM(CASE WHEN dl_grod_id IS NOT NULL AND dl_grod_id != 0 THEN 1 ELSE 0 END) as has_dl_grod_id,
    SUM(CASE WHEN hfalls_id IS NOT NULL AND hfalls_id != 0 THEN 1 ELSE 0 END) as has_hfalls_id
FROM reaches
GROUP BY obstr_type
ORDER BY obstr_type;

-- Find partial dam reaches (DL-GROD type 5)
SELECT reach_id, region, dl_grod_id, grod_id, hfalls_id, lakeflag, river_name
FROM reaches
WHERE obstr_type = 5;

-- Find inconsistent grod_id/dl_grod_id vs obstr_type (O002 logic)
SELECT reach_id, obstr_type, grod_id, dl_grod_id, hfalls_id
FROM reaches
WHERE (grod_id IS NOT NULL AND grod_id != 0 AND obstr_type NOT IN (1, 2, 3, 5))
   OR (dl_grod_id IS NOT NULL AND dl_grod_id != 0 AND obstr_type NOT IN (1, 2, 3, 5));

-- Find inconsistent hfalls_id vs obstr_type (O003 logic)
SELECT reach_id, obstr_type, grod_id, dl_grod_id, hfalls_id
FROM reaches
WHERE hfalls_id IS NOT NULL AND hfalls_id != 0
  AND (obstr_type IS NULL OR obstr_type != 4);
```
