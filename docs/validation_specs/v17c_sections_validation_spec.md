# Validation Spec: v17c_sections and v17c_section_slope_validation Tables

## Table Purpose

**v17c_sections** decomposes the reach network into junction-to-junction segments, enabling efficient hydrologic analysis at multiple scales. A "section" is a contiguous chain of reaches between network junctions (confluences, bifurcations, headwaters, outlets).

**v17c_section_slope_validation** records slope measurements at section endpoints using SWOT water surface elevation observations, validating flow direction and identifying potential topology or data quality issues.

## Column Definitions

### v17c_sections Table

| Column | Type | Description | Constraints |
|--------|------|-------------|-------------|
| section_id | INTEGER | Unique section identifier within region | NOT NULL, PK (with region) |
| region | VARCHAR(2) | Region code | NOT NULL, PK (with section_id), one of {NA, SA, EU, AF, AS, OC} |
| upstream_junction | BIGINT | Reach ID of upstream junction | NOT NULL, must match reach_id in reaches table |
| downstream_junction | BIGINT | Reach ID of downstream junction | NOT NULL, must match reach_id in reaches table |
| reach_ids | VARCHAR | JSON array of reach IDs in order | NOT NULL, valid JSON array of integers |
| distance | DOUBLE | Total section length in meters | ≥ 100m (typically), NULL if computation failed |
| n_reaches | INTEGER | Count of reaches in section | ≥ 1, should match length of reach_ids array |

### v17c_section_slope_validation Table

| Column | Type | Description | Constraints |
|--------|------|-------------|-------------|
| section_id | INTEGER | Reference to v17c_sections | NOT NULL, PK (with region), FK to v17c_sections |
| region | VARCHAR(2) | Region code | NOT NULL, PK (with section_id) |
| slope_from_upstream | DOUBLE | Slope measured from upstream junction (m/m) | Can be negative or NULL if insufficient WSE data |
| slope_from_downstream | DOUBLE | Slope measured from downstream junction (m/m) | Can be positive or NULL if insufficient WSE data |
| direction_valid | BOOLEAN | True if slopes match expected signs | NULL if cannot compute, FALSE indicates anomaly |
| likely_cause | VARCHAR | Reason for invalid direction | One of {lake_section, extreme_slope_data_error, potential_topology_error}, NULL if valid |

## Section Creation Algorithm

### High-Level Steps

```
1. Build reach-level directed graph from reach_topology
   - Nodes: all reaches
   - Edges: upstream->downstream relationships
   - Node attributes: reach_length, width, facc, wse_obs_mean, etc.

2. Identify junctions (connectivity breaks)
   - Headwaters: in_degree = 0
   - Outlets: out_degree = 0
   - Confluences: in_degree > 1
   - Bifurcations: out_degree > 1
   - Single reach: in_degree = out_degree = 1 but marked as junction (rare)

3. For each junction, trace downstream successors
   - Start at first downstream reach after junction
   - Follow chain of single-successor reaches
   - Stop when reaching another junction
   - Record all reaches in chain plus endpoints

4. Create section record
   - section_id: incremental counter per region
   - reach_ids: chain of reaches including both junctions
   - distance: sum of reach_length for all reaches
   - n_reaches: len(reach_ids)
```

### Pseudocode from v17c_pipeline.py (lines 178-248)

```python
def build_section_graph(G: nx.DiGraph, junctions: Set[int]):
    sections = []
    section_id = 0

    for upstream_j in junctions:
        for first_reach in G.successors(upstream_j):
            reach_ids = []
            cumulative_dist = 0
            current = first_reach

            # Trace chain until next junction
            while current not in junctions:
                reach_ids.append(current)
                cumulative_dist += G.nodes[current].get('reach_length', 0)
                succs = list(G.successors(current))
                if not succs:
                    break
                current = succs[0]

            # Downstream junction
            downstream_j = current
            if downstream_j in junctions:
                reach_ids.append(downstream_j)  # INCLUDE downstream junction
                cumulative_dist += G.nodes[downstream_j].get('reach_length', 0)

                sections.append({
                    'section_id': section_id,
                    'upstream_junction': upstream_j,
                    'downstream_junction': downstream_j,
                    'reach_ids': reach_ids,
                    'distance': cumulative_dist,
                    'n_reaches': len(reach_ids),
                })
                section_id += 1

    return pd.DataFrame(sections)
```

### Key Properties

1. **reach_ids ordering:** Upstream to downstream (first = upstream junction)
2. **Reach inclusion:** Reach appears in exactly one section
3. **Junction inclusion:** Junctions appear in sections as BOTH upstream and downstream endpoints
4. **Distance metric:** Sum of individual reach_length values
5. **Single-reach sections:** Sections with n_reaches=1 are POSSIBLE (reach is isolated junction)

## Valid Ranges

### section_id
- **Range:** 0 to (total_sections - 1) per region
- **Distribution:** Non-unique across regions (each region restarts at 0)
- **Expected:** 2,979 - 18,634 sections per region (from 2026-01-27 pipeline run)

### distance
- **Minimum:** Theoretically 0m for empty reaches
- **Typical minimum:** 100m (most reaches are >100m)
- **Maximum:** Few thousand km for longest sections
- **Example distribution (from pipeline):**
  - Median section: 1-2 km
  - Longest sections: 10-50 km
  - Very short sections: <100m (possible but rare)

### n_reaches
- **Minimum:** 1 (single reach is both junction and non-junction neighbor)
- **Typical:** 2-20 reaches per section
- **Maximum:** Rare >100 reach sections (only in complex braided/delta regions)
- **Relationship to distance:** Expected ~distance / average_reach_length

### reach_ids Array
- **Format:** JSON array of integers (as string in DuckDB)
- **Length:** Must equal n_reaches
- **Ordering:** Strictly upstream-to-downstream
- **Uniqueness:** Each reach_id appears in exactly one section's reach_ids array

## Expected Data Distributions (from 2026-01-27 Pipeline Run)

### By Region

| Metric | NA | SA | EU | AF | AS | OC | Total |
|--------|----|----|----|----|----|----|-------|
| Reaches | 38,696 | 42,159 | 31,103 | 21,441 | 100,185 | 15,090 | 248,674 |
| Sections | 6,363 | 7,272 | 4,222 | 3,137 | 18,634 | 2,979 | 42,607 |
| Avg reaches/section | 6.1 | 5.8 | 7.4 | 6.8 | 5.4 | 5.1 | 5.8 |
| Validation records | 3,318 | 3,893 | 2,276 | 1,786 | 9,891 | 1,545 | 22,709 |
| Direction valid | 91.6% | 93.2% | 92.1% | 93.4% | 94.3% | 90.3% | 93.2% |

### reach_ids Array Size Distribution

Expected distribution follows power law:
- ~10% of sections: n_reaches = 1 (isolated junctions)
- ~30-40%: n_reaches = 2-5
- ~40-50%: n_reaches = 6-20
- ~10-15%: n_reaches > 20

## Failure Modes

### F1: reach_ids Array Contains Duplicates
- **Symptom:** Same reach_id appears multiple times in reach_ids array for single section
- **Cause:** Graph has cycles (not a DAG); section tracing looped back on itself
- **Impact:** Invalid section geometry; distance calculation inflated
- **Validation check:** Parse JSON, verify all elements unique

### F2: reach_ids Not in Upstream-to-Downstream Order
- **Symptom:** Intermediate reach_id appears out of sequence
- **Cause:** Graph construction error; section tracing didn't follow direct path
- **Impact:** Slope calculation from_upstream/from_downstream meaningless
- **Validation check:** Verify each consecutive pair has topology relationship

### F3: reach_ids Length Mismatch
- **Symptom:** len(reach_ids_array) != n_reaches
- **Cause:** Data corruption during JSON serialization
- **Impact:** Analysis code breaks when unpacking; distance can be recomputed but n_reaches misleading
- **Validation check:** JSON parse, count array length

### F4: Upstream/Downstream Junction Not in Graph
- **Symptom:** upstream_junction or downstream_junction reach_id not in reaches table
- **Cause:** Corrupted foreign key reference
- **Impact:** Cannot trace back to topology; joins fail
- **Validation check:** Foreign key existence check

### F5: Upstream Junction Not Actually Upstream
- **Symptom:** upstream_junction has n_rch_down=0 OR is not in topology above first reach_id
- **Cause:** Graph construction error; wrong junction selected
- **Impact:** Inverted section direction; slope calculations reversed
- **Validation check:** Verify upstream_junction is predecessor of reach_ids[1]

### F6: reach_ids Contains Non-Junction as Endpoint
- **Symptom:** reach_ids[0] != upstream_junction OR reach_ids[-1] != downstream_junction
- **Cause:** reach_ids array missing or has wrong endpoints
- **Impact:** Section endpoints ambiguous
- **Validation check:** Array endpoints must match junction fields

### F7: Section Disconnection
- **Symptom:** gap(s) in reach_ids chain (not all reaches connected in topology)
- **Cause:** Graph has missing edges or cycles
- **Impact:** Topology inconsistency
- **Validation check:** Verify consecutive pairs connected via reach_topology

### F8: distance Field Invalid
- **Symptom:** distance < sum(reach_length) for reaches in reach_ids
- **Cause:** Calculation error; missed reaches or junction length not included
- **Impact:** Slope computations incorrect
- **Validation check:** Recompute sum from reach table

### F9: NULL reach_ids
- **Symptom:** reach_ids column is NULL for valid section
- **Cause:** Serialization failed; data corruption
- **Impact:** Cannot reconstruct reach chain; critical data missing
- **Validation check:** Non-NULL constraint check

### F10: slope_from_upstream Wrong Sign
- **Symptom:** slope_from_upstream > 0 (should be ≤ 0 for normal flow)
- **Cause:** WSE measurements in wrong order; data error
- **Impact:** Flow direction inverted; slope interpretation wrong
- **Direction valid = FALSE** in validation table

### F11: slope_from_downstream Wrong Sign
- **Symptom:** slope_from_downstream < 0 (should be ≥ 0 for normal flow)
- **Cause:** WSE measurements in wrong order; data error
- **Impact:** Flow direction inverted
- **Direction valid = FALSE** in validation table

### F12: insufficient_wse_Data
- **Symptom:** n_reaches_with_wse < 2 for section
- **Cause:** SWOT coverage gap; only endpoint or no observations
- **Impact:** Cannot compute slope_from_upstream/downstream
- **Result:** slope_from_upstream and slope_from_downstream are NULL

### F13: Extreme Slope Outlier
- **Symptom:** slope_from_upstream or slope_from_downstream > 0.05 (5%)
- **Cause:** SWOT data error; sensor artifact
- **Impact:** Physically unrealistic (rivers rarely >0.5%); suggests data quality issue
- **Flagged as:** likely_cause = "extreme_slope_data_error"

### F14: lake_section Classification
- **Symptom:** direction_valid = FALSE but likely_cause = "lake_section"
- **Cause:** Slopes inverted because water impounds or bifurcates in lake
- **Impact:** Expected behavior for wide/delta sections; NOT a data error
- **Validation:** These are normal; track percentage by region

## Proposed Lint Checks

### New Section Validation Category (S0xx)

**Severity levels:**
- **ERROR** - Data integrity violation; blocks analysis
- **WARNING** - Logical inconsistency; may cause incorrect results
- **INFO** - Statistical observation; helps understand data quality

### Proposed Checks

| ID | Name | Severity | Rule | Description |
|----|------|----------|------|-------------|
| S001 | section_count | INFO | Expected: 2,500 - 20,000 per region | Count of sections by region (baseline check) |
| S002 | reach_ids_parse | ERROR | reach_ids must be valid JSON array | Verify JSON serialization |
| S003 | reach_ids_length | ERROR | len(reach_ids) == n_reaches | Array size consistency |
| S004 | reach_ids_unique | ERROR | All reach_ids unique within section | No duplicate reaches |
| S005 | reach_ids_ordered | WARNING | Each consecutive pair in reach_topology | Upstream-to-downstream order |
| S006 | endpoint_consistency | ERROR | reach_ids[0] == upstream_junction, reach_ids[-1] == downstream_junction | Endpoint matching |
| S007 | junction_foreign_key | ERROR | upstream/downstream junctions exist in reaches | Referential integrity |
| S008 | distance_recompute | WARNING | Recompute from reach_length sum; allow ±100m tolerance | Distance accuracy |
| S009 | n_reaches_bounds | WARNING | 1 ≤ n_reaches ≤ 500 (practical limit) | Section size sanity |
| S010 | orphan_section | WARNING | Both junctions must have topology edges | Sections must connect to network |
| S011 | section_coverage | INFO | All reaches must appear in exactly one section | Network completeness |
| S012 | slope_direction_distribution | INFO | Track % with direction_valid=TRUE/FALSE/NULL by region | Baseline validation stats |
| S013 | slope_extreme_values | WARNING | |slope| > 0.05 only for documented cause | Outlier flagging |
| S014 | slope_wse_coverage | INFO | % sections with sufficient WSE data for slope | Data availability stats |

### SQL Templates for Proposed Checks

#### S002: reach_ids JSON Validity
```sql
SELECT section_id, region, reach_ids
FROM v17c_sections
WHERE region = ?
  AND (reach_ids IS NULL
       OR reach_ids NOT LIKE '[%]'  -- Not array syntax
       OR TRY_CAST(reach_ids AS JSON) IS NULL)
LIMIT 100;
```

#### S003: reach_ids Length Mismatch
```sql
SELECT section_id, region, n_reaches, json_array_length(reach_ids) as array_len
FROM v17c_sections
WHERE region = ?
  AND json_array_length(reach_ids) != n_reaches;
```

#### S004: Duplicate reach_ids
```sql
-- PostgreSQL-style; DuckDB syntax may vary
SELECT section_id, region, reach_ids
FROM v17c_sections
WHERE region = ?
  AND (SELECT COUNT(DISTINCT x)
       FROM json_array_elements(reach_ids) as x)
      != json_array_length(reach_ids);
```

#### S005: Topology Order Verification
```sql
WITH reach_pairs AS (
    SELECT
        s.section_id, s.region,
        json_extract_string(reach_ids, 0)::BIGINT as first_reach,
        json_extract_string(reach_ids, json_array_length(reach_ids)-1)::BIGINT as last_reach
    FROM v17c_sections s
    WHERE region = ?
)
SELECT s.section_id, s.region
FROM reach_pairs rp
JOIN v17c_sections s ON s.section_id = rp.section_id
WHERE NOT EXISTS (
    SELECT 1 FROM reach_topology rt
    WHERE rt.reach_id = s.upstream_junction
      AND rt.neighbor_reach_id = rp.first_reach
      AND rt.direction = 'down'
);
```

#### S008: Distance Recomputation
```sql
SELECT
    s.section_id, s.region,
    s.distance,
    COALESCE(SUM(r.reach_length), 0) as computed_distance,
    ABS(s.distance - COALESCE(SUM(r.reach_length), 0)) as discrepancy
FROM v17c_sections s
LEFT JOIN reaches r ON r.reach_id IN (
    SELECT json_extract_string(reach_ids, i)::BIGINT
    FROM generate_subscripts(reach_ids, 1) as t(i)
)
WHERE s.region = ?
GROUP BY s.section_id, s.region, s.distance
HAVING discrepancy > 100;  -- 100m tolerance
```

#### S012: Validation Direction Distribution
```sql
SELECT
    region,
    direction_valid,
    COUNT(*) as count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY region), 1) as pct
FROM v17c_section_slope_validation
GROUP BY region, direction_valid
ORDER BY region, direction_valid;
```

#### S013: Extreme Slope Values
```sql
SELECT
    section_id, region,
    slope_from_upstream, slope_from_downstream,
    direction_valid, likely_cause
FROM v17c_section_slope_validation
WHERE (ABS(slope_from_upstream) > 0.05 OR ABS(slope_from_downstream) > 0.05)
  AND likely_cause IS NOT NULL
ORDER BY region, ABS(slope_from_upstream) DESC
LIMIT 100;
```

#### S014: WSE Coverage Stats
```sql
SELECT
    region,
    COUNT(*) as total_sections_with_validation,
    SUM(CASE WHEN n_reaches_with_wse >= 2 THEN 1 ELSE 0 END) as sections_with_slope,
    ROUND(100.0 * SUM(CASE WHEN n_reaches_with_wse >= 2 THEN 1 ELSE 0 END)
          / COUNT(*), 1) as coverage_pct
FROM (
    SELECT region, json_array_length(reach_ids) as n_reaches_with_wse
    FROM v17c_section_slope_validation
)
GROUP BY region;
```

## Implementation Roadmap

### Phase 1: Critical Checks (S002, S003, S004, S006, S007)
- Data integrity checks
- Can be implemented in ~30 minutes
- Python code in `src/sword_duckdb/lint/checks/sections.py`

### Phase 2: Topology Checks (S005, S010, S011)
- Relationship validation
- Requires join with reach_topology
- ~1 hour implementation

### Phase 3: Statistics & Monitoring (S001, S012, S013, S014)
- INFO-level reporting
- Useful for tracking data quality trends
- ~1 hour implementation

### Phase 4: Distance Recomputation (S008)
- Most complex; requires reach_length aggregation
- Useful for validation but expensive to compute
- Optional; defer unless needed

## Known Issues & Edge Cases

### Issue 1: Single-Reach Sections
- **Symptom:** n_reaches = 1
- **Frequency:** ~10% of all sections
- **Cause:** Reach is both junction and regular reach (unusual connectivity)
- **Validation:** Not an error; track percentage

### Issue 2: Very Short Sections
- **Symptom:** distance < 100m
- **Frequency:** <1% of sections
- **Cause:** Short connecting reaches between junctions
- **Validation:** Not an error; verify reach_length data quality

### Issue 3: Delta/Braided Regions
- **Symptom:** Many bifurcations; high section count
- **Frequency:** Concentrated in deltas (e.g., Mississippi, Amazon)
- **Cause:** Natural network complexity
- **Validation:** Expected; check connectivity

### Issue 4: Direction Invalid Distribution
- **Symptom:** 90-94% direction_valid=TRUE; 6-10% FALSE; rest NULL
- **Frequency:** All regions
- **Cause:** SWOT slope estimation has inherent uncertainty; some lake/delta sections have complex hydrology
- **Validation:** Within expected range; outliers may indicate topology issues

### Issue 5: Multiple Sections per Junction Pair
- **Symptom:** Same (upstream_junction, downstream_junction) appears multiple times
- **Frequency:** Should NOT occur
- **Cause:** Graph construction error; traced multiple paths
- **Validation check:** SELECT ... GROUP BY upstream_junction, downstream_junction HAVING COUNT(*) > 1

## Relationship to Other Tables

### Joins to reaches
```sql
-- Get section reach details
SELECT s.section_id, r.reach_id, r.reach_length, r.width, r.facc
FROM v17c_sections s
JOIN reaches r ON s.region = r.region
WHERE s.region = 'NA'
  AND r.reach_id IN (
      SELECT json_extract_string(reach_ids, i)::BIGINT
      FROM generate_subscripts(reach_ids, 1) as t(i)
  );
```

### Joins to reach_topology
```sql
-- Verify section connectivity
SELECT s.section_id, s.region, COUNT(*) as topology_edges
FROM v17c_sections s
-- This join is complex; see S005 template above
WHERE s.region = 'NA';
```

### Joins to v17c_section_slope_validation
```sql
SELECT s.*, v.slope_from_upstream, v.direction_valid
FROM v17c_sections s
LEFT JOIN v17c_section_slope_validation v
  ON s.section_id = v.section_id
  AND s.region = v.region
WHERE s.region = 'NA';
```

## Creation Source Code

**File:** `/Users/jakegearon/projects/SWORD/src/sword_v17c_pipeline/v17c_pipeline.py`

**Key Functions:**
- `build_section_graph()` (lines 178-248) - Main section creation
- `build_reach_graph()` (lines 99-152) - Graph construction prerequisite
- `identify_junctions()` (lines 155-175) - Junction detection
- `compute_junction_slopes()` (lines 453-568) - Slope validation

**Table Creation:**
- `create_v17c_tables()` (lines 908-933) - Schema definition

**Data Insertion:**
- `save_sections_to_duckdb()` (lines 647-700) - Insert operation with provenance

## Testing Strategy

### Unit Tests Needed

```python
# tests/test_v17c_sections.py

def test_section_coverage():
    """All reaches appear in exactly one section"""
    # Query: SELECT COUNT(DISTINCT section_id) as num_sections
    #        FROM v17c_sections WHERE region = 'NA'

def test_reach_ids_format():
    """reach_ids is valid JSON array"""
    # Parse each reach_ids value; assert valid JSON

def test_endpoint_consistency():
    """reach_ids endpoints match junction columns"""
    # For each section: assert reach_ids[0] == upstream_junction

def test_distance_accuracy():
    """distance recomputable from reach_length"""
    # Aggregate reach_length for all reaches in section

def test_topology_connectivity():
    """Consecutive reaches connected in reach_topology"""
    # Verify topology edges exist for consecutive pairs
```

### Integration Tests Needed

```python
def test_section_graph_connectivity():
    """Section graph is directed acyclic (no cycles)"""
    # Build section graph; run topological_sort

def test_section_reach_coverage():
    """Every reach in region appears in some section"""
    # Count reaches in all sections vs. total in region
```

## Reference Data (2026-01-27)

Pipeline execution captured:
- **Total sections:** 42,607
- **Total reaches covered:** 248,674 (100% of v17b reaches)
- **Sections with SWOT validation data:** 22,709 (53.3%)
- **Average section length:** 1.8 km (range 0.1 - 50+ km)
- **Average reaches per section:** 5.8 (range 1 - 500+)

**By region detailed metrics:** See "Expected Data Distributions" table above.

## Recommendations

### For Immediate Implementation

1. **Implement S002, S003, S004 (reach_ids integrity)** - Quick wins, catch data corruption early
2. **Add S001 section count baseline** - Monitor for pipeline changes
3. **Expose S012 in reporting** - Track validation quality metrics

### For Monitoring

1. **Track direction_valid % by region** - Early warning for topology issues
2. **Monitor extreme slopes (S013)** - Identify outlier SWOT data
3. **Report orphan sections quarterly** - Detect network fragmentation

### For Future Work

1. **Implement S005 ordering check** - Catch graph construction bugs
2. **Implement S008 distance recomputation** - Quality assurance before export
3. **Add section-level statistics to metadata** - Support downstream analytics

## File Locations

| File | Purpose |
|------|---------|
| `/Users/jakegearon/projects/SWORD/src/sword_v17c_pipeline/v17c_pipeline.py` | Section creation algorithm |
| `/Users/jakegearon/projects/SWORD/src/sword_v17c_pipeline/README.md` | Pipeline documentation |
| `/Users/jakegearon/projects/SWORD/data/duckdb/sword_v17c.duckdb` | Live database with tables |
| `/Users/jakegearon/projects/SWORD/docs/validation_specs/validation_spec_v17c_mainstem_variables.md` | Related v17c variables spec |
| `/Users/jakegearon/projects/SWORD/src/sword_duckdb/lint/checks/` | Where lint checks will live |

## See Also

- [v17c Mainstem Variables Validation Spec](validation_spec_v17c_mainstem_variables.md)
- [v17c Pipeline README](../../src/sword_v17c_pipeline/README.md)
