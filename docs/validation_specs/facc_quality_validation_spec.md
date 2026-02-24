# Validation Specification: facc_quality

## Summary

| Property | Value |
|----------|-------|
| **Type** | Quality Flag (Metadata Column) |
| **Applied to** | Reaches table |
| **Introduced** | v17c (facc violation detection workflow) |
| **Purpose** | Track facc corrections and identify suspect/unfixable values |
| **Data Type** | VARCHAR |

---

## 1. Official Definition

**facc_quality** is a flag that indicates the quality or origin status of a `facc` (flow accumulation) value in a reach. It serves as metadata to document:
1. Which reaches have been corrected vs. original values
2. Why reaches were flagged (suspect, unfixable)
3. Which corrections were automated vs. manual

**Source:** Internal tracking column added by `fix_facc_violations()` workflow in SWORD DuckDB processing.

---

## 2. Valid Values

| Value | Meaning | How Set | Mutable? |
|-------|---------|---------|----------|
| NULL | Not yet assessed | Default (no facc_quality assigned) | Yes |
| 'original' | Original MERIT Hydro value (never checked/fixed) | Not currently set in code | Yes |
| 'traced' | Automatically fixed via upstream tracing algorithm | `fix_facc_violations()` when good source found | Yes |
| 'suspect' | Flagged as potentially corrupted but no fix available | `fix_facc_violations()` when tracing failed | Yes |
| 'unfixable' | Manually flagged as unable to be fixed | topology_reviewer.py manual review | Yes |
| 'manual_fix' | Manually corrected by user in topology_reviewer GUI | topology_reviewer.py interactive editor | Yes |

---

## 3. How facc_quality is Set

### 3.1 New Detection Pipeline (2026-02-04)

**Location:** `src/sword_duckdb/facc_detection/`

The new detection pipeline uses `detect_hybrid()` with multiple detection rules:

| Rule | Criteria | Description |
|------|----------|-------------|
| entry_point | facc_jump > 10 AND ratio_to_median > 50 | Bad facc enters network |
| jump_entry | path_freq invalid AND facc_jump > 20 AND FWR > 500 | D8 error with missing metadata |
| junction_extreme | FWR > 15000 AND end_reach = 3 AND facc_jump > 10 | Extreme at junctions |
| headwater_extreme | n_rch_up = 0 AND facc > 500K AND FWR > 5000 | Impossible headwater facc |

**Run command:**
```bash
python -m src.sword_duckdb.facc_detection.cli \
    --db data/duckdb/sword_v17b.duckdb \
    --all \
    --export-geojson \
    --output-dir output/facc_detection/
```

**Output:** GeoJSON files for QGIS review (`entry_point.geojson`, `jump_entry.geojson`, etc.)

### 3.2 RF Regressor Correction (2026-02) — REMOVED

RF-based correction was superseded by topology-aware denoising (`correct_facc_denoise.py`).
Code removed in rf-features cleanup.

### 3.3 Legacy Detection: fix_facc_violations() Method

**Location:** `/Users/jakegearon/projects/SWORD/src/sword_duckdb/workflow.py` (lines 3517-3763)

**Algorithm Overview:**
```python
def fix_facc_violations(
    dry_run=True,
    width_facc_ratio_threshold=5000.0,
    max_upstream_hops=10,
    downstream_increment=10.0
):
    """
    1. PHASE 1: Identify corrupted reaches
       - Measure facc/width ratio
       - Flag as corrupted if ratio > 5000 AND facc > 50000 km²

    2. PHASE 2: Trace upstream for good values
       - For each corrupted reach, traverse upstream
       - Search up to 10 hops for "good" facc source
       - "Good source" criteria:
         a) Reasonable facc/width ratio (< 1000)
         b) Significantly LOWER facc (< 50% of corrupted value)

    3. PHASE 3: Apply fixes or flag
       - IF good source found:
         * new_facc = good_facc + (hops × 10)
         * Set facc_quality = 'traced'
         * Update both reaches AND nodes
       - ELSE (no good source):
         * Leave facc unchanged
         * Set facc_quality = 'suspect'
    """
```

**Detection Criteria for Corruption:**

Reaches are identified as "corrupted" when BOTH conditions hold:
1. `facc / width > 5000` (facc is extremely high relative to width)
2. `facc > 50000` km² (facc is in mainstem range)

**Rationale:** Small channels (<200m wide) should have low-moderate facc (<5000 km²). If a small channel has mainstem-level facc, it's likely due to data corruption or inappropriate flow accumulation assignment.

**Example corruptions found:**
- Tributary with width=50m but facc=2,000,000 km² (ratio=40,000)
- Secondary distributary with width=100m but facc=500,000 km² (ratio=5,000)

### 3.4 Manual Correction: topology_reviewer GUI

**Location:** `deploy/reviewer/app.py`

**Interactive Workflow:**

The GUI provides four options for manually addressing facc issues:

1. **Approve Current Value**
   - Keep existing facc unchanged
   - No facc_quality update (remains NULL or previous value)

2. **Apply Traced Fix** (if algorithm suggested one)
   - Accept the automated upstream trace suggestion
   - Set facc_quality = 'traced'
   - Update facc value

3. **Manually Edit Value**
   - User enters custom facc value
   - Set facc_quality = 'manual_fix'
   - Log edit to sword_operations table with reason

4. **Flag as Unfixable**
   - Mark facc as beyond repair (e.g., too much uncertainty)
   - Set facc_quality = 'unfixable'
   - facc value remains unchanged
   - Log to edit_flag column with reason

---

## 4. Relationship to facc Column

**Critical Point:** `facc_quality` is METADATA about the `facc` value, not a replacement for it.

| Scenario | facc value | facc_quality | Interpretation |
|----------|-----------|--------------|-----------------|
| Good value from MERIT | 1,250 | NULL / 'original' | Trust facc directly |
| Corrected corruption | 45,000 (new) | 'traced' | facc was fixed upstream tracing; use with annotation |
| Suspected corruption | 2,000,000 | 'suspect' | facc value uncertain; flag in analysis |
| User-corrected | 50,000 (manual) | 'manual_fix' | User judgment; document reason in edit_flag |
| Beyond repair | 2,000,000 | 'unfixable' | Give up; document why in edit_flag |

---

## 5. Why Only 0.1% of Reaches Have facc_quality Set?

### Current Status (v17c as of Feb 2026)

The workflow that populates `facc_quality` is **optional** and must be explicitly invoked:

```python
workflow = SWORDWorkflow(user_id="jake")
sword = workflow.load('data/duckdb/sword_v17c.duckdb', 'NA')

# This must be called manually - it doesn't run automatically:
result = workflow.fix_facc_violations(
    dry_run=False,  # Actually apply fixes
    update_nodes=True
)
```

**Reasons for low coverage:**

1. **Computational Expense:** The algorithm requires:
   - Building reach ID → index mappings
   - Computing facc/width ratios
   - Tracing upstream paths (up to 10 hops × number of corrupted reaches)
   - Database UPDATEs per corrected reach

   Running this on 248K reaches takes substantial time (1-5 min per region).

2. **Risk Tolerance:** The corruption detection threshold is **conservative**:
   - Only flags `facc/width > 5000 AND facc > 50000`
   - Misses subtler corruption (e.g., ratio=1500 or facc=30000)
   - Intentional to avoid over-fixing legitimate edge cases

3. **Manual Review Workflow:** The intent is:
   - Run algorithm in `dry_run=True` mode (report only)
   - Review flagged reaches in topology_reviewer GUI
   - Manually approve/edit individual corrections
   - Only "trusted" corrections get `facc_quality` set

4. **Incomplete Rollout:** As of v17c-updates branch, the workflow has not been run to completion on all regions. Expected coverage after full rollout: ~2-5% (reaches with actual facc/width ratio violations).

### Population Breakdown (Expected Post-Rollout)

Based on algorithm parameters:

| Category | Expected % | Count (248K reaches) | facc_quality value |
|----------|-----------|----------------------|-------------------|
| No corruption detected | ~95-98% | ~236K-243K | NULL |
| Traced (fixable) | ~1-3% | ~2.5K-7.5K | 'traced' |
| Suspect (unfixable) | ~0.2-1% | ~0.5K-2.5K | 'suspect' |
| Unfixable (manual) | ~0.5% | ~1.2K | 'unfixable' |
| Manual fixes | ~0.5% | ~1.2K | 'manual_fix' |

**Why not 100% coverage?**

Most reaches have reasonable facc values that don't trigger the corruption threshold. Only severely corrupted cases (small channel with mainstem facc) are detected.

---

## 6. Failure Modes

| Failure | Cause | Impact | Detection |
|---------|-------|--------|-----------|
| FM1 | facc_quality set but facc unchanged | Inconsistent metadata | Manual review |
| FM2 | 'traced' but upstream hop count > max | Algorithm exceeded bounds | Compare against max_upstream_hops=10 |
| FM3 | 'suspect' but traceable source exists | Insufficient upstream exploration | Increase max_upstream_hops and retry |
| FM4 | 'unfixable' but value obviously wrong | Premature flagging in GUI | Check edit_flag reason column |
| FM5 | Manual correction lost on recalculation | Provenance not preserved | Check sword_operations log |

---

## 7. Dependency Graph

**facc_quality depends on:**
- `facc` column (the actual values being assessed)
- `width` column (used to detect corruption ratio)
- `reach_topology` table (used for upstream tracing)
- `rch_id_up` array (parent reach ID lookups)

**Other columns depend on facc_quality:**
- None directly; facc_quality is read-only metadata for filtering/analysis

---

## 8. Proposed Lint Checks

### F007: facc_quality Distribution

**Category:** Attributes
**Severity:** INFO
**Purpose:** Report coverage of facc_quality assessments

```python
@register_check("F007", Category.ATTRIBUTES, Severity.INFO, ...)
def check_facc_quality_distribution(conn, region=None):
    """
    Report how many reaches have been assessed for facc corruption.
    """
    query = """
    SELECT
        region,
        COUNT(*) as total,
        COUNT(CASE WHEN facc_quality IS NULL THEN 1 END) as not_assessed,
        COUNT(CASE WHEN facc_quality = 'traced' THEN 1 END) as traced_fixes,
        COUNT(CASE WHEN facc_quality = 'suspect' THEN 1 END) as suspect,
        COUNT(CASE WHEN facc_quality = 'unfixable' THEN 1 END) as unfixable,
        COUNT(CASE WHEN facc_quality = 'manual_fix' THEN 1 END) as manual_fixes
    FROM reaches
    WHERE region = ?
    GROUP BY region
    """
```

**Output Format:**
```
Region: NA
  Not assessed: 147,892 (99.8%)
  Traced fixes: 185 (0.12%)
  Suspect: 42 (0.03%)
  Unfixable: 28 (0.02%)
  Manual fixes: 53 (0.04%)
```

### F008: Suspect Reach Review

**Category:** Attributes
**Severity:** WARNING
**Purpose:** Flag suspect reaches that may need attention

```python
@register_check("F008", Category.ATTRIBUTES, Severity.WARNING, ...)
def check_suspect_reaches(conn, region=None):
    """
    List reaches marked as 'suspect' (corruption detected but no fix available).
    These should be investigated to determine if facc values are actually wrong.
    """
    query = """
    SELECT reach_id, region, facc, width,
           facc / NULLIF(width, 0) as facc_width_ratio,
           river_name, edit_flag
    FROM reaches
    WHERE facc_quality = 'suspect'
        AND region = ?
    ORDER BY facc DESC
    """
```

### F009: facc_quality vs Monotonicity

**Category:** Topology + Attributes
**Severity:** WARNING
**Purpose:** Check if facc_quality corrections resolved monotonicity violations

```python
@register_check("F009", Category.ATTRIBUTES, Severity.WARNING, ...)
def check_facc_quality_effectiveness(conn, region=None):
    """
    Among reaches flagged as 'traced' (auto-corrected),
    verify that the correction resolved the facc violation.
    """
    query = """
    SELECT r1.reach_id, r1.facc_quality,
           r1.facc as r1_facc, r2.facc as r2_facc,
           CASE
               WHEN r2.facc >= r1.facc * 0.95 THEN 'fixed'
               ELSE 'still_violated'
           END as fix_status
    FROM reaches r1
    JOIN reach_topology rt ON r1.reach_id = rt.reach_id
    JOIN reaches r2 ON rt.neighbor_reach_id = r2.reach_id
    WHERE r1.facc_quality = 'traced'
        AND rt.direction = 'down'
        AND r1.region = ?
    """
```

---

## 9. Code Path

### Main Implementation

**File:** `/Users/jakegearon/projects/SWORD/src/sword_duckdb/workflow.py`

**Function:** `fix_facc_violations()` (lines 3517-3763)

**Key sections:**
- Lines 3579-3590: PHASE 1 - Identify corrupted reaches
- Lines 3593-3636: PHASE 2 - Trace upstream for good values
- Lines 3638-3663: Track corrections and quality flags
- Lines 3701-3749: Apply fixes and set facc_quality

### Interactive GUI

**File:** `deploy/reviewer/app.py`

**Key sections:**
- Lines 189-191: Set facc_quality on manual edits
- Lines 224-228: Retrieve facc_quality for display
- Lines 967-977: Query monotonicity issues, filter by facc_quality
- Lines 985-991: Query headwater issues, exclude already-fixed
- Lines 996-1004: Query suspect reaches
- Lines 1178-1182: Flag reaches as 'unfixable'

---

## 10. Usage Examples

### Example 1: Identify Reaches Needing Manual Review

```sql
-- Find all reaches that were flagged as suspect
-- (corruption detected but algorithm couldn't fix)
SELECT
    reach_id, river_name, facc, width,
    facc / NULLIF(width, 0) as ratio,
    edit_flag
FROM reaches
WHERE facc_quality = 'suspect'
    AND region = 'NA'
ORDER BY facc DESC
LIMIT 20;
```

### Example 2: Verify Corrections Were Applied

```sql
-- Check that 'traced' corrections had higher facc/width ratio
-- (indicating they were indeed corrupted before fix)
SELECT
    COUNT(*) as traced_count,
    ROUND(AVG(facc), 0) as avg_facc_after,
    ROUND(AVG(wth), 0) as avg_width,
    ROUND(AVG(facc / NULLIF(wth, 0)), 0) as avg_ratio_after
FROM reaches
WHERE facc_quality = 'traced'
    AND region = 'NA';
```

### Example 3: Exclude Fixed Values from Analysis

```sql
-- When doing flow analysis, use facc with confidence
-- NULL/traced = high confidence
-- suspect/unfixable/manual = requires caution
SELECT
    reach_id, facc, facc_quality,
    CASE
        WHEN facc_quality IN (NULL, 'traced') THEN 'high_confidence'
        WHEN facc_quality = 'original' THEN 'original_merit'
        ELSE 'requires_caution'
    END as facc_confidence
FROM reaches
WHERE region = 'NA'
LIMIT 10;
```

---

## 11. Integration with v17c Pipeline

**Current Status:** facc_quality is **not** part of the automatic v17c pipeline. It must be managed separately through:

1. **Automated detection:** `fix_facc_violations()` method
2. **Manual review:** topology_reviewer GUI
3. **Provenance tracking:** sword_operations table (logs who changed what and why)

**Future (v18+):** Consider integrating facc_quality into:
- Automatic data quality checks
- Export metadata (which reaches have corrected facc values)
- Version history (track when corrections were made)

---

## 12. Summary

| Aspect | Details |
|--------|---------|
| **Purpose** | Quality metadata flag for facc values (original vs corrected vs suspicious) |
| **Valid values** | NULL, 'original', 'traced', 'suspect', 'unfixable', 'manual_fix' |
| **Set by** | `fix_facc_violations()` (automated) + topology_reviewer GUI (manual) |
| **Population** | ~0.1-2% of reaches (only corrupted/reviewed ones) |
| **Mutable** | Yes (can change through manual edits or re-running fix_facc_violations) |
| **Dependency** | facc, width, reach_topology |
| **Lint checks** | F007 (distribution), F008 (suspect flagging), F009 (fix effectiveness) |

---

## 13. References

1. **workflow.py** - `fix_facc_violations()` implementation (lines 3517-3763)
2. **topology_reviewer.py** - Interactive reach editor and facc_quality updates
3. **facc_validation_spec.md** - Comprehensive facc validation documentation
4. **RECONSTRUCTION_SPEC.md** - Original algorithms and corruption modes
5. **lint/checks/attributes.py** - Proposed lint check implementations

