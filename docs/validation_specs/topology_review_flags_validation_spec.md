# Validation Specification: topology_suspect, topology_approved

**Version:** 1.0
**Date:** 2025-02-02
**Author:** Variable Audit System

---

## 1. Overview

This document specifies the purpose, usage, workflow, and validation rules for two SWORD reach_topology metadata columns used for manual review and approval of suspicious topology edges:

| Variable | Description | Type | Table | Default |
|----------|-------------|------|-------|---------|
| `topology_suspect` | Flag indicating edge may have incorrect direction | BOOLEAN | reach_topology | FALSE |
| `topology_approved` | Flag indicating reviewer approved this edge despite suspicion | BOOLEAN | reach_topology | FALSE |

These columns implement a **manual review workflow** for topology optimization tools and topology quality assurance workflows.

---

## 2. Purpose and Context

### 2.1 Problem Statement

The SWORD database topology is derived from legacy NetCDF multi-dimensional arrays ([4, N] for reach neighbors). While mostly correct, some topology edges may be suspect for reasons including:

1. **Flow accumulation reversals:** Upstream reach has higher FACC than downstream reach (violates hydrologic principle)
2. **Topology ratio violations:** Upstream FACC >> downstream FACC (suggests possible direction error)
3. **Optimizer recommendations:** Automated algorithms suggest direction reversal
4. **Low confidence zones:** Remote regions with poor validation data

### 2.2 Solution: Manual Review Workflow

These flags enable a **two-phase review process**:

1. **Detection Phase:** Tools identify suspect edges (e.g., topology_optimizer, topology_reviewer UI, lint checks)
2. **Review Phase:** Humans examine suspects and either:
   - **Approve the edge as-is:** Set `topology_approved = TRUE` (even if suspicious)
   - **Reject and reverse:** Reverse edge direction, clear flags
   - **Accept and ignore:** Leave both flags as FALSE

### 2.3 Related Tools

| Tool | Role | Sets Flags | Reads Flags |
|------|------|-----------|------------|
| `topology_optimizer.py` | MILP-based topology optimization | Sets `topology_suspect` | Reads both |
| `topology_reviewer.py` | Streamlit UI for manual review | Sets `topology_approved` | Reads both |
| Lint framework | Topology quality checks | None (yet) | N/A |

---

## 3. Column Definitions

### 3.1 reach_topology Table Schema

**Location:** `src/sword_duckdb/schema.py`, lines 303-316

```sql
CREATE TABLE reach_topology (
    -- Composite primary key (includes region for efficient filtering)
    reach_id BIGINT NOT NULL,
    region VARCHAR(2) NOT NULL,
    direction VARCHAR(4) NOT NULL,  -- 'up' or 'down'
    neighbor_rank TINYINT NOT NULL,  -- 0-3

    -- Neighbor reach ID
    neighbor_reach_id BIGINT NOT NULL,

    -- Review flags (NEW in v17c)
    topology_suspect BOOLEAN DEFAULT FALSE,    -- May have direction error
    topology_approved BOOLEAN DEFAULT FALSE,   -- Reviewer approved this edge

    PRIMARY KEY (reach_id, region, direction, neighbor_rank)
);
```

### 3.2 topology_suspect

**Definition:** `BOOLEAN` flag indicating that this topology edge (reach_id → neighbor_reach_id) may have incorrect direction or connectivity.

**Semantics:**
- `TRUE`: Edge flagged as suspect by tool (optimizer, linter, manual marking)
- `FALSE` (default): Edge appears normal, not flagged
- `NULL`: Treated as FALSE (default casting)

**When set to TRUE:**
- Automated tools identify potential topology error
- Flow accumulation reversal detected (upstream FACC > downstream FACC)
- Optimizer suggests direction reversal
- Manual reviewer flags for investigation

**Lifetime:**
- Persistent: Survives until explicitly cleared
- Not cleared automatically: Remains TRUE across database updates unless manually reset

### 3.3 topology_approved

**Definition:** `BOOLEAN` flag indicating that reviewer has examined this suspect edge and approved it to remain as-is (or determined it's not actually suspect).

**Semantics:**
- `TRUE`: Reviewer examined this edge and approved its current direction
- `FALSE` (default): Edge not yet reviewed OR reviewer determined edge needs direction reversal
- `NULL`: Treated as FALSE

**When set to TRUE:**
- Reviewer examined the edge context (geometry, FACC, river names, etc.)
- Reviewer determined current direction is correct despite suspicion
- Reviewer took manual responsibility for this edge
- Tools should **exclude** this edge from further suspicious-edge lists

**Lifetime:**
- Persistent: Survives database updates
- Can be cleared if edge needs re-review
- Cleared when edge direction is reversed

---

## 4. Current Population Statistics

**As of 2025-02-02 (sword_v17c.duckdb):**

```
topology_suspect | topology_approved | Count
─────────────────┼───────────────────┼───────
FALSE            | FALSE             | 495,299  (99.93%)
TRUE             | FALSE             | 353      (0.07%)
FALSE            | TRUE              | 2        (0.00%)
```

**Summary:**
- **495,299 normal edges** (FALSE, FALSE)
- **353 suspect edges** awaiting review (TRUE, FALSE)
- **2 approved edges** (FALSE, TRUE) - edge was not suspect but reviewer marked for tracking

---

## 5. Workflow and State Transitions

### 5.1 Edge Lifecycle States

```
                       ┌─────────────────────────────────┐
                       │  Normal Edge Creation           │
                       │  (FALSE, FALSE)                 │
                       └─────────────────────────────────┘
                                    ↓
                       ┌─────────────────────────────────┐
                       │  Suspect Detection              │
                       │  (TRUE, FALSE)                  │
                       └─────────────────────────────────┘
                        ↙             ↓              ↘
                  Approve       Reverse Dir.      Investigate
                       ↓             ↓                  ↓
              (FALSE, TRUE)    (FALSE, FALSE)    More review...
                               [direction            ↓
                                reversed]        Approve or
                                                 Reverse Dir.
```

### 5.2 State Definition Table

| State | topology_suspect | topology_approved | Meaning |
|-------|------------------|-------------------|---------|
| **A: Normal** | FALSE | FALSE | Standard edge, not flagged |
| **B: Suspect (new)** | TRUE | FALSE | Tool flagged as suspect, awaiting review |
| **C: Approved** | FALSE | TRUE | Reviewer approved, edge stays as-is |
| **D: Approved after fix** | FALSE | FALSE | Edge was reversed, now normal |
| **E: Suspect approved** | TRUE | TRUE | Both flags set (reviewer accepted suspicion) |

### 5.3 Transition Rules

**Creation (new topology edge):**
- Default: State A (FALSE, FALSE)

**Suspect detection (by tool):**
- A → B: Set `topology_suspect = TRUE`
- B → B: Already suspect, no change

**Manual review - Approve:**
- B → C: Set `topology_approved = TRUE`
- E → C: Clear `topology_suspect = FALSE`, keep `topology_approved = TRUE`
- Operation: `UPDATE reach_topology SET topology_approved = TRUE WHERE reach_id = ? AND neighbor_reach_id = ?`

**Manual review - Reverse direction:**
- B → D: Clear both flags, reverse direction
- Operation:
  ```sql
  -- Reverse: old_up ← old_down becomes old_down ← old_up
  UPDATE reach_topology SET direction = 'up'
  WHERE reach_id = old_upstream AND direction = 'down'
    AND neighbor_reach_id = old_downstream;
  UPDATE reach_topology SET direction = 'down'
  WHERE reach_id = old_downstream AND direction = 'up'
    AND neighbor_reach_id = old_upstream;
  -- Clear flags
  UPDATE reach_topology SET topology_suspect = FALSE, topology_approved = FALSE
  WHERE reach_id IN (old_upstream, old_downstream)
    AND neighbor_reach_id IN (old_upstream, old_downstream);
  ```

**Query to find edges needing review:**
```sql
SELECT * FROM reach_topology
WHERE topology_suspect = TRUE
  AND topology_approved = FALSE
ORDER BY reach_id;
```

---

## 6. How Flags Are Set

### 6.1 Topology Optimizer (`topology_optimizer.py`)

**Purpose:** Automated MILP-based topology optimization to correct suspected flow direction errors.

**Detection logic:**
1. Build directed graph from reach_topology
2. Compute phi (distance to outlet) via Dijkstra
3. Solve MILP to minimize "uphill" edges (against phi gradient)
4. Compare new solution with current topology
5. Flag edges that MILP suggests reversing as `topology_suspect = TRUE`

**Code location:** `topology_optimizer.py`, class `TopologyOptimizer`, method `optimize()`

**Example:** If MILP determines that edge (A → B) should be (B → A), sets:
```python
conn.execute("""
    UPDATE reach_topology SET topology_suspect = TRUE
    WHERE reach_id = ? AND neighbor_reach_id = ?
""", [upstream_reach, downstream_reach])
```

### 6.2 Topology Reviewer UI (`topology_reviewer.py`)

**Purpose:** Streamlit web UI for human review of suspect edges with visualization and FACC context.

**Review modes:**
1. **Ratio Violations:** Upstream FACC >> downstream FACC
2. **Monotonicity Issues:** FACC not monotonically decreasing
3. **Headwater Anomalies:** Headwaters with unusually high FACC
4. **Lake Sandwiches:** Rivers between lakes
5. **Type Mismatches:** Lake/type classification issues

**Approval logic:**
- User examines suspect edge on map
- Views upstream/downstream chains (FACC, width, river names)
- Clicks "Approve" button
- Sets `topology_approved = TRUE` for that edge

**Code location:** `topology_reviewer.py`, function `render_fix_panel()`

```python
conn.execute("""
    UPDATE reach_topology SET topology_approved = TRUE
    WHERE reach_id = ? AND neighbor_reach_id = ?
      AND region = ?
""", [upstream_reach, downstream_reach, region])
```

### 6.3 Lint Framework (Planned)

**Proposed checks:**
- **T013:** topology_suspect coverage (INFO - monitor flag population)
- **T014:** topology_approved coverage (INFO - track review progress)
- **T015:** Unapproved suspect edges (WARNING - items needing review)

**Not yet implemented** - lint framework currently reads but doesn't set these flags.

---

## 7. Valid Values

### 7.1 Boolean Semantics

| Value | Meaning | Valid? | Default? |
|-------|---------|--------|----------|
| `TRUE` | Flag set (yes) | ✓ | No |
| `FALSE` | Flag clear (no) | ✓ | Yes |
| `NULL` | Treated as FALSE | ✗ For new rows; OK for backfill | No |

### 7.2 Independence

**These flags are INDEPENDENT:**
- `topology_suspect = TRUE` does **not** automatically set `topology_approved`
- `topology_approved = TRUE` does **not** automatically clear `topology_suspect`
- Both can be TRUE simultaneously (edge is suspect but reviewer approved)
- Both can be FALSE simultaneously (edge is normal OR was previously suspect but fixed)

### 7.3 Constraints

**No hard constraints enforced at database level**, but these invariants should hold:

1. **Suspect edges should be reviewed eventually:** Ideally, all TRUE/FALSE edges should become TRUE/TRUE or be reversed to FALSE/FALSE
2. **Approved edges are authoritative:** If `topology_approved = TRUE`, tools should not further modify this edge without manual review clearance
3. **Reversals should clear flags:** After reversing direction, both flags should reset to FALSE

---

## 8. Existing Lint Checks

**Current coverage:**

| Check ID | Name | Severity | Status | Scope |
|----------|------|----------|--------|-------|
| T001 | dist_out_monotonicity | ERROR | Active | Catches flow direction issues indirectly |
| T002 | path_freq_monotonicity | WARNING | Active | Catches flow issues indirectly |
| T003 | facc_monotonicity | WARNING | Active | Flags FACC reversals (suspect edges detected here) |
| T004 | orphan_reaches | WARNING | Active | Reaches with no neighbors |
| T005 | neighbor_count_consistency | ERROR | Active | n_rch_up/down match topology |
| T007 | topology_reciprocity | WARNING | Active | If A→B then B→A |

**Specific to review flags:**

| Check ID | Name | Status | Notes |
|----------|------|--------|-------|
| T013 | topology_suspect_coverage | Planned | Monitor proportion of flagged edges |
| T014 | topology_approved_coverage | Planned | Track review completion |
| T015 | unapproved_suspects | Planned | Warn on unreviewed suspect edges |

---

## 9. Proposed Lint Checks

### 9.1 Proposed: T013 - topology_suspect Coverage

**Purpose:** Monitor proportion of flagged edges to detect systematic issues.

**Severity:** INFO (informational, not an error)

**Logic:**
```python
@register_check("T013", Category.TOPOLOGY, Severity.INFO,
                "topology_suspect coverage statistics")
def check_topology_suspect_coverage(conn, region=None):
    """
    Count flagged vs total topology edges.
    Flag if >1% of edges are suspect (systematic issue indicator).
    """
    query = """
    SELECT
        COUNT(*) as total_edges,
        SUM(CASE WHEN topology_suspect THEN 1 ELSE 0 END) as suspect_edges,
        ROUND(100.0 * SUM(CASE WHEN topology_suspect THEN 1 ELSE 0 END)
              / COUNT(*), 2) as suspect_pct
    FROM reach_topology
    {where_region}
    """

    suspect_pct = result['suspect_pct'][0]
    if suspect_pct > 1.0:
        yield {
            'reach_id': 'N/A (summary)',
            'message': f'{suspect_pct:.2f}% of edges flagged (expected <1%)',
            'severity': 'INFO'
        }
```

**Example output:**
```
T013: topology_suspect coverage
  ├─ Total edges: 495,654
  ├─ Suspect edges: 353
  ├─ Suspect %: 0.071%
  └─ Status: PASS (below 1% threshold)
```

### 9.2 Proposed: T014 - topology_approved Coverage

**Purpose:** Track progress of topology review workflow.

**Severity:** INFO

**Logic:**
```python
@register_check("T014", Category.TOPOLOGY, Severity.INFO,
                "topology_approved coverage statistics")
def check_topology_approved_coverage(conn, region=None):
    """
    Count approved vs suspect edges.
    Alert if many suspect edges remain unapproved.
    """
    query = """
    SELECT
        SUM(CASE WHEN topology_suspect AND NOT topology_approved THEN 1 ELSE 0 END)
            as unreviewed_suspects,
        SUM(CASE WHEN topology_suspect AND topology_approved THEN 1 ELSE 0 END)
            as approved_suspects,
        SUM(CASE WHEN NOT topology_suspect AND topology_approved THEN 1 ELSE 0 END)
            as approved_normal
    FROM reach_topology
    {where_region}
    """

    unreviewed = result['unreviewed_suspects'][0]
    approved = result['approved_suspects'][0]
    approved_normal = result['approved_normal'][0]

    review_rate = 100.0 * approved / (approved + unreviewed) if (approved + unreviewed) > 0 else 0

    yield {
        'message': f'Reviewed {approved}/{approved + unreviewed} suspect edges ({review_rate:.1f}%)',
        'severity': 'INFO'
    }
```

**Example output:**
```
T014: topology_approved coverage
  ├─ Unreviewed suspect: 351
  ├─ Approved suspect: 2
  ├─ Review rate: 0.6%
  └─ Status: WARN (low review completion)
```

### 9.3 Proposed: T015 - Unapproved Suspects

**Purpose:** Highlight edges that need manual review.

**Severity:** WARNING

**Logic:**
```python
@register_check("T015", Category.TOPOLOGY, Severity.WARNING,
                "suspect edges awaiting review")
def check_unapproved_suspects(conn, region=None):
    """
    List suspect edges that have not yet been reviewed/approved.
    These are candidates for topology_reviewer.py manual review.
    """
    query = """
    SELECT
        t.reach_id, t.region, t.direction, t.neighbor_reach_id,
        r1.facc as upstream_facc, r2.facc as downstream_facc,
        r1.facc / NULLIF(r2.facc, 0) as facc_ratio
    FROM reach_topology t
    JOIN reaches r1 ON t.reach_id = r1.reach_id AND t.region = r1.region
    JOIN reaches r2 ON t.neighbor_reach_id = r2.reach_id AND t.region = r2.region
    WHERE t.topology_suspect = TRUE
      AND (t.topology_approved = FALSE OR t.topology_approved IS NULL)
      {where_region}
    ORDER BY r1.facc / NULLIF(r2.facc, 0) DESC
    """
```

**Example output:**
```
T015: Unapproved suspect edges (353 total)
  ├─ 78331000025 → 78331000016 (facc_ratio: 4.2)
  ├─ 82100900405 → 82100900585 (facc_ratio: 3.8)
  ├─ 82282000073 → 82282000063 (facc_ratio: 3.5)
  └─ ... 350 more edges ...
```

---

## 10. Quality and Consistency Rules

### 10.1 Invariants

1. **Suspect edges should eventually be resolved:**
   ```sql
   -- Count suspect edges that are still unreviewed (>30 days old)
   -- Alert if significant backlog
   ```

2. **Approved edges should not change without review:**
   ```sql
   -- If topology_approved = TRUE, edge should not be modified
   -- Modifications require clearing topology_approved first
   ```

3. **Flag consistency with FACC:**
   ```sql
   -- Suspect edges should often (but not always) have FACC reversals
   -- Not all FACC reversals are topology errors (tributaries, measurement error)
   ```

### 10.2 Edge Cases

| Edge Case | Expected Behavior |
|-----------|-------------------|
| Newly created edges | Both flags FALSE (default) |
| Edge reversal by user | Both flags should be reset to FALSE |
| Edge deleted/recreated | Start with fresh FALSE, FALSE state |
| Approved edge with FACC change | `topology_approved` remains TRUE (reviewer responsibility) |
| Import from v17b | Both flags should be FALSE initially |

### 10.3 Potential Failure Modes

| Failure | Cause | Detection |
|---------|-------|-----------|
| Suspect flag not cleared | Incomplete optimization | T015 lint check |
| Approved but still broken | Reviewer error | Downstream lint check (T001, T002, T003) |
| Flags set but not documented | Tool ran without logging | Manual audit of `sword_operations` table |
| Orphaned flags | Edge deleted but flags remain | Referential integrity check (planned) |

---

## 11. Dependencies and Related Tables

### 11.1 Related Columns in reach_topology

| Column | Relationship |
|--------|--------------|
| `reach_id` | Primary entity of this edge |
| `neighbor_reach_id` | Destination of this edge |
| `direction` | Flow direction ('up' or 'down') - may be reversed if suspect edge is fixed |
| `neighbor_rank` | Edge ordering (0-3) |

### 11.2 Related Columns in reaches

| Column | Relationship | Purpose |
|--------|-------------|---------|
| `facc` | Flow accumulation | Used to detect suspect edges (monotonicity) |
| `dist_out` | Distance to outlet | Used to validate MILP optimization |
| `stream_order` | Log of path_freq | Depends on correct topology |
| `path_freq` | Traversal count | Depends on correct topology |
| `n_rch_up` / `n_rch_down` | Neighbor counts | Should be recalculated after reversals |

### 11.3 Related Tables

| Table | Usage |
|-------|-------|
| `sword_operations` | Records when flags are set/cleared with provenance |
| `sword_value_snapshots` | Tracks state changes before/after reviews |
| `lint_fix_log` | Historical record of manual topology fixes |

---

## 12. Database Operations

### 12.1 Setting topology_suspect (by tools)

```sql
-- Set suspect flag (e.g., by topology_optimizer)
UPDATE reach_topology
SET topology_suspect = TRUE
WHERE reach_id = ? AND region = ?;

-- Reset after investigating
UPDATE reach_topology
SET topology_suspect = FALSE
WHERE reach_id = ? AND region = ?;
```

### 12.2 Setting topology_approved (by reviewer)

```sql
-- Approve a suspect edge as-is
UPDATE reach_topology
SET topology_approved = TRUE
WHERE reach_id = ? AND neighbor_reach_id = ? AND region = ?;

-- Clear approval (need re-review)
UPDATE reach_topology
SET topology_approved = FALSE
WHERE reach_id = ? AND neighbor_reach_id = ? AND region = ?;
```

### 12.3 Reversing Direction (with flag reset)

```sql
-- Safe direction reversal with flag reset
BEGIN TRANSACTION;

  -- Get original edges
  SELECT reach_id, neighbor_reach_id INTO @old_up, @old_dn
  FROM reach_topology
  WHERE reach_id = ? AND neighbor_reach_id = ? AND direction = 'down';

  -- Reverse the direction
  UPDATE reach_topology SET direction = 'up'
  WHERE reach_id = @old_up AND direction = 'down' AND neighbor_reach_id = @old_dn;

  UPDATE reach_topology SET direction = 'down'
  WHERE reach_id = @old_dn AND direction = 'up' AND neighbor_reach_id = @old_up;

  -- Clear suspect/approved flags
  UPDATE reach_topology SET topology_suspect = FALSE, topology_approved = FALSE
  WHERE reach_id IN (@old_up, @old_dn);

  -- Update neighbor counts (should be recalculated anyway)
  -- UPDATE reaches SET n_rch_up = ..., n_rch_down = ... (see reconstruction.py)

COMMIT;
```

### 12.4 Query Suspicious Edges

```sql
-- All suspect edges awaiting review
SELECT * FROM reach_topology
WHERE topology_suspect = TRUE AND topology_approved = FALSE
ORDER BY reach_id;

-- Only approved edges
SELECT * FROM reach_topology
WHERE topology_approved = TRUE;

-- Both flags set (rare - reviewer acknowledged suspicion)
SELECT * FROM reach_topology
WHERE topology_suspect = TRUE AND topology_approved = TRUE;

-- Summary statistics
SELECT
  topology_suspect,
  topology_approved,
  COUNT(*) as edge_count
FROM reach_topology
GROUP BY topology_suspect, topology_approved;
```

---

## 13. Provenance and Tracking

### 13.1 Recommended: Log Flag Changes in sword_operations

**When flag is set**, record in audit trail:

```python
workflow.record_operation(
    operation_type='FLAG_TOPOLOGY',
    table_name='reach_topology',
    entity_ids=[reach_id, neighbor_reach_id],
    reason='Suspected by MILP optimization tool',
    operation_details={
        'flag': 'topology_suspect',
        'value': True,
        'optimizer_version': 'v2.1'
    }
)
```

**When flag is cleared** (approved):

```python
workflow.record_operation(
    operation_type='APPROVE_TOPOLOGY',
    table_name='reach_topology',
    entity_ids=[reach_id, neighbor_reach_id],
    reason='Manually reviewed and approved by user',
    operation_details={
        'flag': 'topology_approved',
        'value': True,
        'reviewer': 'jake@example.com',
        'facc_ratio': 3.2
    }
)
```

### 13.2 Query Audit Trail

```sql
-- All operations on topology flags
SELECT * FROM sword_operations
WHERE table_name = 'reach_topology'
  AND operation_type IN ('FLAG_TOPOLOGY', 'APPROVE_TOPOLOGY')
ORDER BY started_at DESC;

-- Who reviewed what
SELECT
  operation_details->>'reviewer' as reviewer,
  COUNT(*) as approvals_made,
  MAX(started_at) as last_approval
FROM sword_operations
WHERE table_name = 'reach_topology'
  AND operation_type = 'APPROVE_TOPOLOGY'
GROUP BY operation_details->>'reviewer';
```

---

## 14. Code References

| Component | File | Lines | Description |
|-----------|------|-------|-------------|
| Schema: reach_topology | schema.py | 303-316 | Table definition with flag columns |
| Optimizer: Flag setting | topology_optimizer.py | ~450-500 | Sets topology_suspect during MILP |
| Reviewer: Flag approval | topology_reviewer.py | ~1015-1020 | Filters unapproved edges |
| Reviewer: UI approval | topology_reviewer.py | ~1170-1180 | User clicks "Approve" button |
| Lint (planned): T013 | lint/checks/topology.py | TBD | Suspect coverage statistics |
| Lint (planned): T014 | lint/checks/topology.py | TBD | Approved coverage statistics |
| Lint (planned): T015 | lint/checks/topology.py | TBD | List unapproved suspects |

---

## 15. Summary

### 15.1 Key Points

1. **Two independent flags** track topology review workflow:
   - `topology_suspect`: Indicates edge flagged by tool as potentially erroneous
   - `topology_approved`: Indicates human reviewer examined and approved edge

2. **Current state** (as of v17c):
   - 353 suspect edges (0.07%) awaiting review
   - 2 approved edges (fully reviewed)
   - Workflow is early-stage with low review completion

3. **Intended workflow:**
   - Tools flag suspect edges (topology_suspect = TRUE)
   - Humans review using topology_reviewer.py UI
   - Reviewer either approves (topology_approved = TRUE) or reverses direction
   - Approved edges excluded from future suspicious-edge lists

4. **Not yet implemented:**
   - Lint checks T013, T014, T015 for flag monitoring
   - Provenance logging in sword_operations table
   - Automated flag clearing on direction reversal

5. **Quality assurance:**
   - Flags are persistent and intentional
   - No automatic clearing - manual review required
   - Approved edges are authoritative unless explicitly un-reviewed

### 15.2 Validation Assessment

**Current coverage:**
- Basic functionality works (flags set, queried, and approved in UI)
- Early deployment with low flag population
- No systematic validation of flag consistency yet

**Gaps:**
- No lint checks monitoring flag usage
- No provenance tracking of who set/approved flags
- No automatic flag cleanup after edge reversal
- Audit trail relies on manual logging

**Recommendations:**
1. Implement T013/T014/T015 lint checks
2. Add provenance logging to workflow.py
3. Test flag reset on edge reversal
4. Document flag semantics in field data dictionary
5. Monitor flag population growth (if >1%, investigate systematic issues)

---

## 16. Related Documentation

| Document | Relevance |
|----------|-----------|
| [v17c Pipeline README](../../src/sword_v17c_pipeline/README.md) | v17c pipeline details |
| [validation_spec_dist_out.md](validation_spec_dist_out.md) | dist_out depends on correct topology |
| [validation_spec_v17c_mainstem_variables.md](validation_spec_v17c_mainstem_variables.md) | v17c topology uses these flags |
| [end_reach_trib_flag_validation_spec.md](end_reach_trib_flag_validation_spec.md) | end_reach affected by topology |
| [facc_validation_spec.md](facc_validation_spec.md) | FACC reversals trigger suspect detection |
