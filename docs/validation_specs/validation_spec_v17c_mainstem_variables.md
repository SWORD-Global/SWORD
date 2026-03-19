# Validation Spec: v17c Mainstem Variables

## Variables

| Variable | Type | Description |
|----------|------|-------------|
| `is_mainstem` | BOOLEAN | True if reach is on the mainstem path from best_headwater to best_outlet |
| `hydro_dist_out` | DOUBLE | Hydrologic distance to outlet following main channel (m) |
| `hydro_dist_hw` | DOUBLE | Hydrologic distance from headwater following main channel (m) |
| `best_headwater` | BIGINT | Upstream headwater reach_id selected by width/path_length criteria |
| `best_outlet` | BIGINT | Downstream outlet reach_id selected by width/path_length criteria |
| `pathlen_hw` | DOUBLE | Cumulative path length from best_headwater to this reach (m) |
| `pathlen_out` | DOUBLE | Cumulative path length from this reach to best_outlet (m) |

## Summary

- **Source:** Computed (v17c NEW) via `v17c_pipeline.py`
- **Algorithm:** Width-prioritized topological traversal + Dijkstra distances
- **Dependencies:** reach_topology (v17b), reach_length, width
- **Storage:** `reaches` table, also creates `v17c_sections` and `v17c_section_slope_validation` tables

## Code Path

### Primary File
`/Users/jakegearon/projects/SWORD/src/sword_v17c_pipeline/v17c_pipeline.py`

### Key Functions

| Function | Lines | Description |
|----------|-------|-------------|
| `compute_hydro_distances()` | 255-320 | Computes `hydro_dist_out` and `hydro_dist_hw` via multi-source Dijkstra |
| `compute_best_headwater_outlet()` | 323-410 | Computes `best_headwater`, `best_outlet`, `pathlen_hw`, `pathlen_out` via topological sort |
| `compute_mainstem()` | 413-446 | Computes `is_mainstem` by finding shortest path between (best_hw, best_out) pairs |
| `build_reach_graph()` | 99-152 | Constructs NetworkX DiGraph from reach_topology table |
| `save_to_duckdb()` | 575-644 | Updates reaches table with computed v17c attributes |

## Algorithm Details

### 1. hydro_dist_out / hydro_dist_hw Computation (lines 255-320)

```python
def compute_hydro_distances(G: nx.DiGraph) -> Dict[int, Dict]:
    # Find outlets (out_degree=0) and headwaters (in_degree=0)
    outlets = [n for n in G.nodes() if G.out_degree(n) == 0]
    headwaters = [n for n in G.nodes() if G.in_degree(n) == 0]

    # hydro_dist_out: Multi-source Dijkstra from all outlets on REVERSED graph
    # Weight = reach_length of target node
    R = G.reverse()
    lengths = nx.multi_source_dijkstra_path_length(R, outlets, weight=reach_length)

    # hydro_dist_hw: For each node, max distance from ANY headwater
    for hw in headwaters:
        lengths = nx.single_source_dijkstra_path_length(G, hw, weight=reach_length)
        dist_hw[node] = max(dist_hw[node], lengths[node])
```

**Key insight:** `hydro_dist_out` finds shortest path to ANY outlet; `hydro_dist_hw` finds LONGEST path from any headwater.

### 2. best_headwater / best_outlet Computation (lines 323-410)

**Selection criteria:** Width (primary), then path length (secondary)

```python
def compute_best_headwater_outlet(G: nx.DiGraph) -> Dict[int, Dict]:
    topo = list(nx.topological_sort(G))

    # UPSTREAM PASS (topological order): Track headwater sets and select best
    for n in topo:
        preds = list(G.predecessors(n))
        if not preds:
            # Headwater: best_hw = self
            best_hw[n] = n
            pathlen_hw[n] = 0
        else:
            # Merge headwater sets from predecessors
            # Select by max(width, pathlen)
            candidates = [(width[p], pathlen_hw[p] + reach_len, best_hw[p]) for p in preds]
            best = max(candidates, key=lambda x: (x[0], x[1]))
            best_hw[n] = best[2]
            pathlen_hw[n] = best[1]

    # DOWNSTREAM PASS (reverse topological order): Track outlet and select best
    for n in reversed(topo):
        succs = list(G.successors(n))
        if not succs:
            # Outlet: best_out = self
            best_out[n] = n
            pathlen_out[n] = 0
        else:
            # Select by max(width, pathlen)
            candidates = [(width[s], pathlen_out[s] + reach_len, best_out[s]) for s in succs]
            best = max(candidates, key=lambda x: (x[0], x[1]))
            best_out[n] = best[2]
            pathlen_out[n] = best[1]
```

### 3. is_mainstem Computation (lines 413-446)

```python
def compute_mainstem(G: nx.DiGraph, hw_out_attrs: Dict) -> Dict[int, bool]:
    # Group reaches by (best_headwater, best_outlet) pairs
    paths = defaultdict(list)
    for node, attrs in hw_out_attrs.items():
        key = (attrs['best_headwater'], attrs['best_outlet'])
        paths[key].append(node)

    # For each unique (hw, out) pair, compute shortest path and mark as mainstem
    for (hw, out), nodes in paths.items():
        path = nx.shortest_path(G, hw, out)
        for n in path:
            is_mainstem[n] = True
```

**Critical:** Uses `nx.shortest_path` which finds the path with fewest edges, NOT weighted by reach_length.

## Dependencies (from v17b)

| Source Table | Columns Used |
|--------------|--------------|
| `reach_topology` | reach_id, direction, neighbor_reach_id |
| `reaches` | reach_id, reach_length, width, n_rch_up, n_rch_down, dist_out, path_freq |

## Failure Modes

### F1: is_mainstem Gaps (Path Discontinuity)
- **Symptom:** Mainstem path has gaps (consecutive reaches on path, but one is not marked mainstem)
- **Cause:** `nx.shortest_path` fails due to graph disconnection or uses alternate route
- **Impact:** Broken visual rendering of mainstem; downstream analysis fails

### F2: hydro_dist_out Non-Monotonic
- **Symptom:** Downstream reach has HIGHER hydro_dist_out than upstream reach
- **Cause:** Graph has cycles (not a DAG), or Dijkstra path goes through unexpected route
- **Impact:** Flow direction interpretation breaks

### F3: best_headwater Unreachable
- **Symptom:** Cannot trace path from reach to its best_headwater
- **Cause:** best_headwater assignment incorrect (graph changed after assignment), or graph not fully connected
- **Impact:** pathlen_hw values inconsistent

### F4: best_outlet Unreachable
- **Symptom:** Cannot trace path from reach to its best_outlet
- **Cause:** Same as F3 but downstream
- **Impact:** pathlen_out values inconsistent

### F5: hydro_dist_out vs pathlen_out Inconsistency
- **Symptom:** hydro_dist_out != pathlen_out for same reach
- **Cause:** These use DIFFERENT algorithms - hydro_dist_out uses Dijkstra to ANY outlet; pathlen_out tracks path to SPECIFIC best_outlet
- **Note:** This is EXPECTED behavior, not a bug

### F6: Infinity Values
- **Symptom:** hydro_dist_out or hydro_dist_hw = inf (stored as NULL)
- **Cause:** Reach is disconnected from all outlets/headwaters
- **Impact:** Missing coverage in distance metrics

### F7: is_mainstem Cycles
- **Symptom:** Mainstem path forms a loop
- **Cause:** Graph has cycles
- **Impact:** Incorrect mainstem classification

### F8: Orphan Mainstem Reaches
- **Symptom:** is_mainstem=True but no upstream OR downstream mainstem neighbor
- **Cause:** Incomplete path computation or topology error
- **Impact:** Broken mainstem visualization

## Proposed Lint Checks

### New Topology Checks (T0xx)

| ID | Severity | Rule | Rationale |
|----|----------|------|-----------|
| T008 | ERROR | `hydro_dist_out` must decrease downstream | Flow direction integrity for v17c distances |
| T009 | WARNING | `is_mainstem` path must be continuous | Mainstem should form unbroken path from headwater to outlet |
| T010 | WARNING | `best_headwater` must be reachable upstream | Path integrity check |
| T011 | WARNING | `best_outlet` must be reachable downstream | Path integrity check |

### New v17c Validation Checks (V0xx - New Category)

| ID | Severity | Rule | Rationale |
|----|----------|------|-----------|
| V001 | ERROR | `hydro_dist_out` decreases downstream | Core flow direction property |
| V002 | INFO | `hydro_dist_out` != `pathlen_out` is expected | Document expected difference |
| V003 | WARNING | `pathlen_hw + pathlen_out` consistent with network distances | Path length sanity check |
| V004 | WARNING | `is_mainstem` reaches have both upstream and downstream mainstem neighbors (except HW/outlet) | Continuity check |
| V005 | ERROR | No NULL `hydro_dist_out` for connected reaches | Coverage check |
| V006 | INFO | `is_mainstem` percentage by region | Statistics (expect 96-99%) |
| V007 | WARNING | `best_headwater` is actually a headwater (in_degree=0) | Assignment validity |
| V008 | WARNING | `best_outlet` is actually an outlet (out_degree=0) | Assignment validity |

## Detailed Check Specifications

### T008: hydro_dist_out Monotonicity
```sql
-- Find violations where downstream reach has higher hydro_dist_out
WITH reach_pairs AS (
    SELECT
        r1.reach_id, r1.region,
        r1.hydro_dist_out as dist_up,
        r2.hydro_dist_out as dist_down
    FROM reaches r1
    JOIN reach_topology rt ON r1.reach_id = rt.reach_id AND r1.region = rt.region
    JOIN reaches r2 ON rt.neighbor_reach_id = r2.reach_id AND rt.region = r2.region
    WHERE rt.direction = 'down'
        AND r1.hydro_dist_out IS NOT NULL
        AND r2.hydro_dist_out IS NOT NULL
)
SELECT * FROM reach_pairs
WHERE dist_down > dist_up + 100  -- 100m tolerance
```

### T009: is_mainstem Continuity
```sql
-- Find mainstem reaches without continuous mainstem path
WITH mainstem_reaches AS (
    SELECT reach_id, region FROM reaches WHERE is_mainstem = TRUE
),
mainstem_neighbors AS (
    SELECT
        r.reach_id, r.region,
        SUM(CASE WHEN rt.direction = 'up' AND r2.is_mainstem THEN 1 ELSE 0 END) as ms_up,
        SUM(CASE WHEN rt.direction = 'down' AND r2.is_mainstem THEN 1 ELSE 0 END) as ms_down
    FROM mainstem_reaches r
    JOIN reach_topology rt ON r.reach_id = rt.reach_id AND r.region = rt.region
    JOIN reaches r2 ON rt.neighbor_reach_id = r2.reach_id AND rt.region = r2.region
    GROUP BY r.reach_id, r.region
)
SELECT * FROM mainstem_neighbors
WHERE ms_up = 0 AND ms_down = 0  -- Isolated mainstem (error)
   OR (ms_up = 0 AND r.n_rch_up > 0)  -- Missing upstream mainstem
   OR (ms_down = 0 AND r.n_rch_down > 0)  -- Missing downstream mainstem
```

### V003: pathlen Consistency Check
```sql
-- Compare pathlen_hw + pathlen_out with total network path
SELECT
    r.reach_id, r.region,
    r.pathlen_hw, r.pathlen_out,
    r.pathlen_hw + r.pathlen_out as total_pathlen,
    hw.pathlen_out as hw_to_outlet,  -- Path from headwater to outlet
    ABS((r.pathlen_hw + r.pathlen_out) - hw.pathlen_out) as discrepancy
FROM reaches r
JOIN reaches hw ON r.best_headwater = hw.reach_id AND r.region = hw.region
WHERE r.pathlen_hw IS NOT NULL
  AND r.pathlen_out IS NOT NULL
  AND hw.pathlen_out IS NOT NULL
  AND ABS((r.pathlen_hw + r.pathlen_out) - hw.pathlen_out) > 1000  -- 1km tolerance
```

### V007/V008: best_headwater/outlet Validity
```sql
-- Check that best_headwater is actually a headwater
SELECT r.reach_id, r.region, r.best_headwater, hw.n_rch_up
FROM reaches r
JOIN reaches hw ON r.best_headwater = hw.reach_id AND r.region = hw.region
WHERE r.best_headwater IS NOT NULL
  AND hw.n_rch_up > 0  -- Not actually a headwater

-- Check that best_outlet is actually an outlet
SELECT r.reach_id, r.region, r.best_outlet, out.n_rch_down
FROM reaches r
JOIN reaches out ON r.best_outlet = out.reach_id AND r.region = out.region
WHERE r.best_outlet IS NOT NULL
  AND out.n_rch_down > 0  -- Not actually an outlet
```

## Edge Cases

### 1. Deltas (Multiple Outlets)
- **Behavior:** Each reach gets assigned ONE best_outlet (by width preference)
- **Impact:** is_mainstem forms ONE main path even in deltas
- **Validation:** Check that delta regions have reasonable mainstem % (lower expected)

### 2. Disconnected Networks
- **Behavior:** Each component gets its own headwaters/outlets
- **Impact:** hydro_dist_out computed within component only
- **Validation:** NULL hydro_dist_out indicates disconnection

### 3. Cycles in Graph
- **Behavior:** Pipeline warns but continues; topological sort fails; mainstem undefined
- **Detection:** `nx.is_directed_acyclic_graph(G)` check in pipeline (line 851)
- **Validation:** Run lint check T006 (connected_components) + DAG validation

### 4. Single-Reach Networks
- **Behavior:** best_headwater = best_outlet = self; is_mainstem = True
- **Validation:** Not an error, but track count

### 5. Width = 0 or NULL
- **Behavior:** Falls back to path_length for selection
- **Impact:** May select unexpected headwater/outlet
- **Validation:** Check width coverage before running pipeline

## Relationship Clarifications

### Q: Is pathlen_out same as hydro_dist_out?

**NO.** They are computed differently:

| Attribute | Algorithm | Target |
|-----------|-----------|--------|
| `hydro_dist_out` | Multi-source Dijkstra from ALL outlets | Shortest path to ANY outlet |
| `pathlen_out` | Topological traversal following max(width) | Path length to SPECIFIC best_outlet |

**Example:** A reach near a small tributary outlet might have:
- `hydro_dist_out = 1000m` (to nearby small outlet)
- `pathlen_out = 50000m` (to main river outlet via wide channel)

### Q: Is subnetwork_id same as network?

**PARTIALLY.**

- `network` (v17b): Existing field in reaches table, assigned during original SWORD creation
- `subnetwork_id` (SWORD_graph.py): Computed via `nx.weakly_connected_components()` during graph construction

They SHOULD match but are computed independently. Validation should check consistency.

## Pipeline Results Reference (2026-01-27)

From README.md:

| Region | Reaches | Sections | Mainstem | Direction Valid |
|--------|---------|----------|----------|-----------------|
| NA | 38,696 | 6,363 | 38,057 (98.3%) | 91.6% |
| SA | 42,159 | 7,272 | 41,342 (98.1%) | 93.2% |
| EU | 31,103 | 4,222 | 30,240 (97.2%) | 92.1% |
| AF | 21,441 | 3,137 | 20,746 (96.8%) | 93.4% |
| AS | 100,185 | 18,634 | 96,671 (96.5%) | 94.3% |
| OC | 15,090 | 2,979 | 14,899 (98.7%) | 90.3% |
| **Total** | **248,674** | **42,607** | **241,955 (97.3%)** | **93.2%** |

**Expected ranges for validation:**
- Mainstem %: 96-99% (lower in deltas)
- Direction valid: 90-95%

## Known Issues

### Issue #74: T001 dist_out Monotonicity (16 violations in NA)
- **Status:** Pre-existing from previous facc fixes
- **Impact:** Does NOT affect v17c variables (they use hydro_dist_out)
- **Reference:** https://github.com/ealtenau/SWORD/issues/74

## Recommendation

### Priority 1: Implement Core Checks
1. **T008** - hydro_dist_out monotonicity (ERROR)
2. **V005** - No NULL hydro_dist_out for connected reaches (ERROR)
3. **V007/V008** - best_headwater/outlet validity (WARNING)

### Priority 2: Implement Path Integrity Checks
4. **T009** - is_mainstem continuity (WARNING)
5. **V003** - pathlen consistency (WARNING)

### Priority 3: Informational Checks
6. **V002** - Document hydro_dist_out vs pathlen_out difference (INFO)
7. **V006** - Regional statistics (INFO)

### Implementation Notes
- Add new `V0xx` category for v17c-specific checks OR extend TOPOLOGY category
- Consider adding `--v17c-only` flag to lint CLI for running just v17c checks
- Add to CI pipeline to catch regressions

## Files to Modify

| File | Change |
|------|--------|
| `src/sword_duckdb/lint/core.py` | Add `V17C` category if desired |
| `src/sword_duckdb/lint/checks/topology.py` | Add T008-T011 checks |
| `src/sword_duckdb/lint/checks/v17c.py` | NEW: Add V001-V008 checks |
| `src/sword_duckdb/lint/checks/__init__.py` | Import new v17c module |

## Deep Audit Results (2026-02-02)

### Coverage Statistics

| Region | Total | hydro_dist_out | is_mainstem | hydro% | mainstem% |
|--------|-------|----------------|-------------|--------|-----------|
| AF | 21,441 | 21,441 | 20,746 | 100.0% | 96.8% |
| AS | 100,185 | 100,185 | 96,671 | 100.0% | 96.5% |
| EU | 31,103 | 31,103 | 30,240 | 100.0% | 97.2% |
| NA | 38,696 | 38,696 | 38,057 | 100.0% | 98.3% |
| OC | 15,090 | 15,090 | 14,899 | 100.0% | 98.7% |
| SA | 42,159 | 42,159 | 41,342 | 100.0% | 98.1% |

**✓ 100% coverage** - All reaches have `hydro_dist_out` values.

### hydro_dist_out Distribution

| Region | Min | Median | Max | Mean |
|--------|-----|--------|-----|------|
| AF | 0 | 876 km | 6,747 km | 1,310 km |
| AS | 0 | 877 km | 6,083 km | 1,223 km |
| EU | 0 | 420 km | 3,513 km | 726 km |
| NA | 0 | 510 km | 5,537 km | 974 km |
| OC | 0 | 112 km | 3,374 km | 318 km |
| SA | 0 | 1,756 km | 5,988 km | 1,890 km |

### Issues Found

#### Issue 1: Monotonicity Violations (1,210 total)

| Region | Violations |
|--------|------------|
| AF | 101 |
| AS | 617 |
| EU | 147 |
| NA | 121 |
| OC | 98 |
| SA | 126 |

**Cause:** `hydro_dist_out` increases downstream (should decrease). These are topology anomalies where the Dijkstra algorithm finds a shorter path through a different outlet.

**Recommended action:** Create issue to investigate; may indicate topology problems or delta complexity.

#### Issue 2: Invalid best_headwater (123 in NA)

123 reaches in NA have `best_headwater` pointing to a reach with `n_rch_up > 0` (not actually a headwater).

**Cause:** Width-prioritized selection may select a non-headwater if graph has issues.

**Recommended action:** Investigate NA topology; these may be legitimate (e.g., bifurcations) or bugs.

#### Issue 3: Mainstem Continuity (1 violation in NA)

1 reach in NA is marked as `is_mainstem=TRUE` but has no upstream mainstem neighbor despite having `n_rch_up > 0`.

**Cause:** Shortest path computation selected a different route.

**Recommended action:** Minor issue; investigate specific reach.

### Summary

| Check | Status | Notes |
|-------|--------|-------|
| Coverage | ✅ PASS | 100% of reaches have hydro_dist_out |
| best_outlet validity | ✅ PASS | 100% valid |
| NULL coverage | ✅ PASS | No NULL for connected reaches |
| Monotonicity | ⚠️ WARNING | 1,210 violations (0.49%) |
| best_headwater validity | ⚠️ WARNING | 123 invalid in NA (0.32%) |
| Continuity | ⚠️ WARNING | 1 violation in NA |

**Overall:** Variables are well-computed with minor edge cases. Monotonicity violations warrant investigation but may be expected in complex delta topologies
