# Validation Spec: dist_out

## Summary
- **Source:** Computed (graph traversal from topology + reach lengths)
- **Units:** meters
- **Official definition:** "distance from the river outlet for each reach" (v17b PDD, Table 3/5)
- **v17 Update:** "Distance from outlet recalculation based on shortest paths between outlets and headwaters" (v17b PDD, Version History)

## Code Path

### Primary Reconstruction (ReconstructionEngine)
- **File:** `/Users/jakegearon/projects/SWORD/src/sword_duckdb/reconstruction.py`
- **Lines:** 1382-1497 (reach), 1858-1917 / 3400-3431 (node)

### AttributeSpec Definition
```python
# Lines 282-289
"reach.dist_out": AttributeSpec(
    name="reach.dist_out",
    source=SourceDataset.COMPUTED,
    method=DerivationMethod.PATH_ACCUMULATION,
    source_columns=[],
    dependencies=["reach.reach_length", "reach_topology"],
    description="Distance from outlet (m): graph traversal from outlet upstream, accumulating reach lengths"
)

# Lines 593-599
"node.dist_out": AttributeSpec(
    name="node.dist_out",
    source=SourceDataset.COMPUTED,
    method=DerivationMethod.INTERPOLATED,
    source_columns=[],
    dependencies=["reach.dist_out", "node.reach_id"],
    description="Node distance from outlet: interpolated from reach dist_out by position"
)
```

### Algorithm (Reach)
1. Build upstream/downstream adjacency maps from `reach_topology` table
2. Find outlets: reaches with no downstream neighbors OR `dist_out = 0`
3. BFS from outlets upstream
4. For each reach: `dist_out = parent_dist_out + parent_length`
5. Outlets have `dist_out = 0`

```python
# Pseudocode from _reconstruct_reach_dist_out (lines 1440-1462)
queue = [(outlet_id, 0.0) for outlet_id in outlets]
while queue:
    reach_id, dist = queue.pop(0)
    new_dist_out[reach_id] = dist
    for upstream_id in upstream_map.get(reach_id, []):
        upstream_dist = dist + reach_lengths.get(reach_id, 0)
        queue.append((upstream_id, upstream_dist))
```

### Algorithm (Node)
- **Simple:** Inherit from parent reach's `dist_out`
- **Full (not yet implemented):** Interpolate based on node position within reach

### Reactive Recalculation (SWORDReactive)
- **File:** `/Users/jakegearon/projects/SWORD/src/sword_duckdb/reactive.py`
- **Lines:** 158-164 (reach), 199-204 (node)

```python
# reach.dist_out dependency graph
DependencyNode(
    name='reach.dist_out',
    table='reaches',
    depends_on=['reach.len', 'reach.topology'],
    recalc_func='_recalc_reach_dist_out',
    change_types=[ChangeType.GEOMETRY, ChangeType.TOPOLOGY]
)

# node.dist_out dependency graph
DependencyNode(
    name='node.dist_out',
    table='nodes',
    depends_on=['reach.dist_out', 'node.len'],
    recalc_func='_recalc_node_dist_out',
    change_types=[ChangeType.GEOMETRY, ChangeType.TOPOLOGY]
)
```

### Reactive Algorithm (reach) - Lines 503-578
```python
# Different from reconstruction - uses in-memory arrays
def _recalc_reach_dist_out(self):
    # Outlet reaches: dist_out = reach.len
    # Upstream reaches: dist_out = reach.len + max(downstream dist_out values)
    for idx in outlet_indices:
        dist_out[idx] = reaches.len[idx]
    for idx in upstream_order:
        max_dn_dist = max(dist_out[dn_idx] for dn_idx in downstream_neighbors)
        dist_out[idx] = reaches.len[idx] + max_dn_dist
```

### Reactive Algorithm (node) - Lines 778-812
```python
# node.dist_out = reach_base_dist + cumulative_node_length
# where reach_base_dist = reach.dist_out - reach.len
base_dist = reaches.dist_out[r] - reaches.len[r]
cumsum = np.cumsum(nodes.len[sorted_indices])
for i, idx in enumerate(sorted_indices):
    nodes.dist_out[idx] = base_dist + cumsum[i]
```

## Known Issues and Investigation Results

### N006: Boundary dist_out discontinuities
Investigation of 2,596 violations in v17c (#192) revealed two distinct root causes:

| Type | Cause | Gap Size | Severity | Description |
|------|-------|----------|----------|-------------|
| **Type A** | **Cross-Basin Merge** | **>100 km** | **ERROR** | False topological links between distinct basins (e.g., Burnett vs Murray-Darling). Reaches are erroneously assigned to a much longer network. |
| **Type B** | **Braided Path Difference** | **1–50 km** | **INFO/WARN** | Inherent to single-scalar `dist_out`. At a bifurcation, `dist_out` matches the longest path; shorter parallel channels will show a gap at the junction. |

#### Example: Type A (Burnett River, OC)
Reach `56360400073` is erroneously connected to the Murray-Darling system.
- Downstream neighbor 1 (Correct): `56360400063` (dist_out ~345 km)
- Downstream neighbor 2 (False Merge): `56360400103` (dist_out ~3,500 km)
- Result: `56360400073` inherits ~3,500 km, creating a ~3,155 km gap with its legitimate neighbor.

#### Example: Type B (Kuskokwim River, NA)
A reach bifurcates into two channels of different lengths that later rejoin.
- Gap is exactly equal to the difference in path length between the two branches.
- This is a valid representation of the longest path but flags as a discontinuity on the shorter branch.

## Dependencies

### Upstream Dependencies
| Dependency | Source | Impact |
|------------|--------|--------|
| `reach.reach_length` | Computed from centerline geometry | Direct input to accumulation |
| `reach_topology` | Stored table (`rch_id_up`, `rch_id_down`) | Defines traversal order |
| `node.node_length` | Computed from centerline geometry | Node-level interpolation |

### Downstream Dependents
| Dependent | Relationship |
|-----------|--------------|
| `node.dist_out` | Interpolated from reach value |
| `reach.slope` | Uses `node.dist_out` for regression |
| `reach.main_side` | Uses topology + `dist_out` |
| `reach.path_order` | Partitioned by `path_freq`, ordered by `dist_out` |

## Failure Modes

| # | Mode | Description | Severity | Check ID |
|---|------|-------------|----------|----------|
| 1 | **Non-monotonic downstream** | dist_out increases going downstream | ERROR | T001 |
| 2 | **Disconnected network** | Orphan reaches not traversed from outlets | WARNING | T004 |
| 3 | **Negative values** | dist_out < 0 (except -9999 fill) | ERROR | T008 (new) |
| 4 | **Zero at non-outlet** | dist_out = 0 for non-outlet reaches | ERROR | T009 (new) |
| 5 | **Inconsistent with reach_length** | dist_out difference != reach_length | WARNING | T010 (new) |
| 6 | **Node > Reach mismatch** | Node dist_out outside reach's dist_out range | WARNING | T011 (new) |
| 7 | **Topology cycle** | Cycle in topology causes infinite loop | ERROR | T012 (new) |
| 8 | **Algorithm divergence** | Reconstruction vs reactive produce different values | WARNING | T013 (new) |
| 9 | **NaN/NULL values** | Missing dist_out values | WARNING | T014 (new) |
| 10 | **Fill value misuse** | -9999 for valid reaches | INFO | T015 (new) |

## Existing Lint Checks

### T001: dist_out_monotonicity
- **File:** `/Users/jakegearon/projects/SWORD/src/sword_duckdb/lint/checks/topology.py`
- **Lines:** 20-100
- **Severity:** ERROR
- **Default Threshold:** 100.0 meters tolerance
- **Logic:** Joins reaches to downstream neighbors via topology, checks that `dist_out_up > dist_out_down - tolerance`

## Proposed New Checks

| ID | Severity | Rule | Rationale |
|----|----------|------|-----------|
| T008 | ERROR | `dist_out >= 0 OR dist_out = -9999` | Negative values are physically impossible |
| T009 | ERROR | `dist_out = 0` implies `end_rch = 2` (outlet) | Only outlets should have zero distance |
| T010 | WARNING | For adjacent reaches A->B: `abs(A.dist_out - B.dist_out - A.reach_length) < tolerance` | dist_out should accumulate exactly by reach_length |
| T011 | WARNING | `node.dist_out` between `reach.dist_out - reach_length` and `reach.dist_out` | Node must be within its parent reach's distance range |
| T012 | ERROR | No cycles in directed topology graph | Cycles break BFS algorithm |
| T013 | WARNING | `reconstruction.dist_out ~= reactive.dist_out` | Both algorithms should agree |
| T014 | WARNING | `dist_out IS NOT NULL AND dist_out != NaN` | Missing values break downstream calcs |
| T015 | INFO | Count of `-9999` fill values per region | Track data completeness |

## Edge Cases

### Multi-outlet networks
- **Case:** River networks with multiple outlets (distributary deltas)
- **Handling:** Each sub-network has its own outlet(s) with `dist_out = 0`. Algorithm handles this via `end_rch = 2` flag or `n_rch_down = 0`.

### Braided/anastomosing channels
- **Case:** Side channels that rejoin main channel
- **Handling:** Both paths valid; each reach accumulates from its actual downstream neighbor. May result in different dist_out for parallel reaches.

### Lake sandwiches
- **Case:** River reach between two lake reaches (3,167 cases globally)
- **Handling:** dist_out propagates through normally. Check should not flag these.

### Ghost reaches (type=6)
- **Case:** Placeholder reaches with no real geometry
- **Handling:** May have `dist_out = -9999`. Exclude from monotonicity checks.

### Cross-basin connections
- **Case:** Reaches that connect different level-2 Pfafstetter basins
- **Handling:** dist_out resets at basin boundaries. Check within-basin only.

### Tidal reaches (lakeflag=3)
- **Case:** Tidally influenced sections where flow direction reverses
- **Handling:** Still have topological dist_out. May want separate tolerance.

### Zero-length reaches
- **Case:** Reaches with `reach_length = 0` (e.g., end_reach markers)
- **Handling:** dist_out should equal downstream neighbor's dist_out. Tolerance check.

## Implementation Notes

### Reconstruction vs Reactive Discrepancy
The two algorithms differ subtly:
- **Reconstruction:** Outlets start at `dist_out = 0`, upstream adds current reach length
- **Reactive:** Outlets start at `dist_out = reach.len`, upstream adds current reach length

This causes a systematic offset where reactive values are higher by one reach_length at each point. The v17 PDD implies the reconstruction version is correct (outlets have dist_out = 0).

### BFS vs DFS
Current implementation uses BFS (`queue.pop(0)`). For very deep networks, DFS might be more memory efficient. Performance should be tested on AS region (largest).

### Tolerance Selection
The 100m default tolerance in T001 accounts for:
- Floating point precision
- Node length summation rounding
- v17b bug fixes that affected <2% of reaches

## Recommendation

1. **Keep T001** as primary monotonicity check with ERROR severity
2. **Add T008** (negative value) as ERROR - catches data corruption
3. **Add T009** (zero at non-outlet) as ERROR - catches outlet identification bugs
4. **Add T010** (length consistency) as WARNING - validates accumulation logic
5. **Add T014** (NULL/NaN) as WARNING - ensures completeness
6. **Defer T011-T013** until node-level validation is prioritized

### Priority Order
1. T008 (negative values) - simple, high impact
2. T009 (zero at non-outlet) - simple, catches real bugs
3. T014 (NULL/NaN) - completeness metric
4. T010 (length consistency) - validates algorithm correctness
5. T015 (fill value tracking) - informational

### SQL for T008 (Negative Values)
```sql
SELECT reach_id, region, dist_out
FROM reaches
WHERE dist_out < 0 AND dist_out != -9999
  AND (region = ? OR ? IS NULL)
```

### SQL for T009 (Zero at Non-Outlet)
```sql
SELECT r.reach_id, r.region, r.dist_out, r.end_rch
FROM reaches r
WHERE r.dist_out = 0
  AND r.end_rch != 2  -- not an outlet
  AND (r.region = ? OR ? IS NULL)
```

### SQL for T010 (Length Consistency)
```sql
WITH pairs AS (
    SELECT
        r1.reach_id,
        r1.region,
        r1.dist_out as dist_up,
        r2.dist_out as dist_down,
        r1.reach_length,
        ABS(r1.dist_out - r2.dist_out - r1.reach_length) as delta
    FROM reaches r1
    JOIN reach_topology rt ON r1.reach_id = rt.reach_id AND r1.region = rt.region
    JOIN reaches r2 ON rt.neighbor_reach_id = r2.reach_id AND rt.region = r2.region
    WHERE rt.direction = 'down'
      AND r1.dist_out > 0 AND r1.dist_out != -9999
      AND r2.dist_out > 0 AND r2.dist_out != -9999
)
SELECT * FROM pairs WHERE delta > ?  -- threshold parameter
```
