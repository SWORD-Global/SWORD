# Validation Spec: v17c Path Topology Variables

## Variables

| Variable | Type | Description |
|----------|------|-------------|
| `main_path_id` | BIGINT | Unique identifier for (best_headwater, best_outlet) pairs; groups all reaches flowing from same headwater to same outlet |
| `dist_out_short` | DOUBLE | Shortest-path distance to any outlet via Dijkstra (meters) |
| `rch_id_up_main` | BIGINT | reach_id of selected main upstream neighbor (NULL for headwaters) |
| `rch_id_dn_main` | BIGINT | reach_id of selected main downstream neighbor (NULL for outlets) |

## Summary

- **Source:** Computed (v17c NEW) via `assign_attribute.py` in v17c_pipeline
- **Algorithm:** NetworkX graph operations (topological grouping, Dijkstra, reach selection)
- **Dependencies:** reach_topology (v17b), is_mainstem, width, reach_length, best_headwater, best_outlet
- **Storage:** `reaches` table in sword_v17c.duckdb

## Code Path

### Primary File
`/Users/jakegearon/projects/SWORD/src/sword_v17c_pipeline/assign_attribute.py`

### Key Functions

| Function | Lines | Description |
|----------|-------|-------------|
| `main_path()` | 276-312 | Groups edges by (best_headwater, best_outlet) pairs and assigns unique main_path_id to each group |
| `distance_measures()` | 736-828 | Computes dist_out_short via multi-source Dijkstra from all outlets |
| `assign_main_connection()` | 830-897 | Selects rch_id_up_main and rch_id_dn_main based on mainstem edges and width |
| `choose_main_reach()` | 834-870 | Helper that selects main upstream/downstream reach using mainstem edge + width criteria |

## Algorithm Details

### 1. main_path_id Computation (lines 276-312)

```python
def main_path(DG):
    groups = {}

    # Step 1: Assign node-level best_headwater and best_outlet to edges
    for u, v, k, data in DG.edges(keys=True, data=True):
        bhw = DG.nodes[u].get('best_headwater')  # From upstream pass
        bow = DG.nodes[v]['best_outlet']         # From downstream pass

        key = (bhw, bow)
        if key not in groups:
            groups[key] = []
        groups[key].append((u, v, k))

    # Step 2: Assign unique main_path_id to each group
    for idx, ((bhw, bow), edges) in enumerate(groups.items(), start=1):
        for u, v, k in edges:
            DG[u][v][k]["main_path_id"] = idx

    # Step 3: Verify connectivity of each main_path_id group
    check_main_path_id_components(DG)
```

**Key Properties:**
- All reaches with same (best_headwater, best_outlet) pair get same main_path_id
- main_path_id values are sequential integers starting at 1
- Each main_path_id forms a SINGLE weakly-connected component (verified by check_main_path_id_components)

### 2. dist_out_short Computation (lines 753-769)

```python
def distance_measures(G):
    # 1. Find all outlets (out_degree == 0)
    outlet_nodes = [n for n in G.nodes() if G.out_degree(n) == 0]

    # 2. Multi-source Dijkstra from ALL outlets (working backwards on reversed graph)
    dist_out_node = {n: float("inf") for n in G.nodes()}
    for outlet in outlet_nodes:
        dist_out_node[outlet] = 0.0

    # 3. Reverse graph and run multi-source Dijkstra
    revDG = DG.reverse()
    dist_out_node.update(
        nx.multi_source_dijkstra_path_length(revDG, outlet_nodes, weight="weight")
    )

    # 4. Assign to edges based on downstream node (v)
    for u, v, k, d in G.edges(keys=True, data=True):
        d["dist_out_short"] = dist_out_node.get(v, float("inf"))
```

**Key Properties:**
- dist_out_short is assigned to each edge based on its downstream node (v)
- Represents shortest weighted path distance (sum of reach_lengths) to nearest outlet
- May be different from dist_out (which is a v17b pre-computed attribute)
- 0 at outlet nodes; increases upstream

### 3. rch_id_up_main / rch_id_dn_main Computation (lines 830-897)

```python
def assign_main_connection(G):
    for u, v, k, data in G.edges(keys=True, data=True):
        reach_id = data["reach_id"]

        # Find upstream edges terminating at u
        upstream_edges = [
            (attrs["reach_id"], attrs)
            for uu, vv, kk, attrs in G.in_edges(u, keys=True, data=True)
        ]

        # Find downstream edges starting from v
        downstream_edges = [
            (attrs["reach_id"], attrs)
            for uu, vv, kk, attrs in G.out_edges(v, keys=True, data=True)
        ]

        # Select main upstream and downstream reaches
        data["rch_id_up_main"] = choose_main_reach(upstream_edges)
        data["rch_id_dn_main"] = choose_main_reach(downstream_edges)


def choose_main_reach(candidates):
    """
    Selection logic (lines 834-870):

    CASE 1: All candidates have SAME main_path_id
        a) Prefer reach with is_mainstem == True
        b) Else choose largest width

    CASE 2: Candidates have DIFFERENT main_path_ids
        a) Prefer reach with is_mainstem == True
        b) Fallback: choose largest width
    """
    if not candidates:
        return None

    reach_ids = [c[0] for c in candidates]
    attrs = [c[1] for c in candidates]
    main_path_ids = {a.get("main_path_id") for a in attrs}

    # CASE 1: Same main_path_id
    if len(main_path_ids) == 1:
        stem_edges = [rid for rid, a in candidates if a.get("is_mainstem")]
        if stem_edges:
            return stem_edges[0]

        # Else pick widest
        return max(candidates, key=lambda x: x[1].get("width", 0))[0]

    # CASE 2: Different main_path_ids
    stem_edges = [rid for rid, a in candidates if a.get("is_mainstem")]
    if stem_edges:
        return stem_edges[0]

    # Fallback: widest
    return max(candidates, key=lambda x: x[1].get("width", 0))[0]
```

**Key Properties:**
- For headwaters: rch_id_up_main is NULL (no upstream neighbors)
- For outlets: rch_id_dn_main is NULL (no downstream neighbors)
- Non-null rch_id_up_main/rch_id_dn_main must exist in reaches.reach_id
- Selection prefers mainstem edges when available
- Fallback to width when mainstem edges not available

## Dependencies (from v17b)

| Source Table | Columns Used |
|--------------|--------------|
| `reach_topology` | reach_id, direction, neighbor_reach_id (used indirectly via graph construction) |
| `reaches` | reach_id, reach_length, width, best_headwater, best_outlet, is_mainstem |

## Data Type Specifications

| Variable | Type | Min | Max | NULL Allowed | Notes |
|----------|------|-----|-----|--------------|-------|
| main_path_id | BIGINT | 1 | ~42,607 | NO | Sequential integers per region |
| dist_out_short | DOUBLE | 0.0 | ~6,747,000 | NO | Meters; 0 at outlets |
| rch_id_up_main | BIGINT | 10000000000 | 99999999999 | YES | NULL for headwaters |
| rch_id_dn_main | BIGINT | 10000000000 | 99999999999 | YES | NULL for outlets |

## Expected Distributions

### main_path_id Distribution (by region)

| Region | Count | Unique main_path_ids |
|--------|-------|----------------------|
| AF | 21,441 | ~3,137 |
| AS | 100,185 | ~18,634 |
| EU | 31,103 | ~4,222 |
| NA | 38,696 | ~6,363 |
| OC | 15,090 | ~2,979 |
| SA | 42,159 | ~7,272 |

Expected ratio: 1 main_path_id per 6-8 reaches (average mainstem path length).

### dist_out_short Distribution (by region)

| Region | Min (km) | Median (km) | Max (km) | Mean (km) |
|--------|----------|-------------|----------|-----------|
| AF | 0 | 876 | 6,747 | 1,310 |
| AS | 0 | 877 | 6,083 | 1,223 |
| EU | 0 | 420 | 3,513 | 726 |
| NA | 0 | 510 | 5,537 | 974 |
| OC | 0 | 112 | 3,374 | 318 |
| SA | 0 | 1,756 | 5,988 | 1,890 |

Note: Large variation in SA, OC due to geography (deltas, archipelagos).

### rch_id_up_main / rch_id_dn_main Population

Expected distribution:
- rch_id_up_main: ~95% non-null (5% headwaters + terminals)
- rch_id_dn_main: ~95% non-null (5% outlets + terminals)

## Failure Modes

### F1: main_path_id Discontinuity
- **Symptom:** Two reaches on same visual path have different main_path_ids
- **Cause:** best_headwater or best_outlet changed between reaches (should not happen)
- **Impact:** Breaks mainstem path logic; visualization problems
- **Detection:** Query for reaches with same (best_hw, best_out) pair that got different main_path_id

### F2: dist_out_short Not Monotonic Downstream
- **Symptom:** Downstream reach has HIGHER dist_out_short than upstream reach
- **Cause:** Dijkstra found alternative route to different outlet OR topology issue
- **Impact:** Counterintuitive for flow analysis
- **Expected:** ~1,210 violations globally (0.49%) - known in deltas
- **Detection:** Check monotonicity along reach_topology paths

### F3: rch_id_up_main Invalid Reach
- **Symptom:** rch_id_up_main value not in reaches.reach_id
- **Cause:** Data corruption or algorithm error
- **Impact:** Downstream analysis breaks
- **Detection:** Foreign key check

### F4: rch_id_dn_main Invalid Reach
- **Symptom:** rch_id_dn_main value not in reaches.reach_id
- **Cause:** Data corruption or algorithm error
- **Impact:** Downstream analysis breaks
- **Detection:** Foreign key check

### F5: Headwater Has rch_id_up_main
- **Symptom:** Reach with n_rch_up == 0 (headwater) has non-null rch_id_up_main
- **Cause:** Algorithm error in upstream neighbor detection
- **Impact:** Incorrect topology interpretation
- **Expected:** 0 violations
- **Detection:** Check for any reach where n_rch_up == 0 AND rch_id_up_main IS NOT NULL

### F6: Outlet Has rch_id_dn_main
- **Symptom:** Reach with n_rch_down == 0 (outlet) has non-null rch_id_dn_main
- **Cause:** Algorithm error in downstream neighbor detection
- **Impact:** Incorrect topology interpretation
- **Expected:** 0 violations
- **Detection:** Check for any reach where n_rch_down == 0 AND rch_id_dn_main IS NOT NULL

### F7: Orphan main_path_id Reaches
- **Symptom:** Reach has main_path_id but cannot find any reach with same main_path_id
- **Cause:** Data corruption during import
- **Impact:** Isolated path segments
- **Detection:** GROUP BY main_path_id; check each group is non-empty

### F8: NULL dist_out_short for Non-Outlet
- **Symptom:** Reach with n_rch_down > 0 has NULL dist_out_short
- **Cause:** Graph disconnection (reach not reachable from any outlet)
- **Impact:** Missing distance data
- **Expected:** 0 violations (100% coverage expected)
- **Detection:** Check for NULL dist_out_short where n_rch_down > 0

### F9: main_path_id Points to Unreachable (best_hw, best_out)
- **Symptom:** Reach with main_path_id=X has best_hw/best_out, but X != hash(best_hw, best_out)
- **Cause:** Algorithm error or data inconsistency
- **Impact:** main_path_id grouping is wrong
- **Detection:** Verify that all reaches with main_path_id=X have identical (best_hw, best_out)

## Proposed Lint Checks

### New Path Topology Checks (V0xx - v17c category)

| ID | Severity | Rule | Rationale | Affected Variables |
|----|----------|------|-----------|-------------------|
| V009 | ERROR | dist_out_short must decrease downstream | Flow direction integrity | dist_out_short |
| V010 | ERROR | rch_id_up_main/dn_main must be valid reach_ids | Data integrity | rch_id_up_main, rch_id_dn_main |
| V011 | ERROR | Headwater must have NULL rch_id_up_main | Topology consistency | rch_id_up_main |
| V012 | ERROR | Outlet must have NULL rch_id_dn_main | Topology consistency | rch_id_dn_main |
| V013 | WARNING | All reaches in same main_path_id have same (best_hw, best_out) | Grouping consistency | main_path_id |
| V014 | WARNING | main_path_id groups are weakly connected | Component integrity | main_path_id |
| V015 | INFO | main_path_id distribution statistics | Coverage/distribution | main_path_id |
| V016 | INFO | dist_out_short distribution statistics | Distribution analysis | dist_out_short |

## Detailed Check Specifications

### V009: dist_out_short Monotonicity

```sql
-- Find downstream reaches with HIGHER dist_out_short than upstream
WITH reach_pairs AS (
    SELECT
        r1.reach_id as up_reach,
        r1.dist_out_short as up_dist,
        r2.reach_id as dn_reach,
        r2.dist_out_short as dn_dist,
        r1.region
    FROM reaches r1
    JOIN reach_topology rt ON r1.reach_id = rt.reach_id AND r1.region = rt.region
    JOIN reaches r2 ON rt.neighbor_reach_id = r2.reach_id AND r2.region = r1.region
    WHERE rt.direction = 'down'
        AND r1.dist_out_short IS NOT NULL
        AND r2.dist_out_short IS NOT NULL
)
SELECT * FROM reach_pairs
WHERE dn_dist > up_dist + 100  -- 100m tolerance for floating point errors
ORDER BY (dn_dist - up_dist) DESC
```

**Expected violations:** ~1,210 globally (0.49%) - known issues in deltas

### V010: rch_id Validity

```sql
-- Check rch_id_up_main points to valid reach
SELECT COUNT(*) as invalid_up_main
FROM reaches r
WHERE r.rch_id_up_main IS NOT NULL
  AND r.rch_id_up_main NOT IN (SELECT reach_id FROM reaches WHERE region = r.region)

-- Check rch_id_dn_main points to valid reach
SELECT COUNT(*) as invalid_dn_main
FROM reaches r
WHERE r.rch_id_dn_main IS NOT NULL
  AND r.rch_id_dn_main NOT IN (SELECT reach_id FROM reaches WHERE region = r.region)
```

**Expected violations:** 0

### V011: Headwater Constraint

```sql
-- Headwaters (n_rch_up == 0) should have NULL rch_id_up_main
SELECT COUNT(*) as violations
FROM reaches r
WHERE r.n_rch_up = 0
  AND r.rch_id_up_main IS NOT NULL
```

**Expected violations:** 0

### V012: Outlet Constraint

```sql
-- Outlets (n_rch_down == 0) should have NULL rch_id_dn_main
SELECT COUNT(*) as violations
FROM reaches r
WHERE r.n_rch_down = 0
  AND r.rch_id_dn_main IS NOT NULL
```

**Expected violations:** 0

### V013: main_path_id Grouping Consistency

```sql
-- All reaches with same main_path_id should have identical (best_headwater, best_outlet)
WITH main_path_groups AS (
    SELECT
        main_path_id,
        region,
        COUNT(DISTINCT best_headwater) as hw_count,
        COUNT(DISTINCT best_outlet) as out_count
    FROM reaches
    WHERE main_path_id IS NOT NULL
    GROUP BY main_path_id, region
)
SELECT * FROM main_path_groups
WHERE hw_count > 1 OR out_count > 1
```

**Expected violations:** 0

### V014: main_path_id Component Connectivity

```sql
-- Verify each main_path_id forms a single weakly-connected component
-- (This would require networkx analysis; listed for reference)
-- Manual verification: check_main_path_id_components() in assign_attribute.py
```

**Expected violations:** 0 (verified during assign_attribute.py)

### V015: main_path_id Distribution

```sql
SELECT
    region,
    COUNT(DISTINCT main_path_id) as num_paths,
    COUNT(*) as num_reaches,
    ROUND(COUNT(*) / COUNT(DISTINCT main_path_id), 1) as avg_reaches_per_path
FROM reaches
WHERE main_path_id IS NOT NULL
GROUP BY region
ORDER BY region
```

**Expected output:**
- 1 main_path_id per 6-8 reaches (average mainstem path length)
- Consistent across regions

### V016: dist_out_short Distribution

```sql
SELECT
    region,
    COUNT(*) as cnt,
    MIN(dist_out_short) / 1000 as min_km,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY dist_out_short) / 1000 as p25_km,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY dist_out_short) / 1000 as median_km,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY dist_out_short) / 1000 as p75_km,
    MAX(dist_out_short) / 1000 as max_km,
    AVG(dist_out_short) / 1000 as mean_km,
    STDDEV(dist_out_short) / 1000 as stddev_km
FROM reaches
WHERE dist_out_short IS NOT NULL
GROUP BY region
ORDER BY region
```

**Expected output:** See Expected Distributions table above

## Edge Cases

### 1. Bifurcating Rivers (Deltas)
- **Behavior:** Each reach gets ONE best_outlet (by width); multiple outlets possible
- **Impact:** main_path_id only follows widest channel
- **main_path_id Effect:** Multiple main_path_ids per delta region (correct)
- **dist_out_short Effect:** May have shorter paths to secondary outlets
- **Validation:** Check that deltas have lower mainstem % (< 95%)

### 2. Braided Rivers
- **Behavior:** Temporary islands create multiple parallel paths
- **Impact:** Dijkstra may pick different path
- **main_path_id Effect:** May have multiple main_path_ids for same pair (if paths split/rejoin)
- **dist_out_short Effect:** Shortest path correctly selects fastest route
- **Validation:** Manual inspection in braided regions

### 3. Disconnected Components
- **Behavior:** Some reaches not reachable from outlets
- **Impact:** dist_out_short = infinity (stored as NULL or large value)
- **main_path_id Effect:** Isolated components get separate main_path_ids
- **Validation:** Check connected_components lint check (T006)

### 4. Headwater Reaches Only
- **Behavior:** rch_id_up_main = NULL
- **Impact:** No issue; expected
- **Validation:** Count should be ~5% of total

### 5. Outlet Reaches Only
- **Behavior:** rch_id_dn_main = NULL
- **Impact:** No issue; expected
- **Validation:** Count should be ~0.5% of total

### 6. Single-Reach Networks
- **Behavior:** is both headwater AND outlet; best_hw = best_out = self; rch_id_up_main = rch_id_dn_main = NULL
- **Impact:** Isolated network (e.g., closed lake)
- **main_path_id Effect:** Gets its own main_path_id
- **Validation:** Count and verify connectivity

## Relationships to Other Variables

### vs. best_headwater / best_outlet
- **Relationship:** main_path_id is derived from (best_headwater, best_outlet) pairs
- **Consistency:** If best_headwater/outlet change, main_path_id should change
- **Validation:** All reaches with same main_path_id must have identical (best_hw, best_out)

### vs. is_mainstem
- **Relationship:** rch_id_up_main/dn_main selection PREFERS is_mainstem = True
- **Consistency:** Non-mainstem reaches may have mainstem neighbors as rch_id_up/dn_main
- **Validation:** If is_mainstem = True, rch_id_up/dn_main should prefer mainstem neighbors

### vs. dist_out (v17b)
- **Difference:** dist_out = pre-computed v17b attribute; dist_out_short = v17c recomputed
- **Relationship:** dist_out_short may differ from dist_out due to facc fixes
- **Validation:** Both should decrease downstream (monotonicity)

### vs. reach_topology
- **Relationship:** rch_id_up_main/dn_main must exist in reach_topology
- **Consistency:** rch_id_up_main should be first in reach_topology.direction='up'
- **Validation:** Verify existence of topology relationships

## Known Issues

### Issue: dist_out_short Monotonicity Violations (1,210 global)
- **Status:** Documented; expected in complex deltas
- **Affects:** V009 check (will report ~0.49% violations)
- **Cause:** Multi-source Dijkstra may find shorter path to alternative outlet
- **Impact:** Not a data corruption; represents topology complexity
- **Reference:** See validation_spec_v17c_mainstem_variables.md Issue 1

### Issue: Invalid best_headwater (123 in NA)
- **Status:** Under investigation
- **Affects:** V013 check indirectly
- **Cause:** Width-prioritized selection selected non-headwater
- **Impact:** Downstream analysis may be affected
- **Workaround:** None; requires topology investigation

## Reconstruction Rules (if rebuilding)

**NEVER assume semantics from variable names. Always query v17b to understand actual values before rebuilding.**

### main_path_id Reconstruction
```python
# 1. Get best_headwater and best_outlet for each reach (already computed)
# 2. Group reaches by (best_hw, best_out) pair
# 3. Assign sequential integer ID to each group (start at 1)
# 4. Verify each group is weakly connected using networkx
groups = defaultdict(list)
for reach_id in reaches:
    bhw = reaches[reach_id]['best_headwater']
    bout = reaches[reach_id]['best_outlet']
    groups[(bhw, bout)].append(reach_id)

for idx, ((bhw, bout), reach_ids) in enumerate(groups.items(), start=1):
    for rid in reach_ids:
        reaches[rid]['main_path_id'] = idx
```

### dist_out_short Reconstruction
```python
# 1. Build directed graph from reach_topology
# 2. Find all outlets (out_degree == 0)
# 3. Run multi-source Dijkstra from all outlets (on REVERSED graph)
# 4. Assign result to each edge based on downstream node
import networkx as nx

G = build_graph_from_topology()
outlets = [n for n in G.nodes() if G.out_degree(n) == 0]
R = G.reverse()
distances = nx.multi_source_dijkstra_path_length(R, outlets, weight='reach_length')

for reach_id in reaches:
    # Find downstream neighbor and get distance from that node
    reach_dist = distances.get(downstream_node, inf)
    reaches[reach_id]['dist_out_short'] = reach_dist
```

### rch_id_up_main / rch_id_dn_main Reconstruction
```python
# 1. For each reach, find upstream and downstream neighbors
# 2. Select main neighbor using: is_mainstem > width
# 3. NULL for headwaters (up_main) and outlets (dn_main)

for reach_id in reaches:
    # Upstream candidates
    upstream = get_upstream_neighbors(reach_id)
    rch_id_up_main = None
    if upstream:
        # Prefer mainstem
        mainstem = [r for r in upstream if reaches[r]['is_mainstem']]
        if mainstem:
            rch_id_up_main = max(mainstem, key=lambda r: reaches[r]['width'])['reach_id']
        else:
            rch_id_up_main = max(upstream, key=lambda r: reaches[r]['width'])['reach_id']

    # Downstream candidates (same logic)
    downstream = get_downstream_neighbors(reach_id)
    rch_id_dn_main = None
    if downstream:
        mainstem = [r for r in downstream if reaches[r]['is_mainstem']]
        if mainstem:
            rch_id_dn_main = max(mainstem, key=lambda r: reaches[r]['width'])['reach_id']
        else:
            rch_id_dn_main = max(downstream, key=lambda r: reaches[r]['width'])['reach_id']

    reaches[reach_id]['rch_id_up_main'] = rch_id_up_main
    reaches[reach_id]['rch_id_dn_main'] = rch_id_dn_main
```

**CRITICAL NOTES:**
- Do NOT hardcode assumptions about mainstem vs. width weighting
- Always check if is_mainstem is already assigned before using it
- Query v17b first to see actual distributions of these variables
- Test reconstruction on small region before global rollout
- Verify monotonicity and connectivity after reconstruction

## Testing Strategy

### Unit Tests
1. **test_main_path_id_grouping**: Verify all reaches with same (best_hw, best_out) get same main_path_id
2. **test_dist_out_short_monotonicity**: Check decreasing downstream (with tolerance)
3. **test_rch_id_validity**: Verify rch_id_up/dn_main exist in reaches table
4. **test_headwater_outlet_constraints**: Verify NULL values for headwaters/outlets

### Integration Tests
1. **test_main_path_id_connectivity**: Verify each main_path_id forms weakly-connected component
2. **test_topology_consistency**: Verify rch_id_up_main matches reach_topology
3. **test_width_selection**: Verify rch_id_up/dn_main prefers is_mainstem and width

### Regression Tests
1. Compare with v17b distributions
2. Check that v17b and v17c reach counts match
3. Verify no NULLs introduced for previously non-null values

## Summary Table

| Variable | Coverage | Type | Min | Max | Nullable | Constraint |
|----------|----------|------|-----|-----|----------|------------|
| main_path_id | 100% | BIGINT | 1 | ~42,607 | NO | (best_hw, best_out) grouping |
| dist_out_short | 100% | DOUBLE | 0 m | ~6.7M m | NO | Monotonic downstream |
| rch_id_up_main | ~95% | BIGINT | 10^10 | 10^11 | YES | NULL for headwaters |
| rch_id_dn_main | ~95% | BIGINT | 10^10 | 10^11 | YES | NULL for outlets |

**Overall Assessment:** Four well-defined v17c path topology variables with clear semantics and algorithmic origins. Main quality concern is 1,210 dist_out_short monotonicity violations (0.49%) in deltas - expected but warrants documentation.
