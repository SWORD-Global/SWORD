# Validation Spec: subnetwork_id Variable

## Variable Summary

| Attribute | Type | Description |
|-----------|------|-------------|
| `subnetwork_id` | INT32 | Weakly connected component ID assigned during graph construction |

## Overview

- **Source:** Computed in `src/sword_v17c_pipeline/SWORD_graph.py` during graph building
- **Algorithm:** NetworkX `weakly_connected_components()` on the directed reach-level graph
- **Purpose:** Identifies connected subgraphs where reaches are connected via topology
- **Relationship to `network`:** Different ID schemes - `subnetwork_id` may span multiple `network` values

## Summary Statistics (2026-03-03)

### Global Distribution

| Metric | Value |
|--------|-------|
| Total reaches | 248,673 |
| Distinct subnetwork_id values | 3,027 (globally unique via Pfafstetter offsets) |
| Distinct network values | 247 |
| Reaches where subnetwork_id = network | 0 (Pfafstetter offsets make numerical match impossible) |

### By Region

| Region | Reaches | Subnetworks | Networks | Pfafstetter Band |
|--------|---------|-------------|----------|-----------------|
| AF | 21,441 | 230 | 79 | 1,000,001 - 1,000,230 |
| AS | 100,185 | 854 | 246 | 3,000,001 - 3,000,854 |
| EU | 31,103 | 436 | 103 | 2,000,001 - 2,000,436 |
| NA | 38,696 | 586 | 105 | 7,000,001 - 7,000,586 |
| OC | 15,089 | 676 | 211 | 5,000,001 - 5,000,676 |
| SA | 42,159 | 245 | 53 | 6,000,001 - 6,000,245 |

## Algorithm Details

### Location
`/Users/jakegearon/projects/SWORD/src/sword_v17c_pipeline/SWORD_graph.py`, lines 541-553

### Code Path

```python
def add_network_id(DG):
    # Step 1: Find weakly connected components
    # A weakly connected component is a subgraph where every pair of nodes has
    # a path between them when treating the directed graph as undirected
    components = list(nx.weakly_connected_components(DG))

    # Step 2: Assign a subnetwork ID
    # Each component gets a unique integer ID (1-indexed)
    for subnetwork_id, component_nodes in enumerate(components, start=1):
        # Add subnetwork ID to nodes
        for node in component_nodes:
            DG.nodes[node]['subnetwork_id'] = subnetwork_id

        # Add subnetwork ID to edges
        for u, v, key in DG.subgraph(component_nodes).edges(keys=True):
            DG.edges[u, v, key]['subnetwork_id'] = subnetwork_id
```

### Key Properties

1. **Connected:** Two reaches in the same `subnetwork_id` have a path between them (ignoring direction)
2. **Non-overlapping:** Every reach belongs to exactly one subnetwork_id
3. **ID Assignment:** IDs are assigned in order of discovery (depends on node iteration order)

## Relationship to Other Variables

### subnetwork_id vs network

| Property | subnetwork_id | network |
|----------|---------------|---------|
| **Defined in** | v17c pipeline (SWORD_graph.py) | v17b (original SWORD) |
| **Algorithm** | Weakly connected components | Unknown (pre-existing) |
| **Count** | 855 | 247 |
| **Granularity** | Finer (more components) | Coarser (fewer components) |
| **Span** | Single or multiple networks | Always single |

### Example from NA region

```
subnetwork_id=278 spans network=1 and network=2 (1076 + 691 = 1767 reaches)
subnetwork_id=165 contains only network=1 (5910 reaches)
subnetwork_id=3 contains only network=2 (691 reaches)
```

**Interpretation:** `subnetwork_id` is a more granular decomposition. Some weakly connected components in the graph contain reaches from multiple original `network` components. This suggests that `network` field may have been computed differently or there have been topology modifications.

## Data Quality Checks

### Coverage
- **Status:** ✅ COMPLETE
- **Finding:** All 248,674 reaches have a subnetwork_id assigned
- **NULL values:** 0

### Range
- **Min:** 1 (valid)
- **Max:** 856 (valid)
- **Type:** INT32 (appropriate for component IDs)

### Consistency with network
- **Many subnetwork_ids span multiple networks:** 3 subnetwork_ids in NA (278, 394, 375) span 2 different networks each
- **Expected:** Indicates that the graph topology as used by SWORD_graph differs from original v17b network definition
- **Status:** Requires investigation but not an error

## Failure Modes

### F1: Disconnected Reach
- **Symptom:** Reach has subnetwork_id but no upstream or downstream neighbors
- **Cause:** Graph construction failure or missing topology
- **Impact:** Reach is isolated from other reaches with same subnetwork_id
- **Detection:** Cross-check with `n_rch_up + n_rch_down = 0`

### F2: subnetwork_id=0 or NULL
- **Symptom:** Some reaches have NULL or 0 for subnetwork_id
- **Cause:** Graph construction failed or incomplete processing
- **Impact:** Incomplete component assignment
- **Current Status:** NOT OBSERVED (0 NULLs)

### F3: Impossible Cross-Network Subnetwork
- **Symptom:** All reaches in a subnetwork_id belong to the same network, but that network has OTHER reaches in DIFFERENT subnetwork_ids
- **Cause:** Graph topology differs significantly from v17b definitions
- **Impact:** Indicates topology was modified or network field became obsolete
- **Current Status:** OBSERVED (most subnetwork_ids have multiple network components or vice versa)

### F4: Unreachable Weakly Connected Assumption
- **Symptom:** Two reaches in the same subnetwork_id cannot be connected via undirected path in actual topology
- **Cause:** Topology data corruption
- **Impact:** Invalid subnetwork_id assignment
- **Detection:** Would require rebuilding and comparing

## Proposed Lint Checks

### Implemented Checks (V0xx - v17c Category)

| ID | Severity | Rule | Status (2026-03-03) |
|----|----------|------|---------------------|
| V026 | ERROR | subnetwork_id coverage (no NULLs on connected reaches) | PASS (0/210,682) |
| V027 | ERROR | subnetwork_id within Pfafstetter band for region | PASS (0/248,673) |
| V028 | ERROR | Topology-connected reaches share same subnetwork_id | PASS (0/495,620 edges) |
| V029 | ERROR | No subnetwork_id appears in multiple regions | PASS (0/3,027) |
| V030 | INFO | Isolated reaches form singleton components | PASS (0 isolated) |
| V031 | INFO | Size distribution statistics | 3,027 components, 0 singletons |

## Interpretation Guidance

### What subnetwork_id Means

`subnetwork_id` represents **weakly connected components of the reach-level topology graph**. A weakly connected component is the largest set of nodes where every pair is reachable if you ignore edge directions.

**Example:** In a network with branching:
```
    A → B
   ↙
  C
```

All three (A, B, C) are in the same weakly connected component because:
- A can reach B (follow edge)
- C can reach A (follow edge, ignoring direction)
- Therefore all are connected

### Why Different from network?

The `network` field (v17b) may have used a different algorithm:
- Original SWORD might have used strongly connected components (all nodes must reach all others, respecting direction)
- Or used connected components at a different graph construction stage
- Or was computed before certain topology fixes

**Current state:** 855 subnetwork_ids from 247 networks suggests:
- Some networks are subdivided into multiple subnetwork_ids
- Some subnetwork_ids span multiple networks
- The two schemes are **not equivalent** but **related**

## Validation Strategy

### Step 1: Verify Complete Coverage
```sql
SELECT COUNT(*) FROM reaches WHERE subnetwork_id IS NULL;
-- Should return 0
```

### Step 2: Check Consistency of Weak Connectivity
```sql
-- Verify that all reaches in a subnetwork_id can be connected via undirected topology
-- This requires rebuilding the graph and rechecking connected components
-- See: src/sword_v17c_pipeline/SWORD_graph.py:541-553
```

### Step 3: Document Cross-Network Subnetworks
```sql
-- For production data, document why some subnetwork_ids span multiple networks
SELECT
    subnetwork_id, COUNT(DISTINCT network) as num_nets
FROM reaches
WHERE COUNT(DISTINCT network) > 1
GROUP BY subnetwork_id;
```

### Step 4: Reconcile with Network Field
- **Option A:** Keep both fields (subnetwork_id for graph analysis, network for legacy compatibility)
- **Option B:** Replace network with subnetwork_id (requires downstream impact assessment)
- **Recommendation:** Keep both for now, document in data dictionary

## Edge Cases

### 1. Single-Reach Subnetwork
- **Behavior:** A subnetwork_id with only one reach is valid if that reach is isolated
- **Example:** Island reaches or disconnected lake systems
- **Expected count:** < 1% of reaches

### 2. Subnetwork Spanning Multiple Regions
- **Behavior:** Currently NOT EXPECTED - subnetwork_id computed per-region
- **Verification:** All reaches with same subnetwork_id should have same region

### 3. Subnetwork ID Reassignment
- **Current:** IDs are 1-indexed sequentially starting from 1 per region
- **Stability:** IDs may differ between pipeline runs (depends on component discovery order)
- **Recommendation:** Use for analysis within a single database version; not for stable identifiers across releases

## Relationship to Graph Construction

### When subnetwork_id is Assigned

1. **SWORD_graph.py** loads reaches and builds `DG` (MultiDiGraph)
2. **add_network_id()** is called on the directed graph
3. `nx.weakly_connected_components()` decomposes the graph
4. Each component gets an ID 1, 2, 3, ...
5. These are written to:
   - `DG.nodes[node]['subnetwork_id']` for nodes (junctions)
   - `DG.edges[u, v, key]['subnetwork_id']` for edges (reaches)

### Then Propagated to Reaches Table

Through the pipeline:
1. **SWORD_graph.py** → creates network graph with subnetwork_id
2. **create_edges_gdf()** → extracts edges (reaches) with attributes including subnetwork_id
3. **assign_attribute.py** → may merge back to reaches table
4. **v17c_pipeline.py** or database script → writes to sword_v17c.duckdb

**Current Status:** subnetwork_id is present in reaches table with full coverage (2026-02-02)

## Files Involved

| File | Purpose | Status |
|------|---------|--------|
| `SWORD_graph.py` | Compute subnetwork_id via weakly connected components | ✅ Active |
| `assign_attribute.py` | Propagate to reaches table | ✅ Active |
| `docs/validation_specs/subnetwork_id_validation_spec.md` | This spec | ✅ New |

## Recommendations

### Priority 1: Document Purpose
- [ ] Add to data dictionary: "subnetwork_id = weakly connected component ID from reach topology graph"
- [ ] Explain why it differs from `network` field
- [ ] Update README.md with subnetwork_id description

### Priority 2: Implement Lint Checks
- [ ] C005: subnetwork_id coverage (INFO)
- [ ] C006: Cross-network subnetwork detection (WARNING)
- [ ] C007: Isolated reach detection (INFO)

### Priority 3: Investigation
- [ ] Why do some subnetwork_ids span multiple networks? Is this expected?
- [ ] Should `network` field be deprecated in favor of subnetwork_id?
- [ ] Are subnetwork_id values stable across pipeline runs?

### Priority 4: Testing
- [ ] Add test to verify weakly connected assumption (expensive, but validates correctness)
- [ ] Add regression test comparing subnetwork_id distribution between versions

## Known Issues

### None Identified

**Status:** subnetwork_id correctly computed and completely populated. All 6 lint checks pass.
See `docs/technical/subnetwork_id_audit_2026-03-03.md` for full production results.

## References

- **NetworkX weakly_connected_components:** https://networkx.org/documentation/stable/reference/algorithms/generated/networkx.algorithms.components.weakly_connected_components.html
- **Graph component definitions:** https://en.wikipedia.org/wiki/Connected_component_(graph_theory)
- **SWORD_graph.py implementation:** `src/sword_v17c_pipeline/SWORD_graph.py:541-553`
- **Related validation spec:** `validation_spec_v17c_mainstem_variables.md` (mentions subnetwork relationships)

## Audit History

- **2026-02-02** - Initial validation spec creation based on database audit
- **2026-03-03** - Full audit with V026-V031 lint checks. All pass. Updated stats for Pfafstetter offsets.
