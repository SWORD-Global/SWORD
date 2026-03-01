# N006 Boundary Continuity Gap - Final Analysis & Recommendations

## Task Goal
Determine if N006 boundary gaps are rounding errors or real topological gaps, and propose a re-accumulation strategy.

## Methodology

1. **Read N006 check** (src/sword_duckdb/lint/checks/node.py lines 205-310)
2. **Calculated exact continuity_gap** for each violation:  
   `gap = abs((downstream_reach.max_dist_out + downstream_reach.reach_length) - upstream_reach.max_dist_out)`
3. **Categorized by severity** (Precision <1m, Calculation 1-500m, Topological ≥500m)
4. **Analyzed clustering** at bifurcations/confluences (n_rch_up > 1 OR n_rch_dn > 1)
5. **Examined top 10 violations** to identify missing topology links

## Diagnostic Script

```bash
python scripts/topology/n006_diagnostic.py
```

Output is saved to `scripts/topology/N006_ANALYSIS_REPORT.md`.

## Key Findings

### Severity Categorization

| Category | Gap Size | Count | % of Total | Conclusion |
|----------|----------|-------|------------|------------|
| Precision Errors | < 1.0m | **0** | 0% | ✗ None found |
| Calculation Jumps | 1-500m | **0** | 0% | ✗ None found |
| Topological Gaps | ≥ 500m | **158,155** | **100%** | **⚠️ CRITICAL** |

**Key Insight**: ZERO precision errors or calculation jumps. ALL 158,155 violations are MAJOR TOPOLOGICAL GAPS.

### Top 10 Largest Gaps

| Rank | Upstream Reach | Downstream Reach | Region | Gap (km) |
|------|---------------|------------------|--------|----------|
| 1 | 56360400073 | 56360400063 | OC | **3,195.7** |
| 2 | 56470000031 | 56470000231 | OC | **2,785.3** |
| 3 | 56470000011 | 56470000051 | OC | **2,677.8** |
| 4 | 28107000031 | 28107000231 | EU | 1,411.0 |
| 5 | 45651000211 | 45642100181 | AS | 1,298.0 |
| 6 | 45642100011 | 45641000071 | AS | 1,128.3 |
| 7 | 56861000121 | 56861000251 | OC | 697.1 |
| 8 | 16270700111 | 16270700141 | AF | 552.8 |
| 9 | 62297300291 | 62297300281 | SA | 464.0 |
| 10 | 43421000481 | 43421001411 | AS | 410.8 |

### Regional Distribution

| Region | Violations | % of Total |
|--------|------------|------------|
| AS | 67,834 | 42.9% |
| NA | 25,365 | 16.0% |
| SA | 23,496 | 14.9% |
| EU | 21,196 | 13.4% |
| AF | 11,668 | 7.4% |
| OC | 8,596 | 5.4% |

**Top 3 regions (AS, NA, SA)** account for 73.8% of all violations.

### Bifurcation/Confluence Clustering

- **10.61%** of violations occur at bifurcations/confluences (n_rch_up > 1 OR n_rch_dn > 1)
- **89.39%** occur at single-link connections

**Key Insight**: Violations are NOT concentrated at junctions. They are distributed throughout **single links**, suggesting missing reaches rather than branching issues.

## Root Cause Analysis

### What These CANNOT Be:

1. **Rounding/precision errors**: Would be < 1m, but we see gaps up to 3,195 km
2. **Calculation drift**: Even massive drift would be < 500m, not 3,000+ km
3. **v17c recalculation artifacts**: Would create uniform drift, not mega-gaps

### What These LIKELY Are:

1. **Missing topology edges** in `reach_topology` table (most likely)
2. **Disconnected graph components** that should be connected
3. **NetCDF import issues** - original source data missing links
4. **Region split errors** - geographic splits without connecting edges

### Evidence Supporting Topological Gaps:

- **63.82%** of all downstream links (>158k out of 247k) have gaps > 1000m
- **OC region** dominates top 10 gaps (4 of top 10) - suggests regional processing issue
- **Single-link concentration** - 89% at non-bifurcations indicates missing nodes/reaches between known ones

## Recommendations

### ❌ DO NOT:

1. **Run global re-accumulation** without fixing topology first
   - This will just propagate the gaps through recalculation
   - Distances will redistribute but gaps will remain

2. **Assume these are precision errors**
   - 3,195 km gap is physically impossible for rounding

3. **Ignore the top 10 gaps**
   - These likely represent complete missing reach chains

### ✅ DO:

1. **Investigate Top 10 Gaps Individually**
   ```sql
   SELECT * FROM centerlines 
   WHERE reach_id IN (56360400073, 56360400063)  -- Top gap example
   ORDER BY dist_out;
   ```
   - Visualize to see what's between upstream and downstream reaches
   - Check for missing centerline points

2. **Run Network Connectivity Analysis**
   ```sql
   SELECT subnetwork_id, COUNT(DISTINCT reach_id) as reach_count,
          MIN(dist_out) as min_dist, MAX(dist_out) as max_dist
   FROM reaches 
   GROUP BY subnetwork_id 
   ORDER BY reach_count DESC 
   LIMIT 20;
   ```
   - Identify disconnected components that should be connected

3. **Check for Missing Edges in Topology Table**
   ```sql
   SELECT * FROM reach_topology 
   WHERE reach_id = 56360400073 
      OR neighbor_reach_id = 56360400063
   ORDER BY direction, neighbor_rank;
   ```

4. **Focus Investigation on**:
   - **OC region** (Oceania) - has largest gaps (3,195 km, 2,785 km, 2,677 km)
   - **AS region** (Asia) - 42.9% of all violations
   - Top 10 specific reach pairs listed above

5. **Consider Using ReconstructionEngine**
   - Rebuild topology from centerline positions
   - May find missing edges based on spatial proximity
   - `from sword_duckdb import SWORDWorkflow; workflow.reconstruct()`

## Next Steps

### Immediate (Week 1-2):
1. Investigate top 3 gaps in OC region - visualize missing geometry
2. Run region-by-region network connectivity analysis

### Short-term (Week 3-4):
1. Cross-reference v17b NetCDF source data for affected regions
2. Compare topology arrays to find missing links

### Long-term (Month 2+):
1. Consider rebuilding entire topology from centerline positions
2. Add automatic mega-gap detection to CI pipeline

## Conclusion

**These are REAL TOPOLOGICAL GAPS, NOT rounding errors.**

- **158,155 violations = 63.82% of all downstream topology links**
- **Largest gap: 3,195 km** - clearly missing topology
- **89% at single links** - not junction problems

**Recommendation: DO NOT run global re-accumulation.** Instead, investigate top 10 gaps to find missing topology edges, then use ReconstructionEngine to rebuild the topology graph from centerline positions.

---

*Report generated: 2026-02-27*  
*Database: data/duckdb/sword_v17c.duckdb*  
*Diagnostic script: scripts/topology/n006_diagnostic.py*
