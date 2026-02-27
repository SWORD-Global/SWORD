# N006 Boundary Continuity Gap Analysis

## Executive Summary

**Critical Issue Found**: 158,155 N006 violations = **63.8% of all downstream topology links** contain boundary continuity gaps > 1000m. These are **TOPLOGICAL ERRORS**, not precision artifacts.

**Largest gap**: 3,195 km (3.2 million meters) - clearly indicates missing reaches in topology graph.

---

## Diagnostic Results

### Severity Categorization

| Category | Gap Size | Count | % of Violations | Status |
|----------|----------|-------|-----------------|--------|
| Precision Errors | < 1.0m | 0 | 0% | None found |
| Calculation Jumps | 1-500m | 0 | 0% | None found |
| Topological Gaps | ≥ 500m | 158,155 | 100% | **CRITICAL** |

### Top 10 Largest Gaps

| Upstream | Downstream | Region | Gap (km) |
|----------|------------|--------|----------|
| 56360400073 | 56360400063 | OC | 3,195.7 |
| 56470000031 | 56470000231 | OC | 2,785.3 |
| 56470000011 | 56470000051 | OC | 2,677.8 |
| 28107000031 | 28107000231 | EU | 1,411.0 |
| 45651000211 | 45642100181 | AS | 1,298.0 |
| 45642100011 | 45641000071 | AS | 1,128.3 |
| 56861000121 | 56861000251 | OC | 697.1 |
| 16270700111 | 16270700141 | AF | 552.8 |
| 62297300291 | 62297300281 | SA | 464.0 |
| 43421000481 | 43421001411 | AS | 410.8 |

### Regional Distribution

| Region | Violations | % of Total |
|--------|------------|------------|
| AS | 67,834 | 42.9% |
| NA | 25,365 | 16.0% |
| SA | 23,496 | 14.9% |
| EU | 21,196 | 13.4% |
| AF | 11,668 | 7.4% |
| OC | 8,596 | 5.4% |

**Note**: AS (Asia) and OC (Oceania) combined account for ~50% of violations.

### Bifurcation/Confluence Analysis

- **10.61%** of violations occur at bifurcations/confluences (n_rch_up > 1 OR n_rch_dn > 1)
- **89.39%** occur at single-link connections
- **Implication**: Violations are NOT concentrated at junctions - they're distributed throughout single links

---

## Root Cause Analysis

### What These Gaps CANNOT Be:

1. **Rounding Errors**: Precision errors would be < 1m, but we see 3,000+ km gaps
2. **Calculation Drift**: Even with significant drift, 1-500m is plausible; 3,000km is not
3. **v17c recalculation artifacts**: Would affect all links uniformly, not create mega-gaps

### What These Gaps LIKELY Are:

1. **Missing Topology Links**: The most likely cause - edges in reach_topology table are missing
2. **Disconnected Components**: NetworkX graph components that should be connected
3. **NetCDF Import Issues**: Original source data missing links
4. **Region Split Errors**: Geographic regions incorrectly split without connecting edges

### Evidence:

- **63.8% of all links** have gaps > 1000m - this is systemic, not random
- **OC region** dominates top gaps (3 of top 10) - suggests regional processing issue
- **Single-link concentration** - 89% at non-junctions suggests missing nodes/reaches between known ones

---

## Recommendations

### DO NOT:

❌ Run `reaccumulate_dist_out` without fixing topology first  
   - This will just propagate the gaps through recalculation  
   - The gaps will remain, distances just get redistributed

❌ Assume these are precision errors  
   - 3,195 km gap is physically impossible for rounding

❌ Ignore the top 10 gaps  
   - These likely represent complete missing reach chains

### DO:

✅ **Investigate Top 10 Gaps Individually**  
   - Query the centerline geometries for these reach pairs
   - Check if there are missing centerline points
   - Visualize to see what's between upstream and downstream reaches

✅ **Run Network Connectivity Analysis**  
```sql
SELECT DISTINCT subnetwork_id, COUNT(*) as components
FROM reaches 
GROUP BY subnetwork_id
ORDER BY components DESC;
```

✅ **Check for Missing Edges in Topology Table**  
```sql
SELECT rt.reach_id, rt.neighbor_reach_id
FROM reach_topology rt
WHERE rt.reach_id = 56360400073  -- Top gap
   OR rt.neighbor_reach_id = 56360400063;
```

✅ **Cross-Reference with NetCDF Source Data**  
   - Retrieve v17b NetCDF for the affected region (OC)
   - Compare topology arrays to find missing links

✅ **Consider Reconstruction Engine**  
   - Use `ReconstructionEngine` to rebuild topology from centerline positions
   - May find missing edges based on spatial proximity

✅ **Focus Investigation on**:  
   - **OC region** (largest gaps, highest concentration)  
   - **AS region** (42.9% of all violations)  
   - Top 10 specific reach pairs listed above

---

## Diagnostic Script

Run to reproduce results:
```bash
python scripts/topology/n006_diagnostic.py
```

---

## Next Steps

1. **Immediate**: Investigate top 3 gaps (OC region) - visualize missing geometry
2. **Short-term**: Run region-by-region network analysis for connectivity
3. **Long-term**: Consider rebuilding topology from centerline positions
4. **Preventive**: Add automatic detection of mega-gaps in CI pipeline

---

**Report Generated**: 2026-02-27  
**Database**: `data/duckdb/sword_v17c.duckdb`  
**Total Downstream Links**: 247,822  
**Violations**: 158,155 (63.82%)
