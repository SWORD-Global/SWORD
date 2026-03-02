# Handover: Topology Healing Investigation (Issue #191)

## Context
We have identified **5,313 topological reversals** (fACC decreases downstream) and **4,816 physical inversions** (WSE increases downstream). Most of these were inherited from v17b.

## What is FIXED (Infrastructure)
1.  **N013 (CL-Node Sync):** Logic bug fixed. Centerlines are now perfectly synced to nodes via `workflow.sync_centerline_node_ids()`.
2.  **N006 (Distance Math):** Logic bug fixed. Distances are now continuous (0m gap) using the new optimized `recalculate_dist_out.py` with node interpolation.
3.  **Performance:** The BFS traversal engine in `sword_class.py` is now **super-optimized** (O(1) lookups + Bulk Updates). Recalculating 11M nodes takes 4 minutes instead of 4 hours.

## What is UNFINISHED (The Topology Challenge)
We experimentally flipped 1,112 high-confidence reaches, but it caused **30,000 disconnections** because many new paths led to "dead ends" (sinks) instead of the ocean. **We have reverted to the v17b baseline topology for now.**

## Next Steps for the Next Agent
1.  **Implement a "Global Path Verifier":**
    - Before flipping a reach, the solver must trace the *new* downstream path all the way to a known `best_outlet`.
    - If the path is broken (hits a sink), the flip must be rejected OR the missing bridge must be found spatially.
2.  **Leverage `FACC` Snapping:** Use the methodology in `scripts/maintenance/test_flip_confidence.py` to prove the flip is hydrologically consistent.
3.  **Bridge the Mega-Gaps:** The top 10 gaps are ~3,000km. These require manual topological joins or cross-basin investigation.

## Relevant Files
- `REVERSED_VALIDATION_REPORT.csv`: Scores for all 776 suspected chains.
- `suspected_reversed_reaches.geojson`: Geometries for all uphill chains.
- `scripts/analysis/plot_inversion_profiles.py`: Use this to see WSE vs Dist_Out for any reach.
