# Investigation Report: Physically Reversed Topology (Issue #191)

## Summary
A systematic investigation of A030 (WSE Monotonicity) and T017 (Distance Jumps) has identified thousands of reaches that are physically reversed. While 1,112 reaches were experimentally flipped using a "Local Hydrological Snap" logic, this caused a massive network fragmentation (30,000 disconnected reaches). The database has been reverted to the v17b baseline topology while we refine the "Healer" logic.

## Key Findings & Lessons Learned

### 1. Legacy Inheritance (v17b)
We confirmed that the majority of reversed flows were **inherited from v17b**.
*   *Evidence:* In reach `49207000256` (v17b), Flow Accumulation (`fACC`) decreases downstream. This proves the error existed in the source topology, but was "silent" because v17b didn't calculate mainstem path variables like `hydro_dist_out`.

### 2. The "Math Jitter" Myth (N006)
We initially suspected 158,000 missing links.
*   *Reality:* The errors were actually ~1.6km "jitter" caused by a conceptual misalignment: node `dist_out` was defined at the upstream end, while the lint check compared it to the downstream reach's upstream end.
*   *Fix:* We implemented a global recalculation using **Interpolated Node Distances**. N006 violations are now 0 on the baseline topology.

### 3. The Fragmentation Trap (The Revert)
Experimental flips of 1,112 reaches proved that **Local Confidence != Global Connectivity**.
*   *Failure Mode:* A reach can have a perfect "FACC Snap" when flipped, but if the *new* downstream path doesn't eventually reconnect to the ocean, the entire upstream basin (thousands of reaches) becomes a "sink" and loses its distance attributes.
*   *Status:* All flips were **reverted** to the v17b baseline to restore network integrity.

## Challenge for the Next Agent: "The Global Path Verifier"
To fix the topology without breaking the network, the next agent must implement a solver that:
1.  **Simulates a flip.**
2.  **Traces the NEW downstream path** all the way to a known `best_outlet`.
3.  **Validates the Re-entry:** Ensures the flipped segment "re-enters" the main network at a junction where the drainage areas and elevations align.
4.  **Batch Flips:** Handles entire 30+ reach chains as a single atomic unit.

---
*Last Updated: 2026-03-01*
*Current Database State: v17b Baseline Topology (Restored)*
