# SWORD v17c Release Notes Audit — May 2026

**Auditor:** Firepass (autonomous audit agent)
**Date:** 2026-05-04
**Scope:** `docs/v17c_release_notes.md` — all rederive / node-edit / coordinate-change claims for versions 0.0.8 through 0.0.11
**Method:** Read-only DuckDB queries against `data/duckdb/sword_v17c.duckdb` and `netCDF4` comparison against v17b NetCDF files.

---

## Executive Summary

| Finding | Severity | Status |
|---------|----------|--------|
| 0.0.10 reach count understated (293 → 303); region counts wrong | **Factual** | Fixed in release notes |
| Reach 35301100891 description omits geographic rederivation | **Factual** | Fixed in release notes |
| "334 total rederived" framing hides 344 coordinate-changed reaches | **Factual** | Fixed + Known Limitations added |
| 10 "unfixable" reaches mischaracterized as pristine v17b | **Factual** | Fixed in release notes |
| 0.0.11 descending dist_out count off by 1 (18,552 → 18,553) | **Factual** | Fixed in release notes |
| N013 reach IDs in ops log use node_id, not reach_id | **Cosmetic** | Documented in audit |
| 0.0.8 region counts verified correct | — | Confirmed |
| POM example reach 13341000591 gap verified (223 m) | — | Confirmed |
| 0.0.11 lakeflag count verified (11,112,454) | — | Confirmed |

---

## Discrepancy 1: 0.0.10 Reach Count and Region Breakdown

**Original text (lines 27–33):**

> Node geolocation fixed on **293** additional reaches. … By region: AS:183, SA:35, AF:27, EU:19, OC:18, NA:6. Combined with the 41 reaches fixed in 0.0.8, total rederived: 334 reaches (0.13%). 10 reaches remain unfixable.

**Evidence:**

```sql
SELECT region, COUNT(DISTINCT entity_ids[1]) as n_reaches
FROM sword_operations
WHERE reason LIKE '%scrambled%'
GROUP BY region ORDER BY region;
```

| region | n_reaches |
|--------|-----------|
| AF     | 29        |
| AS     | 189       |
| EU     | 21        |
| NA     | 7         |
| OC     | 19        |
| SA     | 38        |
| **Total** | **303** |

All 303 operations have `status = 'COMPLETED'`. Zero `FAILED` or `IN_PROGRESS` scrambled operations exist.

**Root cause:** The original 293 figure appears to have been a pre-run estimate or a count of *successful* rederivations before the final batch completed. The DB records 303 completed `Fix scrambled node geolocation (0.0.10)` operations.

**Corrected text:**

> Node geolocation fixed on **303** additional reaches. … By region: AS:189, SA:38, AF:29, EU:21, OC:19, NA:7. Combined with the 41 reaches fixed in 0.0.8, total rederived: **344** reaches (0.14%). **10 reaches were fixed but still trigger the scrambled-node detector** (centerline geometry issues inherited from v17b); see Known Limitations.

**Severity:** Factual — understates the scope of 0.0.10 changes by 10 reaches.

---

## Discrepancy 2: Reach 35301100891 — Omitted Geographic Rederivation

**Original text (lines 34–38):**

> Reach 35301100891 node_order rotation fixed. This AS reach had node_ids rotated by one position … `node_order` now matches `node_id` ascending … Node dist_out and all interpolated distance columns recalculated.

**Evidence:**

```python
# v17b vs v17c comparison
>100m: 74/75 nodes moved >100m, max: 12.687 km
```

Operation 850 in `sword_operations` is logged as `Fix scrambled node geolocation (0.0.10)` for this reach (entity_ids starts with node 35301100890011).

**Root cause:** The release notes describe only the node_order rotation, but the reach was ALSO rederived geographically (74 of 75 nodes moved >100 m, max ~12.6 km). Both operations are present in the ops log.

**Corrected text:**

> **Reach 35301100891 node_order rotation and geolocation fixed.** This AS reach had node_ids rotated by one position (2, 3, …, 75, 1 instead of 1, 2, …, 75) since v17b. Additionally, 74 of 75 nodes were rederived from the centerline (max shift ~12.6 km on node 35301100890011) due to scrambled geolocation detected by POM test 6b. `node_order` now matches `node_id` ascending, `dn_node_id` = 0011, `up_node_id` = 0751. Node dist_out and all interpolated distance columns recalculated to match the corrected node_order.

**Severity:** Factual — omits a major coordinate change that breaks D0↔D2 continuity.

---

## Discrepancy 3: "334 Total Rederived" Framing

**Original text (line 31–33):**

> Combined with the 41 reaches fixed in 0.0.8, total rederived: 334 reaches (0.13%). 10 reaches remain unfixable.

**Evidence:**

Full v17b → v17c node coordinate comparison across all 6 regions:

| Region | Reaches with nodes moved >100 m | Nodes moved >100 m |
|--------|-----------------------------------|---------------------|
| AF     | 37                                | 758                 |
| AS     | 209                               | 6,438               |
| EU     | 26                                | 663                 |
| NA     | 7                                 | 292                 |
| OC     | 22                                | 275                 |
| SA     | 43                                | 711                 |
| **Total** | **344**                        | **9,137**           |

The 344 includes:
- 41 from 0.0.8 (POM node geolocation)
- 303 from 0.0.10 (scrambled node geolocation)
- The 10 "unfixable" reaches (which were also processed in 0.0.10)

**Root cause:** The 334 figure (41 + 293) excludes the 10 unfixable reaches that were ALSO touched by 0.0.10 scrambled ops. POM's count of 344 is the correct total of reaches with ANY node coordinate change vs v17b.

**Corrected text:** See release notes — the 0.0.10 section now states 344 total, and a new Known Limitations entry documents the 344 / 9,137 figures explicitly.

**Severity:** Factual — the 334 framing implies the 10 unfixable are separate from the rederived set, when they are actually part of it.

---

## Discrepancy 4: 10 "Unfixable" Reaches Mischaracterized

**Original text (line 32–33):**

> 10 reaches remain unfixable (centerline geometry issues inherited from v17b).

**Evidence:**

The 10 currently-detected scrambled reaches (still triggering the detector in v17c):

| reach_id     | region | n_nodes | max gap (km) | Also N013? | Scrambled op? |
|--------------|--------|---------|--------------|------------|---------------|
| 83242100011  | NA     | 70      | 6.55         | No         | Yes (0.0.10)  |
| 28106600011  | EU     | 87      | 1.35         | No         | Yes (0.0.10)  |
| 14278900061  | AF     | 58      | 7.40         | Yes        | Yes (0.0.10)  |
| 31241401301  | AS     | 67      | 6.41         | Yes        | Yes (0.0.10)  |
| 34100005185  | AS     | 42      | 0.79         | Yes        | Yes (0.0.10)  |
| 35416100503  | AS     | 12      | 1.11         | No         | Yes (0.0.10)  |
| 35444000035  | AS     | 46      | 0.64         | No         | Yes (0.0.10)  |
| 43462001171  | AS     | 12      | 1.11         | No         | Yes (0.0.10)  |
| 45362000031  | AS     | 23      | 1.66         | No         | Yes (0.0.10)  |
| 48294000081  | AS     | 72      | 5.57         | Yes        | Yes (0.0.10)  |

All 10 have `Fix scrambled node geolocation (0.0.10)` operations with `status = 'COMPLETED'`. They were NOT left pristine — the rederive was attempted but the centerline geometry is so degraded that the fix did not resolve the detector criteria (gap > 3× median spacing and > 0.4 km).

Additionally, 2 N013 reaches (45570000125, 42211000503) were fixed by both N013 and scrambled ops and do NOT currently trigger the detector — they are NOT "unfixable."

**Root cause:** "Unfixable" implies the reaches were skipped or untouched. In reality, all 10 were processed but the underlying v17b centerline geometry is too degraded to fully resolve.

**Corrected text:**

> **10 reaches fixed but still trigger scrambled-node detector.** These reaches were processed by `rederive_nodes` but retain centerline geometry issues inherited from v17b that exceed the POM test 6b threshold: 83242100011 (NA), 28106600011 (EU), 14278900061 (AF), 31241401301, 34100005185, 35416100503, 35444000035, 43462001171, 45362000031, 48294000081 (AS). Four of these also received N013 closure-bug fixes. They are included in the 303 operation count and in the 344 coordinate-changed total.

**Severity:** Factual — mischaracterizes the state of the 10 reaches.

---

## Discrepancy 5: 0.0.11 Descending dist_out Count Off by 1

**Original text (line 11–12):**

> About 8% of reaches (18,552 globally) had nodes stored in descending dist_out order

**Evidence:**

v17b NetCDF analysis (all 6 regions):

| Region | Descending reaches |
|--------|-------------------|
| AF     | 1,846             |
| AS     | 6,946             |
| EU     | 2,272             |
| NA     | 3,630             |
| OC     | 1,315             |
| SA     | 2,544             |
| **Total** | **18,553**     |

v17c has 0 descending reaches (verified by DB query).

**Root cause:** Likely a rounding or off-by-one error in the original count. The NetCDF files contain 18,553 reaches with strictly decreasing dist_out in storage order.

**Corrected text:**

> About 7.5% of reaches (**18,553** globally) had nodes stored in descending dist_out order

**Severity:** Factual — minor, but precision matters for POM validation.

---

## Discrepancy 6: N013 Reach IDs in Ops Log Use Node IDs

**Original text (lines 50–51):**

> Reaches 14278900061 (AF), 31241401301, 48294000081, 45570000125, 34100005185, 42211000503 (AS)

**Evidence:**

The `sword_operations` table stores `entity_ids` as arrays of **node_ids**, not reach_ids. For the N013 operations:

- `14278900060011` (first node of reach 14278900061)
- `31241401300011` (first node of reach 31241401301)
- etc.

This is an internal implementation detail — the ops log correctly records the affected entities (nodes). The release notes correctly list reach IDs for human readability.

**Severity:** Cosmetic — no action needed in release notes, documented here for audit completeness.

---

## Verified Claims (No Issues Found)

### 0.0.8 Region Counts

```sql
SELECT region, COUNT(DISTINCT entity_ids[1]) as n_reaches
FROM sword_operations
WHERE reason LIKE '%0.0.8%' AND reason LIKE '%POM node geolocation%'
GROUP BY region ORDER BY region;
```

| region | n_reaches |
|--------|-----------|
| AF     | 8         |
| AS     | 20        |
| EU     | 5         |
| OC     | 3         |
| SA     | 5         |
| **Total** | **41** |

✅ Matches release notes: SA:5, EU:5, AF:8, AS:20, OC:3.

### POM Example Reach 13341000591

v17c max consecutive gap: **223 m** (between nodes 13341000590721 and 13341000590731, node_orders 72–73).
✅ Matches release notes claim: "drops from 13.9 km to 223 m."

### 0.0.11 Lakeflag Propagation

- Total nodes: **11,112,454**
- Nodes with non-NULL lakeflag: **11,112,454** (100%)
- Nodes with lakeflag mismatch vs parent reach: **0**

✅ Matches release notes claim.

### N013 Reaches Existence

All 6 reaches exist in the DB and have operations:
- 14278900061 (AF): 2 ops (N013 + scrambled)
- 31241401301 (AS): 2 ops (N013 + scrambled)
- 48294000081 (AS): 2 ops (N013 + scrambled)
- 45570000125 (AS): 2 ops (N013 + scrambled)
- 34100005185 (AS): 2 ops (N013 + scrambled)
- 42211000503 (AS): 2 ops (N013 + scrambled)

✅ All 6 exist and were processed. All 6 are inside the 303 scrambled bucket.

---

## New Known Limitations Entry

Added to release notes Section 4:

> **Node coordinate changes vs v17b (breaking change):** 344 reaches (0.14%) have at least one node shifted >100 m relative to v17b NetCDF, affecting 9,137 nodes. This breaks SWOT D0↔D2 time-series continuity at affected nodes. The 344 comprises 41 reaches fixed in 0.0.8 + 303 fixed in 0.0.10 (the latter includes 10 reaches still triggering the scrambled-node detector). A correspondence table mapping v17b node positions to v17c node positions is provided separately (`data/exports/v17c_beta/node_correspondence/`).

---

## Data Integrity Check

No evidence of database corruption was found. All operations have consistent status, entity IDs resolve to valid nodes/reaches, and the v17c data passes internal consistency checks (zero N008/G002/G003 violations per release notes). The discrepancies are confined to documentation accuracy, not data integrity.

### Sub-threshold finding: OC reach 51111300061

A full v17b NetCDF ↔ v17c DuckDB scan (independent of the ops log) identifies one additional reach with node coordinate changes beyond the 344 in the correspondence table:

- **OC reach 51111300061**: 14 of 99 nodes have x and/or y differences vs v17b, max distance moved 7.2 m. No `sword_operations` entry for these node_ids. `reach_length` and `node_length` are unchanged within floating-point roundoff (max diff ~10⁻⁹ m). Changes appear to be meter-scale, sub-threshold coordinate drift likely from OC rebuild/serialization stages; no evidence of a logged coordinate update operation.

This reach is correctly excluded from the correspondence table (POM's threshold is >100 m; max move here is 7.2 m). The precise defensible wording for POM is: *"344 reaches have at least one node shifted >100 m vs v17b NetCDF."* This is the language now used in release notes Section 4.

---

## Files Changed

1. `docs/v17c_release_notes.md` — corrected factual errors in 0.0.8, 0.0.10, 0.0.11 sections; added Known Limitations entry for coordinate changes.
2. `docs/technical/release_notes_audit_2026-05.md` — this file.
