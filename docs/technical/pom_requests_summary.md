# POM (Pierre-Olivier Malaterre) Requests — Implementation Summary

> **Living document.** Update this when POM-related checks, columns, or issues change.
> Location: `docs/technical/pom_requests_summary.md`

## Background

Pierre-Olivier Malaterre (POM), INRAE, provided SWORD with [`sword_validity.m`](../../src/_legacy/sword_validity.m) — a MATLAB validation script containing 15 test suites (with sub-tests) that check topology, node consistency, ID format, river naming, and type distribution. POM also requested two new columns (`dn_node_id`/`up_node_id` on reaches, `node_order` on nodes) needed because v17c flow direction changes mean node IDs can be decreasing within a reach.

This document maps POM's original MATLAB tests to our Python lint framework and tracks implementation status.

## New Columns ([Issue #149](https://github.com/SWORD-Global/SWORD/issues/149))

| Column | Table | Type | Description |
|--------|-------|------|-------------|
| `dn_node_id` | reaches | BIGINT | Downstream boundary node ID |
| `up_node_id` | reaches | BIGINT | Upstream boundary node ID |
| `node_order` | nodes | INTEGER | 1-based position within reach (1=downstream, n=upstream by dist_out) |

**Status:** Implemented and deployed ([PR #165](https://github.com/SWORD-Global/SWORD/pull/165)). Verified on production v17c (248,674 reaches, 11.1M nodes).

**Source:** POM emails Feb 3 ("please also include `nodes_ids`") + Feb 4 ("Filled with an integer from 1 to n, 1 being for the first downstream, and n for the last upstream"). Already present in v17c NetCDF via [`sword_read.m:83-86`](../../src/_legacy/sword_read.m).

## POM Test → Lint Check Mapping

### Reach Connectivity (Tests 1–5)

| POM Test | What it checks | Our Lint | Status |
|----------|---------------|----------|--------|
| 1a | Duplicate upstream neighbors | T005 (pre-existing) | Implemented |
| 1b | n_rch_up matches actual count | T005 (pre-existing) | Implemented |
| 1c | Upstream neighbor ID = 0 (invalid) | T012 (pre-existing) | Implemented |
| 2a | Duplicate downstream neighbors | T005 (pre-existing) | Implemented |
| 2b | n_rch_down matches actual count | T005 (pre-existing) | Implemented |
| 2c | Downstream neighbor ID = 0 (invalid) | T012 (pre-existing) | Implemented |
| 3a–3b | Upstream neighbor exists + reciprocity | T007, T012 (pre-existing) | Implemented |
| 3c | Self-referencing upstream | **T013** (new) | Implemented |
| 3d | Same reach in both up AND down | **T014** (new) | Implemented |
| 3e | Suspicious but possible upstream links | Not implemented | Low priority (opt_warning_3e=0 in POM's defaults) |
| 4a–4b | Downstream neighbor exists + reciprocity | T007, T012 (pre-existing) | Implemented |
| 4c | Self-referencing downstream | **T013** (new) | Implemented |
| 4d | Same reach in both up AND down | **T014** (new) | Implemented |
| 4e | Suspicious but possible downstream links | Not implemented | Low priority (opt_warning_4e=0 in POM's defaults) |
| 5a | Orphan reaches (no neighbors at all) | T004 (pre-existing) | Implemented |
| 5b | Shortcut connections (A→B→C and A→C) | **T015** (new) | Implemented |

### Reach Distance (Tests 6–7)

| POM Test | What it checks | Our Lint | Status |
|----------|---------------|----------|--------|
| 6a | Connected reach centroid distance too far | G012 (pre-existing, 500m endpoint threshold) | Subsumed — T016 dropped as redundant |
| 6b | Adjacent node spacing >400m | **N003** (new) | Implemented |
| 7a | dist_out not increasing upstream (reaches) | T001 (pre-existing) | Implemented |
| 7b | dist_out excessive jump between reaches | **T017** (new, 30km threshold) | Implemented |

### Node Allocation & Ordering (Tests 8–10)

| POM Test | What it checks | Our Lint | Status |
|----------|---------------|----------|--------|
| 8a | First node index < last node index within reach | Implicit in node_order computation | Covered by column logic |
| 8b | Node count matches n_nodes | **N008** (new) | Implemented |
| 9a | Node geolocation within parent reach geometry | **N012** (new, 500m threshold) | Implemented ([#185](https://github.com/SWORD-Global/SWORD/issues/185)) |
| 9b | Node indexes contiguous within reach | **N010** (new) | Implemented |
| 9c | Centerline points allocated to correct reach | Not implemented | Unnecessary — CL points define reach geometry (distance always 0) |
| 9d | Centerline points allocated to correct node | **N013** (new, 500m threshold) | Implemented ([#186](https://github.com/SWORD-Global/SWORD/issues/186)) |
| 10a | Node dist_out increasing with within-reach downstream→upstream order | **N004** (new; by `node_order`) | Implemented |
| 10b | Node dist_out jump >600m | **N005** (new) | Implemented |
| 10c | Boundary node dist_out continuity across reaches | **N006** (new, 1000m threshold) | Implemented |
| 10d | Boundary node geolocation across reaches | **N007** (new, 400m threshold) | Implemented |

### Node Reversal & Geolocation (Test 11)

| POM Test | What it checks | Our Lint | Status |
|----------|---------------|----------|--------|
| 11a–b | Boundary node geolocation (up/down) | **N007** (new) | Implemented (combined up+down) |
| 11c–d | Tributary entering inside a reach (mitigating 11a–b) | Not separate check | Informational in POM's script |

### ID Format (Test 12)

| POM Test | What it checks | Our Lint | Status |
|----------|---------------|----------|--------|
| 12a | Node order coherent with node ID | Superseded by `node_order` column | Informational only after flow corrections |
| 12b | Reach ID = 11 digits, valid type suffix | **T018** (new) | Implemented |
| 12c | Node ID = 14 digits, matches parent reach | **T018** (new) | Implemented |

### Type Consistency (Test 13)

| POM Test | What it checks | Our Lint | Status |
|----------|---------------|----------|--------|
| 13 | Up/down reaches of a set should not be type 1 (river) when set is non-river | C004 (pre-existing, lakeflag/type cross-tab) | Partially covered |

### River Name (Test 14)

| POM Test | What it checks | Our Lint | Status |
|----------|---------------|----------|--------|
| 14a | river_name = 'NODATA' coverage | **T019** (new) | Implemented |
| 14b | river_name disagrees with all neighbors | **T020** (new) | Implemented |

### SWOT & Type Distribution (Test 15)

| POM Test | What it checks | Our Lint | Status |
|----------|---------------|----------|--------|
| 15a | SWOT observation coverage (reaches unseen) | FL001 (pre-existing) | Implemented |
| 15b–g | Type distribution (reaches) | C003 (pre-existing) | Implemented |
| 15b_n–g_n | Type distribution (nodes) | Not separate check | Node-level type not tracked |
| 15b_b–g_b | Reach length vs node length by type | G002 (pre-existing) | Implemented |

### WSE Monotonicity (from [`sword_validity.m`](../../src/_legacy/sword_validity.m) line 433)

| POM Test | What it checks | Our Lint | Status |
|----------|---------------|----------|--------|
| WSE downstream | WSE should decrease downstream | **A030** (new) | Implemented |

## Summary Statistics

| Category | Total POM sub-tests | Covered by pre-existing lint | New lint checks added | Not implemented |
|----------|-------------------|-----------------------------|-----------------------|-----------------|
| Connectivity (1–5) | 15 | 9 | 4 (T013, T014, T015, T016→dropped) | 2 (3e, 4e — disabled in POM defaults) |
| Distance (6–7) | 4 | 1 | 2 (N003, T017) | 1 (6a subsumed by G012) |
| Node allocation (8–10) | 10 | 0 | 8 (N004–N008, N010, N012, N013) | 2 (8a implicit via N004, 9c unnecessary) |
| Node boundary (11) | 4 | 0 | 1 (N007 combined) | 3 (11c/d informational) |
| ID format (12) | 3 | 0 | 1 (T018 combined) | 2 (12a covered by N004) |
| Type (13) | 1 | 1 | 0 | 0 |
| River name (14) | 2 | 0 | 2 (T019, T020) | 0 |
| SWOT/type dist (15) | ~20 | 3 | 0 | ~17 (informational distributions) |
| WSE | 1 | 0 | 1 (A030) | 0 |
| **Totals** | **~60** | **14** | **20 new checks** | **~27 (mostly informational/low-priority)** |

## New Lint Checks Added for POM

### Topology (T-series)

| Check | Severity | Description | POM Test |
|-------|----------|-------------|----------|
| T013 | ERROR | Self-referencing topology (reach lists itself as neighbor) | 3c/4c |
| T014 | ERROR | Bidirectional neighbor (same reach in both up and down) | 3d/4d |
| T015 | INFO | Shortcut edges (A→B→C and A→C) | 5b |
| T017 | WARNING | dist_out jump >30km between connected reaches | 7b |
| T018 | ERROR | Reach/node ID format (11-digit reach, 14-digit node) | 12b/12c |
| T019 | INFO | river_name = 'NODATA' coverage | 14a |
| T020 | INFO | river_name disagrees with neighbor consensus | 14b |
| T022 | ERROR | Connected reach centroids >50km apart (cross-basin false merge) | N006 spatial validation |

### Node (N-series)

| Check | Severity | Description | POM Test |
|-------|----------|-------------|----------|
| N003 | WARNING | Adjacent node spacing >400m within reach | 6b |
| N004 | WARNING | Node dist_out not increasing with node_order | 10a |
| N005 | WARNING | Node dist_out jump >600m between adjacent nodes | 10b |
| N006 | WARNING | Boundary node dist_out mismatch >1000m across reaches | 10c |
| N007 | WARNING | Boundary node geolocation >400m across reaches | 10d/11a-d |
| N008 | ERROR | Actual node count != reaches.n_nodes | 8b |
| N010 | INFO | Node indexes not contiguous within reach | 9b |
| N012 | WARNING | Node (x,y) >500m from parent reach geometry | 9a |
| N013 | WARNING | Centerline point >500m from assigned node | 9d |

### Attributes (A-series)

| Check | Severity | Description | POM Test |
|-------|----------|-------------|----------|
| A030 | WARNING | WSE increases downstream (should decrease) | line 433 |

## Current Release-Gate Baseline (v17c, 2026-04-09)

Checks run against `sword_v17c.duckdb` after resyncing `node_order`,
`dn_node_id`, and `up_node_id` from node `dist_out`:
248,673 reaches, 11.1M nodes, 66.9M centerlines.

### Must Pass (all currently passing)

| Check | Total checked | Notes |
|-------|--------------|-------|
| T001 | 103,864 | Reach-level `dist_out_dijkstra` monotonicity clean |
| T004 | 248,673 | No orphan reaches |
| T005 | 248,673 | Neighbor counts match topology |
| T007 | 495,620 | Full topology reciprocity |
| T012 | 495,620 | No dangling topology references |
| T013 | 248,673 | No self-referencing topology |
| T014 | 248,673 | No bidirectional paradoxes |
| T015 | 244,902 | No shortcut edges |
| T018 | 11,361,127 | All reach/node IDs well-formed |
| T022 | 247,810 | No cross-basin false merges |
| G002 | 248,673 | Node-length sums consistent with reach length |
| N004 | 11,112,454 | `dist_out` now increases with `node_order` after metadata resync |
| N008 | 248,673 | Node counts match `n_nodes` |
| N010 | 248,673 | Node indexes contiguous |

### Nonblocking Findings with Recorded Disposition

| Check | Violations | Sev | Disposition | Status summary |
|-------|-----------|-----|-------------|----------------|
| A030 | 658 | WARN | Defended nonblocking | MERIT DEM `wse`, not SWOT; remaining inversions are source-noise limited and not a release blocker ([#195](https://github.com/SWORD-Global/SWORD/issues/195)). |
| G012 | 22 | INFO | Deferred to v18 | Same inherited endpoint-gap family as N007; geometry fixes remain a v18 task. |
| N003 | 3,031 | WARN | Deferred to v18 | v17b source node spacing, 0.03% of nodes. The 0.0.12 coordinate-continuity revert restores inherited spacing residuals; projection-based `node_order` repair reduces this from the post-revert 3,456 count but does not move coordinates ([#193](https://github.com/SWORD-Global/SWORD/issues/193)). |
| N005 | 151 | WARN | Deferred to v18 | Large within-reach `dist_out` jumps are sparse-node source-data cases in the same family as N003. The 0.0.12 `node_order` repair now recomputes midpoint distances across all restored-coordinate reaches; remaining violations are inherited sparse-node spacing cases. |
| N006 | 2,633 | WARN | Defended nonblocking | Boundary `dist_out` gaps are expected on bifurcation-rejoin structures; single-scalar `dist_out` cannot stay continuous on all edges ([#192](https://github.com/SWORD-Global/SWORD/issues/192)). |
| N007 | 25 | WARN | Deferred to v18 | Remaining boundary geometry gaps are inherited geometry/topology cases after the N007 measurement fixes ([#188](https://github.com/SWORD-Global/SWORD/issues/188), [#190](https://github.com/SWORD-Global/SWORD/issues/190)). |
| N012 | 13 | WARN | Accepted residual | Sparse ghost/Arctic node-geolocation outliers accepted as residual ([#185](https://github.com/SWORD-Global/SWORD/issues/185)). |
| N013 | 26,744 | WARN | Accepted residual | v17c-0.0.12 restores v17b node geometry for D0-D2 continuity; these centerline-node offsets are inherited from the v17b baseline ([#194](https://github.com/SWORD-Global/SWORD/issues/194)). |
| T017 | 553 | WARN | Defended nonblocking | Same braided-network path-length artifact as N006, at the reach scale ([#191](https://github.com/SWORD-Global/SWORD/issues/191)). Reduced from 701 after fully restoring reach dist_out to v17b (0.0.9). |
| T020 | 197 | INFO | Informational | GRWL river-name inconsistencies only; not a release blocker ([#196](https://github.com/SWORD-Global/SWORD/issues/196)). |
| T023 | 0 | ERROR | Must-pass (added 0.0.8) | end_reach outlet consistency: outlet reaches must have end_reach=2. |

### Informational (tracked, not gate failures)

| Check | Value | Notes |
|-------|-------|-------|
| C003 | 23,012 | Unreliable reaches in type distribution summary |
| C004 | 0 | Resolved in 0.0.10. All 248,673 reaches now have consistent lakeflag/type (down from 6,166 mismatches). 1,196 provisionally defaulted to river and tagged `clf_provisional_river`. |
| FL001 | 8,457 | Reaches without SWOT observations |
| T019 | 127,406 (51.2%) | Reaches with `river_name='NODATA'`; source-data limitation |

### May 29, 2026 SWOT slope observation correction

POM's review of AS reach 35301100891 showed that node order, `node_order`,
`dn_node_id`, and `up_node_id` were correct in 0.0.12, but `slope_obs_p50`
still carried a stale negative signed reach-level RiverSP slope. Operation
1001 replaces `reaches.slope_obs_*` with pass-level slopes fit from RiverSP
node `wse_sm` against current beta12 `nodes.dist_out`; reach-level values are
then percentiles across those pass slopes. The stored SWORD DEM/MERIT
`reaches.slope` remains unchanged and positive for the canonical reach
(`15.121399` m/km).

POM's May 29 CSV adds a newer Test 16 slope-sign suite that is not present in
the checked-in `src/_legacy/sword_validity.m` file. Local parity diagnostics
are archived at
`outputs/pom_test16_slope_parity_20260529.json`.
The key interpretation is:

- Test 16a's 221 SRTM/static-slope warnings match exactly the 221
  restored-coordinate reaches whose node sequence was intentionally repaired.
  This is expected because `reaches.slope` is a nonnegative SRTM/MERIT slope
  magnitude in m/km, not a signed value to flip.
- Those 221 repaired reaches are not a retained topology-flip set. Historical
  flow-correction topology flips were fully reverted before 0.0.12 and
  `v17c_flow_corrections` is empty in the current database. The 221 are the
  `node_order` repair subset from operation 999: 3,725 nodes across first-digit
  POM regions 1-6 only (36, 19, 56, 52, 20, 38 reaches respectively). The
  related boundary-node IDs changed on 45 reaches.
- Test 16b is the row directly affected by operation 1001. Negative
  `slope_obs_p50` medians decrease from 18,805 to 9,988.
- Test 16c/16d endpoint-WSE diagnostics remain warning-level checks. Endpoint
  WSE differences are noisy/static and are not equivalent to the pass-level
  RiverSP node-WSE slope fit now stored in `slope_obs_*`.
- POM-region diagnostics in first-digit regions 7-9 belong to the broader
  slope observation or endpoint-WSE warning populations, not to the 221
  operation-999 `node_order` repair reaches.

After this correction, external checkers and downstream software should not
flip either `reaches.slope` or `reaches.slope_obs_*` based on a reversed-node
or changed-node-order reach list. `node_order` defines the downstream-to-
upstream node sequence; slope variables are already expressed in that delivered
SWORD convention. Unit convention remains mixed by source: `slope` is m/km,
while `slope_obs_*` is m/m.

Post-apply validation:

| Metric | Before | After |
|--------|--------|-------|
| Reaches with `slope_obs_p50` | 175,155 | 173,732 |
| Reaches with negative `slope_obs_p50` | 18,805 | 9,988 |
| Global median `slope_obs_p50` | — | 0.0002740105 m/m |
| Reach 35301100891 `slope_obs_p50` | -0.0136548684 m/m | +0.0142687580 m/m |

By-region post-apply `slope_obs_p50` coverage and negative median counts:

| Region | Reaches with `slope_obs_p50` | Negative medians | Median `slope_obs_p50` |
|--------|------------------------------|------------------|------------------------|
| AF | 16,609 | 669 | 0.000248 |
| AS | 70,821 | 4,121 | 0.000456 |
| EU | 19,433 | 1,448 | 0.000201 |
| NA | 24,045 | 1,419 | 0.000417 |
| OC | 10,773 | 841 | 0.000160 |
| SA | 32,051 | 1,490 | 0.000139 |

### May 30, 2026 MERIT/SRTM WSE resampling decision

POM's follow-up RiverObs check found that static `nodes.wse` from
SRTM/MERIT is used downstream, at least for outlier detection and possibly
uncertainty estimation. Therefore any correction to static `nodes.wse`,
`reaches.wse`, or `reaches.slope` is a science-impacting data rebuild, not a
minor export repair.

Decision for v17c beta 0.0.12: keep the delivered static MERIT/SRTM fields
unchanged from v17b. Do not produce a 0.0.12c that resamples MERIT/SRTM WSE.
The current release candidate remains the 0.0.12b/0.0.12 data state that fixes
file ordering, node coordinate continuity, `node_order`, `dn_node_id`,
`up_node_id`, and `slope_obs_*`, while preserving legacy static DEM-derived
fields.

The key example is AF reach 11447000031. In v17b, using downstream/upstream
by `dist_out`, node 11447000030011 has `wse=361.0` m and node
11447000030501 has `wse=185.8` m. That endpoint-WSE inversion is inherited
from v17b. Corrected 0.0.12 ordering can make this inherited problem visible
to POM's signed endpoint-WSE check, but the static WSE values and static
`reaches.slope` were not changed by 0.0.12.

Read-only diagnostics showed why this should be deferred:

- Direct MERIT `elv` sampling at current 0.0.12 centerline coordinates fixes
  reach 11447000031: the downstream node median would be about 183.2 m, the
  endpoint WSE slope becomes positive, and the centerline-derived static slope
  diagnostic drops from `8.037` m/km to about `0.300` m/km.
- The same direct-cell diagnostic is mixed globally. Among 4,727 Test
  16f-style static slope / endpoint-WSE sign-mismatch candidates, 3,899 had
  valid endpoint samples; 1,719 became fixed/flat, 2,180 still mismatched,
  and 828 lacked endpoint coverage.
- More importantly, direct-cell sampling is not faithful to the legacy SWORD
  attachment method. Legacy SWORD attached MERIT attributes by reading MERIT
  `elv`, `wth`, and `upa`, filtering MERIT pixels to `upa >= 10 km2`, and
  assigning centerline points to the nearest remaining MERIT river pixel
  before aggregating. In the diagnostic candidate set, only about 19-29% of
  direct sampled centerline points fell on cells with `upa >= 10`, depending
  on region. A proper correction therefore requires reproducing that
  UPA-filtered nearest-pixel workflow, not simple node or centerline raster
  lookup.

The final v17b NetCDF files do not retain centerline `p_height`; they expose
centerline `x`, `y`, `reach_id`, and `node_id` only. Recomputing static
MERIT/SRTM WSE and slope would require reattaching external MERIT rasters,
then recomputing node medians/variances, reach medians/variances, and static
reach slopes using the legacy convention. That work is deferred to a
post-release/v18 investigation.

Wording rule: it is safe to say the issue is present in v17b. Do not claim it
was introduced in v17b unless v16 is checked. The defensible explanation is
that this is an inherited static MERIT/SRTM WSE artifact that 0.0.12 ordering
can expose, while the values themselves remain unchanged from v17b.

## Not Implemented (with rationale)

| POM Test | Why skipped |
|----------|-------------|
| 3e/4e | Disabled in POM's own defaults (`opt_warning_3e=0`, `opt_warning_4e=0`) — suspicious but possible |
| 8a | Implicit in node_order column computation — covered by N004 |
| 9c | Centerline-to-reach allocation — unnecessary (CL points define reach geometry, distance always 0) |
| 11c/11d | Informational (tributary mitigation) — not an error condition |
| 15b_n–g_n | Node-level type distribution — node type derived from reach type, redundant |
| T016 ([#152](https://github.com/SWORD-Global/SWORD/issues/152)) | Closed — subsumed by pre-existing G012 (endpoint alignment at 500m) |

## GitHub Issues

### Implementation (closed)

| Issue | Title | Status |
|-------|-------|--------|
| [#149](https://github.com/SWORD-Global/SWORD/issues/149) | Add nodes_ids and node_order columns (POM request) | Closed (PR [#165](https://github.com/SWORD-Global/SWORD/pull/165)) |
| [#150](https://github.com/SWORD-Global/SWORD/issues/150) | Lint T013/T014: self-referencing and bidirectional topology | Closed |
| [#151](https://github.com/SWORD-Global/SWORD/issues/151) | Lint T015: redundant shortcut connections | Closed |
| [#152](https://github.com/SWORD-Global/SWORD/issues/152) | Lint T016: connected reach centroid distance >30km | Closed (subsumed by G012) |
| [#153](https://github.com/SWORD-Global/SWORD/issues/153) | Lint T017: dist_out excessive jump between neighbors | Closed |
| [#154](https://github.com/SWORD-Global/SWORD/issues/154) | Lint T018: reach and node ID format validation | Closed |
| [#155](https://github.com/SWORD-Global/SWORD/issues/155) | Lint T019/T020: river name validation | Closed |
| [#156](https://github.com/SWORD-Global/SWORD/issues/156) | Lint A030: WSE monotonicity downstream | Closed |
| [#157](https://github.com/SWORD-Global/SWORD/issues/157) | Node-level lint: dist_out, spacing, and boundary checks (N003-N007) | Closed |
| [#158](https://github.com/SWORD-Global/SWORD/issues/158) | Node/centerline allocation validation (POM Tests 8/9) | Closed |
| [#185](https://github.com/SWORD-Global/SWORD/issues/185) | Lint N012: node geolocation outside parent reach geometry (POM Test 9a) | Closed (12 violations, all ghost/Arctic — accepted) |
| [#186](https://github.com/SWORD-Global/SWORD/issues/186) | Lint N013: centerline point too far from assigned node (POM Test 9d) | Closed |

### Investigation (diagnose first, fix only after discussing with Jake)

| Issue | Check | Title | Priority |
|-------|-------|-------|----------|
| [#187](https://github.com/SWORD-Global/SWORD/issues/187) | N007/G012 | DuckDB reach geometries missing endpoint overlap vertices | **Closed** — premise incorrect; DuckDB and PG have identical endpoint connectivity (37,976/38,346 touching in NA) |
| [#188](https://github.com/SWORD-Global/SWORD/issues/188) | N007 | Fix N007 distance formula: antimeridian wrapping + check all 4 boundary pairs | **Closed** — fixed in `8b7ca76` (ST_Distance_Spheroid + all 4 endpoint combos) |
| [#189](https://github.com/SWORD-Global/SWORD/issues/189) | N007 | Investigate 31 extreme bad topology links (>5km boundary gap) | **Closed** — 4 v17c-added links reverted, 27 v17b-inherited confirmed (GRWL gaps) |
| [#190](https://github.com/SWORD-Global/SWORD/issues/190) | N007 | Investigate 57 moderate topology gaps (800m–5km boundary) | **Closed** — all v17b-inherited, needs geometry fixes, deferred to v18 |
| [#191](https://github.com/SWORD-Global/SWORD/issues/191) | T017 | Investigate 553 dist_out jumps >30km between connected reaches | **Closed** — duplicate of #192 |
| [#192](https://github.com/SWORD-Global/SWORD/issues/192) | N006 | Investigate 2,596 boundary dist_out gaps >1km | **Closed** — all path-length artifacts at junctions, spatially verified |
| [#193](https://github.com/SWORD-Global/SWORD/issues/193) | N003 | Investigate 3,456 node spacing gaps >400m | **Closed** — v17b source data, defer to v18 |
| [#194](https://github.com/SWORD-Global/SWORD/issues/194) | N013 | ~~Investigate 89,364 centerline-node misallocations >500m~~ **Resolved** — 99.7% fixed (311 remain, accepted as residual) | P1 |
| [#195](https://github.com/SWORD-Global/SWORD/issues/195) | A030 | Investigate 4,816 WSE inversions downstream | **Closed** — MERIT DEM WSE, not SWOT; DEM noise, not actionable |
| [#196](https://github.com/SWORD-Global/SWORD/issues/196) | T020 | Investigate 197 river name disagreements with neighbors | **Closed** — GRWL source data, informational |

## Source Files

| File | Purpose |
|------|---------|
| [`src/_legacy/sword_validity.m`](../../src/_legacy/sword_validity.m) | POM's original MATLAB validation (4300+ lines, 15 test suites) |
| [`src/_legacy/updates/formatting_scripts/pom_flag_edits.py`](../../src/_legacy/updates/formatting_scripts/pom_flag_edits.py) | Earlier POM corrections (node count, dist_out ordering, ghost reach fixes) |
| [`src/sword_duckdb/lint/checks/topology.py`](../../src/sword_duckdb/lint/checks/topology.py) | T013–T020 implementations |
| [`src/sword_duckdb/lint/checks/node.py`](../../src/sword_duckdb/lint/checks/node.py) | N003–N013 implementations |
| [`src/sword_duckdb/lint/checks/attributes.py`](../../src/sword_duckdb/lint/checks/attributes.py) | A030 implementation |
| [`src/sword_duckdb/column_order.py`](../../src/sword_duckdb/column_order.py) | Canonical column ordering (includes dn_node_id, up_node_id, node_order) |
