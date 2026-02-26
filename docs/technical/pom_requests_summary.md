# POM (Pierre-Olivier Malaterre) Requests — Implementation Summary

> **Living document.** Update this when POM-related checks, columns, or issues change.
> Location: `docs/technical/pom_requests_summary.md`

## Background

Pierre-Olivier Malaterre (POM), INRAE, provided SWORD with `sword_validity.m` — a MATLAB validation script containing 15 test suites (with sub-tests) that check topology, node consistency, ID format, river naming, and type distribution. POM also requested two new columns (`dn_node_id`/`up_node_id` on reaches, `node_order` on nodes) needed because v17c flow direction changes mean node IDs can be decreasing within a reach.

This document maps POM's original MATLAB tests to our Python lint framework and tracks implementation status.

## New Columns (Issue #149)

| Column | Table | Type | Description |
|--------|-------|------|-------------|
| `dn_node_id` | reaches | BIGINT | Downstream boundary node ID |
| `up_node_id` | reaches | BIGINT | Upstream boundary node ID |
| `node_order` | nodes | INTEGER | 1-based position within reach (1=downstream, n=upstream by dist_out) |

**Status:** Implemented and deployed (PR #165). Verified on production v17c (248,674 reaches, 11.1M nodes).

**Source:** POM emails Feb 3 ("please also include `nodes_ids`") + Feb 4 ("Filled with an integer from 1 to n, 1 being for the first downstream, and n for the last upstream"). Already present in v17c NetCDF via `sword_read.m:83-86`.

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
| 9a | Node geolocation within parent reach geometry | **N012** (new, 500m threshold) | Implemented (#185) |
| 9b | Node indexes contiguous within reach | **N010** (new) | Implemented |
| 9c | Centerline points allocated to correct reach | Not implemented | Unnecessary — CL points define reach geometry (distance always 0) |
| 9d | Centerline points allocated to correct node | **N013** (new, 500m threshold) | Implemented (#186) |
| 10a | Node dist_out increasing with node_id | **N004** (new) | Implemented |
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
| 12a | Node order coherent with node ID | **N004** (new) | Implemented |
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

### WSE Monotonicity (from `sword_validity.m` line 433)

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
| **Totals** | **~60** | **14** | **19 new checks** | **~27 (mostly informational/low-priority)** |

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

### Node (N-series)

| Check | Severity | Description | POM Test |
|-------|----------|-------------|----------|
| N003 | WARNING | Adjacent node spacing >400m within reach | 6b |
| N004 | WARNING | Node dist_out not increasing with node_id | 10a/12a |
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

## Production Results (v17c, 2026-02-26)

Checks run against `sword_v17c.duckdb` (248,673 reaches, 11.1M nodes, 66.9M centerlines).

### Passing (zero violations)

| Check | Total checked | Notes |
|-------|--------------|-------|
| T013 | 495,652 edges | No self-referencing topology |
| T014 | 248,673 reaches | No bidirectional paradoxes |
| T015 | 244,902 reaches | No shortcut edges |
| T018 | 11,361,127 IDs | All reach/node IDs well-formed |
| N008 | 248,673 reaches | Node counts match n_nodes |
| N010 | 248,673 reaches | Node indexes contiguous |
| N004 | 11,112,454 nodes | 1 single violation (noise) |

### Failing (investigation needed)

| Check | Violations | Sev | Investigation issue | Root cause summary |
|-------|-----------|-----|--------------------|--------------------|
| N013 | 89,364 | WARN | [#194](https://github.com/SWORD-Global/SWORD/issues/194) | Systematic CL-node misallocation, AS=46K. Needs root cause diagnosis. |
| A030 | 4,816 | WARN | [#195](https://github.com/SWORD-Global/SWORD/issues/195) | Mix of real backwater/dams + data errors (max 4,121m increase in AS) |
| N003 | 3,456 | WARN | [#193](https://github.com/SWORD-Global/SWORD/issues/193) | Node spacing gaps, AS=72% of violations |
| N006 | 2,596 | WARN | [#192](https://github.com/SWORD-Global/SWORD/issues/192) | Boundary dist_out discontinuities |
| T017 | 553 | WARN | [#191](https://github.com/SWORD-Global/SWORD/issues/191) | dist_out jumps >30km, max 3,201km (OC) |
| N007 | 467 | WARN | [#188](https://github.com/SWORD-Global/SWORD/issues/188), [#189](https://github.com/SWORD-Global/SWORD/issues/189), [#190](https://github.com/SWORD-Global/SWORD/issues/190) | 3 root causes: date line bug (5–9), wrong-end connections (195), genuinely far (263) |
| T020 | 197 | INFO | [#196](https://github.com/SWORD-Global/SWORD/issues/196) | River name disagreements, many semicolon-concatenated |
| N012 | 12 | WARN | [#185](https://github.com/SWORD-Global/SWORD/issues/185) | 12 misplaced nodes, all ghost/Arctic reaches |

### Informational (no action)

| Check | Value | Notes |
|-------|-------|-------|
| T019 | 127,401 (51.2%) | Reaches with river_name='NODATA'. Source data limitation. |

## Not Implemented (with rationale)

| POM Test | Why skipped |
|----------|-------------|
| 3e/4e | Disabled in POM's own defaults (`opt_warning_3e=0`, `opt_warning_4e=0`) — suspicious but possible |
| 8a | Implicit in node_order column computation — covered by N004 |
| 9c | Centerline-to-reach allocation — unnecessary (CL points define reach geometry, distance always 0) |
| 11c/11d | Informational (tributary mitigation) — not an error condition |
| 15b_n–g_n | Node-level type distribution — node type derived from reach type, redundant |
| T016 (#152) | Closed — subsumed by pre-existing G012 (endpoint alignment at 500m) |

## GitHub Issues

### Implementation (closed)

| Issue | Title | Status |
|-------|-------|--------|
| #149 | Add nodes_ids and node_order columns (POM request) | Closed (PR #165) |
| #150 | Lint T013/T014: self-referencing and bidirectional topology | Closed |
| #151 | Lint T015: redundant shortcut connections | Closed |
| #152 | Lint T016: connected reach centroid distance >30km | Closed (subsumed by G012) |
| #153 | Lint T017: dist_out excessive jump between neighbors | Closed |
| #154 | Lint T018: reach and node ID format validation | Closed |
| #155 | Lint T019/T020: river name validation | Closed |
| #156 | Lint A030: WSE monotonicity downstream | Closed |
| #157 | Node-level lint: dist_out, spacing, and boundary checks (N003-N007) | Closed |
| #158 | Node/centerline allocation validation (POM Tests 8/9) | Closed |
| #185 | Lint N012: node geolocation outside parent reach geometry (POM Test 9a) | Open |
| #186 | Lint N013: centerline point too far from assigned node (POM Test 9d) | Open |

### Investigation (diagnose first, fix only after discussing with Jake)

| Issue | Check | Title | Priority |
|-------|-------|-------|----------|
| [#187](https://github.com/SWORD-Global/SWORD/issues/187) | N007/G012 | DuckDB reach geometries missing endpoint overlap vertices | P1 |
| [#188](https://github.com/SWORD-Global/SWORD/issues/188) | N007 | Fix N007 distance formula: antimeridian wrapping + check all 4 boundary pairs | P1 |
| [#189](https://github.com/SWORD-Global/SWORD/issues/189) | N007 | Investigate 31 extreme bad topology links (>5km boundary gap) | P2 |
| [#190](https://github.com/SWORD-Global/SWORD/issues/190) | N007 | Investigate 57 moderate topology gaps (800m–5km boundary) | P3 |
| [#191](https://github.com/SWORD-Global/SWORD/issues/191) | T017 | Investigate 553 dist_out jumps >30km between connected reaches | P2 |
| [#192](https://github.com/SWORD-Global/SWORD/issues/192) | N006 | Investigate 2,596 boundary dist_out gaps >1km | P2 |
| [#193](https://github.com/SWORD-Global/SWORD/issues/193) | N003 | Investigate 3,456 node spacing gaps >400m | P2 |
| [#194](https://github.com/SWORD-Global/SWORD/issues/194) | N013 | Investigate 89,364 centerline-node misallocations >500m | P1 |
| [#195](https://github.com/SWORD-Global/SWORD/issues/195) | A030 | Investigate 4,816 WSE inversions downstream | P2 |
| [#196](https://github.com/SWORD-Global/SWORD/issues/196) | T020 | Investigate 197 river name disagreements with neighbors | P3 |

## Source Files

| File | Purpose |
|------|---------|
| `src/_legacy/sword_validity.m` | POM's original MATLAB validation (4300+ lines, 15 test suites) |
| `src/_legacy/updates/formatting_scripts/pom_flag_edits.py` | Earlier POM corrections (node count, dist_out ordering, ghost reach fixes) |
| `src/sword_duckdb/lint/checks/topology.py` | T013–T020 implementations |
| `src/sword_duckdb/lint/checks/node.py` | N003–N013 implementations |
| `src/sword_duckdb/lint/checks/attributes.py` | A030 implementation |
| `src/sword_duckdb/column_order.py` | Canonical column ordering (includes dn_node_id, up_node_id, node_order) |
