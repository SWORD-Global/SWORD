# SWORD v17c Beta Release Notes

**Version:** v17c beta 0.0.10
**Date:** April 2026
**Authors:** James H. Gearon, Tamlin M. Pavelsky, Niek Collot d'Escury
**Base version:** SWORD v17b (March 2025, UNC)

## Changelog

### 0.0.10 (April 2026)
- **Node geolocation fixed on 293 additional reaches.** `rederive_nodes`
  recomputes node x/y from centerline spatial partitioning for reaches where
  consecutive node gaps exceed 3x the reach's median spacing and 0.4 km
  absolute (POM test 6b criteria). By region: AS:183, SA:35, AF:27, EU:19,
  OC:18, NA:6. Combined with the 41 reaches fixed in 0.0.8, total rederived:
  334 reaches (0.13%). 10 reaches remain unfixable (centerline geometry
  issues inherited from v17b).
- **Reach 35301100891 node_order rotation fixed.** This AS reach had node_ids
  rotated by one position (2, 3, ..., 75, 1 instead of 1, 2, ..., 75) since
  v17b. `node_order` now matches `node_id` ascending, `dn_node_id` = 0011,
  `up_node_id` = 0751. Node dist_out and all interpolated distance columns
  recalculated to match the corrected node_order.
- **Reach lakeflag and type reconciled for lake classification.** About
  6,200 reaches had inconsistent lakeflag and type fields (lakeflag=1 with
  type=1, or lakeflag=0 with type=3). Inconsistent reaches were either
  orphaned (skipped by both river and lake processing) or double-counted.
  Resolution combined three methods: 1,015 manual reviews through the
  Streamlit QA app, a gradient-boosted classifier trained on those reviews
  (82% precision, 6% FPR, applied at high confidence only: p>0.8 for lake,
  p<0.2 for river), and direct corrections from HarP v1.1 lake
  classifications. After reconciliation, 99.4% of reaches have consistent
  lakeflag and type. The remaining 1,196 (0.5%) are in the classifier's
  uncertain zone and queued for manual review. The type column now diverges
  from the reach ID last digit on 2,316 reaches (0.9%); the type column is
  authoritative.
- **Node lakeflag restored to v17b source values.** Previous scripts (HarP
  corrections, reviewer sync) had propagated reach-level lakeflag changes to
  nodes, overwriting the independently derived GRWL-based node
  classifications. Node lakeflag is the mode of 30 m GRWL centerline pixels
  within each ~200 m node segment; reach lakeflag is the mode of node
  lakeflags. These are independent spatial scales and can legitimately
  differ. All 11,112,454 node lakeflags restored from v17b NetCDF source.
  Reach-level lakeflag/type reconciliation is unaffected.
- **Five reaches fixed for N013 closure-bug damage.** Reaches 14278900061
  (AF), 31241401301, 48294000081, 45570000125, 34100005185 (AS) had
  corrupted x/y and cl_id_min/cl_id_max from the N013 closure bug
  (documented in 0.0.8). Nodes rederived from their own centerlines using
  the fixed code; node_length restored from v17b NetCDF to preserve exact
  sum-equals-reach_length consistency.
- **SWOT reach filters aligned with node filters.** `build_reach_filter_sql`
  now includes cross-track distance (10-60 km) and valid time_str filters,
  matching the node-level filters. Code change only; no DB data affected.
- **`rederive_scrambled_nodes.py` safeguards added.** After rederiving nodes,
  the script now automatically recalculates node dist_out (prevents N004
  violations) and verifies node_length sums (catches G002 regressions).
- **Node ordering normalized in NetCDF export.** About 8% of reaches
  (18,552 globally) had nodes stored in descending dist_out order in v17b
  NetCDF due to arbitrary GRWL centerline digitization direction. Nodes
  are now exported in ascending node_order (downstream-first) for all
  reaches. This fixes slope sign reversals in processing code that
  assumes the first node in the array is the downstream end. The
  underlying data is unchanged; only the file ordering is corrected.

### 0.0.9 (April 2026)
- **Flow correction fully reverted.** All 810 flow-corrected reaches restored
  to v17b topology after discovering a scoring tautology in
  `score_section_confidence` (`slope_from_upstream = -slope_from_downstream`).
  Topology diff vs v17b: 0. `v17c_flow_corrections` table is empty.
- **dist_out corruption fixed.** 1,976 reaches had BFS-corrupted `dist_out`
  from a January `CALCULATE_DIST_OUT` bug (wrong outlet, 93–99% error).
  Restored v17b values. T017 violations: 701 → 557.
- **Pipeline re-run with `skip_flow_correction=True`.** All 6 regions pass
  all gates.

### 0.0.8 (April 2026)
- **Node `dist_out` unified to midpoint convention.** All six node-level
  distance columns (`dist_out`, `hydro_dist_out`, `dist_out_dijkstra`,
  `hydro_dist_hw`, `pathlen_hw`, `pathlen_out`) now use the same midpoint
  interpolation formula: `reach_value - reach_length + cumsum(node_length)
  - 0.5 * node_length`. Previously `dist_out` preserved v17b endpoint
  values while the other five used midpoint, causing a systematic ~100 m
  discrepancy on single-path networks. On such networks the three
  outlet-distance columns are now exactly equal at every node. Breaking
  change from v17b convention; all POM release gate checks pass.
- **Node geolocation fixed on 41 reaches.** `rederive_nodes` recomputes
  node x/y from centerline spatial partitioning for 41 reaches (SA:5,
  EU:5, AF:8, AS:20, OC:3) where source NetCDF had contradictory
  CL-to-node mappings causing >2 km geographic jumps between consecutive
  nodes. Node lengths now use boundary-to-boundary distances (sum equals
  `reach_length` exactly). POM example reach 13341000591: max consecutive
  node jump drops from 13.9 km to 223 m.
- **`rederive_nodes` closure bug fixed.** The N013 centerline sync inside
  `rederive_nodes` captured a stale `reach_id` from an outer loop instead
  of the current plan's reach. Fixed to use `plan["reach_id"]`.

### 0.0.7 (April 2026)
- **Flow-corrected reach node_order and dist_out restored.** When pipeline runs
  skip the facc stage (e.g. when only rerunning distance and mainstem
  computations), `flipped_reach_ids` was empty, so `update_node_columns` never
  reversed `node_order` or swapped `dn_node_id`/`up_node_id` for the 810
  flow-corrected reaches. `0.0.6` shipped with `node_order` matching v17b
  `node_id` order on those reaches (Pierre-Olivier Malaterre detected 389
  inverted reaches). `0.0.7` loads the flipped reach IDs from the
  `v17c_flow_corrections` table when facc is skipped, so the node-order
  reversal and `dn_node_id`/`up_node_id` swap always run.
- **Node `dist_out` recomputed for flow-corrected reaches.** After reversing
  `node_order`, v17b node `dist_out` values are stale on 810 reaches (computed
  from the wrong flow direction). `0.0.7` recomputes them using cumulative
  `node_length` midpoint offsets from the corrected downstream boundary, so
  `dist_out` is monotonic with `node_order` and matches the new flow direction.
- **New lint check N018.** Catches the regression above automatically: flags
  flow-corrected reaches whose `node_order` is not reversed from v17b
  `node_id` order. ERROR severity, wired into the POM release gate.

### 0.0.6 (April 2026)
- **Canonical export ordering restored.** The `0.0.5` flat-file exports were
  written in DuckDB storage order through the generic maintenance exporter,
  which does not preserve a stable logical row order unless it is told to do
  so. As a result, node arrays in the distributed files could be split across
  multiple blocks for the same reach even though the per-row attributes stayed
  aligned.
- **Nodes now export in reach-contiguous logical order.** `0.0.6` writes nodes
  ordered by `reach_id`, then `node_order`, then `node_id` across NetCDF,
  GeoPackage, and Parquet. This restores a clean in-file node sequence for
  downstream consumers that expect each reach to occupy one contiguous node
  block and to run from downstream (`node_order=1`) to upstream (`node_order=n`).
- **Observed regression removed.** In Africa, the count of reaches whose NetCDF
  node rows were split into multiple file blocks dropped from 536 in `0.0.5`
  to 0 in `0.0.6`. Pierre-Olivier Malaterre's example reach `13341000591` now
  appears as one contiguous node block with `node_order = 1..93`.

### 0.0.5 (April 2026)
- **Promoted corrected bundle to a new version.** The corrected April 2
  reissue is now published as `0.0.5` instead of reusing `0.0.4`, so testers
  do not keep hitting stale downloads or cached copies under the same name.
  This `0.0.5` bundle supersedes all prior `0.0.4` beta uploads on Drive.
- **Release regressions from distributed `0.0.4` artifacts fixed.** The
  `0.0.5` bundle corrects the specific tester-reported regressions from the
  prior `0.0.4` uploads: reversed node `dist_out` relative to `node_order`,
  stale `dn_node_id` / `up_node_id` orientation on flow-corrected reaches,
  missing `nodes` layers in some GeoPackages, NULL `dist_out_dijkstra` at
  isolated ghost coastal outlets, and inconsistent single-node node-distance
  anchoring relative to the other node-level outlet distances.
- **Release artifacts renamed and resynced.** NetCDF, GeoPackage, Parquet,
  release notes, and SHA256 manifests now use `0.0.5` filenames consistently
  in the local beta folder and Google Drive beta folder.

### 0.0.4 (March 2026)
- **Node propagation to 11.1M nodes.** Five v17c columns (`best_headwater`,
  `best_outlet`, `pathlen_hw`, `pathlen_out`, `subnetwork_id`) were NULL on
  all nodes. Now propagated from parent reaches.
- **Dijkstra ghost outlet fix.** Ghost reaches (type=6) with out_degree=0 no
  longer report `dist_out_dijkstra=0`. All sinks are used as Dijkstra sources
  for full coverage (94–99% per region); ghost sinks receive NULL. Real outlet
  counts: NA=7, SA=1, EU=2, AF=1, AS=11, OC=1.
- **Bifurcation routing fix.** At multi-successor nodes, `rch_id_dn_main`
  now follows the mainstem chain unconditionally (was falling back to
  score-based ranking, which could disagree with `is_mainstem`).
  V023 (pathlen_out step consistency) violations: 2 → 0.
- **`hydro_dist_hw` computed.** Mainstem distance from headwater via
  `rch_id_up_main` chain walk (mirror of `hydro_dist_out`). Was stale from a
  prior pipeline run.
- **facc monotonicity fix (T003).** 419 reaches corrected via iterative
  downstream propagation. T003 violations: 392 → 0.
- **Routing weights retrained on `effective_slope`.** SWOT `slope_obs_p50`
  (where reliable and n>=5) replaces MERIT DEM slope in routing score training.
  Slope share: 8% → 3%, width: 67% → 71%. CV accuracy unchanged (88.5%).
- **Node-level distance interpolation.** `hydro_dist_out`, `hydro_dist_hw`, and
  `dist_out_dijkstra` added to nodes table, interpolated by node position within
  reach (using v17b `dist_out` offset). `pathlen_hw` and `pathlen_out` changed
  from flat reach copies to per-node interpolated values.
- **`hydro_dist_hw` convention fix.** Reach-level values shifted from
  downstream-end anchor to upstream-end anchor, matching `dist_out`,
  `hydro_dist_out`, and `dist_out_dijkstra`. Headwater reaches now
  report 0 (was `reach_length`). Node-level values unchanged.
- **F006 junction conservation fix.** 2 remaining junction violations
  (OC 53130100215, AS 45311901585) resolved by setting downstream facc
  to sum of upstream facc. F006 violations: 2 → 0.
- **13 AS `main_side` reverted to v17b.** Reaches had `main_side` changed
  from 0 (main) to 1 (side) by an undetermined prior operation; 9 of 13
  are linear reaches where side-channel classification is impossible.
- **Distance convention documented.** Variable reference now includes a
  convention table specifying the measurement anchor, zero-point, and
  ghost reach behavior for all distance variables, plus node-level
  interpolation formulas.
- **Variable reference updated.** 7 missing variables added, 8 type mismatches
  fixed, `cl_ids` shape corrected to `cl_id_min`/`cl_id_max`.
- **Node dist_out reactive recalc fix.** `CALCULATE_DIST_OUT` operations now
  correctly recalculate node-level `dist_out` by sorting nodes via `node_order`
  (geometric position) instead of `node_id`. This fixes an issue where
  flow-corrected reaches had incorrect node distances after recalculation
  (e.g., downstream-most node getting downstream reach's `dist_out`).
- **April 2 artifact reissue.** The published April 1 `0.0.4` files still had
  stale orientation metadata on 639 flow-corrected reaches. The reissued
  NetCDF, GeoPackage, and Parquet bundles recompute `node_order`,
  `dn_node_id`, and `up_node_id` from node `dist_out`, restore
  `dist_out_dijkstra = reach_length` for isolated ghost coastal outlets,
  and align single-node `node.dist_out` with the same midpoint anchor used
  by the other node-level outlet distances.
- **Ghost coastal outlet dist_out_dijkstra fix.** Isolated ghost coastal
  outlets (type=6 sinks with upstream neighbors but no path to real
  hydrologic outlets) now receive `dist_out_dijkstra = reach_length`,
  matching v17b `dist_out` behavior. Previously these 703 reaches in NA
  (similar counts in other regions) had NULL, but they should report the
  distance from their start to the ocean outlet point.
- **Exports regenerated (April 1 and 2, 2026).** The initial April 1 bundle
  captured the code fixes above. The April 2 reissue refreshed NetCDF,
  GeoPackage, and Parquet artifacts plus SHA256SUMS after repairing published
  `node_order` / boundary-node orientation metadata, ghost coastal outlet
  `dist_out_dijkstra`, and single-node node-distance anchoring.

### 0.0.3 (March 2026)
- **Routing weights learned from human labels.** Replaced the handcrafted
  lexicographic 3-tuple `(effective_width, log_facc, pathlen)` with a
  weighted scalar score trained on 1,967 human-labeled junction decisions:
  `1.97*log1p(ew) + 0.23*log1p(facc) - 0.23*log1p(slope) + 0.23*log1p(pathlen)
  + 0.29*stream_order`. Two new signals vs prior releases: slope (negative
  = prefer lower gradient) and stream_order. All routing functions use the
  same score to prevent divergence. (Weights retrained in 0.0.4; see above.)
- **Caroline's reviewer fixes synced.** 127 C001 lakeflag fixes (NA) and
  10 C004 type fixes (NA) from the Streamlit reviewer app.
- **NA PostgreSQL geometry fix.** 38,696 NA reaches had NULL geometry in
  PostgreSQL — v17b geometry overwrite via dblink silently failed. Rewrote
  to read from v17b GPKG files on disk with verification gates.
- Fixed `dn_node_id`, `up_node_id`, and `node_order` for 810 flow-corrected
  reaches where node ordering was stale (based on v17b `dist_out`). 639
  reaches now have `dn_node_id != min(node_id)`, reflecting the inverted
  flow direction.
- Removed redundant `rch_id_up_1..4` / `rch_id_dn_1..4` vector variables
  from NetCDF export. Only the `[4, N]` matrices `rch_id_up` and `rch_id_dn`
  are exported, matching v17b format.
- Fixed `swot_orbits` NetCDF type from string back to int64 (matching v17b).
- Fixed 15 integer columns widened from int32 to int64 in NetCDF export
  (`n_nodes`, `lakeflag`, `n_rch_up`, `stream_order`, etc.). All shared
  variable types now match v17b exactly.
- Synced `rch_id_up_1..4` / `rch_id_dn_1..4` DB columns from
  `reach_topology` table (694 were stale after flow corrections).

### 0.0.2 (March 2026)
- Renamed `is_mainstem_edge` to `is_mainstem`
- Mainstem algorithm refactored: computed per `main_path_id` group (see Section 2.1)
- Fixed `n_rch_up`/`n_rch_down` stale counts at 148 flow-corrected reaches
- Recomputed `facc` at 807 flow-corrected reaches using topological propagation; resolves all F006 junction conservation violations and T003 monotonicity violations at these reaches
- OC reach 51111300061 incomplete split fully reverted to v17b state (434 orphan centerlines, 73 orphan nodes restored)

### 0.0.1 (March 2026)
- Initial v17c beta release
- Includes `node_order`, `dn_node_id`, `up_node_id` (node ordering within reaches)

---

## 1. Overview

SWORD v17c extends v17b with three additions: computed mainstem topology,
SWOT observation statistics, and flow accumulation corrections. No reaches,
nodes, or centerlines were added or removed. v17c contains the same
248,673 reaches, 11.1M nodes, and 66.9M centerline points as v17b across
all six regions (NA, SA, EU, AF, AS, OC).

Each region is distributed as a single NetCDF4 file
(`{region}_sword_v17c_beta.nc`). The group structure matches v17b
(centerlines, nodes, reaches), and the `area_fits` and `discharge_models`
subgroups under reaches pass through from v17b unchanged. Reach arrays are
ordered stably by `reach_id`. Node arrays are grouped contiguously by
`reach_id` and ordered within each reach by `node_order` (downstream to
upstream).

Reach coordinate columns (`x`, `y`, `x_min`, `x_max`, `y_min`, `y_max`)
match v17b values across all formats (NetCDF, DuckDB, PostgreSQL).

All new variables use a fill value of -9999 where no observation or
computation produced a value.

For a complete variable catalog, see
[v17c_variable_reference.md](v17c_variable_reference.md).

---

## 2. New Variables

### 2.1 Mainstem Topology (reaches group)

`is_mainstem` is computed per `main_path_id` group: each group (a mainstem
path plus its tributary branches) gets one canonical chain, identified by a
greedy walk from the group's shared `best_headwater`. At each junction the
algorithm selects the upstream branch with the highest weighted routing
score: `2.02*log1p(ew) + 0.17*log1p(facc) - 0.08*log1p(slope) +
0.35*log1p(pathlen) + 0.23*stream_order`. These weights were learned from 1,967
human-labeled junction decisions via logistic regression on pairwise
log1p-difference features, using SWOT slope where reliable (else MERIT DEM).
The negative slope weight captures the
geomorphic pattern that mainstem channels have lower gradients than
tributaries. Mainstem reaches within each group are then assigned
`rch_id_up_main` / `rch_id_dn_main` from the chain; non-mainstem reaches
use the same weighted score. Ghost reaches (type=6) are excluded from
mainstem but still participate in routing topology. ~10% of mainstem
reaches have no mainstem neighbor at `main_path_id` group boundaries —
this is expected by design.

| Variable | Type | Units | Description |
|----------|------|-------|-------------|
| `dist_out_dijkstra` | float64 | meters | Dijkstra shortest-path distance from the upstream end of the reach to any network outlet (outlet = 0; NULL for ghost reaches) |
| `hydro_dist_out` | float64 | meters | Mainstem distance from the upstream end of the reach to `best_outlet` via `rch_id_dn_main` chain (outlet = `reach_length`) |
| `hydro_dist_hw` | float64 | meters | Mainstem distance from `best_headwater` to the upstream end of the reach via `rch_id_up_main` chain (headwater = 0) |
| `rch_id_up_main` | int64 | — | Main upstream neighbor reach_id (mainstem-preferred) |
| `rch_id_dn_main` | int64 | — | Main downstream neighbor reach_id (mainstem-preferred) |
| `best_headwater` | int64 | — | Routing-score-prioritized headwater reach_id for the network component |
| `best_outlet` | int64 | — | Routing-score-prioritized outlet reach_id for the network component |
| `pathlen_hw` | float64 | meters | Cumulative path length from `best_headwater` |
| `pathlen_out` | float64 | meters | Cumulative path length to `best_outlet` |
| `is_mainstem` | int32 | — | 1 if reach is on a mainstem path, 0 otherwise |
| `main_path_id` | int64 | — | Unique identifier for each mainstem path group |
| `subnetwork_id` | int32 | — | Connected component ID (Pfafstetter-offset, globally unique; see Section 4) |
| `dn_node_id` | int64 | — | Node ID at the downstream end of the reach (lowest `dist_out`) |
| `up_node_id` | int64 | — | Node ID at the upstream end of the reach (highest `dist_out`) |

Eight of these variables also appear at node level. Six are interpolated
by node position within the reach: `dist_out`, `hydro_dist_out`,
`hydro_dist_hw`, `dist_out_dijkstra`, `pathlen_hw`, and `pathlen_out`.
Three are flat copies from the parent reach: `subnetwork_id`,
`best_headwater`, and `best_outlet`. For flow-corrected reaches (810),
`node_order` is derived from reversed `node_id` order (since v17b
`dist_out` is stale), and node `dist_out` is recomputed to match the
corrected flow direction.

All six interpolated distance columns use the same midpoint formula:
`reach_value - reach_length + cumsum(node_length) - 0.5 * node_length`.
This places each node at the geometric center of its `node_length`
segment. On a single-path network (no junctions), node-level `dist_out`,
`hydro_dist_out`, and `dist_out_dijkstra` are exactly equal.

`node_order` is a node-level variable (not in the reaches table): 1-based
position within a reach, ordered by `dist_out` ascending (1 = downstream
end, n = upstream end).

**Distance convention note.** All four distance variables anchor at the
upstream end of the reach. `dist_out` and `hydro_dist_out` assign
`reach_length` at the outlet. `dist_out_dijkstra` assigns 0 at the
outlet; the offset at any reach equals the outlet's `reach_length`.
`hydro_dist_hw` assigns 0 at the headwater and increases downstream.
See the variable reference for the full convention table.

### 2.2 SWOT Observation Statistics

Percentile-based summaries computed from available SWOT observations. All
percentile, range, and MAD variables share the units of the underlying
measurement.

**Reaches and nodes:**

| Variable | Type | Units | Description |
|----------|------|-------|-------------|
| `wse_obs_p10`–`wse_obs_p90` | float64 | meters | WSE percentiles (10th through 90th, in steps of 10) |
| `wse_obs_range` | float64 | meters | WSE observation range (p90 - p10) |
| `wse_obs_mad` | float64 | meters | WSE median absolute deviation |
| `width_obs_p10`–`width_obs_p90` | float64 | meters | Width percentiles |
| `width_obs_range` | float64 | meters | Width observation range |
| `width_obs_mad` | float64 | meters | Width median absolute deviation |
| `n_obs` | int32 | — | Total SWOT observation count |

**Reaches only:**

| Variable | Type | Units | Description |
|----------|------|-------|-------------|
| `slope_obs_p10`–`slope_obs_p90` | float64 | m/km | Slope percentiles |
| `slope_obs_range` | float64 | m/km | Slope observation range |
| `slope_obs_mad` | float64 | m/km | Slope median absolute deviation |
| `slope_obs_adj` | float64 | m/km | Adjusted slope |
| `slope_obs_slopeF` | float64 | — | Slope F-statistic |
| `slope_obs_reliable` | int32 | — | 0 = unreliable, 1 = reliable |
| `slope_obs_quality` | int32 | — | Integer quality category (0–8; see Section 3) |
| `slope_obs_n` | int64 | — | Number of slope observations |
| `slope_obs_n_passes` | int64 | — | Number of SWOT passes used |
| `slope_obs_q` | int64 | — | Bitfield quality flag (see Section 3) |

### 2.3 Flow Accumulation Corrections

A two-stage denoise pipeline corrected flow accumulation (`facc`) values
to address three systematic error modes in MERIT Hydro's D8
(eight-direction flow routing) upstream area: bifurcation cloning,
junction inflation, and raster-vector misalignment. The pipeline corrected
96,589 of 248,673 reaches (38.8%). Uncorrected reaches retain v17b values.
See [facc_correction_methodology.md](technical/facc_correction_methodology.md)
for the full algorithm description.

| Variable | Type | Group | Description |
|----------|------|-------|-------------|
| `facc` | float64 | reaches, nodes | Flow accumulation (km^2). Corrected values where applicable; v17b values otherwise. |
| `facc_quality` | int32 | reaches, nodes | 1 = corrected by denoise_v3; fill_value = not flagged |

After correction, junction conservation violations (downstream facc < sum
of upstream facc) are resolved in all regions. In 0.0.2, facc was
additionally recomputed at 807 flow-corrected reaches via topological
propagation, resolving remaining monotonicity violations at those reaches.

### 2.4 Other New or Updated Variables

| Variable | Type | Group | Description |
|----------|------|-------|-------------|
| `type` | int32 | reaches | Reach classification (1=river, 3=lake_on_river, 4=dam, 5=unreliable, 6=ghost). Not present in v17b NetCDF; added in v17c so NetCDF users can filter by reach type without needing the database. |
| `dl_grod_id` | int64 | reaches | DL-GROD (Deep Learning Global River Obstruction Database; He et al. 2025) dam/obstruction ID |
| `edit_flag` | string | reaches | Tag for manually edited reaches (e.g., `lake_sandwich`, `harp_lake`) |

---

## 3. Flag Encoding Reference

### facc_quality

| Value | Meaning |
|-------|---------|
| 1 | `denoise_v3` — corrected by the facc denoising pipeline |
| -9999 (fill) | Not flagged; facc unchanged from v17b |

CF attributes: `flag_values = [1]`, `flag_meanings = "denoise_v3"`.

### slope_obs_quality

| Value | Meaning |
|-------|---------|
| 0 | reliable |
| 1 | small_negative |
| 2 | moderate_negative |
| 3 | large_negative |
| 4 | negative |
| 5 | below_ref_uncertainty |
| 6 | high_uncertainty |
| 7 | noise_high_nobs |
| 8 | flat_water_noise |

CF attributes: `flag_values = [0,1,2,3,4,5,6,7,8]`,
`flag_meanings = "reliable small_negative moderate_negative large_negative negative below_ref_uncertainty high_uncertainty noise_high_nobs flat_water_noise"`.

### slope_obs_reliable

| Value | Meaning |
|-------|---------|
| 0 | Unreliable |
| 1 | Reliable |
| -9999 (fill) | Not computed (no SWOT slope observations) |

### slope_obs_q (bitfield)

| Bit | Value | Meaning |
|-----|-------|---------|
| 1 | 1 | Negative slope |
| 2 | 2 | Low number of passes |
| 3 | 4 | High variance |
| 4 | 8 | Extreme value |
| 5 | 16 | Clipped |

Flags combine by addition. A value of 0 indicates no quality issues.
Example: 5 = negative slope (1) + high variance (4).

### is_mainstem

| Value | Meaning |
|-------|---------|
| 0 | Not on mainstem |
| 1 | On mainstem path |

---

## 4. Known Limitations

- **SWOT observation coverage:** SWOT statistics are fill_value (-9999) for
  reaches and nodes lacking SWOT data.

- **facc correction scope:** 96,589 reaches corrected (38.8%); the
  remaining 152,084 retain v17b values. Node-level facc propagates from
  the parent reach.

- **Lake sandwich corrections:** 1,252 reaches reclassified to
  `lakeflag = 1` where a narrow, shorter-than-neighbor reach sat between
  lake reaches (tagged `edit_flag = "lake_sandwich"`). ~1,755 similar
  cases remain (narrow connecting channels, chains).

- **HarP lake corrections:** 7,425 reaches reclassified from
  `lakeflag = 0` (river) to `lakeflag = 1` (lake) based on HarP v1.1
  (Hydrography and River Planform) lake classification data. 200,201
  child nodes updated to match. Tagged `edit_flag = "harp_lake"`.
  Existing tags preserved (comma-delimited when multiple apply).

- **area_fits and discharge_models:** Direct copies from v17b. Not
  recomputed against v17c facc or SWOT values.

- **`subnetwork_id` vs `network`:** `subnetwork_id` uses Pfafstetter-
  offset enumeration (globally unique). v17b `network` uses per-region
  1-based IDs. Different component counts (v17c finds more via weakly
  connected components; 19 subnetworks span multiple v17b networks).
  `network` is retained unchanged from v17b.

- **Topology reciprocity gaps (resolved):** 151 non-reciprocal pairs
  (both reaches listing each other as upstream) were introduced during
  flow correction revert. Fixed by completing the revert for all
  affected reciprocal entries. Zero non-reciprocal pairs remain.

- **Flow correction oscillation:** 389 reaches (0.16%) in AF/AS/EU/NA/SA
  had ambiguous WSE slope signals causing bidirectional flow correction
  scores. These were reverted to v17b topology.

- **main_path_id consistency:** 3,134 reaches have `main_path_id` values
  inconsistent with current `(best_headwater, best_outlet)` tuples (V013-
  V015 lint checks). 80 reaches in NA have `best_headwater` pointing to
  non-headwater reaches. Requires recomputing `main_path_id` from current
  headwater/outlet assignments.

- **River naming:** 51.2% of reaches are unnamed (NODATA), ranging from
  26% (AF) to 69% (OC). 2.6% of mainstem 1:1 links have local name
  discontinuities (name changes between adjacent reaches with no junction).

- **Width fill values (A003):** ~1,266 reaches have `width=0` (unmeasured) or
  `width=-1` (GRWL lake fill). These are v17b fill values, not data errors.
  Present in both v17b and v17c unchanged. The A003 lint check is downgraded
  to WARNING for this reason.

- **`lakeflag`/`type` mismatch:** ~5,770 reaches have `lakeflag=1` (lake)
  but `type=1` (river), introduced by HarP and lake-sandwich corrections
  which updated `lakeflag` but not `type`. `type` is encoded in the last
  digit of `reach_id`, so changing it would change reach IDs. Policy for
  reach_id changes under discussion. Affects users filtering on both
  `lakeflag` and `type` at river-lake boundaries. Tracked in issue #208.

---

## 5. Quality Audits

Validation checks performed on the v17c data:

| Audit | Finding |
|-------|---------|
| **Geometry** | DuckDB geometries (rebuilt from NetCDF) lack endpoint overlap vertices present in v17b (210,533 reaches affected: 173K +1 point, 37K +2 points). `reach_length` unchanged. Reach coordinate columns (`x`, `y`, `x_min`, `x_max`, `y_min`, `y_max`) copied from v17b to ensure consistency across all formats. |
| **n_nodes / reach_length** | Internally consistent. Zero N008/G002/G003 violations. |
| **path_freq gaps** | v17b had 4,952 connected non-ghost reaches with invalid path_freq (0 or -9999). Resolved in v17c; remaining nodata values are correctly attributed to ghost reaches (type=6). |
| **subnetwork_id** | 3,027 components across 248,673 reaches verified. Pfafstetter banding correct. Zero cross-region collisions. 19 subnetworks (0.6%) span multiple v17b networks (expected). |
| **Topology integrity** | T001 (dist_out_dijkstra monotonicity), T012 (referential integrity), T013 (self-reference), T014 (bidirectional): all pass. T005/T007: zero non-reciprocal edges (151 from incomplete flow correction revert resolved in beta 0.0.1). Note: reach-level v17b `dist_out` is stale at 807 flow-corrected reaches where topology direction was updated but reach `dist_out` retains its v17b value — use `dist_out_dijkstra` or `hydro_dist_out` for distance routing. Node-level `dist_out` is recomputed for all reaches (midpoint interpolation). |
| **n_rch_up/n_rch_down** | 148 scalar count mismatches corrected (flow corrections flipped reach_topology but did not recalculate counts). Zero mismatches across all 248,673 reaches. |
| **OC reach split revert** | Incomplete `break_reaches()` split of OC reach 51111300061 (434 orphan centerlines, 73 orphan nodes) fully reverted to v17b state. |
| **River name formatting** | 291 formatting issues corrected (separators, whitespace). Automated checks now enforce "; " separator and alphabetical ordering. |
| **Flow direction** | 1,112 experimental topology flips reverted after causing 30K disconnected reaches. 807 reaches across all regions retain corrected topology that differs from v17b (175 sections, median 5 reaches/section). OC has 119 of these (26 SWOT-validated sections). Non-OC corrections were retained from the flow correction pipeline; reach-level `dist_out` is stale at these reaches — use `dist_out_dijkstra`. Node-level `dist_out` is recomputed via midpoint interpolation. |
| **HarP lake corrections** | 7,425 reaches reclassified lakeflag 0 to 1 from HarP v1.1 data. 200,201 nodes propagated. Tagged `edit_flag = "harp_lake"`. |

For POM (Pierre-Olivier Malaterre) validation results, see
[pom_validation_report.md](technical/pom_validation_report.md).

---

## 6. File Format

- **Format:** NetCDF4 (one file per region)
- **Naming:** `{region}_sword_v17c_beta.nc` where region is `na`, `sa`,
  `eu`, `af`, `as`, `oc`
- **Groups:** `centerlines`, `nodes`, `reaches`
  - `reaches/area_fits` and `reaches/discharge_models` subgroups (from v17b)
- **Ordering:** Reach, node, and centerline arrays match v17b ordering
- **Fill value:** -9999 for all numeric variables (int32, int64, float64)
- **Checksums:** SHA256 hashes listed in `SHA256SUMS_{version}.txt`
- **Additional formats:** GeoPackage and GeoParquet exports available
  (reaches and nodes per region, with geometry)

---

## 7. Methodology Documentation

| Document | Description |
|----------|-------------|
| [facc_correction_methodology.md](technical/facc_correction_methodology.md) | Facc denoise algorithm, detection rules, correction model |
| [pom_requests_summary.md](technical/pom_requests_summary.md) | POM validation check tracker (19 checks, production results) |
| [pom_validation_report.md](technical/pom_validation_report.md) | POM validation production results |
| [v17c_variable_reference.md](v17c_variable_reference.md) | Complete variable catalog for NetCDF export |
| [SWORD_v17b_Technical_Documentation.md](technical/SWORD_v17b_Technical_Documentation.md) | v17b baseline reference |
