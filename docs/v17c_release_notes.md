# SWORD v17c Release Notes

**Version:** v17c
**Date:** July 2026 (data exported May 2026)
**Base version:** SWORD v17b (March 2025, UNC)

## Changelog

### 0.0.12 (May 2026)
- **Node coordinates restored to v17b for SWOT D0-D2 continuity.** The
  344 reaches whose nodes were rederived in 0.0.8 and 0.0.10 now use v17b
  node `x`/`y`, `node_length`, `cl_id_min`, and `cl_id_max` values again.
  A small OC split-revert residue on reach 51111300061 was included in the
  same pass. Post-revert validation found zero node coordinate differences
  relative to v17b NetCDF across all 11,112,454 nodes.
- **Centerline-to-node assignments resynchronized.** `centerlines.node_id`
  was restored from v17b on affected reaches so node geometry and
  centerline allocation remain internally consistent.
- **Node order repaired on restored-coordinate reaches.** Projection-based
  ordering over the v17b-restored node coordinates updates `node_order` on
  3,725 nodes across 221 of the 344 affected reaches, and updates 45
  `dn_node_id`/`up_node_id` boundary pairs. The repair preserves node
  coordinates and recomputes the six node distance fields from the current
  midpoint convention across all 344 restored-coordinate reaches, including
  a distance-only cleanup for 289 inherited `dist_out` rows on five
  unchanged-order reaches. AS reach 35301100891 now encodes POM's intended
  downstream-to-upstream sequence (`2, 3, ..., 23, 25, 26, 24, 27, ...,
  73, 75, 74, 1`) without moving node lat/lon.
- **SWOT slope observations recomputed from node WSE.** `slope_obs_*` on
  reaches now comes from pass-level regressions of RiverSP node `wse_sm`
  against the current beta12 `nodes.dist_out`, then reach-level percentiles
  of those pass slopes. This replaces stale reach-level signed RiverSP slopes
  whose sign could reflect older node ordering. Median `slope_obs_p50`
  coverage is 173,732 reaches; negative medians decrease from 18,805 before
  the correction to 9,988 after the correction. POM's canonical AS reach
  35301100891 changes from `slope_obs_p50=-0.0136548684` to
  `+0.0142687580` m/m while retaining `slope=15.121399` m/km.
  Downstream users should read both `slope` and `slope_obs_*` directly from
  0.0.12; no external sign flip is needed for reaches whose node order changed.
  The 221 operation-999 `node_order` repair reaches are all in first-digit POM
  regions 1-6. Test 16 slope-observation and endpoint-WSE diagnostics in
  first-digit regions 7-9 are separate warning populations, not additional
  0.0.12 node-order repairs.
- **Static MERIT/SRTM slope semantics unchanged.** The legacy `reaches/slope`
  variable remains the nonnegative MERIT/SRTM slope magnitude in m/km; it is
  not a signed flow-direction variable. 0.0.12 does not change
  `reaches/slope`, `reaches/wse`, or `nodes/wse` relative to v17b. Endpoint-WSE
  sign checks may still warn where inherited MERIT/SRTM node WSE values are
  locally inconsistent with flow direction; those warnings are data-quality
  diagnostics, not instructions to flip `reaches/slope` or `slope_obs_*`.
  MERIT/SRTM WSE resampling was evaluated after POM's Test 16 feedback and
  deferred: the legacy SWORD workflow used UPA-filtered nearest MERIT river
  pixels and centerline aggregation, so a faithful correction is a
  science-data reconstruction task rather than a final beta patch.
- **v17c analytic fields preserved.** The revert does not undo the v17c
  midpoint node-distance convention, lakeflag/type reconciliation, SWOT
  WSE/width observation statistics, metadata restoration, or export ordering
  changes. No RiverObs software change or coordinate-substitution mapping table
  is required for D0-D2 coordinate continuity.
- **Exports regenerated and verified.** NetCDF, GeoPackage, and GeoParquet
  artifacts were regenerated for all six regions. Export row counts match
  the DuckDB source for reaches and nodes in GeoPackage/GeoParquet, and for
  reaches, nodes, and centerlines in NetCDF.

### 0.0.11 (April 2026)
- **Node ordering normalized in NetCDF export.** About 7.5% of reaches
  (18,553 globally) had nodes stored in descending dist_out order in all
  previous SWORD versions due to arbitrary GRWL centerline digitization
  direction. Nodes are now exported in ascending node_order
  (downstream-first) for all reaches. This fixes slope sign reversals
  in processing code that assumes the first node in the array is the
  downstream end. The underlying data is unchanged; only the file
  ordering is corrected. This is an export storage-order fix, not a retained
  topology reversal and not the smaller 0.0.12 operation-999 `node_order`
  repair set.
- **Restored metadata variables in NetCDF export.** river_name_local,
  river_name_en, version, add_flag, and swot_obs_source were
  accidentally omitted from the export spec in 0.0.4. Restored in
  0.0.11 on both reaches and nodes where applicable.
- **Node lakeflag propagated from reach.** Node lakeflag now matches
  parent reach lakeflag across all 11,112,454 nodes, per JPL request.

### 0.0.10 (April 2026)
- **Node geolocation fixed on 303 additional reaches.** `rederive_nodes`
  recomputes node x/y from centerline spatial partitioning for reaches where
  consecutive node gaps exceed 3x the reach's median spacing and 0.4 km
  absolute (POM test 6b criteria). By region: AS:189, SA:38, AF:29, EU:21,
  OC:19, NA:7. Combined with the 41 reaches fixed in 0.0.8, total rederived:
  344 reaches (0.14%). **10 reaches were fixed but still trigger the
  scrambled-node detector** (centerline geometry issues inherited from v17b);
  see Known Limitations.
- **Reach 35301100891 node_order rotation and geolocation fixed.** This AS
  reach had node_ids rotated by one position (2, 3, ..., 75, 1 instead of
  1, 2, ..., 75) since v17b. Additionally, 74 of 75 nodes were rederived
  from the centerline (max shift ~12.6 km on node 35301100890011) due to
  scrambled geolocation detected by POM test 6b. `node_order` now matches
  `node_id` ascending, `dn_node_id` = 0011, `up_node_id` = 0751. Node
  dist_out and all interpolated distance columns recalculated to match the
  corrected node_order.
- **Reach lakeflag and type reconciled for lake classification.** About
  6,200 reaches had inconsistent lakeflag and type fields (lakeflag=1 with
  type=1, or lakeflag=0 with type=3). Inconsistent reaches were either
  orphaned (skipped by both river and lake processing) or double-counted.
  Resolution combined three methods: 1,015 manual reviews through the
  Streamlit QA app, a gradient-boosted classifier trained on those reviews
  (82% precision, 6% FPR, applied at high confidence only: p>0.8 for lake,
  p<0.2 for river), and direct corrections from HarP v1.1 lake
  classifications. Lakeflag and type are now 100% consistent across all
  248,673 reaches. The type column now diverges from the reach ID last
  digit on 2,316 reaches (0.9%) as of 0.0.10 (2,648 by 0.0.12 after
  subsequent type corrections); the type column is authoritative.
- **Six reaches fixed for N013 closure-bug damage.** Reaches 14278900061
  (AF), 31241401301, 48294000081, 45570000125, 34100005185, 42211000503
  (AS) had corrupted x/y and cl_id_min/cl_id_max from the N013 closure
  bug (documented in 0.0.8). Nodes rederived from their own centerlines
  using the fixed code; node_length restored from v17b NetCDF to preserve
  exact sum-equals-reach_length consistency. Full sweep confirms zero
  reaches with node_length mismatch above 0.1% globally.
- **SWOT reach filters aligned with node filters.** `build_reach_filter_sql`
  now includes cross-track distance (10-60 km) and valid time_str filters,
  matching the node-level filters. Code change only; no DB data affected.
- **`rederive_scrambled_nodes.py` safeguards added.** After rederiving nodes,
  the script now automatically recalculates node dist_out (prevents N004
  violations) and verifies node_length sums (catches G002 regressions).

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
  `hydro_dist_hw`, `pathlen_hw`, `pathlen_out`) now use midpoint
  interpolation from reach-level values. Previously `dist_out` preserved
  v17b endpoint values while the other five used midpoint, causing a systematic ~100 m
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
  convention relative to the other node-level outlet distances.
- **Release artifacts renamed and resynced.** NetCDF, GeoPackage, Parquet,
  release notes, and SHA256 manifests now use `0.0.5` filenames consistently
  in the local beta folder and Google Drive beta folder.

### 0.0.4 (March 2026)
- **Node propagation to 11.1M nodes.** Five v17c columns (`best_headwater`,
  `best_outlet`, `pathlen_hw`, `pathlen_out`, `subnetwork_id`) were NULL on
  all nodes. Now propagated from parent reaches.
- **Dijkstra ghost outlet fix.** Ghost reaches (type=6) with out_degree=0 no
  longer report `dist_out_dijkstra=0`. All sinks are used as Dijkstra sources
  for full coverage, with outlet values following the v17b convention
  (`dist_out_dijkstra = reach_length`). Real outlet counts: NA=7, SA=1,
  EU=2, AF=1, AS=11, OC=1.
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
  downstream-end reporting to upstream-end reporting, matching `dist_out`,
  `hydro_dist_out`, and `dist_out_dijkstra`. Headwater reaches now
  report 0 (was `reach_length`). Node-level values unchanged.
- **F006 junction conservation fix.** 2 remaining junction violations
  (OC 53130100215, AS 45311901585) resolved by setting downstream facc
  to sum of upstream facc. F006 violations: 2 → 0.
- **13 AS `main_side` reverted to v17b.** Reaches had `main_side` changed
  from 0 (main) to 1 (side) by an undetermined prior operation; 9 of 13
  are linear reaches where side-channel classification is impossible.
- **Distance convention documented.** Variable reference now includes a
  convention table specifying endpoint reporting, zero-point, and
  ghost reach behavior for all distance variables, plus node-level
  interpolation formulas.
- **Variable reference updated.** 7 missing variables added, 8 type mismatches
  fixed, multi-dimensional array documentation corrected. (The NetCDF export
  itself uses the v17b `cl_ids [2, N]` format.)
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
  and align single-node `node.dist_out` with the same midpoint convention used
  by the other node-level outlet distances.
- **Ghost coastal outlet dist_out_dijkstra fix.** Isolated ghost coastal
  outlets (type=6 sinks with upstream neighbors but no path to real
  hydrologic outlets) now receive `dist_out_dijkstra = reach_length`,
  matching v17b `dist_out` behavior. Previously these 703 reaches in NA
  (similar counts in other regions) had NULL, but they should report the
  reach-level outlet distance to the ocean outlet point.
- **Exports regenerated (April 1 and 2, 2026).** The initial April 1 bundle
  captured the code fixes above. The April 2 reissue refreshed NetCDF,
  GeoPackage, and Parquet artifacts plus SHA256SUMS after repairing published
  `node_order` / boundary-node orientation metadata, ghost coastal outlet
  `dist_out_dijkstra`, and single-node node-distance conventions.

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

v17c **retains every v17b variable** (no columns removed) and adds new columns
on top. v17b's static fields are preserved unchanged; the only changes to
existing variables are the specific corrections noted below (`facc`, node
`dist_out`, and `lakeflag`/`type`). The additions fall into three groups:

1. **Actual SWOT-derived observational data** (new). For the first time,
   SWORD carries measured SWOT water-surface elevation, width, and slope
   aggregated per reach and node from real overpasses — percentile summaries
   (`wse_obs_*`, `width_obs_*`, `slope_obs_*`), spread statistics, and
   observation counts (`n_obs`). These are new columns derived from SWOT
   observations; they do not modify the v17b static `wse`, `width`, or
   `slope` variables, which are retained as-is.
2. **Computed mainstem topology and routing** (new): mainstem identification,
   best-headwater/outlet routing, new distance metrics, and a globally
   unique `subnetwork_id`.
3. **One correction to an existing v17b variable**: a global
   flow-accumulation (`facc`) denoise that fixes systematic MERIT Hydro D8
   routing artifacts on 95,880 reaches (38.6%).

Alongside these, v17c ships data-quality fixes to v17b-inherited fields
(node geolocation and ordering repairs, lakeflag/type reconciliation). No
reaches, nodes, or centerlines were added or removed. v17c contains the same
248,673 reaches, 11.1M nodes, and 66.9M centerline points as v17b across all
six regions (NA, SA, EU, AF, AS, OC).

Each region is distributed as a single NetCDF4 file
(`{region}_sword_v17c.nc`). The group structure matches v17b
(centerlines, nodes, reaches), and the `area_fits` and `discharge_models`
subgroups under reaches pass through from v17b unchanged. Reach and
centerline arrays match v17b canonical row ordering. Node arrays are
grouped contiguously by `reach_id` and ordered within each reach by
`node_order` (downstream to upstream).

Reach coordinate columns (`x`, `y`, `x_min`, `x_max`, `y_min`, `y_max`)
match v17b values across all formats (NetCDF, DuckDB, PostgreSQL). Node
coordinate columns (`x`, `y`) also match v17b after the v17c continuity
revert.

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
| `dist_out_dijkstra` | float64 | meters | Dijkstra shortest-path outlet distance reported at the reach upstream endpoint (outlet = `reach_length`; values retained for ghost reaches) |
| `hydro_dist_out` | float64 | meters | Mainstem outlet distance reported at the reach upstream endpoint via `rch_id_dn_main` chain (outlet = `reach_length`) |
| `hydro_dist_hw` | float64 | meters | Mainstem headwater distance reported at the reach upstream endpoint via `rch_id_up_main` chain (headwater = 0) |
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
`best_headwater`, and `best_outlet`. Historical flow-correction topology
flips were fully reverted in 0.0.9, so the current v17c database does not
retain a flow-corrected reach set requiring reversed `node_id` order.

All six interpolated distance columns use midpoint offsets:
`offset = cumsum(node_length) - 0.5 * node_length`. `dist_out`,
`hydro_dist_out`, `dist_out_dijkstra`, and `pathlen_hw` use
`reach_value - reach_length + offset`; `hydro_dist_hw` and `pathlen_out`
use `reach_value + reach_length - offset`. This places each node at the
geometric center of its `node_length` segment. On a single-path network
(no junctions), node-level `dist_out`, `hydro_dist_out`, and
`dist_out_dijkstra` are exactly equal.

`node_order` is a node-level variable (not in the reaches table): 1-based
position within a reach, ordered by `dist_out` ascending (1 = downstream
end, n = upstream end).

**Distance convention note.** These are endpoint-reported reach scalars, not
traversal-origin claims. `dist_out`, `hydro_dist_out`, and
`dist_out_dijkstra` are outlet-distance values reported at the reach
upstream endpoint and assign `reach_length` at the outlet. `hydro_dist_hw`
is also reported at the reach upstream endpoint, but measures distance from
`best_headwater` and assigns 0 at the headwater.
See the variable reference for the full convention table.

### 2.2 SWOT Observation Statistics

These are v17c's new SWOT-derived observational columns: water-surface
elevation, width, and slope **measured by SWOT** and aggregated per reach and
node across all qualifying overpasses, as percentile summaries with spread
statistics and observation counts. They are distinct from — and do not modify
— the v17b static `wse`, `width`, and `slope` variables, which are retained
unchanged. All percentile, range, and MAD variables share the units of the
underlying measurement.

**SWOT slope derivation.** Reach `slope_obs_*` values are computed from
pass-level linear regressions of RiverSP node water-surface elevation
(`wse_sm`) against node `dist_out`, then aggregated to reach-level percentiles
across passes. `slope_obs_p50` is populated on 173,732 reaches (the remainder
lack sufficient SWOT slope observations); of these, 9,988 have a negative
median slope, flagged via `slope_obs_quality` / `slope_obs_q`.

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
| `slope_obs_p10`–`slope_obs_p90` | float64 | m/m | Slope percentiles |
| `slope_obs_range` | float64 | m/m | Slope observation range |
| `slope_obs_mad` | float64 | m/m | Slope median absolute deviation |
| `slope_obs_adj` | float64 | m/m | Adjusted slope |
| `slope_obs_slopeF` | float64 | — | Slope F-statistic |
| `slope_obs_reliable` | int32 | — | 0 = unreliable, 1 = reliable |
| `slope_obs_quality` | int32 | — | Integer quality category (0–8; see Section 3) |
| `slope_obs_n` | int32 | — | Number of RiverSP node observations used in pass-level slope fits |
| `slope_obs_n_passes` | int32 | — | Number of SWOT passes used |
| `slope_obs_q` | int32 | — | Bitfield quality flag (see Section 3) |

### 2.3 Flow Accumulation Corrections

A two-stage denoise pipeline corrected flow accumulation (`facc`) values
to address three systematic error modes in MERIT Hydro's D8
(eight-direction flow routing) upstream area: bifurcation cloning,
junction inflation, and raster-vector misalignment. In the v17c database,
95,880 of 248,673 reaches (38.6%) carry corrected values
(`facc_quality = denoise_v3`). Uncorrected reaches retain v17b values.

Relative to v17b, the correction raised `facc` on 80,538 reaches
(median +60%) and lowered it on 15,342 (median 30% decrease). The largest changes
are at bifurcation children, which under D8 each inherit the full parent
drainage area; v17c splits that area width-proportionally among the
distributary branches. See
[facc_correction_methodology.md](technical/facc_correction_methodology.md)
for the full algorithm description.

| Variable | Type | Group | Description |
|----------|------|-------|-------------|
| `facc` | float64 | reaches, nodes | Flow accumulation (km^2). Corrected values where applicable; v17b values otherwise. |
| `facc_quality` | int32 | reaches, nodes | 1 = corrected by denoise_v3; fill_value = not flagged |

After correction, junction conservation violations (downstream facc < sum
of upstream facc) are resolved in all regions. (In 0.0.2, facc was
additionally recomputed at 807 then-flow-corrected reaches via topological
propagation; the flow-correction topology itself was fully reverted to
v17b in 0.0.9.)

### 2.4 Other New or Updated Variables

`dl_grod_id` integrates a newly added obstruction dataset, DL-GROD (Deep
Learning Global River Obstruction Database; He et al. 2025), populated on
26,120 reaches.

| Variable | Type | Group | Description |
|----------|------|-------|-------------|
| `type` | int32 | reaches | Reach classification (1=river, 3=lake_on_river, 4=dam, 5=unreliable, 6=ghost). Not present in v17b NetCDF; added in v17c so NetCDF users can filter by reach type without needing the database. |
| `dl_grod_id` | int64 | reaches | DL-GROD (Deep Learning Global River Obstruction Database; He et al. 2025) dam/obstruction ID; populated on 26,120 reaches |
| `edit_flag` | string | reaches | Comma-delimited edit provenance tags (e.g., `harp_lake,clf_reconcile`). Also contains v17b numeric codes and the literal string `NaN` (v17b's no-edit placeholder); see the variable reference for the full value list |

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

The working database also carries a `below_noise` category (427 reaches, slope
below the SWOT noise floor) that is not among codes 0-8; in this release those
reaches export with the `slope_obs_quality` fill value (-9999). They still
carry a `slope_obs_p50` value, so filter on `slope_obs_p50 != -9999` rather
than on `slope_obs_quality` alone when selecting reaches with a computed slope.

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

- **facc correction scope:** 95,880 reaches corrected (38.6%); the
  remaining 152,793 retain v17b values. Node-level facc propagates from
  the parent reach.

- **Lake sandwich corrections:** 1,252 reaches were reclassified to
  `lakeflag = 1` where a narrow, shorter-than-neighbor reach sat between
  lake reaches. The later lakeflag/type reconciliation (0.0.10) rewrote
  many `edit_flag` tags, so 483 reaches carry a `lake_sandwich` tag in
  the v17c database. ~1,755 similar cases remain (narrow connecting
  channels, chains).

- **HarP lake corrections:** 7,425 reaches were reclassified from
  `lakeflag = 0` (river) to `lakeflag = 1` (lake) based on HarP v1.1
  (Hydrography and River Planform) lake classification data, with child
  node lakeflag updated to match (node lakeflag matches the parent reach
  on all 11,112,454 nodes as of 0.0.11). The later lakeflag/type
  reconciliation (0.0.10) folded many of these into combined tags, so
  3,981 reaches carry a `harp_lake` tag in the v17c database; node
  `edit_flag` is not tagged. Tags are comma-delimited when multiple
  apply.

- **Neighbor-array fill convention (vs v17b):** In the NetCDF `rch_id_up` and
  `rch_id_dn` `[4, N]` arrays, empty neighbor slots use the fill value
  `-9999`, whereas v17b used `0`. The neighbor relationships themselves are
  identical to v17b; only the empty-slot sentinel differs. Code that diffs the
  raw arrays against v17b, or that treats `0` as "no neighbor," should account
  for this.

- **Swapped neighbor vectors on two reaches (GeoParquet/DuckDB only):** Reaches
  45220300256 (AS) and 71382000696 (NA) have their denormalized
  `rch_id_up_1..4` / `rch_id_dn_1..4` columns swapped in the GeoParquet and
  DuckDB exports (the single downstream neighbor appears in `rch_id_up_1`, and
  `rch_id_dn_1..4` are 0). The canonical NetCDF `rch_id_up`/`rch_id_dn` arrays
  and the topology table are correct for both reaches; only the denormalized
  vector columns in these two formats are affected.

- **area_fits and discharge_models:** Direct copies from v17b. Not
  recomputed against v17c facc or SWOT values.

- **`subnetwork_id` vs `network`:** `subnetwork_id` uses Pfafstetter-
  offset enumeration (globally unique). v17b `network` uses per-region
  1-based IDs. Different component counts (v17c finds more via weakly
  connected components; 19 subnetworks span multiple v17b networks).
  `network` is retained unchanged from v17b.

- **Flow correction fully reverted:** experimental flow-direction
  corrections (810 reaches at peak, including 389 with ambiguous
  oscillating WSE slope signals) were fully reverted to v17b topology in
  0.0.9 after a scoring tautology was found. v17c topology is
  identical to v17b; `v17c_flow_corrections` is empty.

- **main_path_id consistency:** 19 `(best_headwater, best_outlet)` tuples map
  to more than one `main_path_id` (V015 lint check), affecting 230 reaches.
  The related continuity checks (V013, V014) and `best_headwater` validity
  (V007) report zero violations.

- **River naming:** 51.2% of reaches are unnamed (NODATA), ranging from
  26% (AF) to 69% (OC). 2.6% of mainstem 1:1 links have local name
  discontinuities (name changes between adjacent reaches with no junction).

- **Width fill values (A003):** ~1,266 reaches have `width=0` (unmeasured) or
  `width=-1` (GRWL lake fill). These are v17b fill values, not data errors.
  Present in both v17b and v17c unchanged. The A003 lint check is downgraded
  to WARNING for this reason.

- **Node geolocation corrections deferred:** The 0.0.8 and 0.0.10
  rederived-node coordinate edits were reverted in v17c to preserve SWOT
  D0-D2 time-series continuity. Some node sequence/geolocation anomalies
  inherited from v17b therefore remain. Treat full geolocation repair as a
  v18 or separately approved release-envelope change.

---

## 5. Quality Audits

Validation checks performed on the v17c data:

| Audit | Finding |
|-------|---------|
| **Flow accumulation (facc)** | 95,880 reaches (38.6%) corrected via the denoise pipeline (`facc_quality = denoise_v3`); the flag exactly matches the set whose `facc` differs from v17b (0 flagged-but-unchanged, 0 changed-but-unflagged). Raised on 80,538 reaches (median +60%), lowered on 15,342 (median 30% decrease). Junction-conservation (F006) and downstream-monotonicity (T003) violations resolved in all regions. |
| **SWOT observation data (`wse_obs_*`, `width_obs_*`, `slope_obs_*`)** | New SWOT-derived columns aggregated from real overpasses; they do not modify the v17b static `wse`/`width`/`slope`. `slope_obs_p50` populated on 173,732 reaches (9,988 negative, flagged via `slope_obs_quality`); derived from pass-level node-WSE regressions (see §2.2). |
| **Geometry** | DuckDB geometries (rebuilt from NetCDF) lack endpoint overlap vertices present in v17b (210,533 reaches affected: 173K +1 point, 37K +2 points). `reach_length` unchanged. Reach coordinate columns (`x`, `y`, `x_min`, `x_max`, `y_min`, `y_max`) copied from v17b to ensure consistency across all formats. |
| **Node coordinate continuity** | v17c restores v17b node `x`/`y`, `node_length`, `cl_id_min`, and `cl_id_max` for the 344 previously rederived reaches plus OC reach 51111300061 split-revert residue. Global node coordinate diff vs v17b NetCDF: 0. |
| **n_nodes / reach_length** | Internally consistent. Zero N008/G002/G003 violations. |
| **path_freq gaps** | v17b had 4,952 connected non-ghost reaches with invalid path_freq (0 or -9999). Resolved in v17c; remaining nodata values are correctly attributed to ghost reaches (type=6). |
| **subnetwork_id** | 3,027 components across 248,673 reaches verified. Pfafstetter banding correct. Zero cross-region collisions. 19 subnetworks (0.6%) span multiple v17b networks (expected). |
| **Topology integrity** | T001 (dist_out_dijkstra monotonicity), T012 (referential integrity), T013 (self-reference), T014 (bidirectional): all pass. T005/T007: zero non-reciprocal edges (151 from incomplete flow correction revert resolved in beta 0.0.1). Historical flow-correction topology flips were fully reverted in 0.0.9; `v17c_flow_corrections` is empty in the current database. Node-level `dist_out` is recomputed for all reaches using the midpoint interpolation convention. |
| **n_rch_up/n_rch_down** | 148 scalar count mismatches corrected (flow corrections flipped reach_topology but did not recalculate counts). Zero mismatches across all 248,673 reaches. |
| **OC reach split revert** | Incomplete `break_reaches()` split of OC reach 51111300061 (434 orphan centerlines, 73 orphan nodes) fully reverted to v17b state. |
| **River name formatting** | 291 formatting issues corrected (separators, whitespace). Automated checks now enforce "; " separator and alphabetical ordering. |
| **Flow direction** | Experimental topology flips were ultimately reverted. The 1,112-flip experiment caused ~30K disconnected reaches and was rolled back; the later retained flow-correction family was also fully reverted in 0.0.9 after the scoring tautology was found. Current v17c does not retain topology that differs from v17b because of this flow-correction pipeline. |
| **HarP lake corrections** | 7,425 reaches reclassified lakeflag 0 to 1 from HarP v1.1 data, with node lakeflag propagated from parent reaches. After the 0.0.10 lakeflag/type reconciliation rewrote tags, 3,981 reaches carry a `harp_lake` tag in v17c. |
| **lakeflag/type consistency** | ~6,200 inconsistent reaches reconciled (0.0.10) via 1,015 manual reviews, a gradient-boosted classifier (82% precision, applied only at high confidence), and HarP v1.1 corrections. All 248,673 reaches are consistent under the two primary rules (no lakeflag=1 with type=1, no lakeflag=0 with type=3); some rarer lakeflag/type edge combinations (e.g. lakeflag=3 tidal with type=1) remain and are expected. `type` is authoritative and diverges from the reach ID last digit on 2,648 reaches (1.1%) in v17c due to in-place corrections. |

For POM (Pierre-Olivier Malaterre) validation results, see
[pom_validation_report.md](technical/pom_validation_report.md).

---

## 6. File Formats

v17c is distributed in five formats. The NetCDF files are the
canonical release artifact (full group structure including centerlines and
v17b subgroups); the other formats carry reaches and nodes with geometry.

- **NetCDF4:** `{region}_sword_v17c.nc`, one file per region, where
  region is `na`, `sa`, `eu`, `af`, `as`, `oc`
  - Groups: `centerlines`, `nodes`, `reaches` (plus `reaches/area_fits`
    and `reaches/discharge_models` subgroups from v17b)
  - Ordering: reach and centerline arrays match v17b canonical ordering;
    node arrays are reach-contiguous and sorted by `node_order`
  - Fill value: -9999 for all numeric variables (int32, int64, float64)
- **GeoPackage:** `sword_{REGION}_v17c.gpkg` per region
  (reaches and nodes layers)
- **Shapefile:** `{region}_sword_{reaches,nodes}_hb{XX}_v17c.shp`,
  split by HydroBASINS Pfafstetter level-2 basin within each region (v17b
  convention; required by the shapefile 2 GB size limit). Shapefile DBF
  format truncates attribute names to 10 characters, so fields use
  abbreviated names; the mapping table
  `shapefile_field_name_mapping.csv` ships alongside the shapefiles. The
  NetCDF/GeoPackage/GeoParquet names are authoritative.
- **GeoParquet:** `sword_{REGION}_v17c_{reaches,nodes}.parquet`
  per region
- **DuckDB:** `sword_{REGION}_v17c.duckdb` per region, with
  `reaches` and `nodes` tables (geometry stored as GEOMETRY type; written
  with DuckDB 1.3 — open with DuckDB >= 1.3 and the spatial extension)
- **Global files:** in addition to the per-region files, whole-planet
  merged tables are provided as `sword_global_v17c_{reaches,nodes}.parquet`
  and `sword_global_v17c_{reaches,nodes}.duckdb` (all six regions
  combined; each table carries a `region` column).
- **Checksums:** SHA256 hashes for all distributed files listed in
  `SHA256SUMS.txt`

---

## 7. Methodology Documentation

| Document | Description |
|----------|-------------|
| [facc_correction_methodology.md](technical/facc_correction_methodology.md) | Facc denoise algorithm, detection rules, correction model |
| [pom_requests_summary.md](technical/pom_requests_summary.md) | POM validation check tracker (19 checks, production results) |
| [pom_validation_report.md](technical/pom_validation_report.md) | POM validation production results |
| [v17c_variable_reference.md](v17c_variable_reference.md) | Complete variable catalog for NetCDF export |
| [SWORD_v17b_Technical_Documentation.md](technical/SWORD_v17b_Technical_Documentation.md) | v17b baseline reference |
