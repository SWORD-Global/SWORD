# Global FACC correction: critical assessment and route to full coverage

Prepared by Fable (Claude) for Jake Gearon, 2026-10-01. Read-only review of the checkout
`/Users/jakegearon/projects/SWORD` (branch `codex/noam-node-support-facc`, HEAD `60dd958`),
the frozen artifacts under `output/facc_detection/global_assessment_20260930/global_execution/`
(called `GX` below), the NOAM tree under `output/facc_detection/node_support_rederivation_20260912/`
(`NS`), `PROGRESS.md`, the handover documents, and the Sol/Astra conversation archive.
Nothing in the databases, accepted products or frozen bundles was modified. New work lives only
under `output/facc_detection/fable_review_20261001/`.

All numbers below were verified against the cited artifacts or produced by the two new builds in
Section 7; agent-derived statements are cited to file and line where they depend on code.

---

## 0. Executive assessment

1. **The 4.08 M missing nodes are vetoed matches, not unmatched rivers.** In the final census
   (20,828 residual cases, 4.09 M nodes) only 12,125 nodes (0.3%) had no candidate D8 route.
   3.42 M nodes are `ROUTE_WITHHELD`: a route was found and a gate rejected it. Another 0.66 M
   are withheld at "ownership" or "registration" after an accepted route. 87,643 candidate routes
   were examined and discarded.
2. **The gates are not scientific constraints; they are an implementation that manufactures
   missingness.** Every decision is boolean (no likelihood, no prior, no width). Candidate routes are
   single whole-section D8 traces seeded only in the upstream quarter, so one divergence anywhere
   makes every alternative wrong for the rest of its length; the proximity gates then reject them
   (median coverage within 1 km on failing cases is 0.52, not a near miss). 968 K nodes pass every
   gate and are still withheld (ownership, ambiguity, registration). 1.19 M fail only "identity not
   demonstrated", a proof that needs a complete source-qualified MHV chain and cannot be built for
   84% of the missing population. The MHV "contradiction" veto (711 K) is mostly route-independent
   (SWORD's own projection steps backwards on a link) and vetoes the Blue Earth control itself through
   an 80%-overlap cardinality artifact that the control checker then pins in place. The executed
   engine has no bifurcation share model at all: 527 K nodes on split-terminal sections are
   unassignable by construction.
3. **The missing population is where the correction matters most.** 73% of the 27,810 reaches where
   released v17c exceeds v17b by more than 2× are in the missing population (the Lena delta reaches:
   v17b 2.45 M km², v17c 682 M km², engine value stops upstream of the delta).
4. **Compute is not the bottleneck.** The last full pass over all 20,828 residual cases took
   ~16 CPU-hours (≈40 min wall on 42 workers). A full-globe rerun with different decision rules is
   ~1.5 h of wall time on River. The ~180 run directories produced in 25 h were orchestration,
   audits and censuses, not method.
5. **The method that worked was replaced in production.** On 2026-09-08/09 Jake and Sol defined
   the right rules (geometry chooses the route, UPA checks it afterward, generous corridor, D8
   continuity rescues weak geometry, "rejected cannot mean permanently unmapped"), reaching 20/22 and
   then 22/22 NA basins and 94% of 454 outlets within 20% of HydroBASINS. The NOAM expansion
   drivers then ran with the strict `MatchRules` defaults (coverage ≥ 0.98 within 250 m, median
   ≤ 150 m) and later `ConnectivityPolicy` v2 (Spearman ≤ −0.98, reversals ≤ 2%). The continuity
   rescue tier was never wired into the expansion drivers.
6. **What the new values actually are:** a careful resample of the same MERIT UPA raster. On the
   155 K reaches with engine values, new/v17b has median 1.000 and IQR [0.97, 1.03]; ~20% differ
   from v17b by more than 20%, ~5% are below half of v17b. The value added is (a) fixing v17b's
   wrong-river samples and (b) undoing v17c's junction-sum inflation. On 4,423 HydroBASINS
   outlets: median |Δ| 0.68% (new) vs 2.38% (v17b) vs 22.8% (v17c).
7. **Full coverage exists now, with flags, in two products.** A network-constrained fill
   (Section 7.1) gives every one of the 11,112,454 nodes and 248,673 reaches a value and a tier in
   9 seconds (63% engine, 4% bounded, 33% cleaned v17b between trusted bounds): 83.3% of 6,262
   HydroBASINS outlets within 20%, median |Δ| 1.1%, against 72.7% / 3.4% for v17b and 41.9% / 30.9%
   for v17c. The **final product** (Section 7.3b) adds the global matcher run from River: 63.3%
   engine, 32.0% matcher, 3.4% bridged, 1.3% fill; tier E falls to 0.5% of reaches; 84.4% of outlets
   within 20%, median 0.96%; on the 1,839 outlets with no engine value 70.9% (v17b 62%, v17c 29%).
8. **The upgrade for the weak third is prototyped and measured.** A probabilistic section
   matcher (HMM/Viterbi: distance is evidence, D8 connectivity is the transition model, no old-facc
   input) reproduces the engine's accepted values at 99.1% within ±5% on 348 K NA nodes, runs the
   whole covered NA set in 4 seconds, and on engine-withheld sections yields usable, confidence-
   flagged values for 78–83% of nodes (89–92% with a tighter lost-stretch rule), with the genuinely
   divergent stretches flagged for bridging (Section 7.2). At basin outlets it is level with v17b
   and far above v17c; its gain is the interior profile, which outlets do not test. A global run is
   ~20 CPU-minutes of matching plus tile I/O: 1–2 h wall on River, then the fill and report re-run
   in seconds.

**In five sentences, for the record.** v17b read MERIT's drainage area at the nearest river cells
and sometimes read the wrong river. v17c tried to make those values conserve by only ever raising
them downstream, which summed the wrong-river errors into the mainstems. The September effort
re-read MERIT along each river section with a flow-direction trace, which works, but wrapped it in
pass/fail rules strict enough to throw away a third of the world. This review replaces the rules
with a likelihood (how far, how connected, how confident), keeps everything the engine got right,
and bounds everything it did not between trusted neighbours. The product is a value and a confidence
tier for every node, with the external check reported per tier.

---

## 1. What was actually done (plain language)

### 1.1 The problem being corrected

- **v17b** sampled MERIT Hydro UPA with a nearest-pixel rule: the 20 nearest "river" pixels
  (UPA > 10 km², 100 km² on some tiles), median normally, but the **maximum of the 20 when the
  centerline sat more than 100 m from the nearest river pixel**, then smoothed along GRWL segments
  (`src/_legacy/development/merging_databases/Merge_Databases_v06.py:186-224`,
  `Merge_Tools_v06.py:975`). Where a tributary mouth sits near a mainstem, the tributary inherits
  the mainstem's area (Gasconade and Missouri both got pixel 1,337,230.5 km²). Lakes received the
  segment maximum everywhere (557,997 km² on 16 consecutive Lake Huron reaches). Within a reach the
  node profile is smooth; the error is wrong-river, not per-node jitter (NA: only 360 of 38,696
  reaches have an upstream node exceeding a downstream node by >5%).
- **v17c** (Feb 2026 biphase pipeline, `docs/technical/facc_correction_methodology.md`;
  `correct_facc_denoise.py:328-380`) enforced conservation in topological order: junction =
  Σ(corrected upstream) + lateral, 1:1 link = corrected parent + max(base − parent base, 0),
  width-proportional bifurcation splits, isotonic smoothing. Every rule is one-sided: it raises,
  never lowers, and a child's own good sample is discarded in favour of parent + increment. So a
  parent error never decays, offsets add at junctions, and where v17b had cloned a mainstem value onto
  a tributary the junction rule **sums the clone** (Missouri 1.34 M → 3.56 M km²; Kansas 154 K →
  1.08 M; Lena 2.46 M → 682 M, ×278; 145 Lena reaches exceed Earth's land area). The v2 pipeline
  (2026-02-09) had a guard, "raised parent: do NOT propagate", which held the Lena to ×3; the guard
  was deleted in commit `e976fb2` (2026-02-12, message "T003=0, F006=0"). T003 and F006 are
  lower-bound inequalities, satisfiable by inflation; no upper-bound check exists and no facc check
  is in any release gate. Net effect: 95,880 reaches changed (80,538 raised, 15,342 lowered); Σ facc
  grew ×1.7 (OC) to ×15.2 (AS). At 6,262 HydroBASINS outlets the median |Δ| went from 3.36% (v17b)
  to 30.9% (v17c); within-20% from 73% to 42%. This is what Mike Durand's 2026-08-25 email flagged:
  conserved, but magnitudes "way off". Inflation is concentrated in large basins (≥ 1e6 km²: median
  ratio 3.35, 78% above 2×), side channels (main_side 1/2: 61% / 51% above 2×), tidal (35%) and
  canals (43%).

### 1.2 The rederivation method (as settled 2026-09-12, still the stated boundary)

For each **physical section** (maximal chain of reaches with one upstream and one downstream
neighbour; 43,218 globally, `GX/method_readiness/contexts/{R}/section_inventory.parquet`):

1. Seed D8 traces near the section's upstream end on MERIT's flow-direction grid; follow the
   grid downstream; keep traces that reach the section's downstream end.
2. Score candidate routes by **geometry only** (centerline coverage, endpoint distances, median
   line distance). Drainage-area magnitude does not enter route choice (agreed 2026-09-08 as the
   anti-circularity rule).
3. Register every SWORD node, in downstream order, to its own stretch of the chosen route
   ("ordered primary-owned centerline support"); node FACC = max native UPA over its support;
   reach FACC = max(node). No median station, no per-node route choice.
4. Incoming arms (tributary sections) are mapped independently; the junction position on the
   route must be consistent ("incoming ownership").
5. Bifurcations: split/share/rejoin accounting with width-proportional shares (the v17c rule),
   counted once at the rejoin.
6. MHV (MERIT HydroVector; attributes `LINKNO, strmOrder, strmDrop, lengthkm, Slope, fromnode,
   tonode`, no area field) is used as a topology/identity bridge, qualified by Pfaf-2 basin file.
7. HydroBASINS is evaluation-only.

### 1.3 The gates that were actually applied (from the shared modules)

| Rule set | Date | Thresholds |
|---|---|---|
| `MatchRules` strict tier (`scripts/analysis/match_merit_sword_segments.py:30-44`) | 09-06 | coverage within 250 m ≥ 0.98; endpoint ≤ 250 m; start ≤ 250 m; median line ≤ 150 m; score margin ≥ 0.20 |
| `MatchRules` review tier | 09-06 | coverage within 500 m ≥ 0.90; endpoint ≤ 1,000 m; start ≤ 2,000 m; median ≤ 500 m |
| `ContinuityRules` (`prototype_merit_continuity_rescue.py:45-66`) — the tier that reached 20/22 and 22/22 NA basins | 09-08 | local coverage ≥ 0.75 within 1 km; local median ≤ 500 m; D8 continuity + non-decreasing UPA rescue weak geometry |
| Node `Rules` (`assign_merit_section_nodes.py:29-39`) | 09-12 | each support point ≤ 1,000 m; reach median ≤ 500 m; section coverage ≥ 0.90 within 500 m; 12,000 trace steps |
| `ConnectivityPolicy` v2 (`noam_connectivity_policy.py:22-64`) — used by the NOAM and global drivers | 09-22 | coverage ≥ 0.75 at 500 m **and** ≥ 0.95 at 1,000 m; median ≤ 500 m; endpoint/start ≤ 2,000 m; anchors ≥ 0.80 within 1 km; **Spearman(node index, station) ≤ −0.98; reversals ≤ 2%**; MHV interior identity opt-in (09-29): ≥ 6 anchors, coverage 0.80, join tolerance 0.25 / 1,000 m |
| Network-first (`global_network_first_joint_source_proof_v1_20261001.py:1-8`) | 10-01 | "No distance or area threshold selects a river. A unique directed corridor that retains every original anchor and every physically available incident role can certify identity." Controls only; never dispatched. |

### 1.3b What the frozen global engine actually executes (verified in code)

Roots: `ENGINE` = `GX/river_execution/global-mhv-guided-full-residual-v4-20261001T154600Z/code`;
`FROZEN` = `GX/river_execution/global-fullscope-controls-v3-20260930T184840Z/frozen_sources` (the
bundle that ran on River; four repo copies under `scripts/analysis/` have drifted from it, read the
frozen ones). The driver strips `scripts/analysis` from `sys.path` and imports only the bundle
(`ENGINE/run_global_facc_mhv_guided_residual_v2_20261001.py:103-109`).

**Candidate routes.** Start cells are spread over the **upstream quarter** of the SWORD line only;
each anchor contributes the k nearest cells with UPA > 10 km² within 14/28/56-cell radii (three
schedules, `FROZEN/noam_connectivity_policy.py:26-30`; `FROZEN/prototype_d8_continuity_snap.py:128-182`).
Each start is traced downstream along D8 for up to 12,000 cells with no UPA-based stop, and the
route is trimmed at the trace cell nearest the SWORD downstream end
(`FROZEN/match_merit_sword_segments.py:110-137`). Routes converging within their first 25% are
grouped and only the best legacy-scored member survives (`:78-107`). Mean 4.2 alternatives per case,
max 172; 87,643 in total. Consequence: a route is a single whole-section trace; if it leaves the
SWORD river anywhere (braid, lake, canal, wrong valley at the seed), every alternative is wrong for
the rest of its length, and the census shows exactly that (median coverage within 1 km = 0.52 on
failing cases).

**Gates** (all evaluated per alternative in `structural_reasons`,
`FROZEN/noam_connectivity_policy.py:196-263`, thresholds `:26-64`; all hard; no soft score anywhere):

| Gate | Measure | Threshold | MHV can waive | Line |
|---|---|---|---|---|
| coverage_500m_below_policy | fraction of centerline points within 500 m of route | < 0.75 | yes | :219 |
| coverage_1000m_below_policy | fraction within 1 km | < 0.95 | yes | :221 |
| median_line_distance_above_policy | median point-to-route distance | > 500 m | yes | :223 |
| downstream_endpoint_above_policy | route end to SWORD downstream end | > 2,000 m | yes | :225 |
| upstream_start_above_policy | start cell to SWORD upstream end | > 2,000 m | yes | :227 |
| ordered_anchor_support_below_policy | fraction of ≤ 9 anchors within 1 km | < 0.80 | yes | :229 |
| section_order_not_preserved | Spearman(point index, nearest route step) | > −0.98 or NaN | **no** | :231 |
| section_order_reversals_above_policy | fraction of consecutive points whose step goes up | > 0.02 | **no** | :234 |
| native_upa_decreases_downstream | any UPA drop > max(0.01 km², 1e-6·UPA) along route | any | **no** | :237-240 |
| dominant_contribution_occurs_off_section | largest UPA step (Δ/UPA ≥ 0.05) more than 2 km from SWORD | any | **no** | :241-249 |
| mhv_interior_identity_contradicted | MHV disposition CONTRADICTS_* | any | (is the veto) | :250 |
| same_river_identity_not_demonstrated | identity not CONFIRMED / NO_COMPETING | any | yes | :258 |

Selection (`:291-374`): zero eligible → `no_structurally_eligible_route`; one → accept; several →
accept only a unique MHV-compatible route or a unique strict Pareto dominator, else
`ambiguous_structural_routes`.

**The MHV waiver** ("compatible full MHV identity", `FROZEN/noam_mhv_interior.py:171-310, 420-589`)
requires, per Pfaf-2 asset: 6–96 anchors; every anchor within min(5 km, nearest + 50 m) of a link;
≥ 80% unambiguous; consecutive links joined by a unique directed path ≤ 2,048 hops (a path only in
reverse = CONTRADICTS); no backward station step larger than max(1 m, 1% of link length)
(= CONTRADICTS); both the SWORD line and the route mapped this way; and ≥ 80% overlap between the two
link chains in both directions (else CONTRADICTS). Arms must join within 25% / 1 km of the expected
position. Separately, `NO_UNIQUE_COMPLETE_SOURCE_MHV_CHAIN` and the like are *guidance* statuses that
only decide whether extra start cells are added (`ENGINE/global_mhv_guided_native_starts_20261001.py:163-236`).

**Assignment.** Points register to route steps by dynamic programming (monotone, min squared
distance) under distance caps 1,000/1,500/2,000/3,000 m (+5,000 with MHV); values must be
bit-identical at every feasible cap (`FROZEN/merit_node_registration.py:52-162`). Node = max UPA over
its registered points; reach = max over nodes. Any unresolved incoming arm or any split inside the
section → `OWNERSHIP_REVIEW_WITHHELD` (`FROZEN/pilot_noam_connectivity_routes.py:1391-1486`).
There is **no bifurcation share model in the executed engine**: split-terminal sections cannot be
assigned by construction (2,868 cases / 527 K nodes), and the never-run `assemble()` would mark every
reach from a split to its rejoin as `COMPONENT_SHARE_BINDING_REQUIRED` (≈ 27 K reaches / 1.2 M nodes).

**Priors and scoring.** No v17b/v17c facc, width, stream order or path_freq enters selection or
values. v17b facc appears only in the (unrun) assembly monotonicity audit.

**Cost.** 0.8–9.7 s per case; 7–39 min per region; the whole 20,828-case residual in ≈ 40 min wall.

**Saved route evidence** (per case, on River under
`/not_backed_up/jgearon/SWORD/runs/global-mhv-guided-full-residual-v4-20261001T154600Z/method_readiness/runs/guided_full_residual_v2/{REGION}/cases/<sha24>/`;
only the 10 controls are local): `distinct_routes.parquet` (per representative route: step, row,
col, x, y, UPA), `route_evidence.json` (all gate metrics, anchors, identity and MHV dispositions),
`interior_mhv_evidence.parquet`, `decision.json`, `outcome.json`, bound MHV crops; if assigned,
`assignment/sections/*/support.parquet`. About 3.3 MB per case (route-only part 0.25–0.85 MB).
Geometry, order, UPA, contribution and MHV gates can be re-scored offline from this; new seeds,
longer traces and arm re-matching need the rasters.

### 1.3c Specific defects in the executed gates (with recoverable populations)

1. **The MHV veto is route-independent.** The contradiction usually comes from SWORD's *own*
   projection onto MHV stepping backwards along a link (tolerance max(1 m, 1% of link length),
   `noam_mhv_interior.py:269-278, 461-465`), which is identical for every candidate route. In 1,086
   cases (277 K nodes) the best route passes every geometry, order and UPA gate with identity
   confirmed and dies only here; the SWORD-lane-only population is 2,767 cases / 500 K nodes. A
   SWORD-lane reversal should yield CANNOT_JUDGE, not CONTRADICTS.
2. **The 80% chain-overlap test is a cardinality artifact.** Sections end at confluences, so the
   route chain often has one more MHV link than the SWORD chain. Control `NA:v17c_chain_74288200046`
   (Blue Earth) has coverage 1.0, median 51 m, endpoint 30 m, ρ = −0.99999, arms confirmed, and is
   vetoed because 3 links vs 4 links gives overlap 0.75. ≈ 658 cases / 78 K nodes. The control
   checker (`ENGINE/check_global_mhv_guided_controls_v2_20261001.py:72-73`) requires identical
   statuses, so fixing this "fails controls" by design.
3. **Spearman on tied data.** About three centerline points fall on each MERIT cell, which caps |ρ|
   below 0.98 for short sections even when perfectly ordered (3 cells × 3 points → 0.949); a section
   collapsing onto one cell gives NaN → fail (`policy:112-115`).
4. **Ownership cascade.** 2,129 cases / 347 K nodes are withheld because a sibling arm's route was
   withheld; the arm catalog is frozen within a run, and sibling arms are re-judged by a *stricter*
   matcher that rejects on a 0.20 score margin, contradicting the policy's own rule that margins do
   not resolve ambiguity.
5. **The identity waiver dies on one outlier anchor** (one anchor on a sibling link makes the MHV
   graph look disconnected → CANNOT_JUDGE → no waiver): 1,641 cases / 554 K nodes.
6. **Ambiguity withholds without checking whether it matters**: 1,455 cases / 310 K nodes, usually
   two eligible routes whose node values may be identical.
7. **Seeding** by nearest cells with UPA > 10 km² over the upstream quarter can miss the main stem
   entirely; guided starts help only when guidance is READY (21% of cases).

### 1.4 How the effort went circuitous (dated)

- 08-29 → 09-12 (Sol): diagnosis, reproduction of v17b to 0.05%, D8 route prototype, section-level
  matching, continuity rescue (20/22 → 22/22 NA basins), MHV receiver resolver, full NA envelope
  78.3% of sections mapped, 454 outlets at 1.14% median error. Jake catches the median-station
  shortcut on 09-12 ("why wouldn't we continually match it as granularly as we can at the sword node
  level").
- 09-12 → 09-30 (Astra advising, Sol implementing): node-level assignment with new acceptance gates
  bolted on at the same time (Astra, 09-13: "I changed the acceptance rules before establishing
  which rules the physical problem actually required"). Coverage grows by 14 recovery batches:
  399,818 nodes carried from the 09-11 cohort + 385,400 added over nine days (largest single gain,
  +124,935 nodes, was a bookkeeping fix: the topology audit had been comparing new values to
  unrepaired v17b neighbours). 57 `REVIEW_*` documents, 32 `v22_*` directories. MHV demoted to
  diagnostic on 09-22; Jake discovers on 09-29 that it "cannot change which route gets selected".
  Astra, 09-29: "I kept directing recovery of successive failure groups ... that approach became the
  delivery strategy, which was inadequate." Pause 09-30 04:13 UTC at 46% of NOAM nodes.
- 09-30 → 10-01 ("Root" session, global): v5 32.7% (empty incoming-route catalogue, a defect) →
  v10 61.7% → v18 63.3%. After v10, 163 River run directories added 180,498 nodes (+1.6 pp).
  The whole 63% is "calculated", not "proposed": `proposed_nodes = 0` in every integrated ledger;
  5.7 M nodes sit in `GLOBAL_CONTROL_QUALIFICATION_REQUIRED`. Blue Earth, the control that
  motivated node-level assignment, is "withheld by route evidence" in the global engine.

---

## 2. Why the 4.08 M nodes are missing

### 2.1 Node-weighted census (final, `GX/river_execution/global-mhv-guided-mechanism-census-v5-.../retrieved/outputs/`)

| Mechanism (disjoint) | Cases | Missing nodes | Share |
|---|---:|---:|---:|
| geometry_distance_fraction (coverage/median/endpoint gates) | 4,838 | 1,227,970 | 30% |
| mhv_contradiction | 4,343 | 711,278 | 17% |
| ownership_review (route accepted, tributary join unresolved) | 3,417 | 567,921 | 14% |
| identity_not_demonstrated | 1,783 | 560,959 | 14% |
| section_order (Spearman/reversal gate) | 3,948 | 356,033 | 9% |
| route_ambiguity (two plausible routes, neither chosen) | 1,455 | 310,181 | 8% |
| contribution_position (dominant UPA jump "off section") | 666 | 227,267 | 6% |
| ordered_registration + registration_sensitivity + native_prefix | 131 | 105,319 | 3% |
| **executed_search_no_distinct_route (true search failure)** | 213 | **12,125** | **0.3%** |
| execution error | 20 | 20 | 0 |

Outcome status: `ROUTE_WITHHELD` 3,421,549 nodes; ownership/registration withheld 657,504.
MHV guidance stage: `SOURCE_GUIDANCE_WITHHELD` for 3,433,884 nodes (84%) because no unique complete
source-qualified MHV chain could be built; the identity waiver therefore never applies to them.

By waterbody class (missing nodes): ordinary river 2,968,803; lake-on-river 471,449; tidal 470,734;
other 117,180; canal 32,129; dam 12,005; ghost 6,773.

### 2.2 What the proximity numbers actually look like

Two populations, two pictures, one conclusion.

NOAM route-failure diagnostics, 2026-09-21 (`NS/noam_route_failure_diagnosis_v1/point_diagnostics.parquet`,
3,916,339 centerline points on 1,391 failed sections):

| Statistic | Value |
|---|---|
| median point-to-nearest-saved-route distance | 60 m |
| points within 250 / 500 / 1,000 / 2,000 m | 82% / 86% / 89% / 91% |
| p90 / p95 / p99 | 1.4 km / 14.7 km / 139 km |
| sections with ≥ 90% of points within 500 m | 68% |
| sections with ≥ 90% of points within 1 km | 79% |
| sections with ≥ 50% of points within 1 km | 92% |

Global residual, 2026-10-01 (closest alternative per case, v11 metrics joined to v5 census):
median `coverage_1000m` 0.52 against a 0.95 threshold; median endpoint distance 6.3 km against 2 km;
median line distance 2.4 km. Only ~183 K nodes are "near misses" under a relaxed band.

So: in NOAM most failed sections were near misses; in the global residual the engine's best
candidate is typically on the river for only half its length. Both say the same thing about the
method. Candidate routes are whole-section D8 traces seeded at the upstream end; when the seed lands
in the wrong valley, or the trace leaves the river at a braid, lake or canal, every candidate is
grossly wrong for part of its length and no threshold relaxation helps. The fix is per-node
candidate states joined by a continuity model (Section 4), which assigns the matched part and
re-anchors the rest instead of discarding 200 km because of 100 km.

Two further facts from the census:

- **968 K nodes (24%) pass every gate and are still withheld**: ownership unresolved 568 K,
  more than one eligible route 310 K, registration 90 K. Their routes are saved. They are
  recoverable by policy alone (assign along the accepted route; pick the likelier route and flag).
- **The engine's own 63% has a registration defect**: at 379 single-downstream reach links the
  first 1–3 nodes of the downstream reach received tiny values (22 km² on the Mackenzie mainstem
  against 1.39 M upstream; 127 km² on the Mississippi against 2.37 M). About 7 K nodes; present in
  every source including `FIXED_CONTROL_CLEAR`. The doctrine "no cumulative-max value repair"
  (`NS/METHOD.md:9`) is wrong for this quantity: drainage area is non-decreasing downstream along a
  single channel by definition. The fill pass repairs and flags these.

### 2.3 Which "unresolved" outcomes are ambiguous rivers and which are avoidable (Q2)

- **Avoidable, implementation:** geometry_distance_fraction (1.23 M), section_order (0.36 M),
  ordered/registration/native_prefix (0.11 M), execution errors, and the part of
  identity_not_demonstrated (0.56 M) where the only failing gates are proximity + identity. The
  rank-order gate deserves special mention: nearest-point projection of a sinuous SWORD line onto a
  less sinuous D8 path is non-monotonic by construction at every meander bend (HANDOVER witness
  3000016: successive points project from station 4,120 m back to 2,154 m on one MHV link). A
  Spearman threshold of −0.98 converts ordinary planform disagreement into rejection.
- **Avoidable, semantics:** ownership_review (0.57 M) and contribution_position (0.23 M). For FACC
  the question is only *where along the route the UPA jumps*; that is read directly from the UPA
  profile. Requiring an independently proven tributary join before assigning any node is a proof
  obligation, not a physical requirement.
- **Genuinely ambiguous (needs flags, not refusal):** route_ambiguity (0.31 M, two plausible
  routes), part of mhv_contradiction (0.71 M — but see the census tags: most are
  `incomplete_or_disconnected_source_graph` / `interior_anchors_not_uniquely_mapped`, i.e. the MHV
  graph could not be assembled, which is "cannot judge", not "contradicted"), braided/deltaic tidal
  sections (0.47 M) where D8 cannot represent the network at all, and lakes (0.47 M) where the
  within-lake profile is a convention.
- **True search failures:** 12,125 nodes.

Disjoint partition of the missing nodes by the closest alternative's failure set (census agent,
`scratchpad/census/p2_disjoint_partition.csv`):

| Group (first that applies) | Cases | Nodes | Share |
|---|---:|---:|---:|
| execution error | 20 | 20 | 0.0% |
| no route alternative | 213 | 12,125 | 0.3% |
| passes every gate, withheld (ambiguous 310 K / ownership 568 K / registration 90 K) | 4,985 | 967,685 | 23.7% |
| MHV contradiction | 4,343 | 711,278 | 17.4% |
| order or structure (section order 356 K, contribution off-section 227 K, prefix 16 K) | 4,632 | 599,036 | 14.7% |
| identity not demonstrated (with geometry 630 K, alone 561 K) | 4,501 | 1,190,877 | 29.2% |
| geometry only | 2,120 | 598,052 | 14.7% |

The closest alternative fails exactly one gate for 1.25 M nodes. "Identity not demonstrated" is the
single most common tag (45% of nodes) and the only failure for 561 K; 1,190,809 of those 1,190,877
nodes also had MHV guidance withheld because no unique complete source chain could be assembled.
Read plainly: for 1.2 M nodes the method found a route, measured nothing structurally wrong with it,
and refused it because a proof it demands could not be constructed from MHV. That is the definition
of a manufactured requirement.

Other structure in the missing population:

- Size: 50% of missing nodes are in 2,305 cases; 77% are in cases of ≤ 1,000 nodes (≤ 130 km).
  Three chains exceed 10,000 nodes (Nile 2,211 km, Jubba 2,018 km, a Bolivian chain 2,184 km):
  real linear SWORD chains with no junction reaches.
- Context: 46% of missing nodes are in ghost-context sections (chains whose upstream tip is a ghost
  reach, i.e. headwaters), 24% in all-ordinary sections, 16% lake context, 8% unreliable.
  Headwaters are where the engine's upstream-end seeding is weakest.
- Class: tidal reaches are 76% missing, canals 82%, lake-on-river 49%, ordinary 32%.
- Some "ordinary river" chains are not rivers: the two largest geometry-only cases follow the
  Karakum canal (EU 28106500241, 9,815 nodes) and the Nara/Hakra canal (AS 45585900196, 5,831
  nodes); no raster drainage area exists for them. These need an explicit flag, not a search.
- Where values are missing the baselines are worse: at 1,839 HydroBASINS outlets with no v18 value,
  v17b median |Δ| is 7.5% (62% within 20%), v17c 62% (29% within 20%); where the failure is
  "section order", v17b's median |Δ| is 73% (141 outlets), i.e. v17b sampled the wrong river there
  too, so a v17b-based fill is weakest exactly for that group.

### 2.4 Largest conceptual and implementation mistakes (Q1)

1. **Boolean proof instead of likelihood.** Every decision is a hard gate; evidence never
   accumulates. A section 95% matched at 60 m with a 1 km excursion scores the same as no match.
2. **Whole-section acceptance semantics.** One divergent kilometre rejects 200 km of good match.
   Node-level assignment was the right call on 09-12, but acceptance stayed section-level.
3. **Nearest-point projection + rank-order test** as the "section order" check (2.3 above).
4. **Identity as a precondition, with a waiver that cannot be earned.** MHV identity requires a
   complete source-qualified chain with ≥ 6 anchors and 0.80 coverage; it fails to assemble for 84%
   of missing nodes. Missing MHV then acts as a veto in practice even though the stated rule is
   "neutral".
5. **Ownership as a precondition for any value.** 568 K nodes have an accepted route and no value.
6. **Production ran the strict rules, not the rules that worked.** `ContinuityRules` (09-08) is
   imported only by the holdout validator and two helpers; the expansion drivers call `match_segment`
   with default `MatchRules()`; the global drivers use `ConnectivityPolicy` v2.
7. **Progress measured as "passed all checks" rather than coverage × external accuracy** (Astra's
   own words, 09-29). This is what turned a method problem into 180 audit directories.
8. **Per-case windows of 400 M cells** (2 GB per case) and "long native windows" for a problem
   whose per-node footprint is a few km. This drove memory reservations of 96–192 GB per region and
   made every rerun a River event.

---

## 3. What the existing evidence already supports (Q6)

**Scope of every accuracy number in this document.** The only external reference is HydroBASINS
level-6 upstream area, compared at one SWORD reach per basin unit: the 6,262 "exact single-exit"
units (of 9,282 units SWORD touches) where the SWORD outlet reach and the unit exit pair exactly.
That is a basin-outlet test: one node per basin, biased toward large single-outlet rivers, silent on
tidal, deltaic and multi-exit units, and nested (an upstream unit's outlet is inside the downstream
unit). The prototype's 600 outlets are the subset inside local tile coverage. There is no
independent node-level truth anywhere; every node-level statement here is agreement between methods
(new vs engine, new vs v17b) or an internal consistency check (monotonicity, conservation). When the
report says "83% within 20%", read "83% of 6,262 basin outlets".

**Supported:**
- v17c is a regression against v17b in magnitude, worldwide, at every regional median
  (6,262 outlets: 3.36% → 30.9% median |Δ|).
- Where the engine assigned values (63% of nodes, 155 K reaches), they are better than both: on the
  4,423 outlets with all three, median |Δ| 0.68% vs 2.38% vs 22.8%; within 20%: 90% / 77% / 47%.
  Improvement over v17b by > 5 pp at 1,104 outlets, worse at 175.
- The new values are mostly v17b re-found (median ratio 1.000); the differences are concentrated
  where v17b sampled the wrong river (≈1 in 5 reaches).
- Blue Earth and the three wrong-trunk controls behave as intended in the NOAM pipeline.

**Added by this review (7.1, 7.2):**
- The engine's accepted values are reproduced by a gate-free likelihood model at 99.1% within ±5%
  (348 K NA nodes), so they do not depend on the proof apparatus that withheld the rest.
- A complete candidate exists for every node; at all 6,262 outlets it scores 1.1% median / 83%
  within 20%, and even on the 1,365 outlets whose reach is tier E it beats v17b (7.8% vs 10.2%
  median; 64.5% vs 59.4% within 20%) and v17c (72.7%; 25.1%).
- Of the engine-withheld sections on local tiles, 25–40% are matchable at full confidence with no
  old-facc input; most of the remainder is partially matched with the lost stretches identifiable
  by a value drop across a teleport.

**Not established:**
- Node accuracy anywhere except at basin outlets (HydroBASINS is nested, not independent; MHV and
  MERIT share lineage).
- At outlets of the hard (engine-withheld) population, the HMM alone is not better than v17b
  (55–58% vs 61% within 20% on 145 NA/EU/OC outlets) because of outlet-node snapping and
  tidal outlets beyond MERIT's coastline; its gain is at nodes and reach interiors, which outlets do
  not test. The anchored fill does at least as well as the HMM at those outlets (64.5% on tier-E
  outlets), so the final product should take interiors from the matcher and protect outlets and
  junctions with junction-aware aggregation and the fill's bounds.
- That "calculated" values would survive the engine's own promotion criteria (`proposed_nodes = 0`).
- That any of the 20,828 residual cases is physically unmatchable (the census says so explicitly:
  "no mechanism label asserts permanent irrecoverability"); the prototype shows about 10% of withheld
  sections (unreliable/ghost context, lowland deltas) have no MERIT channel within 1–2 km, which is
  "not representable in D8", not "unmatchable in principle".

---

## 4. A matching strategy that processes the whole remaining population in hours (Q3)

### 4.1 Idea

Treat section-to-raster correspondence as **map matching** (Newson & Krumm 2009, HMM/Viterbi),
which is the standard solution to exactly this problem: noisy positions along a known ordered path,
to be snapped onto a network with continuity. States are candidate UPA cells per node; the emission
model turns distance into evidence; the transition model is D8 reachability; decoding gives the
single most probable route *and* a posterior per node. No section-level veto exists; ambiguity is a
flat posterior, reported as such.

The structural difference from the engine: the engine proposes a handful of whole-section traces
seeded in the upstream quarter and then asks whether any of them passes twelve tests; the matcher
considers every plausible cell at every node and asks which continuous sequence best explains the
whole section. A divergence of 10 km in the middle of a 200 km section costs the engine the section;
it costs the matcher one teleport and a tier downgrade on that stretch.

### 4.2 Pseudocode (per physical section, nodes ordered downstream → upstream, i = 0 at the outlet end)

```
1  candidates(i): cells within R = 1.5 km of node i:
       top-K (K = 25) by UPA  ∪  all cells with UPA ≥ 1 km² within 300 m, capped at 40 by distance
   (independent of v17b/v17c; small rivers are not drowned by a big neighbour)
2  emission  E(i,s) = −½ (d(i,s)/σ_i)²            σ_i = max(150 m, 0.75·width_i)
   optional weak plausibility prior from WIDTH only:  −½ (ln(UPA_s/F(width_i))/1.5)²
   (F fitted on tier-A reaches; no old facc enters route choice — anti-circularity rule of 09-08)
3  transition s(i) → t(i+1): trace D8 downstream from t for ≤ L cells (L = max(60, 4×node spacing));
       passes through s  → cost 0  (UPA monotonicity follows automatically)
       otherwise         → teleport cost J = ½ (2000 m/σ_i)²; +J again if UPA_t > 1.05·UPA_s
   traces are computed once per candidate cell and cached; connectivity is a set-membership test
4  Viterbi → MAP cell per node; forward–backward → posterior p_i of the MAP cell and the
   posterior-weighted 10–90% band of ln(UPA) per node
5  node facc = UPA at MAP cell;  reach facc = max over its nodes
6  section diagnostics: fraction of connected transitions, teleports, median displacement,
   median posterior, ratio to v17b (diagnostic only)
   tier A: all transitions connected and median p ≥ 0.8;  B: ≤ 2 teleports or median p ≥ 0.5;  C: else
7  post-pass over the SWORD graph (seconds): check head/tail consistency across section boundaries,
   junction conservation; downgrade tier where neighbours disagree; never alter values to fit
```

How it handles the hard cases:

- **Displaced geometry:** distance enters as a Gaussian penalty, so a river 1.5 km off its D8
  valley line still matches if the D8 path is continuous and nothing closer is continuous. The
  "section order" problem disappears: order is enforced by the transition model, not by projecting
  points onto a line.
- **Confluences:** a tributary entering between nodes i+1 and i appears as a UPA step at node i on a
  connected path; no join proof is required to assign values. The step position *is* the join.
- **Bifurcations / deltas / tidal:** D8 cannot represent them; the matcher is run for diagnostics
  but values on distributaries come from the parent's matched value with width-proportional shares
  (the existing split/share/rejoin rule), tier D. Only the parent needs a raster match.
- **Lakes:** match the inlet and outlet nodes; interpolate monotonically through the lake; tier D.
- **Missing MHV evidence:** MHV is not in the loop. It can be added later as a second, optional
  emission term (distance to a qualified link) without changing anything else.
- **Identity:** replaced by posterior probability. Two continuous routes of similar likelihood give
  p ≈ 0.5 at the divergent nodes; that is reported as ambiguity (tier C) instead of refusal.

### 4.3 Cost

Measured (7.2): 13,000 nodes per CPU-second single-core, 116,000 nodes/s on 16 cores with numba;
all covered NA (492 K nodes) in 4.2 s. The global 11.1 M nodes are about 20 CPU-minutes of
matching. The real cost is I/O: 1,150 paired UPA/DIR tiles (≈54 GB compressed on River; ≈180 MB per
decoded 5° tile, ≈200 GB decoded for all land, so stream windows per tile with sections sorted
spatially, or keep a chunked Zarr; validate mosaics for empty fill first — three empty cells were
found locally). Expected wall time on River with 32 workers: 1–2 hours for the whole globe, not just
the residual. Locally this Mac (16 cores, 128 GB, 86 GB free disk) can do NA/EU/OC subsets with the
tiles already on disk, which is what the prototype did.

### 4.4 Best-supported estimate with explicit confidence (Q4)

Yes, scientifically defensible, provided the three populations stay distinct in the product:

| Tier | Meaning | Source of value |
|---|---|---|
| A | Node on a continuous D8 route with high value-posterior (engine value, or HMM tier A) | native UPA |
| B | HMM route with ≤ 2 teleports or moderate posterior, node not inside a lost stretch | native UPA |
| C | HMM ambiguous (flat posterior, fragmented route, no channel within reach) | native UPA on MAP route; band reported; candidate for bridging |
| D | distributary / lake convention; `bif_child` head nodes; nodes inside a lost stretch bridged between anchored ends | parent's UPA × width share; monotone interpolation between anchors |
| E | network-constrained fill | cleaned v17b profile clipped between trusted neighbours |

No tier is "independently qualified"; the only external check is HydroBASINS at outlets and it is
reported per tier. Each node carries `facc_new`, `tier`, `post_value`, `band_lo/hi`,
`in_lost_stretch`, `teleport`, `method`, plus the original v17b and released v17c values; the fill
adds `LB`, `UB`, `clipped`, `bound_conflict`, `trust_distance`, `monotone_fixed`, `computed_raw`.

---

## 5. Plan (ranked by recoverable population, time, and validation) (Q5)

| # | Change | Recoverable population | Implementation | Validation |
|---|---|---|---|---|
| 1 | Replace gates with the HMM matcher (4.2, with the 7.2 changes) and run it on **all** 43,218 sections, not only the residual | measured on local tiles: 78–83% of engine-withheld nodes become usable with no old-facc input (89–92% with the tighter lost-stretch rule); the rest becomes flagged bridges/fill. Expect tier E to fall from 32.5% to roughly 6–10% of nodes (extrapolation from three regions, not a global measurement) | prototype done; global run ≈ 20 CPU-min of matching + tile I/O, 1–2 h wall on River | engine agreement (99.1% reproduced), HydroBASINS by tier (600 outlets so far), the four controls, posterior calibration |
| 2 | Network-constrained fill (tier E) to close every remaining node today | all remaining nodes (100% coverage) — **done, 7.1** | 9 s | 6,262 outlets: 1.1% median, 83% within 20%; lint checks; bound conflicts |
| 3 | Stop the proof apparatus: promote tier A/B/C/D/E values with flags; retire "calculated vs proposed vs qualified" | unblocks the 5.7 M nodes held in `GLOBAL_CONTROL_QUALIFICATION_REQUIRED` | a decision, not code | none needed beyond the report |
| 3a | Policy-only recovery from saved evidence, no re-tracing: assign along already-accepted routes (ownership 568 K, registration 90 K); pick the likelier of two eligible routes (310 K); SWORD-lane MHV reversal → CANNOT_JUDGE instead of veto (277–500 K); chain-overlap cardinality fix (78 K); Spearman tie fix | ≈ 1.3–1.5 M nodes | hours, on River where the evidence lives; all gates are re-scorable offline from `distinct_routes.parquet` + `route_evidence.json` | same as #1; superseded by #1 if the global HMM run happens first |

Order of operations for delivery (status as of 2026-10-02 02:30 UTC):

1. Done: fill for all 11.1 M nodes (7.1); matcher prototype validated (7.2); integration (7.3).
2. Done: global matcher run on River, 141 s (7.2, run `fable-hmm-global-v1-20261002T010550Z`).
3. Done: final integrated product and report (7.3b).
4. In progress (2026-10-02, approved by Jake): **v2 joint junction matching** — max-product message
   passing on the section tree with a pairwise junction factor (tributary tail must be D8-upstream of
   the receiver head within L cells and UPA(head) ≥ 0.95 × UPA(tail); bifurcations excluded from
   coupling), components processed in topological order, same outputs plus junction flags
   (`river_bundle_v2/`). Local result of the pairwise version: exact (brute-force verified on 54
   junctions), zero extra cost, reproduces v1 bit-for-bit with coupling off; fixes the head side
   (NA tributary links violating the junction factor 215 → 40; heads below 0.8× the largest tail
   39 → 21) but leaves the reach-level gauge score unchanged, because 27 of the 40 reach-level misses
   are legitimate within-reach tributary inflows (the nearest-node comparison, 97%, is the right one
   there) and 13 are tributary tails sitting on the receiving river, which a pairwise factor cannot
   see since that cell does drain through the head. 46% of covered NA junctions have decoded tails
   summing to more than 1.2× the head. Approved next: a joint conservation term,
   −J·max(0, ln(Σ tails / (1.1·head))), exact on the tree. v2 ships as 0.0.14 once validated; 0.0.13
   carries the validated v1 product.
   Still open after v2: width check for lake/tidal/ghost classes via class-specific fits; SWORD
   topology edits for the wrong edges the width guard exposed (Loire → Vilaine). Independent
   references now exist for node interiors: 101 random USGS gauges (CONUS) and 2,298 SVS gauges vs
   HydroSHEDS (all continents), both in 7.3d–7.3e.

Deliverable spec: one parquet per region for nodes and reaches with `node_id/reach_id, v17b_facc,
v17c_facc, facc_new, tier, posterior, band_lo, band_hi, method, route_id, case_key`; a report with
coverage by region × class × tier, ratio distributions, HydroBASINS by tier, lint-style checks, and
change maps; the method write-up as pseudocode (this document, Section 4).

---

## 5b. Where each of the six questions is answered

| Question | Where |
|---|---|
| 1. Largest conceptual/implementation mistakes, with code and cases | 1.3b, 1.3c, 2.4 |
| 2. Which "unresolved" outcomes are ambiguous rivers vs avoidable limits | 2.1–2.3, 7.2 (measured split: ~1/3 clean, ~2/3 partially divergent, ~10% no channel) |
| 3. Matching strategy for the whole remaining population within hours | 4 (design), 7.2 (measured speed and behaviour), 5 (execution) |
| 4. Best-supported estimate with confidence/ambiguity flags | 4.4 (tiers), 7.1 (delivered product and its per-tier external check) |
| 5. Three highest-impact changes, ranked | 5 (table; unknowns stated as such) |
| 6. What existing evidence supports and what remains unestablished | 3, plus 7.1–7.2 for the new evidence |

## 6. Decisions only Jake can make

1. **River execution.** Authorized by Jake 2026-10-01 (evening). Preflight done: helpers present,
   port 22 open, one bounded authentication succeeded; River has 96 CPUs, 503 GB RAM, 8.6 TB free
   under `/not_backed_up`, no running jobs (one stale pending job 1976). The frozen Python 3.12 env
   lacks numba and duckdb, so the run bundle ships parquet inputs (no DuckDB on River) and a setup
   job that creates `/not_backed_up/jgearon/SWORD/env/fable-hmm-<RUNID>` with numba (network
   install with `--exclude-newer`, offline wheels as fallback). **Submitted 2026-10-02 01:06 UTC:**
   run id `fable-hmm-global-v1-20261002T010550Z`, remote
   `/not_backed_up/jgearon/SWORD/runs/fable-hmm-global-v1-20261002T010550Z`, setup job 3275, main
   job 3276 (32 CPUs, 200 GB). Both finished within five minutes of submission; outputs (1.7 GB)
   retrieved to
   `output/facc_detection/fable_review_20261001/soft_matcher/river_runs/fable-hmm-global-v1-20261002T010550Z/`.
   Bundle and resume procedure: `output/facc_detection/fable_review_20261001/soft_matcher/river_bundle/README.md`.
2. **Priors.** Primary recommendation: no old-facc prior in route choice (your 09-08 position).
   The width-only plausibility prior is defensible; v17b as a weak tie-breaker is not, because the
   product would then be partly circular.
3. **Shipping tiers.** Whether tier E (fill) may ship to the discharge users with its flag, or only
   A–D. My recommendation: ship all tiers with flags; a flagged best estimate beats a NULL for them.

---

## 7. New artifacts from this review

### 7.1 Complete-coverage candidate (network-constrained fill) — delivered

`output/facc_detection/fable_review_20261001/network_fill/` (`README.md` has every rule;
`outputs/fill_nodes_{R}.parquet`, `outputs/fill_reaches_{R}.parquet`, `outputs/report/REPORT.md`,
14 figures). All 11,112,454 nodes and 248,673 reaches carry `facc_fill` and a tier; no nulls, no
non-positive values. Runtime: 9 s for the fill, 22 s for the report, on this Mac.

| Tier | Meaning | Nodes | Reaches |
|---|---|---:|---:|
| A | engine value (4,710 nodes in 1,683 reaches repaired for the boundary defect, flagged `monotone_fixed`, raw kept) | 7,033,381 (63.3%) | 155,002 |
| B | cleaned v17b inside trusted bounds on both sides, no clipping | 14,747 | 333 |
| C | one trusted bound, or clipped | 457,749 (4.1%) | 9,091 |
| D | width split at a bifurcation with trusted parent | 431 | 13 |
| E | cleaned v17b, no trusted bound within reach | 3,606,146 (32.5%) | 84,234 |

HydroBASINS, 6,262 outlets (evaluation only):

| Method | median abs % | p90 abs % | within 20% | within 5% |
|---|---:|---:|---:|---:|
| v17b | 3.4 | 137.5 | 72.7% | 55.4% |
| released v17c | 30.9 | 261.7 | 41.9% | 24.4% |
| engine raw (4,423 outlets with a value) | 0.7 | 20.3 | 89.9% | 80.1% |
| **fill, all tiers** | **1.1** | **58.3** | **83.3%** | **70.6%** |
| fill, tier A outlets (4,423) | 0.7 | 20.3 | 89.9% | 80.1% |
| fill, tier C outlets (452) | 2.1 | 79.0 | 77.0% | 62.6% |
| fill, tier E outlets (1,365) | 7.8 | 301.5 | 64.5% | 42.6% |
| v17b on the same tier-E outlets | 10.2 | 325.9 | 59.4% | 39.5% |
| v17c on the same tier-E outlets | 72.7 | 917.3 | 25.1% | 11.7% |

Network checks (violations / checked): engine-to-engine link drops 379 → 3; 1:1 drops > 5%
(lint scope) v17b 3,473, v17c 0, fill 340; junction deficit F006 v17b 9,179 / 18,318, v17c 0, fill
5,177 (untrusted inflows are never summed, by design; v17c's zero is the inflation). Distributary
larger than parent: v17b 1,540, v17c 147, fill 1,089 (tier E keeps v17b's clones; only 13 reaches
had a trusted parent to split from).

Honest weaknesses: tier E is 65% of tidal, 79% of canal and 44% of lake reaches, and 45% of OC;
35,147 reaches are ≥ 10 hops from any engine value. The lower bound "max of upstream tails"
propagates a trusted big value into a small river wherever SWORD topology is wrong (the Loire is
routed into the Vilaine: 11 reaches inherit 111,946 km²); at the 145 non-A outlets raised more than
1.2×, 64% move closer to the reference and 36% farther. 370 reaches have conflicting bounds (kept at
the upstream value, flagged). A reverse pass would give 42,566 more reaches a trusted upper bound.
None of this is hidden: every node carries `tier`, `LB`, `UB`, `clipped`, `bound_conflict`,
`trust_distance`, `monotone_fixed`, `computed_raw`.

Bottom line: a complete, flagged product exists today that beats v17b at every outlet statistic and
beats released v17c by a wide margin everywhere. Its weakest third (tier E) is still a modest
improvement on v17b and a large improvement on v17c, and it is exactly the population the HMM run
upgrades.

### 7.2 Soft matcher prototype (HMM/Viterbi) — delivered, validated on NA/EU/OC tiles on disk

`output/facc_detection/fable_review_20261001/soft_matcher/` (`README.md` has the algorithm as
implemented, every table, and the figures; `code/soft_matcher.py` is ~500 lines plus numba kernels).
Coverage from local tiles: NA 1,679 sections / 492,002 nodes (29% of NA), EU 451 / 146,323, OC
400 / 101,452. Primary configuration uses **no old-facc prior anywhere** (route choice is distance +
D8 connectivity only); v17b prior, width prior and a facc-free "jump penalty" are variants.

**Speed.** 116,000 nodes/s on 16 cores (13,000 on one core). All covered NA in 4.2 s. All ten
configurations × three regions (7.4 M node-matches) in 2.5 min. The global 11.1 M nodes are about
20 CPU-minutes of matching; the only real cost is reading ~54 GB of tiles.

**Where the engine has values, the matcher reproduces them.** 99.1% of 348,521 NA engine nodes
within ±5% (tier A 99.4%); EU 99.1%, OC 99.2%. Disagreements sit at section ends (13.8% within two
nodes of an end vs 0.5% in the interior). The posterior is calibrated: nodes with value-posterior
≥ 0.99 agree with the engine 99.6% of the time, nodes below 0.5 agree 60–72%; the 10–90% band contains
the engine value for 98–99% of confident nodes. In other words, the engine's accepted values are
simply the Viterbi path, obtained here without any of the twelve gates.

**On sections the engine withheld** (NA 499 sections / 99,399 nodes; EU 184; OC 248):

| Region | tier A (0 teleports, confident) | tier B | tier C | tier-A nodes within 20% of v17b |
|---|---:|---:|---:|---:|
| NA | 29% of sections (20% of nodes) | 61% | 10% | 85% |
| EU | 40% (19%) | 60% | 0% | 89% |
| OC | 24% (13%) | 56% | 21% | 80% |

Most tier-B sections (NA 238 of 307) contain a teleport where the value drops more than 2×
downstream: the SWORD line leaves MERIT's channel for a stretch and the MAP path follows a small
side channel until it rejoins (figure `section_withheld_recovered_tierB.png`: 229 nodes, two
teleports, the lost stretch is nodes 33–72, the outlet still lands within 0.7% of HydroBASINS).
That flag separates a bad class (NA 70,149 nodes, 32% below half of v17b) from a usable class.
Tier C is almost entirely unreliable/ghost context and lowland deltas (New Guinea, Sumatra) with
section displacements of 1–2 km: no MERIT channel exists there.

**Controls against their HydroBASINS outlets:**

| Control | HydroBASINS | soft primary (max) | soft p10-max / priors / jump | v17b | v17c | engine |
|---|---:|---:|---:|---:|---:|---:|
| Blue Earth | 9,241 | 38,735 (+319%, outlet snap) | 8,938 (−3.3%) | +16.9% | +16.9% | −3.3% |
| Big Muddy | 6,124 | 310 (−95%) | 310 (−95%) | −4.6% | −4.6% | – |
| Unnamed Arctic | 8,352 | 77,051 (+823%) | 77,051 (+823%) | −99.6% | +16.5% | – |
| Chenal de l'Ouest | 3,646 | 3,655 (+0.3%) | 3,655 (+0.2%) | +0.3% | +222% | +0.3% |

Chenal de l'Ouest: correct (3,655 km², equal to the engine) under every configuration.
Blue Earth: the Le Sueur step inside the section is reproduced to 0.01% of the engine; the single
outlet node snaps onto the post-confluence Minnesota cell (50 m away) in the primary run, which the
posterior flags (value-posterior 0.76, band 8,938–38,735) and which the p10 aggregator, either prior,
or the jump penalty remove. Big Muddy: the Mississippi is never picked (posterior mass 0.16%), but
the value is 19× too small because MERIT routes the lower Big Muddy 10–15 km from the SWORD line and
nothing better exists within 1.5 km; the matcher is confidently wrong there, and only a width
plausibility check or the 2× drop flag can catch it. Unnamed Arctic: the head nodes of this
bifurcation child sit on the 77,051 km² Tanana trunk; `bif_child` flags it; D8 has no channel for
the slough.

**HydroBASINS (600 outlets inside processed sections: NA 417, EU 137, OC 46; share within 20%).**

| Subset | n | soft primary | soft + jump penalty | soft p10-max | v17b | v17c | engine |
|---|---:|---:|---:|---:|---:|---:|---:|
| all | 600 | 69.8% | 76.3% | 75.8% | 75.5% | 44.7% | 92.7% (n = 455) |
| engine computed | 455 | 74.5% | 82.6% | 81.5% | 80.2% | 50.5% | 92.7% |
| engine withheld ("recovered") | 145 | 55.2% | 56.6% | 57.9% | 60.7% | 26.2% | – |

Median |Δ| over all 600: primary 2.7%, jump penalty 1.8%, p10-max 1.5%, v17b 2.7%, v17c 24.6%,
engine 0.7%. Reading: at basin outlets the matcher with the jump penalty is level with v17b, far
above v17c, and below the engine on the outlets the engine computed; on the hard population it is a
few points below v17b. Of the 126 NA outlets off by more than 20%, 98 are over-estimates from the
outlet node landing on the receiving river (p10-max or the jump penalty fixes 26) and 28 are
under-estimates at tidal/estuary outlets beyond MERIT's coastline; v17b is also off by more than 20%
at 44 of the 98 and 23 of the 28. Outlets test one node per basin, the node most exposed to boundary
snapping; they do not test what the matcher is for, which is the interior profile.

**Node-level usable fraction on engine-withheld sections.** "Usable" = on a connected run, outside
a lost stretch (from a > 2× drop teleport to the next restoring teleport or section end), with a
land cell:

| Variant | NA | EU | OC |
|---|---:|---:|---:|
| primary | 78.0% | 76.2% | 69.5% |
| jump penalty | 83.1% | 79.7% | 77.2% |
| jump penalty, stretch also ends when the value returns to the pre-drop level (diagnostic) | 89% | 92% | 86% |

Usable nodes sit at a median of 1.00 × v17b (61–74% within 20%); lost-stretch nodes under the
tighter rule sit at 0.004 × v17b (90% below half), i.e. the flag separates cleanly. Two caveats the
flag cannot catch: lake inflow channels and head-end snaps produce no drop (22% of usable NA nodes
are below half of v17b, with no independent way to say which side is right), and outlet snaps are
not lost stretches.

**Sensitivity.** With no prior, scaling σ changes nothing in the MAP path (by construction);
teleport distance 1–4 km moves 2–3% of cells; the v17b prior moves 4.2% of cells, almost all at
section ends (it is a boundary repair, not a route chooser: engine agreement stays 99.2%). The
facc-free jump penalty (a connected step with a > 2× UPA increase upstream costs −J) achieves the
same boundary repair as the v17b prior (confluence violations 226 → 88, end jumps 212 → 58) while
changing 0.6% of cells. Width-prior fit on tier-A engine reaches: F(w) = 150.6·w^0.845, residual
SD 1.5 in ln units.

**What this establishes.** (1) The engine's 63% is reproducible by a simple likelihood model; the
gates added nothing to those values. (2) Of the withheld population, roughly a quarter to a third of
sections (and the connected runs inside most of the rest) are recoverable with calibrated confidence
and no old-facc input. (3) The remainder is genuine SWORD/MERIT divergence (lakes, deltas, lowlands,
rivers MERIT routes elsewhere), where the right answer is a flagged bridge between anchored values,
not a local UPA read. (4) Section boundaries must be handled jointly before a global run.

**Global run (River, 2026-10-02 01:06–01:09 UTC, run `fable-hmm-global-v1-20261002T010550Z`).**
Setup job 3275 (numba 0.67.0 + llvmlite 0.49.0 from offline wheels over the frozen 3.12 env, kernel
self-check passed) and main job 3276: 43,218 sections, 11,112,454 nodes, 0 errors, **141 s wall on
32 CPUs** (46–125 K nodes/s per region), tile index 1,150 cells (1,143 ok, 7 with empty dir only).
Outputs in `soft_matcher/river_runs/fable-hmm-global-v1-20261002T010550Z/outputs/`. Section tiers
worldwide: A 27,602 (64%), B 14,883 (34%), C 733 (2%); 5,724 bifurcation-child sections flagged.
Nodes: 94–95% usable (on a connected run, outside a lost stretch), 2.6% inside strict lost stretches,
median value-posterior 1.0, mean displacement 110–145 m per region; 2,038 offshore nodes with no
MERIT land cell (NaN, handled by the fill).

| Region | engine nodes | matcher within ±5% of engine | within ±20% | engine-withheld nodes | usable (strict) | usable within 20% of v17b | median ratio to v17b |
|---|---:|---:|---:|---:|---:|---:|---:|
| AF | 711,228 | 99.5% | 99.6% | 405,547 | 87.3% | 77.7% | 1.000 |
| AS | 2,782,587 | 99.0% | 99.4% | 1,511,502 | 87.2% | 73.0% | 1.000 |
| EU | 746,295 | 99.2% | 99.4% | 427,941 | 88.5% | 72.8% | 1.000 |
| NA | 1,107,385 | 99.0% | 99.2% | 598,320 | 86.5% | 67.8% | 0.999 |
| OC | 352,922 | 99.1% | 99.4% | 333,673 | 86.5% | 72.7% | 1.000 |
| SA | 1,332,964 | 99.3% | 99.5% | 802,090 | 89.4% | 76.8% | 1.000 |

Worldwide, the gate-free matcher reproduces the engine's 7.03 M accepted values at 99% within ±5%,
and supplies a usable, confidence-flagged value for 86–89% of the 4.08 M nodes the engine withheld,
centred exactly on v17b (median ratio 1.00) with the wrong-river tails removed.

**Changes to carry into the global run** (all already prototyped or trivial): jump penalty on
(facc-free); reach value = p10-max or junction-aware aggregation instead of plain max; mark lost
stretches (from a > 2× drop teleport to the rejoin) and bridge them from the anchored ends;
width-plausibility flag for "no plausible MERIT channel within reach" (Big Muddy); `bif_child` head
nodes flagged "not identifiable in D8" and assigned by width share; lakes assigned from the outlet
cell with inflow/outflow matched; corridor-based tile coverage instead of bbox.

### 7.3 Integration (engine + matcher + fill → one product) — delivered

`output/facc_detection/fable_review_20261001/integrate/` (`README.md`, `code/integrate.py`,
`code/verify_final.py`, `code/report_final.py`, `outputs/final_{nodes,reaches}_{R}.parquet`,
`outputs/report/REPORT.md`, figures incl. tier maps and the per-region outlet bar chart). Verified by
273 consistency checks (11,112,454 nodes, every value finite and positive, pass-through columns exact,
reach values/tiers/flags consistent with nodes, no within-reach downstream drops).

Rules as implemented: precedence engine → usable matcher node (value-posterior ≥ 0.5, not a
bifurcation-child head) → bridge across lost stretches (monotone interpolation between anchors;
tier D, one-sided tier C) → fill. Outlet-snap repair: a run of ≤ 10 nodes at a section's downstream
end whose values sit within 20% of the downstream neighbour and above 1.5× the first value past the
run is held at the upstream value (an uncapped version flagged real tributary steps and made 70
outlets worse, so the cap matters). Mirror rule for bifurcation-child heads latched onto the parent
trunk (≤ 50 nodes). Width plausibility: Theil–Sen ln(facc) ~ ln(width) on tier-A reaches per region;
matcher/bridge values more than 2.5 ln-units below or above the fit are demoted to the fill value
(180 low, 17 high); fill lower-bound raises that would land implausibly high are refused and flagged
`topology_suspect` (127). Then the monotone repair and the lint-style checks.

Effect with matcher coverage limited to the local NA/EU/OC tiles (the global run extends it):

| Outlets (within 20% / median abs %) | fill | final |
|---|---:|---:|
| all 6,262 | 83.3% / 1.09 | 83.5% / 1.08 |
| 600 inside matcher coverage | 85.8% / 1.05 | 86.7% / 1.03 |
| 145 inside coverage, no engine value | 64.1% / 9.5 | 66.9% / 5.7 |

Source changes: 203,381 nodes fill → matcher, 46,167 fill → bridge; reach tiers E 84,234 → 78,662.
Engine head defects lifted by upstream matcher values (e.g. 75140500011: 303 → 21,973 km² against
22,328). Big Muddy ends at 5,844 km² (v17b 5,845; HydroBASINS 6,124): the matcher's 310 km² lies
entirely inside a lost stretch and is bridged away. Loire → Vilaine is only partly repaired (three
reaches below the link still carry 111,946 km²; the 42 m canal reach is refused the raise); this is
a SWORD topology edge error and needs a topology fix, not another facc rule.

Known weaknesses: 31.6% of reaches remain tier E until the global run; the width check skips lake,
tidal, ghost and "other" reaches, so wrong low matcher values there survive (72591500013: 75 km²
against 181,235); the refusal rule costs some correct big-river raises (11 outlets HydroBASINS
supports); a single-node hold value can trigger a repair on a one-node dip.

### 7.3b Final global product (engine + global matcher + bridge + fill) — delivered

`output/facc_detection/fable_review_20261001/integrate_global/` (`README.md`, `outputs/report/REPORT.md`,
`outputs/final_{nodes,reaches}_{R}.parquet`, `code/`, 21 figures; the as-specified run and a rejected
variant are kept under `outputs/as_specified/` and `outputs/variant_2c_uncapped/`). 303 consistency
checks pass.

| | fill only | final (global matcher + guards) |
|---|---:|---:|
| node sources | engine 63.3% / fill 36.7% | engine 63.3% / matcher 32.0% / bridge 3.4% / fill 1.3% |
| tier E reaches | 33.9% | **0.5%** |
| all 6,262 outlets within 20% (median abs %) | 83.3% (1.09) | **84.4% (0.96)** |
| 1,839 outlets with no engine value, within 20% (median abs %) | 67.4% | **70.9% (3.44)**; v17b 62%, v17c 29% |
| outlets > 5 pp worse / better than fill | – | 231 / 356 |
| single-downstream link drops > 20% | 324 | 1,899 |

By region, within 20% (final / fill): AF 84.6 / 83.6, AS 87.5 / 86.5, EU 83.4 / 81.7, NA 81.7 / 80.3,
OC 70.7 / 67.8, SA 86.0 / 86.2. Controls: Blue Earth 8,938 km² (−3.3%), Chenal de l'Ouest 3,655
(+0.25%), Big Muddy 5,844 (−4.6%, bridged from v17b-consistent anchors), Loire → Vilaine: the guard
now stops the Loire value at the first canal reach; the dam reach and two river reaches directly
below the wrong SWORD edge still carry 111,941 km² (needs a topology edit, not a facc rule).

Rules added in this round, each with a recorded before/after: repair-side width guard (refuses a
running-max raise that would leave a matcher reach > 2.5 ln-units above F(width) when its own
values are plausible; 18 refusals, none at outlets); long tail holds for 11–100-node end runs where
the fill is also implausible (42 holds; uncapped it wrecked four 2.8 M km² African mainstems whose
SWORD width is wrong, hence the cap); bifurcation-head latch repair (352); width demotions 2,475
low / 233 high; `topology_suspect` 88.

**The honest remaining defect:** sections are still matched independently, so at junctions the two
sides can disagree (the downstream section matched a different channel than its tributaries). The
`junction_inconsistent` flag (head below 0.8× the largest upstream tail, or a > 20% drop on a
single-downstream link) marks 1,378 reaches / 57,274 nodes (AS 522, NA 252, SA 205, EU 163, OC 120,
AF 116); 864 of them are matcher- or bridge-valued on both sides, 119 have the engine on the flagged
side. This is why link drops > 20% rose from 324 to 1,899 (0.8% of links) and why 231 outlets lost
more than 5 pp while 356 gained. Listed and unfixed in the diagnostics CSVs: 194 mid-section spikes
carried to a section tail, 28 lake-tail latches. The fix is the
joint junction factor described in 7.2 (match the section tree with one shared junction constraint),
which is the first item for a v2 run. For discharge users who weight conservation above interior
accuracy, the fill-only product remains available as the conservative alternative.

### 7.3c Willamette check against USGS gauges (independent of HydroBASINS and MERIT lineage)

Nearest mainstem reach to each gauge (within 5 km), published USGS drainage areas, percent error:

| Gauge | USGS km² | reach | v17b | released v17c | Feb-2026 CVXPY integrator (anchored to v17b) | **final** |
|---|---:|---|---:|---:|---:|---:|
| Portland 14211720 | 29,008 | 78220000021 | −17.8% | −4.3% | −19.8% | **−0.4%** |
| Salem 14191000 | 18,855 | 78220000141 | −26.0% | −5.3% | −11.7% | **−0.4%** |
| Albany 14174000 | 12,536 | 78220000191 | −10.8% | −10.8% | −17.6% | **+0.6%** |
| Harrisburg 14166000 | 8,858 | 78220000251 | +7.8% | +7.8% | −10.7% | **+1.3%** |
| HydroBASINS outlet 78220000011 | 28,949 | | −15.0% | −1.0% | — | **0.0%** |

All 55 Willamette reaches have values (49 engine, 6 matcher; no flags). v17b was 10–26% low along the
middle and lower mainstem (wrong-cell sampling on a wide floodplain river), and the Feb-2026 CVXPY
integrator inherited that bias through its two v17b anchors; released v17c happened to land near the
mouth because its junction sum partly cancelled the deficit, but is still 5–11% off at Salem and
Albany. The McKenzie confluence step (5,294 → 8,819 km²; USGS McKenzie ≈ 3,460 km²) is reproduced.
Figure: `integrate_global/figures/willamette_profile.png`; table `outputs/diag_willamette_mainstem.csv`.

### 7.3d Random USGS gauge test (not basin-picked)

Sixteen random 2°×2° boxes over CONUS (seed 20261002; six fell on ocean, Mexico or Canada and
returned nothing), all USGS stream sites with a published drainage area from the USGS Water Data
OGC API (`api.waterdata.usgs.gov/ogcapi/v0/collections/monitoring-locations`), kept if ≥ 300 km²,
matched to the nearest SWORD node within 250 m with a consistent river name: 101 gauges, 26 of them
on reaches the engine had withheld. Node-level comparison at the matched node:

| Subset | Method | n | median abs % | p90 abs % | within 10% | within 20% |
|---|---|---:|---:|---:|---:|---:|
| all 101 | v17b | 101 | 7.4 | 38.2 | 55% | 78% |
| | released v17c | 101 | 14.0 | 131.0 | 42% | 62% |
| | fill only | 101 | 0.8 | 19.0 | 73% | 90% |
| | **final** | 101 | **0.5** | **10.0** | **87%** | **99%** |
| engine-withheld 26 | v17b | 26 | 15.9 | 33.7 | 23% | 65% |
| | released v17c | 26 | 17.9 | 138.6 | 15% | 58% |
| | **final** | 26 | **4.8** | **12.6** | **77%** | **100%** |
| engine-computed 75 | engine = final | 75 | 0.3 | 9.2 | 91% | 99% |

Caveats: gauges cluster (ten on the Snake in Hells Canyon, six on the Withlacoochee), so the
effective sample is smaller than 101; the Snake's uniform −10% in v17b and final alike is a
MERIT-vs-USGS delineation difference (non-contributing area), not a matching error; the one +32%
(Colorado above Roaring Fork) is the nearest node sitting just below the confluence the gauge sits
above. Files: `integrate_global/outputs/diag_random_usgs_gauges_NA.csv`, `_summary.csv`,
`figures/random_usgs_gauges_NA.png`. This is the first node-interior, lineage-independent check in
the whole effort; the same should be done for other continents with GRDC catchment areas.

### 7.3e Global gauge check: SVS gauges vs HydroSHEDS upstream area (all continents)

Reference: the SWOT Validation Set v1.1.2 (Saemian & Tourian 2026, DaRUS DARUS-5843; local copy
`~/geodata/canonical/svs/raw/SVS_v1_1_2.nc`), 3,643 gauges already matched by its authors to SWORD
v17c reaches, with HydroSHEDS/RiverATLAS v1.0 `UPLAND_SKM` read at the nearest RiverATLAS segment
within 500 m of the gauge (local copy of RiverATLAS on the Drive mount). Gauges where another segment
within 500 m has a ≥ 3× different area are dropped as ambiguous (995), and areas < 100 km² are dropped;
2,298 gauges remain (USGS 934, ANA 346, HYDAT 208, BOM 207, AWRC 182, GRDC 169, MLIT 129, others 123).
SWORD value = nearest node within 250 m where one exists (2,140 gauges), else the matched reach.
HydroSHEDS is semi-independent of MERIT (different conditioning and 15-arcsec routing; shared SRTM
ancestry); SVS carries no drainage areas of its own.

| Subset | n | v17b median abs / within 20% | released v17c | **final** |
|---|---:|---:|---:|---:|
| all gauges | 2,298 | 5.2% / 78% | 12.3% / 61% | **0.7% / 95%** |
| NA | 1,172 | 4.9% / 79% | 11.8% / 63% | **0.6% / 96%** |
| SA | 357 | 4.2% / 81% | 14.3% / 57% | **0.6% / 94%** |
| EU | 352 | 4.7% / 79% | 11.2% / 62% | **0.5% / 94%** |
| AF | 59 | 8.3% / 76% | 13.1% / 56% | **0.4% / 100%** |
| AS | 135 | 8.2% / 71% | 11.3% / 62% | **1.4% / 90%** |
| OC | 223 | 8.3% / 71% | 14.0% / 54% | **1.2% / 89%** |
| reach from engine | 1,682 | 5.2% / 79% | 10.9% / 64% | **0.6% / 95%** |
| reach from matcher / bridge / fill | 616 | 5.6% / 75% | 19.4% / 51% | **0.8% / 93%** |

Within 10%: final 91%, v17b 64%, v17c 45%. The residual tail is dominated by reference snapping
(a gauge on an anabranch where RiverATLAS carries a 105 km² side segment next to the 860,000 km²
Murray; v17b and final agree with each other at every one of the twelve worst cases). Files:
`integrate_global/outputs/diag_svs_gauges_hydrosheds.csv`, `_summary.csv`,
`figures/svs_gauges_hydrosheds.png`.

### 7.3f Policy search on the residual errors (2026-10-02)

Full note: `integrate_global/POLICY_SEARCH_20261002.md`. Using the 2,298 SVS/HydroSHEDS gauges only
to score candidate rules: a three-read majority vote (engine, matcher, v17b) is rejected (engine
closer 33:4 where it disagrees with the matcher); reverting to v17b where final differs is rejected
(final right 82%, v17b 11% in those 337 cases); demoting `width_flag=low` reaches is unsupported
(n=13). The best compound review rule (differs from v17b >25% and a risk flag) isolates a set with a
30% error rate against 3% outside, but captures only 20% of the SWORD-side errors and final is still
the best estimate inside it (70% vs v17b 15%, fill 44%); globally it marks 30,674 reaches (12%).
Conclusion: no flag-derived value policy improves the product; the residual needs the structural
fixes (joint junction matching, class-specific width checks, SWORD topology edits). Ship with a
`review_priority` column rather than more rules.

### 7.4 Scratch analyses

Displacement statistics, inflation overlap and the census partition are reproduced from the frozen
artifacts cited above (`scratchpad/census/` for the census tables).
