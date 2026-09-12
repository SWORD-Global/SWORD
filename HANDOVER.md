# HANDOVER — Finish node-level SWORD–MERIT FACC rederivation

**Prepared:** 2026-09-12T19:20:44Z

**Repository:** `/Users/jakegearon/projects/SWORD`

**Current branch:** `codex/wse-percentile-tail-handover`

**Starting commit:** `f462947`

**State:** Candidate-only research workflow. No SWORD database has been modified.

## Mission

Take the North America prototype across the finish line by preserving the
validated junction-to-junction SWORD-to-MERIT route matching, replacing the
temporary one-value-per-reach stationing shortcut with ordered node-level
registration, and then rerunning topology and independent HydroBASINS checks.

The core feasibility question is answered positively: selecting a coherent
MERIT D8 route for a whole SWORD centerline section works. The unfinished part
is assigning every SWORD node to the correct ordered position on that fixed
route, especially at confluences, lakes, and split/rejoin structures.

## Critical correction — do not miss this

The production architecture is:

1. Select **one coherent MERIT route at the whole centerline-section level**
   (the SWORD segment between junctions), using geometry plus D8/MHV
   connectivity.
2. Register **every SWORD node** along that already selected route in strict
   upstream-to-downstream order.
3. Assign each node the UPA of its corresponding MERIT route cell.
4. Derive the compatibility reach value only afterward as
   `reach.facc = MAX(node.facc)`.

Do **not** select routes independently per node or per reach. Also do **not**
move a single representative reach station downstream. The current script
`scripts/analysis/assign_merit_section_reaches.py` uses
`representative_step = int(np.median(steps))`; that was a fast feasibility
prototype and is not the final algorithm. Its reach-level values and all
HydroBASINS metrics derived from them are provisional.

This correction is consistent with
`docs/validation_specs/facc_validation_spec.md`: the source workflow samples
FACC for centerline/node support and reach FACC is the maximum node value.

## Read first

1. Repository `AGENTS.md` instructions supplied in the task context.
2. `output/facc_detection/rederivation_probe_20260908/PROGRESS.md`
3. `docs/validation_specs/facc_validation_spec.md`
4. `scripts/analysis/assign_merit_section_reaches.py` — useful prototype, but
   specifically identify and replace the median-step shortcut.
5. Current frozen result and validation records:
   - `output/facc_detection/rederivation_probe_20260908/na_full_run_20260911/ordinary_reach_assignment_v22_v17c_receiver_subchain/reach_candidates.csv`
   - `output/facc_detection/rederivation_probe_20260908/na_full_run_20260911/ordinary_reach_topology_audit_v22_v17c_receiver_subchain/topology_passing_candidates.csv`
   - `output/facc_detection/rederivation_probe_20260908/na_full_run_20260911/ordinary_reach_topology_audit_v22_v17c_receiver_subchain/topology_audit_summary.json`
   - `output/facc_detection/rederivation_probe_20260908/na_full_run_20260911/value_uncertainty_summary_v1.json`
   - `output/facc_detection/rederivation_probe_20260908/na_full_run_20260911/hydrobasins_validation_v1/validation_protocol.json`
   - `output/facc_detection/rederivation_probe_20260908/na_full_run_20260911/hydrobasins_validation_v1/validation_summary.json`

## Locked scientific and data decisions

- Route identity is selected at the **section/whole-centerline level**.
- Values are assigned at the **SWORD node level**, monotonically along the
  fixed MERIT route.
- Reach FACC is a downstream/max-node compatibility aggregate, not a route
  selection primitive.
- Use v17c `main_path_id`, `is_mainstem`, `rch_id_up_main`, and
  `rch_id_dn_main`. Do not use legacy `main_side` as current routing authority.
- MERIT UPA continuity validates a geometry/connectivity-selected route; it
  must not choose whichever candidate resembles v17b FACC.
- HydroBASINS is evaluation-only. It must not choose routes, station positions,
  thresholds, or values.
- MHV is routing/connectivity evidence. Exact upstream MHV product lineage is
  still `NOT FOUND`; River copies are bounded staging extracts, not canonical
  source authority.
- Keep lakes separate from ordinary-river rules. Large UPA jumps through lakes
  may be physically correct because the lake outlet represents all upstream
  contributing area.
- Do not reject a route merely because a real tributary produces a large UPA
  increase that SWORD does not explicitly represent.
- Candidate-only until Jake explicitly authorizes database writes.

## Database and worktree safety

- Never modify `data/duckdb/sword_v17b.duckdb`; it is the pristine reference.
- For this workflow, also keep `data/duckdb/sword_v17c.duckdb` unchanged until
  explicit authorization, even though it is generally the editable database.
- The worktree is heavily dirty with unrelated user work and many untracked
  analysis scripts. Preserve it. Do not mass-add, clean, reset, or overwrite.
- The current branch name belongs to an older WSE task. Before implementation,
  create or move this work to a correctly named `codex/` feature branch only
  after checking with the user or repository workflow; do not commit directly
  to `main`.

## Current bounded result

The conservative v22 ordinary-river prototype contains:

- 7,582 candidate reaches;
- 7,557 topology-passing reaches (99.67% conditional consistency);
- 25 review rows;
- 23 unique violating edges among 8,628 audited incident edges (0.27%);
- residual structure: eight ordinary reciprocal same-main-path edges plus 15
  lake-associated edges.

Repeat matching gives 2.73% section-target discordance and 1.35% positioned-
reach discordance at a 1% value threshold. These are internal route-ambiguity
proxies, not absolute FACC error rates.

The frozen HydroBASINS L6 outlet comparison admits 454 unambiguous exits. The
temporary reach-median candidates have 1.14% median absolute disagreement,
10.40% p90, 29.65% p95, and 94.27% within 20%. v17b has 1.78% median and 83.48%
within 20%; current v17c has 18.05% median and 51.76% within 20%. These results
show strong feasibility but **must be rerun after node-level assignment**.

Frozen v22 cohort SHA256:
`ba0be4b6d44eb4a771579b0680db238d205b1fe387b0c5d797a7d03d25543504`.

## Blue Earth acceptance case

Use SWORD reach `74288200011` as the first node-level acceptance test. Its
selected MERIT route follows the river well; the failure is the prototype's
median reach station, not route identity.

- All target centerline samples project to MERIT path steps 86–299.
- The temporary median step is 192, giving only `6050.64453125 km2`.
- An internal tributary enters at steps 243→244:
  `6058.57 → 8928.80 km2`; the other inflow is about `2870.23 km2` and closure
  is effectively exact.
- The Blue Earth/Minnesota receiving-river confluence is at steps 297→298:
  `8937.945 → 38734.555 km2`; the other inflow is about `29796.604 km2`.
- The SWORD downstream node `74288200010011` projects near step 298 (about
  50 m away), showing why boundary ownership must be topology-aware.
- HydroBASINS L6 is `9241.2 km2`; v17b is `10801.775 km2`.

Expected behavior: ordered Blue Earth nodes should progress through the route,
including the real internal tributary increase to roughly 8,938 km2. The last
tributary node/reach aggregate must remain on the **pre-receiving-river** side;
the downstream receiver's nodes take the post-confluence approximately 38,735
km2 value. Resolve this using SWORD topology and node/reach boundary identity,
not nearest geometry alone.

## Three route-identity regression cases

The HydroBASINS review found four new regressions above 15 percentage points.
Blue Earth is the stationing case above. The other three are route-identity
failures that node-level assignment alone must not promote:

- Big Muddy `74270400011`: target tributary was stationed after joining the
  approximately 1.82 million km2 Mississippi trunk; expected local scale is
  approximately 5,800–6,100 km2.
- Unnamed Arctic reach `81244000011`: projected onto an approximately 77,000
  km2 neighboring trunk despite an 8,352 km2 HydroBASINS outlet and poor
  centerline alignment.
- Chenal de l'Ouest `72120600011`: projected onto an offset approximately
  13,000–21,000 km2 trunk rather than its approximately 3,650 km2 outlet path.

Generalize an independent trunk-identity gate using whole-section alignment
and D8/MHV connectivity. Do not tune candidate values toward HydroBASINS or
v17b.

## Implementation sequence

### 1. Build ordered node-to-route registration

Create a candidate-only script, for example
`scripts/analysis/assign_merit_section_nodes.py`, that consumes the already
validated section route and emits at least:

`section_id, node_id, reach_id, node_order, route_id, path_step,
merit_upa_km2, projection_distance_m, assignment_evidence, review_reason`.

For each section:

- load SWORD nodes in upstream-to-downstream order; note that stored
  `node_order` is 1 at the downstream end and increases upstream, so reverse it
  for downstream traversal;
- keep the selected MERIT route fixed;
- solve an ordered projection with nondecreasing MERIT path step;
- prefer a constrained dynamic-programming/sequence-alignment solution over
  independent nearest-neighbor picks; distance is the primary local cost, with
  no freedom to jump to another route;
- permit multiple nearby SWORD nodes to map to the same raster cell when
  resolution requires it;
- record distance and ambiguity rather than silently forcing poorly aligned
  sections;
- treat shared reach endpoints and confluence cells with explicit topology-
  aware boundary rules;
- when sections overlap, require node assignments to agree within a declared
  tolerance or withhold the conflict.

### 2. Aggregate reach candidates

Only after node assignment, compute each reach candidate as
`MAX(merit_upa_km2)` over its assigned nodes. Retain the full node profile and
provenance; the reach table is a derived export.

### 3. Add acceptance tests before scaling

At minimum test:

- path steps are nondecreasing downstream within every selected route;
- node UPA has no significant downstream decrease except explicitly classified
  distributary behavior;
- reach FACC equals maximum node FACC;
- Blue Earth captures the approximately 8,938 km2 pre-confluence value without
  inheriting the approximately 38,735 km2 receiver value;
- the downstream receiving reach gets the post-confluence value;
- Big Muddy, 812440, and Chenal de l'Ouest are withheld by route identity rather
  than made monotonic on the wrong trunk;
- repeat/overlap assignments agree;
- lake cases do not inherit ordinary-river jump rejection.

### 4. Run the frozen North America production envelope

Use the same route inputs and frozen v22 envelope so the only methodological
change is node registration. Write a new dated/versioned output directory; do
not overwrite v22 or HydroBASINS v1 artifacts.

### 5. Revalidate

- rerun node and reach monotonicity/topology audits;
- rerun repeat-match ambiguity at node and reach levels;
- freeze a new candidate cohort with checksums;
- rerun the HydroBASINS outlet comparison using the correct downstream/pre-
  receiver node-derived reach values;
- regenerate the four priority diagnostics and verify Blue Earth changes for
  the right reason while the other three remain withheld.

### 6. Finish coverage, then lakes

The 7,557 passing reaches are a conservative production envelope, not all of
SWORD NA. After the node engine passes, quantify total section/node/reach
coverage and reason-coded abstentions across the complete North America scope.
Then apply the separate position-preserving lake workflow to the 15 residual
lake-associated boundaries.

## Existing code worth reusing

- Route and D8 matching:
  - `scripts/analysis/match_merit_sword_segments.py`
  - `scripts/analysis/run_merit_segment_validation.py`
  - `scripts/analysis/prototype_merit_continuity_rescue.py`
- Current provisional reach stationing:
  - `scripts/analysis/assign_merit_section_reaches.py`
- v17c route/component work:
  - `scripts/analysis/assign_v17c_mainpath_positions.py`
  - `scripts/analysis/resolve_v17c_split_rejoin_components.py`
  - `scripts/analysis/gate_v17c_contiguous_subchains.py`
  - `scripts/analysis/gate_v17c_branched_prefixes.py`
- Validation:
  - `scripts/analysis/audit_noam_candidate_topology.py`
  - `scripts/analysis/freeze_noam_hydrobasins_validation.py`
  - `scripts/analysis/evaluate_noam_candidates_hydrobasins.py`
- Review outputs:
  - `scripts/analysis/build_hydrobasins_outlier_review_package.py`
  - `scripts/visualization/plot_hydrobasins_outlier_diagnostics.py`

## Current review package

- GeoPackage:
  `output/facc_detection/rederivation_probe_20260908/na_full_run_20260911/hydrobasins_validation_v1/geospatial_review/hydrobasins_outlier_review.gpkg`
  — SHA256 `b770a0d9f0b7cdea400e93b5e821244b63fd4058dbf23f3c30d8f152f14d9b0a`.
- Satellite diagnostic:
  `output/facc_detection/rederivation_probe_20260908/na_full_run_20260911/hydrobasins_validation_v1/geospatial_review/hydrobasins_priority_cases_satellite.png`
  — SHA256 `e588128de44bca2e1707df4f052210d7409afb653507906c963c54f7c3aaf730`.
- GeoPackage layers: 26 outlier points, 26 outlier reaches, 26 HydroBASINS
  polygons, four priority MERIT routes, four priority MERIT stations, and 33
  contextual SWORD section reaches; all are valid EPSG:4326 geometries.

## Verification baseline

The following focused test files previously passed together (41 tests):

```bash
python -m pytest \
  tests/sword_duckdb/test_merit_continuity_rescue.py \
  tests/sword_duckdb/test_merit_outlet_holdout.py \
  tests/sword_duckdb/test_merit_outlet_validation.py \
  tests/sword_duckdb/test_merit_section_holdout.py \
  tests/sword_duckdb/test_merit_segment_matcher.py -q
```

Rerun these before and after the node-level implementation, then add focused
node-registration tests. Run broader SWORD tests only after checking that the
dirty worktree and local data dependencies will not be disturbed.

## Definition of done

- One fixed route per SWORD junction-to-junction section.
- Every covered SWORD node receives a monotonic, provenance-rich MERIT path
  position and UPA value.
- Reach FACC is exactly the maximum assigned node FACC.
- Blue Earth is correct at the internal tributary and receiver boundary.
- Wrong-trunk cases are withheld by a general route-identity rule.
- Ordinary, split/rejoin, and lake behaviors are explicitly separated.
- North America coverage and every abstention are quantified.
- Topology, repeat-match, and HydroBASINS validation are rerun and frozen.
- No production database is changed without explicit authorization.

## Effort assessment

This is medium work, not a rederivation from scratch. The hard route-selection
and connectivity prototypes already exist. The remaining core is an ordered
node-registration engine, confluence boundary handling, regression tests, and
a full rerun. It is realistically finishable within the discussed 2–3 week
window if the next agent preserves the current route evidence and avoids
case-by-case value tuning.
