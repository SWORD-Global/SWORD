# FACC handover for the next session (2026-10-02, from Fable)

Read first: `docs/handovers/FACC_FABLE_ASSESSMENT_20261001.md` (history, diagnosis, method, numbers),
`output/facc_detection/fable_review_20261001/EXECUTIVE_SUMMARY_20261002.md`, and the comparison
document `output/facc_detection/fable_review_20261001/comparison_report/FACC_COMPARISON_v17b_v17c_new.md`
(section 5a lists the defects). Do not redo the diagnosis or the matcher; fix the integration rules.

## State
- **Released:** v17c beta 0.0.13 = v1 product. Branch `release/v17c-0.0.13-facc`, commits `9e60a6c`,
  `9ba64d0`, not pushed. DuckDB `data/duckdb/sword_v17c.duckdb` has the new `facc`/`facc_quality`
  (ops 1002–1004; 1003 was a verified rollback). Backups: `output/facc_detection/fable_review_20261001/release_0.0.13/backup/`,
  DuckDB tables `facc_backup_0012_*`, APFS clone `data/duckdb/backups/sword_v17c_pre_0_0_13_facc_20261002.duckdb`.
  Exports `data/exports/v17c_beta/zenodo_0.0.13/` (33 GB, SHA256SUMS inside). Rollback:
  `scripts/maintenance/apply_facc_rederivation.py --rollback`.
- **Matcher runs on River:** v1 `fable-hmm-global-v1-20261002T010550Z` (141 s), v2 with joint junctions +
  conservation `fable-hmm-global-v2-20261002T140026Z` (225 s); outputs retrieved under
  `output/facc_detection/fable_review_20261001/soft_matcher/river_runs/`. Bundles and resume procedure in
  `soft_matcher/river_bundle/README.md` and `river_bundle_v2/README.md`. Staging recipe:
  `zsh -ic 'source stage_and_submit.sh <RUNID> <bundle>'` (helpers are zsh functions; remote shell is tcsh).
- **Integrated products:** v1 `integrate_global/outputs/final_{nodes,reaches}_{R}.parquet` (shipped as 0.0.13);
  v2 `integrate_global_v2/outputs/` (84.8% of outlets within 20%, junction_inconsistent 1,299). Fill-only
  conservative product `network_fill/outputs/`.
- **References built:** 6,262 HydroBASINS outlets; 101 random USGS gauges (`integrate_global/outputs/diag_random_usgs_gauges_NA.csv`);
  2,298 SVS gauges vs HydroSHEDS (`diag_svs_gauges_hydrosheds.csv`, SVS at `~/geodata/canonical/svs/raw/SVS_v1_1_2.nc`,
  RiverATLAS on the Drive mount). Jake's gauge archives with drainage areas: `~/geodata/canonical/{hydat,usgs_discharge,brazil_ana,uk_nrfa,r_arcticnet,quebec_cehq,...}`.

## What is wrong (Jake's review, all confirmed)
1. **No partitioning at bifurcations/deltas.** D8 sends everything down one distributary; the matcher
   reads raw cells, so other distributaries get hillslope values and the Lena/Ganges/Murray deltas lose
   their area. v17c's width-share rule had the right *shape*; it was only applied inside the fill (1.3%).
2. **Clone sums at braid rejoins** (8 cases: Amazon 9.03 M km², Lena 4.89 M, Amur 3.38 M).
3. **Negative spikes / lost stretches** on trunks (Lena, Missouri, Ohio stretch of 72 nodes, Ganges/Padma,
   lower Murray, Brahmaputra outlet 30 km²): matcher on a side channel for a few km; repair pass too timid.
4. **Width check skips lake/tidal/ghost/other** (66,449 reaches), so wrong low values survive there.
5. **Reference pairing artifacts** inflate the Murray/Orange "errors" and the <1e3 km² basin bin; remove
   pairs where the SWORD reach is a different river than the HydroBASINS unit before reporting.
6. Use `rch_id_up_main` / `rch_id_dn_main` / `main_path_id` / `is_mainstem` (v17c variables), not `main_side`.

## The fix (specified, not yet run; previous agent hit the API limit)
Implement as `integrate_global_v2b/` on the v2 matcher output, in this order, each with before/after on
the three references and the named rivers (Amazon mouth, Lena 34211100221 + delta outlets, Amur, Ganges/
Padma, Brahmaputra outlet, Murray/Darling outlets, Ohio stretch, Missouri spike):
A. **Monotone along every section:** running max from the upstream end (lateral increments only); at
   junctions downstream head ≥ largest inflow. Never lowers, never raises above upstream. Removes dips.
B. **Rejoin dedup:** arms sharing a bifurcation ancestor within 50 reaches (walk `rch_id_up`) count once:
   max(tails) + the other arms' lateral increments, never Σ tails. Flag `rejoin_dedup`.
C. **Partition at every bifurcation:** children = parent × share, propagated down each branch as share +
   own lateral increments until rejoin or end; share from width (v17c rule; equal split if widths
   missing), later from the routing score / `main_path_id`. Apply whenever children clone the parent
   (≥ 0.9×) or carry hillslope values (< 0.1× parent); keep a child that the matcher found as a real
   distinct channel. Flag `bif_width_split`.
D. **Trunk drop bridging:** any node below 1% of the max upstream tail on a reach with width ≥ 500 m or
   v17b ≥ 1e5 km² is a lost stretch regardless of flags; bridge it.
E. **Width plausibility per class** (lake, tidal, ghost, other) with class-specific fits, then the same
   demotion/refusal logic.
F. Report with pairing artifacts removed; then release 0.0.14 with `apply_facc_rederivation.py --source-dir integrate_global_v2b/outputs`.

## Rules that must not be broken
- HydroBASINS, USGS, SVS/HydroSHEDS are evaluation only; never inputs.
- No old-facc value chooses a MERIT route (circularity).
- v17b DuckDB is read-only; every DB write through the maintenance script with backup and `sword_operations` row.
- Report accuracy per tier and per reference; never merge "calculated" with "validated".
