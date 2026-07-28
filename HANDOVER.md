# HANDOVER — Global SWOT WSE percentile-tail investigation

**Date:** 2026-07-28

**Branch:** `codex/wse-percentile-tail-handover`

**Starting commit:** `4e54ccd` (`Merge v17c beta 0.0.12 release prep`)

**Status:** Investigation incomplete; next session should begin with the full River raw-observation audit described below.

## Why this handover exists

The global node-level maps of SWOT WSE percentile spread show a pervasive upper tail, especially across the Tibetan Plateau and Himalayas. The working conclusion is:

- the stored v17c percentile fields cannot yet be presented as a clean map of hydrologic variability;
- the known `>=50 m` audit proves that much of the extreme tail is caused by percentile computation and observation-QC inconsistencies;
- a smaller subset contains plausible reach-coherent physical events, including reservoir filling and operations;
- the much larger `20–50 m` shoulder has not yet been reconstructed from raw RiverSP observations, so its cause is unresolved;
- the current non-emergency filtered map is therefore useful for diagnosis, but not presentation-ready as a literal hydrologic signal.

The next session needs to get to the bottom of the full `>=20 m` population before a final image is sent to Tamlin.

## Non-negotiable interpretation corrections

1. Here, **original** or **stored** means the current v17c node percentile fields. It does not mean a fresh reconstruction from raw RiverSP observations.
2. RiverSP observation latitude/longitude are detected-water centroids. They are not evidence that a fixed SWORD `node_id` moved geographically. Do not revive the earlier “spatial mixing” interpretation based on observation centroids.
3. The global maps do not interpolate. They rasterize nodes into a 3200-pixel-wide Robinson grid and retain the maximum node value in each display cell. This greatly amplifies the visual prevalence of rare high values.
4. Absence from DL-GROD/HydroFALLS is not evidence that an extreme is false. Recent or incomplete dams, including known reservoir-filling examples, may be missing.
5. Do not edit `sword_v17b.duckdb`. No database modification is required for this investigation.

## Locked population and distribution results

Eligible node population: **9,362,955 nodes**.

### Original stored percentile spreads

| Spread | `>=20 m` | `>=50 m` | `>=100 m` | Maximum |
|---|---:|---:|---:|---:|
| p60–p40 | 35,022 | 7,991 | 2,407 | 881.086 m |
| p70–p30 | 91,260 | 21,448 | 6,820 | 1,373.388 m |
| p80–p20 | 177,286 | 42,877 | 13,976 | 1,571.298 m |
| p90–p10 | 354,544 | 86,717 | 28,419 | 1,820.197 m |

For stored p90–p10:

- median: **2.279 m**
- p95: **16.183 m**
- p99: **47.915 m**

### Current non-emergency filtered product

Full-suite population: **9,340,693 nodes**. This excludes 20,396 unresolved `>=50 m` nodes, 1,862 repaired zero outer spreads, and four zero intermediate spreads.

| Spread | `>=20 m` | `>=50 m` | `>=100 m` | Maximum |
|---|---:|---:|---:|---:|
| p60–p40 | 13,577 | 221 | 14 | 162.933 m |
| p70–p30 | 49,447 | 712 | 73 | 176.120 m |
| p80–p20 | 116,547 | 1,559 | 220 | 185.951 m |
| p90–p10 | 284,986 | 2,406 | 602 | 198.997 m |

For filtered p90–p10:

- median: **2.263 m**
- p95: **14.726 m**
- p99: **33.300 m**

## The central unresolved result

The current conservative screen only reconstructed stored p90–p10 values at or above 50 m. It barely touches the large 30–50 m shoulder:

| Displayed filtered p90–p10 | Nodes | Still using stored value | Repaired from raw observations |
|---|---:|---:|---:|
| 30–40 m | 76,611 | 72,196 (94.2%) | 4,415 |
| 40–50 m | 45,611 | 40,494 (88.8%) | 5,117 |
| 30–50 m total | 122,222 | 112,690 (92.2%) | 9,532 |

This is the strongest reason not to treat the current non-emergency map as final. The apparent 30–50 m prevalence is overwhelmingly unaudited stored data, not surviving reconstructed raw-observation evidence.

Within those 112,690 untouched nodes:

- 61,869 (54.9%) have at least one database-only statistical warning: low sample count, tail inflation, tail imbalance, or a thin central distribution;
- only 12,935 (about 11.5%) have a mapped physical-context flag;
- median stored `n_obs` is 30 for 30–40 m and 29 for 40–50 m;
- median p80–p20 is 22.186 m and 30.286 m, respectively;
- median tail-inflation ratio is 1.533 and 1.465;
- median tail-imbalance ratio is 0.883 and 0.893.

These summaries cannot determine which individual nodes are false. They show that a large fraction is statistically suspicious and that raw reconstruction is necessary.

## Reach coherence

The untouched 30–50 m shoulder spans **25,562 reaches**:

- 9,025 affected reaches contain only one flagged node;
- the median affected-node fraction per reach is 5.77%;
- only 7.7% of nodes lie on multi-node reaches where at least half the reach is in the 30–50 m band;
- 35.9% are isolated or occupy less than 10% of their reach.

Many nodes therefore look spatially isolated, but there are coherent reaches on the Peace, Angara, Omo, Jinsha, Yellow River, and elsewhere. Several are lake-like (`lakeflag=1`, commonly `type=3`) and may reflect genuine impoundment filling, reservoir operation, pass offsets, or reach-wide datum effects. Reach coherence is a prioritization signal, not proof of physical variability.

## What the completed `>=50 m` raw audit established

Original stored p90–p10 extremes: **86,717 nodes**.

| Outcome | Nodes |
|---|---:|
| Exact, internally consistent `wse` with `node_q <= 1` falls below 50 m | 63,434 |
| Leave-one-observation correction falls below 50 m | 481 |
| Well-supported extremes retained | 2,406 |
| Unresolved and masked | 20,396 |

Known production mechanisms:

- production percentiles used `APPROX_QUANTILE`, whereas the audit used exact `QUANTILE_CONT`;
- node aggregation used raw `wse` while filtering on `wse_sm_q`, rather than applying the corresponding `node_q`;
- in the `>=50 m` target set, this admitted 1,306,015 rows that passed the production filter but had `node_q` 2 or 3;
- residual extremes often involve `node_q=1` suspect observations, sparse support, or pass/cycle-level offsets and datum jumps;
- some cases are physically real, including the Maerdang and Tuoba reservoir-filling examples.

The important point is that 2,406 retained extremes are not automatically 2,406 genuine hydrologic excursions. They are only the subset that survived the current conservative screen.

## Display amplification

The map uses maximum-node aggregation within display pixels:

- p90–p10 node values `>=20 m`: 3.05% of nodes, but 21.7% of occupied map cells;
- node values `>=40 m`: 0.514% of nodes, but 7.85% of occupied map cells.

On the continuous-log Jet scale, a red-looking pixel does not necessarily mean 50 m. Approximate red tones begin around 13–37 m, with the darkest red approaching 50 m. Always inspect the color bar before interpreting screenshots.

## Current defensible and diagnostic figures

All paths below are under:

`outputs/qc/swot_wse_percentiles_20260727/figures/`

### Emergency presentation map

The emergency product masks every final displayed p90–p10 node at or above 20 m after the existing repair and renders those nodes gray in both percentile-spread maps:

- `SWORD_v17c_global_node_wse_p90-p10_spread_talk_screen_dark_jet_continuous-log_emergency-pending-raw-qc-ge20m_400dpi.png`
- corresponding p80–p20 PNG and PDFs
- `tamlin_wse_percentile_map_talk_screen_jet_continuous-log_independent-scales_emergency-pending-raw-qc-ge20m_manifest.json`

Emergency counts:

- pending raw-QC nodes: **284,987**
- visible mapped nodes: **9,057,572**
- visible raw-repaired nodes: 49,066 quality-matched plus 95 leave-one-observation cases

This is the only currently defensible presentation fallback, but its caption must say that high-spread nodes are masked pending raw-observation QC.

### Non-emergency diagnostic maps

- `SWORD_v17c_global_node_wse_p80-p20_spread_talk_screen_dark_jet_continuous-log_400dpi.png`
- `SWORD_v17c_global_node_wse_p90-p10_spread_talk_screen_dark_jet_continuous-log_400dpi.png`
- `tamlin_wse_percentile_map_talk_screen_jet_continuous-log_independent-scales_manifest.json`

Do not present these as a literal global hydrologic-variability result until the `>=20 m` raw audit is complete.

### Distribution figures

- `SWORD_v17c_node_wse_full_suite_original_vs_non_emergency_filtered_histograms_dark_400dpi.png`
- corresponding PDF and summary JSON
- `SWORD_v17c_node_wse_percentile_spread_distribution_talk_screen_dark_400dpi.png`

The full-suite comparison is 6400 × 3600 pixels at 400 dpi and is the best current diagnostic overview.

Additional interpretation notes:

- `outputs/qc/swot_wse_percentiles_20260727/qc_summary.md`
- `outputs/qc/swot_wse_percentiles_20260727/residual_ge50_interpretation.md`
- `outputs/qc/swot_wse_percentiles_20260727/figures/tamlin_wse_percentile_map_talk_screen_handoff.md`

This root handover supersedes those notes where they differ.

## Scripts involved

Current analysis and rendering work is intentionally left uncommitted and visible in `git status`. Important files include:

- `scripts/analysis/reconstruct_swot_wse_raw_qc.py`
- `scripts/analysis/integrate_swot_wse_raw_qc.py`
- `scripts/analysis/build_talk_screen_leave_one_observation.py`
- `scripts/analysis/summarize_swot_wse_raw_sensitivity.py`
- `scripts/analysis/audit_swot_wse_percentiles.py`
- `scripts/analysis/crosscheck_wse_spread_tail_dl_grod.py`
- `scripts/visualization/render_tamlin_wse_percentile_map.py`
- `scripts/visualization/render_tamlin_wse_percentile_distribution.py`
- `scripts/visualization/render_full_suite_raw_vs_filtered_histograms.py`
- `scripts/visualization/render_swot_wse_raw_qc_figures.py`

Recent implementation details:

- `build_talk_screen_leave_one_observation.py` now reconstructs p10 through p90 after deleting the influential observation. Its latest successful rebuild found 2,887 initial candidates, 481 single-observation-unstable nodes, 2,406 stable nodes, and zero percentile-order failures.
- `render_tamlin_wse_percentile_map.py` implements the non-emergency repair path and supports emergency masking through `SWORD_MAP_PENDING_RAW_QC_GE_M=20`.

Do not stage all untracked scripts blindly. `.napkin.md` and `README.md` also contain pre-existing modifications and must be preserved unless their exact scope is reviewed.

## River access blocker at handover

The `river` shell helper is available only in an interactive zsh startup environment. The latest connection attempt hung before authentication:

- `zsh -ic 'type river'` resolves the helper;
- a PTY call through the helper produced no output for 60 seconds;
- TCP port 22 to `152.19.218.21` timed out;
- the active route used gateway `192.168.1.1`, interface `en0`, local address `192.168.1.157`.

This is a network-route/VPN/campus-access blocker, not a credential or query failure. Re-establish VPN or campus routing before retrying.

Relevant River locations:

- raw RiverSP parquet lake: `/not_backed_up/jgearon/swot/parquet_lake_D`
- prior audit root: `/not_backed_up/jgearon/swot/qc/swot_wse_percentiles_20260727`
- prior remote reconstruction script: `reconstruct_swot_wse_raw_qc.py`

The previous full `>=50 m` scan completed in about 112.5 seconds, so the expanded target is tractable once River is reachable.

## Exact next-session plan

1. Read this handover before inspecting older notes.
2. Restore River connectivity. Verify port 22, then run a trivial command through `river`.
3. Build `stored_ge20_node_targets.parquet` for all **354,544** eligible stored p90–p10 `>=20 m` nodes. Match the existing full-target schema and use `target_scope='NODE_OUTER_GE20'`.
4. Push the target parquet and the current `reconstruct_swot_wse_raw_qc.py` to the remote audit directory.
5. Run bounded, exact reconstructions for:
   - production-filter reproduction;
   - exact `wse` with the internally corresponding `node_q <= 1`;
   - smoothed `wse_sm` with `wse_sm_q`;
   - `node_q = 0` as a strict diagnostic.
6. Initially retain the previous cutoff through **2026-03-22** for direct comparability. Optionally repeat with all currently available observations afterward.
7. Pull summary tables first. Quantify the collapse by stored bins 20–30, 30–40, 40–50, and `>=50 m`; region; sample count; approximate versus exact quantiles; and QC filter.
8. On the reconstructed residual `>=20 m` or `>=30 m` population, run:
   - leave-one-observation sensitivity;
   - leave-one-pass or leave-one-cycle/pass sensitivity;
   - controller-node quality-bit summaries.
9. Assign every candidate to one of five auditable categories:
   - confirmed aggregation/filter artifact;
   - pass/datum-offset pattern;
   - low-sample unstable;
   - reach-coherent physical candidate;
   - unresolved.
10. Cross-check coherent physical candidates against lake and reach type, DL-GROD/HydroFALLS, recent dams, and the observation chronology. Treat dam-catalog absence as unknown, not negative evidence.
11. Re-render the final Tamlin map only from reprocessed values. If presentation timing arrives first, use the emergency `>=20 m` masked version with explicit provisional wording.
12. Update this handover and the QC summary with the resulting decision rule and counts.

## Recommended decision gate

Do not release a “global SWOT variability” figure until the full `>=20 m` reconstruction answers:

1. What fraction of the 284,986 currently displayed p90–p10 `>=20 m` nodes collapse under an internally consistent observation/QC pairing?
2. Among survivors, how many are supported across multiple observations and multiple passes?
3. How many are reach-coherent or temporally consistent with a known physical event?
4. What residual population remains unresolved?

The target outcome is not a surgical classifier for every node. It is a transparent, reproducible screen that removes clear artifacts, preserves defensible physical excursions, and either supports a presentation map or clearly justifies masking the unresolved tail.

## Repository state and preservation

- Base branch was `main`; this handover was created on `codex/wse-percentile-tail-handover`.
- The working tree already contained modified `.napkin.md`, modified `README.md`, and the untracked analysis/rendering scripts listed by `git status`.
- Those files were deliberately not folded into the handover commit.
- The prior May 28 release handover was archived as `docs/handovers/HANDOVER_2026-05-28_v17c-0.0.12.md`.
- Source databases were not modified during this QC and figure work.

## First sentence for the next session

“Resume the global SWOT WSE percentile-tail investigation from `HANDOVER.md`; first restore River access and reconstruct every stored node with p90–p10 `>=20 m` from raw Collection D observations before changing the Tamlin map.”
