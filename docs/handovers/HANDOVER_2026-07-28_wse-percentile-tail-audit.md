# ARCHIVED HANDOVER — Global SWOT WSE percentile-tail investigation

**Date:** 2026-07-28

**Branch:** `codex/wse-percentile-tail-handover`

**Handover commit:** `815026b`

**Status:** Raw reconstruction and first-pass classification complete. The
presentation-safe audited map is rendered. No SWORD database was modified.

## Resolution

The stored node-level p90-p10 upper tail is dominated by a production
aggregation/QC mismatch, not a pervasive global hydrologic signal.

All **354,544** eligible nodes with stored p90-p10 spread at least 20 m were
reconstructed from raw SWOT Collection D RiverSP observations. With the locked
cutoff through 2026-03-22, exact raw `wse` percentiles paired with their
corresponding `node_q <= 1` filter produce:

| Raw reconstruction outcome | Nodes | Share of target |
|---|---:|---:|
| Reconstructed below 20 m | 238,542 | 67.28% |
| Reconstructed at or above 20 m | 108,282 | 30.54% |
| No usable reconstruction | 7,720 | 2.18% |

The largest single finding is therefore the **238,542-node collapse below
20 m**. The remaining 116,002 nodes are not a homogeneous physical tail.
Observation-, cycle/pass-, pass-, smoothed-WSE-, controller-quality-, and
reach-coherence tests separate them into auditable classes.

## Final five-way classification

| Audit category | Nodes | Share | Reaches |
|---|---:|---:|---:|
| `CONFIRMED_AGGREGATION_FILTER_ARTIFACT` | 238,542 | 67.28% | 39,372 |
| `LOW_SUPPORT_UNSTABLE` | 45,632 | 12.87% | 10,529 |
| `PASS_DATUM_OFFSET_PATTERN` | 39,810 | 11.23% | 7,296 |
| `REACH_COHERENT_PHYSICAL_CANDIDATE` | 5,276 | 1.49% | 167 |
| `UNRESOLVED` | 25,284 | 7.13% | 8,033 |

The classes are mutually exclusive:

- **Confirmed aggregation/filter artifact:** exact raw `wse` with
  `node_q <= 1` is below 20 m.
- **Low support/unstable:** fewer than 20 usable observations, instability
  after removing one observation or cycle/pass, or no valid sensitivity
  result.
- **Pass/datum-offset pattern:** stable to single-observation and cycle/pass
  deletion but below 20 m when one recurring pass is removed.
- **Reach-coherent physical candidate:** survives the raw/QC pairing,
  smoothed-WSE agreement, every leave-one-unit screen, multi-cycle and
  multi-pass recurrence in both tails, and occurs in at least three supported
  nodes covering at least half of the reach.
- **Unresolved:** does not meet any preceding rule.

`REACH_COHERENT_PHYSICAL_CANDIDATE` is a review-priority class, not proof of a
hydrologic cause.

## Sensitivity and support

Among the 108,282 exact `node_q <= 1` survivors:

- 27,793 fall below 20 m after removing one observation;
- 27,972 fall below 20 m after removing one cycle/pass group;
- 57,488 have a recurring pass whose removal lowers the spread below 20 m;
- 13,764 pass the recurrence, smoothed-WSE, and all leave-one-unit screens;
- 5,276 of those strong nodes form the 167 reach-coherent candidate reaches.

Strictly good observations are much less supportive:

- only 111,707 targets have any `node_q=0` reconstruction;
- only 5,188 remain at or above 20 m;
- only 2,453 of the 13,764 individually strong nodes remain at or above 20 m
  with `node_q=0`.

Controller quality also limits literal interpretation of the physical-candidate
class:

- p10 controller: 1,179 of 5,276 have `node_q=0`; 3,170 carry the
  classification-suspect bit and 2,207 the geolocation-suspect bit;
- p90 controller: 740 of 5,276 have `node_q=0`; 3,738 carry the
  classification-suspect bit and 2,905 the geolocation-suspect bit.

The RiverSP product description defines `node_q=1` as suspect and warns that
suspect data may contain large errors. Keep controller bitfields visible in
case-level review.

## Robustness to newly available observations

A second reconstruction used all observations available on River on
2026-07-28. It did not materially change the result:

| Exact `wse + node_q <= 1` outcome | Through 2026-03-22 | All available |
|---|---:|---:|
| Below 20 m | 238,542 (67.28%) | 237,839 (67.08%) |
| At or above 20 m | 108,282 (30.54%) | 109,308 (30.83%) |
| Missing | 7,720 | 7,397 |

The March-22-bounded run remains the classification authority because it is
directly comparable with the previous raw audit. The all-available run is a
robustness check only.

## Presentation decision

The final presentation map:

1. leaves stored p80-p20 and p90-p10 values unchanged where stored p90-p10 is
   below 20 m;
2. substitutes the exact `wse + node_q <= 1` percentiles for the 238,542
   triggered nodes that reconstruct below 20 m;
3. withholds all 116,002 remaining triggered nodes in gray, including the
   reach-coherent physical candidates.

This is conservative but defensible. It shows ordinary QC-screened spread
without silently promoting unresolved upper-tail cases into a map of global
hydrologic variability.

Selected two-panel output:

- `outputs/qc/swot_wse_percentiles_20260727/figures/SWORD_v17c_global_node_wse_percentile_spreads_talk_screen_dark_mako_continuous-log_audited-ge20-reprocessed-screen_400dpi.png`
- matching PDF and standalone p80-p20 and p90-p10 PNG/PDF panels
- `outputs/qc/swot_wse_percentiles_20260727/figures/tamlin_wse_percentile_map_talk_screen_mako_continuous-log_audited-ge20-reprocessed-screen_manifest.json`

The map uses a shared 0.1-20 m continuous logarithmic mako scale. It rasterizes
nodes to a 3200-pixel-wide Robinson grid using the maximum observed node value
per occupied display cell; there is no interpolation.

Suggested assertion:

> Most SWORD nodes show modest SWOT-observed WSE percentile spread after exact
> raw-observation screening; the remaining upper tail is withheld pending
> case-level interpretation.

Do not claim that the withheld residual is false, that the candidate class is
confirmed hydrology, or that dam-catalog absence disproves an event.

## Authoritative artifacts

Target cohort:

- `outputs/qc/swot_wse_percentiles_20260727/raw_river/stored_ge20_node_targets.parquet`
- 354,544 unique nodes; SHA-256
  `9d70c1474ea4122679b7b621e75c7142993eac4de789ec0b733a071fd5df9eab`

March-22 reconstruction:

- `outputs/qc/swot_wse_percentiles_20260727/raw_river/ge20_march22/summary/node_ge20_reconstruction_comparison.parquet`
- `outputs/qc/swot_wse_percentiles_20260727/raw_river/ge20_march22/summary/ge20_method_summary.parquet`
- `outputs/qc/swot_wse_percentiles_20260727/raw_river/ge20_march22/residual_sensitivity/`

Classification:

- `outputs/qc/swot_wse_percentiles_20260727/raw_river/ge20_march22/classification/node_ge20_auditable_classification.parquet`
- `outputs/qc/swot_wse_percentiles_20260727/raw_river/ge20_march22/classification/ge20_reach_coherent_physical_candidates.parquet`
- `outputs/qc/swot_wse_percentiles_20260727/raw_river/ge20_march22/classification/ge20_audit_classification.md`

Presentation input:

- `outputs/qc/swot_wse_percentiles_20260727/raw_river/ge20_march22/presentation/node_wse_percentile_ge20_audited_presentation.parquet`
- 11,112,454 unique rows; 9,246,953 displayed eligible nodes; SHA-256
  `fe92cc683fca0f889566024d668ddeca823af473c8a993d5956c8150e1f9996c`

All-available robustness summary:

- `outputs/qc/swot_wse_percentiles_20260727/raw_river/ge20_all_available_20260728/summary/`

## Implementation

New analysis entry points:

- `scripts/analysis/summarize_swot_wse_ge20_reconstruction.py`
- `scripts/analysis/summarize_swot_wse_ge20_residual_sensitivity.py`
- `scripts/analysis/classify_swot_wse_ge20_reconstruction.py`
- `scripts/analysis/build_tamlin_wse_ge20_audited_map_input.py`
- `scripts/visualization/render_tamlin_wse_percentile_map.py`

River work completed through the configured `river` helper and SLURM jobs
1340-1344. Remote audit roots:

- `/not_backed_up/jgearon/swot/qc/swot_wse_percentiles_20260727/ge20_march22`
- `/not_backed_up/jgearon/swot/qc/swot_wse_percentiles_20260727/ge20_all_available_20260728`

## Remaining scientific work

The global screening question is answered. The next useful work is narrower:

1. review the 167 reach-coherent candidate reaches using observation
   chronology, pass geometry, reach/lake type, imagery, and dam context;
2. keep `node_q_b` and smoothed-WSE quality bits visible in every case;
3. separate persistent physical changes from repeatable pass/datum offsets;
4. only create an unmasked physical-event layer from individually corroborated
   cases.

This case review is not required to use the conservative presentation map,
because that map withholds the entire residual tail.

## Repository preservation

- `sword_v17b.duckdb` and `sword_v17c.duckdb` were not modified.
- `.napkin.md`, `README.md`, and unrelated pre-existing untracked scripts were
  left untouched.
- Do not stage all untracked files blindly. Review the exact investigation
  script and documentation scope first.
