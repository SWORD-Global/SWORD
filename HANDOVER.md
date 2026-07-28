# HANDOVER — Stored WSE upper-tail case viewer

**Date:** 2026-07-28

**Branch:** `codex/wse-percentile-tail-handover`

**Parent commit:** `815026b`

**Status:** Stop iterating on presentation maps. The next task is to build a
local HTML case-review viewer for the alarming stored WSE upper tail, beginning
with western China and the Tibetan Plateau. No filtering or treatment decision
has been made, and no SWORD database was modified.

## Immediate concern

The direct-stored p90-p10 map contains a large, visually coherent concentration
of values above the 50 m display cap east and northeast of India, across western
China and the Tibetan Plateau. It is too extensive to dismiss as a few
reservoir-filling examples and too suspicious to present as literal hydrologic
variability without case-level inspection.

Do not explain the cluster away. Build the viewer, inspect representative
nodes and reaches, record the evidence, and only then decide whether filtering,
special treatment, or different scientific framing is warranted.

## Locked decisions from this session

1. The investigation layer must use the **stored SWORD v17c percentile
   fields**, not substitute a recalculation into the map.
2. Do **not** gate the investigation layer at 20 m. The 20 m value was an audit
   trigger, not a scientifically privileged boundary.
3. The selected direct-stored maps use a 50 m color-scale ceiling. That is a
   **display cap, not a data cutoff**. Values above 50 m are retained and shown
   in white.
4. Do not label the upper tail, or the western-China cluster, as reservoirs.
   Reservoir filling is one plausible physical cause for some cases, not an
   explanation for the regional pattern.
5. The acceptable interim wording is:

   > The residual tail may mix unresolved measurement effects with real
   > physical changes, including reservoir filling.

6. Do not choose or apply a production filter during viewer construction.
   Record candidate evidence and preserve every original stored value.
7. Keep the existing raw reconstruction and audit classes available as
   comparison layers. They are diagnostics; they must not silently replace the
   stored values being investigated.

## What the regional count actually shows

The first bounded count used the stored database-only parquet and its existing
basic `eligible` flag. The western-China investigation box is provisionally:

`90–105°E, 25–36°N`

Within that box:

| Stored p90-p10 result | Nodes | Reaches |
|---|---:|---:|
| At least 20 m | 23,018 | — |
| At least 50 m | 9,430 | 1,305 |
| At least 100 m | 3,879 | — |

For the 9,430 nodes at or above 50 m:

- median stored spread is **88.0 m**;
- median stored `n_obs` is **23**;
- 799 nodes have the coarse database `physical_context_flag`;
- the values are distributed across many reaches, not a single reservoir or a
  small handful of sites.

The same 9,430 nodes divide under the completed raw-observation audit as:

| Audit category | Nodes | Share | Distinct reaches |
|---|---:|---:|---:|
| `CONFIRMED_AGGREGATION_FILTER_ARTIFACT` | 4,501 | 47.7% | 1,028 |
| `LOW_SUPPORT_UNSTABLE` | 2,392 | 25.4% | 494 |
| `PASS_DATUM_OFFSET_PATTERN` | 1,072 | 11.4% | 305 |
| `UNRESOLVED` | 926 | 9.8% | 366 |
| `REACH_COHERENT_PHYSICAL_CANDIDATE` | 539 | 5.7% | 20 |

This is the key starting fact. The stored regional pattern is real as a product
feature, but much of it is already known to collapse or become unstable under
quality-matched reconstruction. The 539 reach-coherent candidates still are
not proof of reservoirs or even proof of physical variability.

A broader `73–105°E, 25–40°N` check contains 13,758 eligible stored nodes at or
above 50 m on 1,931 reaches. The highest-density two-degree cells include:

- 100–102°E, 30–32°N: 1,109 nodes on 67 reaches;
- 102–104°E, 26–28°N: 705 nodes on 62 reaches;
- 104–106°E, 26–28°N: 566 nodes on 42 reaches;
- 102–104°E, 28–30°N: 560 nodes on 78 reaches;
- 94–96°E, 28–30°N: 547 nodes on 80 reaches.

Use the western-China box as the default view, but derive the actual connected
hotspot footprint rather than treating this provisional rectangle as a
scientific region.

## Why the global raster looks especially alarming

The current map does not interpolate, but it does rasterize nodes into a
3200-pixel-wide Robinson grid and retain the **maximum node value in each
occupied display cell**. At continental scale, a single extreme node can
control a cell. Values above the 50 m color cap all become the same white
over-range color.

The HTML viewer must therefore show actual node locations and reach context.
When zoomed out, any clustering must distinguish:

- node count;
- maximum spread;
- median spread; and
- fraction above the selected threshold.

Do not reproduce the maximum-only raster as the primary diagnostic view.

## Selected direct-stored maps

These are the clean, annotation-free maps that triggered the current concern:

- p80-p20:
  `outputs/qc/swot_wse_percentiles_20260727/figures/SWORD_v17c_global_node_wse_p80-p20_spread_talk_screen_dark_jet_continuous-log_independent-scales_stored-sword_clean-no-annotations_400dpi.png`
- p90-p10:
  `outputs/qc/swot_wse_percentiles_20260727/figures/SWORD_v17c_global_node_wse_p90-p10_spread_talk_screen_dark_jet_continuous-log_independent-scales_stored-sword_clean-no-annotations_400dpi.png`
- combined map:
  `outputs/qc/swot_wse_percentiles_20260727/figures/SWORD_v17c_global_node_wse_percentile_spreads_talk_screen_dark_jet_continuous-log_independent-scales_stored-sword_clean-no-annotations_400dpi.png`
- authoritative render manifest:
  `outputs/qc/swot_wse_percentiles_20260727/figures/tamlin_wse_percentile_map_talk_screen_jet_continuous-log_independent-scales_stored-sword_clean-no-annotations_manifest.json`
- current wording handoff:
  `outputs/qc/swot_wse_percentiles_20260727/figures/tamlin_wse_percentile_map_stored_sword_50m_handoff.md`

The p80-p20 scale is logarithmic from 0.1 to 50 m. The p90-p10 scale is
logarithmic from 0.5 to 50 m. The scales are panel-specific and the colors are
not numerically comparable between panels.

Reproduction command:

```bash
SWORD_MAP_SOURCE_MODE=stored-sword \
SWORD_MAP_COLORMAP=jet \
SWORD_MAP_SCALE=continuous \
SWORD_MAP_PANEL_SCALES=independent \
SWORD_MAP_SCALE_MAX_M=50 \
SWORD_MAP_EXPORT_SEPARATE=1 \
SWORD_MAP_SHOW_PANEL_ANNOTATIONS=0 \
SWORD_MAP_SHOW_STATUS_LEGEND=0 \
SWORD_MAP_OUTPUT_TAG=clean-no-annotations \
python scripts/visualization/render_tamlin_wse_percentile_map.py
```

## Viewer mission

Build a local HTML viewer that answers four questions:

1. Is the apparent regional mass created mainly by isolated extreme nodes,
   whole reaches, connected river corridors, or display aggregation?
2. Which stored extremes collapse under exact quality-matched reconstruction,
   and what observation, pass, cycle, or controller behavior explains them?
3. Which cases remain reach-coherent and temporally plausible after audit?
4. Among surviving cases, what evidence supports reservoir filling, other
   physical change, repeatable pass/datum effects, or an unresolved mixture?

The first deliverable is an evidence-recording investigation tool, not a new
figure and not a final filter.

## Minimum viewer behavior

### Map

- Open by default to `90–105°E, 25–36°N`.
- Plot actual SWORD nodes, with an optional reach-line layer.
- Toggle stored p90-p10 and stored p80-p20.
- Filter or style by thresholds such as 20, 50, 100, and 200 m without deleting
  any rows.
- Toggle the five audit categories and `physical_context_flag`.
- Provide search for `node_id` and `reach_id`.
- At low zoom, show count, maximum, median, and threshold fraction separately;
  never collapse all four into a maximum-only symbol.
- Keep a basemap or imagery option for physical context if it can be used
  locally and reproducibly.

### Node detail

At minimum show:

- `node_id`, `reach_id`, `node_order`, region, longitude, latitude;
- stored `wse_obs_p10`, `p20`, `p50`, `p80`, and `p90`;
- stored p80-p20 and p90-p10 spreads;
- stored `n_obs`, tail inflation, tail imbalance, and relevant database flags;
- `lakeflag`, parent `type`, `obstr_type`, `grod_id`, `hfalls_id`,
  `parent_dl_grod_id`, and `physical_context_flag`;
- audit category and reason;
- exact `wse + node_q <= 1` spread, `node_q=0` spread, smoothed-WSE spread,
  leave-one-observation minimum, leave-one-cycle/pass minimum, and
  leave-one-pass minimum;
- p10 and p90 controller cycle, pass, granule, `node_q`, `node_q_b`,
  `wse_sm_q`, and `wse_sm_q_b`.

### Reach context

- Show every eligible node along the selected reach in `node_order`.
- Report number and fraction above 20, 50, 100, and 200 m.
- Show the longest contiguous run above the active threshold.
- Plot stored and reconstructed spreads along the reach.
- Make neighboring upstream and downstream reaches discoverable if practical.
- Clearly separate database lake/dam context from analyst interpretation.

### Observation evidence

The first version can use the local reconstruction summaries, but the viewer
should leave a clear route to attach raw RiverSP observations for selected
cases. For a reviewed case, eventually show:

- observation date or cycle/pass chronology;
- raw `wse` and smoothed WSE;
- `node_q`, `node_q_b`, `wse_sm_q`, and `wse_sm_q_b`;
- which observations control stored/reconstructed p10 and p90;
- the result of removing an observation, cycle/pass group, or recurring pass.

Do not block the initial HTML build on a global raw-observation download.
Start with the local regional subset, then fetch raw histories for selected
cases through River.

## Review recording

The viewer must support persistent, reloadable review records. Static HTML can
use browser local storage plus explicit JSON/CSV export and import. Every record
should include:

- stable `node_id` and `reach_id`;
- review timestamp and reviewer;
- stored values and audit category captured at review time;
- disposition:
  `aggregation/filter artifact`, `low support/unstable`,
  `pass/datum pattern`, `plausible reservoir/impoundment`,
  `plausible other physical change`, `mixed`, or `unresolved`;
- confidence;
- evidence checked;
- free-text notes;
- optional bookmark/screenshot reference.

Keep the recorded analyst disposition separate from the automated audit
category. Preserve the original values and maintain an append-only export or
revision history so a changed judgment is auditable.

## Authoritative local inputs

Direct stored product:

- `outputs/qc/swot_wse_percentiles_20260727/node_wse_percentile_qc_database_only.parquet`
- 11,112,454 total nodes; 9,362,955 basic eligible nodes;
- 354,544 eligible stored p90-p10 values at least 20 m;
- 86,717 eligible stored p90-p10 values at least 50 m.

Raw reconstruction comparison:

- `outputs/qc/swot_wse_percentiles_20260727/raw_river/ge20_march22/summary/node_ge20_reconstruction_comparison.parquet`

Five-way audit classification:

- `outputs/qc/swot_wse_percentiles_20260727/raw_river/ge20_march22/classification/node_ge20_auditable_classification.parquet`

Reach-coherent candidates:

- `outputs/qc/swot_wse_percentiles_20260727/raw_river/ge20_march22/classification/ge20_reach_coherent_physical_candidates.parquet`

Strict-good presentation subset, useful only as a comparison layer:

- `outputs/qc/swot_wse_percentiles_20260727/raw_river/ge20_march22/presentation/node_wse_percentile_ge20_strict_good_tail_presentation.parquet`

The previous full audit handover is archived at:

- `docs/handovers/HANDOVER_2026-07-28_wse-percentile-tail-audit.md`

## Existing implementation state

`scripts/visualization/render_tamlin_wse_percentile_map.py` now supports:

- `SWORD_MAP_SOURCE_MODE=stored-sword`;
- configurable output DPI;
- binned, logarithmic, linear, square-root, and quantile scales;
- independent or shared panel scales;
- optional panel annotations and status legend.

It compiled cleanly and passed Ruff after the edits. It remains untracked in
the current worktree, as do the analysis scripts used during the investigation.
Do not stage all untracked files blindly.

Before choosing the viewer stack, inspect the existing reviewer patterns under
`deploy/reviewer/` and the map utilities already in the repository. The user
specifically wants an HTML viewer; do not substitute a notebook-only or
Streamlit-only deliverable.

## Suggested first implementation sequence

1. Build a compact regional investigation dataset for `90–105°E, 25–36°N`
   joining stored values, reconstruction results, and audit classes by stable
   `node_id` and `reach_id`.
2. Verify row uniqueness, threshold counts, and the 9,430-node >=50 m total
   before rendering.
3. Derive reach summaries and a connected-hotspot footprint.
4. Build the HTML map and detail panels from the compact subset.
5. Add review persistence and export/import before doing substantive review.
6. Review a stratified sample spanning magnitude, support, audit class,
   physical-context status, and reach coherence.
7. Fetch raw RiverSP histories only for prioritized cases and attach them to
   the review record.
8. Summarize what proportions are defensibly attributable to known failure
   modes, plausible physical events, and unresolved cases before proposing any
   filter or treatment.

## Stop rules

Stop and report before:

- changing either SWORD database;
- encoding a new production filter;
- replacing stored percentiles in the investigation layer;
- claiming the regional cluster is caused by reservoirs;
- treating missing dam-catalog context as negative evidence;
- broad-staging or committing the dirty worktree.

The next scientific decision comes after recorded case evidence, not from the
global raster alone.

## Repository preservation

- `sword_v17b.duckdb` and `sword_v17c.duckdb` were not modified.
- `.napkin.md`, `README.md`, and unrelated pre-existing edits remain untouched.
- Investigation scripts remain untracked/uncommitted.
- Stage and commit only the active handover and its archived predecessor unless
  the user explicitly expands the scope.
