# G005 Geometry Drift Investigation (2026-03-03)

## Question

Why does `G005` (`reach_length` vs geometry geodesic length) fail in v17c but not v17b?

## Short Answer

v17c `reaches.geom` was modified by adding endpoint overlap vertices (+1 or +2 points per reach) while `reach_length` remained unchanged.

`G005` compares against geometry length, so this creates systematic positive length inflation.

## Evidence

## 1) v17b vs v17c G005 behavior

- v17b: `0` reaches >20% mismatch
- v17c: `1,396` reaches >20% mismatch

`reach_length` and `n_nodes` values are unchanged between v17b and v17c, so drift is geometric, not attribute corruption.

## 2) Geometry point-count delta is exactly overlap pattern

Comparing `ST_NPoints(reaches.geom)` to centerline point count:

- v17b: all `248,673` reaches have `delta_pts = 0`
- v17c:
  - `delta_pts = 0`: `38,140`
  - `delta_pts = 1`: `173,346`
  - `delta_pts = 2`: `37,187`

So `210,533` reaches gained endpoint vertices in v17c.

## 3) Endpoint mismatch confirms extra prepends/appends

In v17c, `210,268` reaches have at least one geometry endpoint not matching their own first/last centerline point (v17b has `0` such mismatches).

## 4) Direct shape relation to v17b is exact

Across all `210,533` changed reaches:

- `173,346` are exactly v17b geometry + one endpoint vertex
- `37,187` are exactly v17b geometry + two endpoint vertices
- `0` are other patterns

No other geometry mutation pattern is present.

## 5) Link to code path

The pattern matches `scripts/maintenance/rebuild_reach_geometry.py`, which explicitly prepends/appends overlap vertices at reach ends.

## Impact

- `G005` failures are produced by inflated geometry length relative to unchanged `reach_length`.
- Most severe failures are short reaches (`n_nodes` 1-2), where adding 30-100 m is a large percentage.

## Practical Fix Options

1. Restore v17b-style geometries for DuckDB `reaches.geom` (full or targeted to `G005>20%` IDs).
2. Keep overlap geometry and redefine length semantics (`reach_length`, node lengths, downstream derived fields) to be overlap-aware.

Option 1 is low-risk and aligns with existing `reach_length`/`n_nodes` consistency checks.

## Decision (Current)

For v17c audit purposes, this is **accepted as OK** and documented:

- `G005` is a known consequence of overlap-vertex geometry convention in `reaches.geom`.
- `reach_length` and `n_nodes` integrity remain valid (no corruption signal).
- We are not changing length semantics in this audit step.

## POM Relation

This issue is **not directly a POM request check**. It is a geometry lint (`G005`) behavior introduced by geometry convention drift, documented in audit notes.

## Other Geometry Failures (Current Snapshot)

Post-audit geometry check counts (v17b vs v17c):

| Check | v17b | v17c | Note |
|---|---:|---:|---|
| G004 self_intersection | 395 | 474 | Pre-existing + modest increase; investigate later |
| G005 reach_length_vs_geom_length | 0 | 1,396 | Explained by overlap-vertex geometry drift |
| G012 endpoint_alignment (>500m) | 21 | 22 | Stable, low-volume residual |
| G013 width_gt_length (rivers) | 4,007 | 3,454 | Improved vs v17b |
| G015 node_reach_distance (>100m) | 42 | 38 | Improved vs v17b |
| G016 node_spacing | 3,004 | 3,004 | Unchanged (source-structure residual) |
| G017 cross_reach_nodes | 29 | 23 | Improved vs v17b |
| G018 dist_out_vs_reach_length | 2,340 | 2,425 | Slight increase (topology/path scalar artifact family) |
| G022 single_node_reaches | 1,906 | 1,996 | Slight increase; informational merge-candidate flag |
| G023 duplicate_centerline_points | 9 | 9 | Unchanged, low-volume |

Operational note:

- G014 (duplicate geometry) and G021 (reach overlap) currently timeout in full/global runs and also timeout at NA-only with a 120s cap. They need query optimization before global status can be treated as a routine QA gate.
