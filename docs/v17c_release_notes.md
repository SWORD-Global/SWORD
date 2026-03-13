# SWORD v17c Release Notes

**Version:** v17c
**Date:** March 2026
**Authors:** Gearon, Pavelsky
**Base version:** SWORD v17b (March 2025, UNC)

---

## 1. Overview

SWORD v17c extends v17b with three additions: computed mainstem topology,
SWOT observation statistics, and flow accumulation corrections. No reaches,
nodes, or centerlines were added or removed. v17c contains the same
248,673 reaches, 11.1M nodes, and 66.9M centerline points as v17b across
all six regions (NA, SA, EU, AF, AS, OC).

Each region is distributed as a single NetCDF4 file
(`{region}_sword_v17c.nc`). The group structure matches v17b (centerlines,
nodes, reaches), and the `area_fits` and `discharge_models` subgroups
under reaches pass through from v17b unchanged. Reach and node ordering
within each file matches v17b.

All new variables use a fill value of -9999 where no observation or
computation produced a value.

---

## 2. New Variables

### 2.1 Mainstem Topology (reaches group)

The variables below define a width-prioritized mainstem through each
connected component of the river network. The algorithm selects the
mainstem by tracing the widest upstream path at each junction.

| Variable | Type | Units | Description |
|----------|------|-------|-------------|
| `dist_out_dijkstra` | float64 | meters | Dijkstra shortest-path distance to any network outlet |
| `hydro_dist_out` | float64 | meters | Mainstem distance to `best_outlet` via `rch_id_dn_main` chain |
| `hydro_dist_hw` | float64 | meters | Mainstem distance from `best_headwater` |
| `rch_id_up_main` | int64 | — | Main upstream neighbor reach_id (mainstem-preferred) |
| `rch_id_dn_main` | int64 | — | Main downstream neighbor reach_id (mainstem-preferred) |
| `best_headwater` | int64 | — | Width-prioritized headwater reach_id for the network component |
| `best_outlet` | int64 | — | Width-prioritized outlet reach_id for the network component |
| `pathlen_hw` | float64 | meters | Cumulative path length from `best_headwater` |
| `pathlen_out` | float64 | meters | Cumulative path length to `best_outlet` |
| `is_mainstem_edge` | int32 | — | 1 if reach is on a mainstem path, 0 otherwise |
| `main_path_id` | int64 | — | Unique identifier for each mainstem path |
| `subnetwork_id` | int32 | — | Connected component ID (equivalent to v17b `network`) |

Five of these variables also appear at node level: `subnetwork_id`,
`best_headwater`, `best_outlet`, `pathlen_hw`, and `pathlen_out`.

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
| `slope_obs_n` | int32 | — | Number of slope observations |
| `slope_obs_n_passes` | int32 | — | Number of SWOT passes used |
| `slope_obs_q` | int32 | — | Bitfield quality flag (see Section 3) |

### 2.3 Flow Accumulation Corrections

A two-stage denoise pipeline corrected flow accumulation (`facc`) values
to address three systematic error modes in MERIT Hydro's D8
(eight-direction flow routing) upstream area: bifurcation cloning,
junction inflation, and raster-vector misalignment. The pipeline corrected
95,913 of 248,674 reaches (38.6%). Uncorrected reaches retain v17b values.

| Variable | Type | Group | Description |
|----------|------|-------|-------------|
| `facc` | float64 | reaches, nodes | Flow accumulation (km^2). Corrected values where applicable; v17b values otherwise. |
| `facc_quality` | int32 | reaches, nodes | 1 = corrected by denoise_v3; fill_value = not flagged |

After correction, junction conservation violations (downstream facc < sum
of upstream facc) and monotonicity violations on non-bifurcating links no
longer occur in any region.

### 2.4 Other New or Updated Variables

| Variable | Type | Group | Description |
|----------|------|-------|-------------|
| `type` | int32 | reaches | Reach classification (1=river, 3=lake_on_river, 4=dam, 5=unreliable, 6=ghost). Present in v17b DuckDB but now included in the NetCDF export. |
| `dl_grod_id` | int64 | reaches | DL-GROD (Deep Learning Global River Obstruction Database; He et al. 2025) dam/obstruction ID |
| `edit_flag` | string | reaches | Tag for manually edited reaches (e.g., `lake_sandwich`) |

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

### is_mainstem_edge

| Value | Meaning |
|-------|---------|
| 0 | Not on mainstem |
| 1 | On mainstem path |

---

## 4. Known Limitations

- **path_freq gaps (inherited from v17b):** 4,952 connected non-ghost
  reaches have invalid `path_freq` values (0 or -9999). These invalid
  values carry over from v17b and remain uncorrected. Of the 4,952
  affected reaches, 91% are 1:1 links fixable by propagation; 9% are
  junctions requiring full traversal recomputation.

- **SWOT observation coverage:** SWOT observation statistics
  (`wse_obs_*`, `width_obs_*`, `slope_obs_*`, `n_obs`) are set to
  fill_value (-9999) for reaches and nodes lacking SWOT data.

- **facc correction scope:** The denoise pipeline corrected 95,913
  reaches (38.6%); the remaining 152,761 reaches retain v17b values.
  Node-level facc values propagate from the parent reach correction.

- **Lake sandwich corrections:** The pipeline reclassified 1,252 reaches
  from their original `lakeflag` to `lakeflag = 1` (lake) where a narrow,
  shorter-than-neighbor reach sat between lake reaches. These reclassified
  reaches carry the tag `edit_flag = "lake_sandwich"`. An estimated 1,755
  similar cases remain uncorrected (narrow connecting channels, chains).

- **area_fits and discharge_models:** These two subgroups are direct
  copies from v17b and were not recomputed against v17c facc or SWOT
  observation values.

---

## 5. File Format

- **Format:** NetCDF4 (one file per region)
- **Naming:** `{region}_sword_v17c.nc` where region is `na`, `sa`, `eu`,
  `af`, `as`, `oc`
- **Groups:** `centerlines`, `nodes`, `reaches`
  - `reaches/area_fits` and `reaches/discharge_models` subgroups (from v17b)
- **Ordering:** Reach, node, and centerline arrays match v17b ordering
  within each file
- **Fill value:** -9999 for all numeric variables (int32, int64, float64)
- **Checksums:** SHA256 hashes for each file listed in `SHA256SUMS.txt`
