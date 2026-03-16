# SWORD v17c Beta Release Notes

**Version:** v17c beta
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
(`{region}_sword_v17c_beta.nc`). The group structure matches v17b
(centerlines, nodes, reaches), and the `area_fits` and `discharge_models`
subgroups under reaches pass through from v17b unchanged. Reach and node
ordering within each file matches v17b.

Reach coordinate columns (`x`, `y`, `x_min`, `x_max`, `y_min`, `y_max`)
are copied from v17b during NetCDF export, so distributed files contain
canonical v17b coordinates. The DuckDB working database rebuilt geometries
from NetCDF without endpoint overlap vertices, causing centroid differences
up to ~8 km for 17,448 reaches; this is a working-database artifact only
and does not affect the distributed NetCDF files.

All new variables use a fill value of -9999 where no observation or
computation produced a value.

For a complete variable catalog, see
[v17c_variable_reference.md](v17c_variable_reference.md).

---

## 2. New Variables

### 2.1 Mainstem Topology (reaches group)

The variables below define a width-prioritized mainstem through each
connected component of the river network. At each junction, the algorithm
selects the upstream path with the highest effective width (primary),
log(facc) (secondary), and cumulative path length (tertiary).

`is_mainstem_edge` identifies the single mainstem chain per network
component by walking `rch_id_dn_main` from `best_headwater` to the outlet.
This produces 2-12% mainstem per region (varying by network complexity).
Ghost reaches (type=6) are excluded from mainstem but still participate in
routing topology.

| Variable | Type | Units | Description |
|----------|------|-------|-------------|
| `dist_out_dijkstra` | float64 | meters | Dijkstra shortest-path distance to any network outlet |
| `hydro_dist_out` | float64 | meters | Mainstem distance to `best_outlet` via `rch_id_dn_main` chain |
| `hydro_dist_hw` | float64 | meters | Distance from `best_headwater` via `rch_id_up_main` chain walk |
| `rch_id_up_main` | int64 | — | Main upstream neighbor reach_id (mainstem-preferred) |
| `rch_id_dn_main` | int64 | — | Main downstream neighbor reach_id (mainstem-preferred) |
| `best_headwater` | int64 | — | Width-prioritized headwater reach_id for the network component |
| `best_outlet` | int64 | — | Width-prioritized outlet reach_id for the network component |
| `pathlen_hw` | float64 | meters | Cumulative path length from `best_headwater` |
| `pathlen_out` | float64 | meters | Cumulative path length to `best_outlet` |
| `is_mainstem_edge` | int32 | — | 1 if reach is on a mainstem path, 0 otherwise |
| `main_path_id` | int64 | — | Unique identifier for each mainstem path |
| `subnetwork_id` | int32 | — | Connected component ID (Pfafstetter-offset, globally unique; see Section 4) |

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
95,913 of 248,673 reaches (38.6%). Uncorrected reaches retain v17b values.
See [facc_correction_methodology.md](technical/facc_correction_methodology.md)
for the full algorithm description.

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

- **SWOT observation coverage:** SWOT statistics are fill_value (-9999) for
  reaches and nodes lacking SWOT data.

- **facc correction scope:** 95,913 reaches corrected (38.6%); the
  remaining 152,761 retain v17b values. Node-level facc propagates from
  the parent reach.

- **Lake sandwich corrections:** 1,252 reaches reclassified to
  `lakeflag = 1` where a narrow, shorter-than-neighbor reach sat between
  lake reaches (tagged `edit_flag = "lake_sandwich"`). ~1,755 similar
  cases remain (narrow connecting channels, chains).

- **area_fits and discharge_models:** Direct copies from v17b. Not
  recomputed against v17c facc or SWOT values.

- **`subnetwork_id` vs `network`:** `subnetwork_id` uses Pfafstetter-
  offset enumeration (globally unique). v17b `network` uses per-region
  1-based IDs. Different component counts (v17c finds more via weakly
  connected components; 19 subnetworks span multiple v17b networks).
  `network` is retained unchanged from v17b.

- **Topology reciprocity gaps (v17b):** 150 reaches (0.06%) have
  `rch_id_dn_main` pointing to a neighbor not in their `rch_id_dn` array
  but present in their `rch_id_up` array. Caused by non-reciprocal entries
  in v17b topology: reach A lists B as upstream, B lists A as upstream,
  creating a graph edge A->B without an explicit downstream entry.
  OC has 0 such cases.

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

---

## 5. Quality Audits

Validation checks performed on the v17c data:

| Audit | Finding |
|-------|---------|
| **Geometry** | DuckDB geometries (rebuilt from NetCDF) lack endpoint overlap vertices present in v17b (210,533 reaches affected: 173K +1 point, 37K +2 points). `reach_length` unchanged. Distributed NetCDF files contain canonical v17b coordinates (copied during export). |
| **n_nodes / reach_length** | Internally consistent. Zero N008/G002/G003 violations. |
| **path_freq gaps** | v17b had 4,952 connected non-ghost reaches with invalid path_freq (0 or -9999). Resolved in v17c; remaining nodata values are correctly attributed to ghost reaches (type=6). |
| **subnetwork_id** | 3,027 components across 248,673 reaches verified. Pfafstetter banding correct. Zero cross-region collisions. 19 subnetworks (0.6%) span multiple v17b networks (expected). |
| **Topology integrity** | T001 (dist_out monotonicity), T012 (referential integrity), T013 (self-reference), T014 (bidirectional): all pass. T005/T007: 150 non-reciprocal edges (v17b inherited). |
| **OC reach split revert** | Incomplete `break_reaches()` split of OC reach 51111300061 (434 orphan centerlines, 73 orphan nodes) fully reverted to v17b state. |
| **River name formatting** | 291 formatting issues corrected (separators, whitespace). Automated checks now enforce "; " separator and alphabetical ordering. |
| **Flow direction** | 1,112 experimental topology flips reverted after causing 30K disconnected reaches. Current v17c topology matches v17b except for OC (26 validated sections flipped per SWOT slope evidence). |

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
- **Checksums:** SHA256 hashes listed in `SHA256SUMS.txt`
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
