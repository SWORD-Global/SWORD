# SWORD v17c Variable Reference

Quick-lookup for all variables in the v17c NetCDF export files (`{region}_sword_v17c_0.0.12.nc`).

**Fill values:** i4 = `-9999`, i8 = `-9999`, f8 = `-9999.0`. The reaches
`edit_flag` variable uses the string fill value `"-9999.0"`; other string
variables have no fill value.

---

## Distance Variable Conventions

SWORD contains four distance-to-outlet variables. They differ in routing
method, zero-point, and reach endpoint reporting convention. The table below
summarizes these conventions; see "Node-Level Interpolation" for how each
extends to nodes.

| Variable | Routing | Reach-level value | Outlet value | Ghost reaches |
|---|---|---|---|---|
| `dist_out` (v17b) | v17b topology | reported at reach upstream endpoint | `reach_length` | has value |
| `hydro_dist_out` | `rch_id_dn_main` chain | reported at reach upstream endpoint | `reach_length` | has value |
| `dist_out_dijkstra` | Dijkstra shortest path | reported at reach upstream endpoint | `reach_length` | has value |
| `hydro_dist_hw` | `rch_id_up_main` chain | reported at reach upstream endpoint | 0 | has value |

**Reach scalar convention.** These reach-level values are endpoint reports,
not traversal-origin claims. `dist_out`, `hydro_dist_out`, and
`dist_out_dijkstra` are outlet-distance scalars reported at the upstream
endpoint of each reach; each reach exceeds its
downstream neighbor by its own `reach_length` on 1:1 links.
`hydro_dist_hw` is also reported at the upstream endpoint, but measures
distance from `best_headwater`; each downstream reach exceeds its
upstream neighbor by the upstream neighbor's `reach_length` on 1:1 links.

**Reference values.** `dist_out`, `hydro_dist_out`, and
`dist_out_dijkstra` all assign an outlet reach a value equal to its
`reach_length` (the upstream endpoint is one reach length from the outlet
point). `hydro_dist_hw` assigns 0 at the headwater.

**Ghost reaches.** All four distance variables retain values for ghost
reaches in 0.0.12, including `dist_out_dijkstra`.

### Node-Level Interpolation

Six distance variables are interpolated per node using a midpoint offset
within the parent reach: `offset = cumsum(node_length) - 0.5 *
node_length`, where the cumulative sum is ordered by `node_order`. This
places each node at the geometric center of its `node_length` segment.
On a single-path network (no junctions), `dist_out`, `hydro_dist_out`,
and `dist_out_dijkstra` are exactly equal at every node.

| Variable | Node formula | node_order=n (upstream) | node_order=1 (downstream) |
|---|---|---|---|
| `dist_out` | `reach.do - reach_length + offset` | `~ reach.do` | `~ reach.do - reach_length` |
| `hydro_dist_out` | `reach.hdo - reach_length + offset` | `~ reach.hdo` | `~ reach.hdo - reach_length` |
| `dist_out_dijkstra` | `reach.dod - reach_length + offset` | `~ reach.dod` | `~ reach.dod - reach_length` |
| `hydro_dist_hw` | `reach.hdh + reach_length - offset` | `~ reach.hdh` | `~ reach.hdh + reach_length` |
| `pathlen_hw` | `reach.plh - reach_length + offset` | `~ reach.plh - reach_length` | `~ reach.plh` |
| `pathlen_out` | `reach.plo + reach_length - offset` | `~ reach.plo + reach_length` | `~ reach.plo` |

Three additional variables (`subnetwork_id`, `best_headwater`,
`best_outlet`) are flat copies from the parent reach.

**Boundary behavior.** The reach-level formulas imply continuous
endpoint values on 1:1 links, but node values are midpoint samples within
their `node_length` segments. Adjacent boundary nodes therefore need not
be identical, and bifurcation-rejoin structures can retain larger
single-scalar `dist_out` gaps.

---

## New v17c Reach Variables

| Variable | NetCDF Type | Units | Fill Value | Encoding | Description |
|---|---|---|---|---|---|
| dist_out_dijkstra | f8 | meters | -9999.0 | | Dijkstra shortest-path outlet distance reported at the reach upstream endpoint; outlet reach = reach_length; values retained for ghost reaches |
| hydro_dist_out | f8 | meters | -9999.0 | | Mainstem outlet distance reported at the reach upstream endpoint via rch_id_dn_main chain; outlet reach = reach_length |
| hydro_dist_hw | f8 | meters | -9999.0 | | Mainstem headwater distance reported at the reach upstream endpoint via rch_id_up_main chain; headwater = 0 |
| rch_id_up_main | i8 | | -9999 | | Main upstream neighbor reach ID (mainstem-preferred) |
| rch_id_dn_main | i8 | | -9999 | | Main downstream neighbor reach ID (mainstem-preferred) |
| subnetwork_id | i4 | | -9999 | | Connected component ID (Pfafstetter-offset, globally unique; differs from v17b `network`) |
| main_path_id | i8 | | -9999 | | ID of the mainstem path this reach belongs to |
| is_mainstem | i4 | | -9999 | 0=not_mainstem; 1=mainstem | Whether reach is on a mainstem path |
| best_headwater | i8 | | -9999 | | Width-prioritized upstream headwater reach ID |
| best_outlet | i8 | | -9999 | | Width-prioritized downstream outlet reach ID |
| pathlen_hw | f8 | meters | -9999.0 | | Cumulative reach_length sum from best_headwater to the downstream end of the reach (headwater = 0, increases downstream) |
| pathlen_out | f8 | meters | -9999.0 | | Cumulative reach_length sum toward best_outlet (outlet = 0, increases upstream) |
| facc_quality | i4 | | -9999 | 1=denoise_v3 | Flow accumulation correction flag |
| dl_grod_id | i8 | | -9999 | | DL-GROD (Deep Learning Global River Obstruction Database; He et al. 2025) dam/obstruction ID |
| wse_obs_p10 | f8 | meters | -9999.0 | | SWOT WSE 10th percentile |
| wse_obs_p20 | f8 | meters | -9999.0 | | SWOT WSE 20th percentile |
| wse_obs_p30 | f8 | meters | -9999.0 | | SWOT WSE 30th percentile |
| wse_obs_p40 | f8 | meters | -9999.0 | | SWOT WSE 40th percentile |
| wse_obs_p50 | f8 | meters | -9999.0 | | SWOT WSE 50th percentile (median) |
| wse_obs_p60 | f8 | meters | -9999.0 | | SWOT WSE 60th percentile |
| wse_obs_p70 | f8 | meters | -9999.0 | | SWOT WSE 70th percentile |
| wse_obs_p80 | f8 | meters | -9999.0 | | SWOT WSE 80th percentile |
| wse_obs_p90 | f8 | meters | -9999.0 | | SWOT WSE 90th percentile |
| wse_obs_range | f8 | meters | -9999.0 | | SWOT WSE range (p90 - p10) |
| wse_obs_mad | f8 | meters | -9999.0 | | SWOT WSE median absolute deviation |
| width_obs_p10 | f8 | meters | -9999.0 | | SWOT width 10th percentile |
| width_obs_p20 | f8 | meters | -9999.0 | | SWOT width 20th percentile |
| width_obs_p30 | f8 | meters | -9999.0 | | SWOT width 30th percentile |
| width_obs_p40 | f8 | meters | -9999.0 | | SWOT width 40th percentile |
| width_obs_p50 | f8 | meters | -9999.0 | | SWOT width 50th percentile (median) |
| width_obs_p60 | f8 | meters | -9999.0 | | SWOT width 60th percentile |
| width_obs_p70 | f8 | meters | -9999.0 | | SWOT width 70th percentile |
| width_obs_p80 | f8 | meters | -9999.0 | | SWOT width 80th percentile |
| width_obs_p90 | f8 | meters | -9999.0 | | SWOT width 90th percentile |
| width_obs_range | f8 | meters | -9999.0 | | SWOT width range (p90 - p10) |
| width_obs_mad | f8 | meters | -9999.0 | | SWOT width median absolute deviation |
| slope_obs_p10 | f8 | meters/meters | -9999.0 | | SWOT slope 10th percentile |
| slope_obs_p20 | f8 | meters/meters | -9999.0 | | SWOT slope 20th percentile |
| slope_obs_p30 | f8 | meters/meters | -9999.0 | | SWOT slope 30th percentile |
| slope_obs_p40 | f8 | meters/meters | -9999.0 | | SWOT slope 40th percentile |
| slope_obs_p50 | f8 | meters/meters | -9999.0 | | SWOT slope 50th percentile (median) |
| slope_obs_p60 | f8 | meters/meters | -9999.0 | | SWOT slope 60th percentile |
| slope_obs_p70 | f8 | meters/meters | -9999.0 | | SWOT slope 70th percentile |
| slope_obs_p80 | f8 | meters/meters | -9999.0 | | SWOT slope 80th percentile |
| slope_obs_p90 | f8 | meters/meters | -9999.0 | | SWOT slope 90th percentile |
| slope_obs_range | f8 | meters/meters | -9999.0 | | SWOT slope range (p90 - p10) |
| slope_obs_mad | f8 | meters/meters | -9999.0 | | SWOT slope median absolute deviation |
| slope_obs_adj | f8 | meters/meters | -9999.0 | | Adjusted SWOT slope (bias-corrected) |
| slope_obs_slopeF | f8 | | -9999.0 | | Slope quality F-statistic |
| slope_obs_reliable | i4 | | -9999 | 0=unreliable; 1=reliable | Whether SWOT slope is reliable |
| slope_obs_quality | i4 | | -9999 | 0..8; see flag reference | SWOT slope quality category |
| slope_obs_n | i4 | | -9999 | | Number of RiverSP node observations used in pass-level slope fits |
| slope_obs_n_passes | i4 | | -9999 | | Number of SWOT passes with slope |
| slope_obs_q | i4 | | -9999 | Integer bitfield (1=negative, 2=low_passes, 4=high_var, 8=extreme, 16=clipped) | SWOT slope quality bitfield |
| n_obs | i4 | | -9999 | | Total number of SWOT observations |

## New v17c Node Variables

| Variable | NetCDF Type | Units | Fill Value | Encoding | Description |
|---|---|---|---|---|---|
| subnetwork_id | i4 | | -9999 | | Connected component ID (from parent reach) |
| best_headwater | i8 | | -9999 | | Width-prioritized upstream headwater reach ID |
| best_outlet | i8 | | -9999 | | Width-prioritized downstream outlet reach ID |
| pathlen_hw | f8 | meters | -9999.0 | | Cumulative path length from headwater, interpolated by node position within reach; single-node reaches use midpoint |
| pathlen_out | f8 | meters | -9999.0 | | Cumulative path length to outlet, interpolated by node position within reach; single-node reaches use midpoint |
| hydro_dist_out | f8 | meters | -9999.0 | | Mainstem distance to best_outlet, interpolated by node position within reach; single-node reaches use midpoint |
| hydro_dist_hw | f8 | meters | -9999.0 | | Distance from best_headwater, interpolated by node position within reach; single-node reaches use midpoint |
| dist_out_dijkstra | f8 | meters | -9999.0 | | Dijkstra shortest-path distance to outlet, interpolated by node position within reach; single-node reaches use midpoint; values retained for ghost reaches |
| wse_obs_p10 | f8 | meters | -9999.0 | | SWOT WSE 10th percentile |
| wse_obs_p20 | f8 | meters | -9999.0 | | SWOT WSE 20th percentile |
| wse_obs_p30 | f8 | meters | -9999.0 | | SWOT WSE 30th percentile |
| wse_obs_p40 | f8 | meters | -9999.0 | | SWOT WSE 40th percentile |
| wse_obs_p50 | f8 | meters | -9999.0 | | SWOT WSE 50th percentile (median) |
| wse_obs_p60 | f8 | meters | -9999.0 | | SWOT WSE 60th percentile |
| wse_obs_p70 | f8 | meters | -9999.0 | | SWOT WSE 70th percentile |
| wse_obs_p80 | f8 | meters | -9999.0 | | SWOT WSE 80th percentile |
| wse_obs_p90 | f8 | meters | -9999.0 | | SWOT WSE 90th percentile |
| wse_obs_range | f8 | meters | -9999.0 | | SWOT WSE range (p90 - p10) |
| wse_obs_mad | f8 | meters | -9999.0 | | SWOT WSE median absolute deviation |
| width_obs_p10 | f8 | meters | -9999.0 | | SWOT width 10th percentile |
| width_obs_p20 | f8 | meters | -9999.0 | | SWOT width 20th percentile |
| width_obs_p30 | f8 | meters | -9999.0 | | SWOT width 30th percentile |
| width_obs_p40 | f8 | meters | -9999.0 | | SWOT width 40th percentile |
| width_obs_p50 | f8 | meters | -9999.0 | | SWOT width 50th percentile (median) |
| width_obs_p60 | f8 | meters | -9999.0 | | SWOT width 60th percentile |
| width_obs_p70 | f8 | meters | -9999.0 | | SWOT width 70th percentile |
| width_obs_p80 | f8 | meters | -9999.0 | | SWOT width 80th percentile |
| width_obs_p90 | f8 | meters | -9999.0 | | SWOT width 90th percentile |
| width_obs_range | f8 | meters | -9999.0 | | SWOT width range (p90 - p10) |
| width_obs_mad | f8 | meters | -9999.0 | | SWOT width median absolute deviation |
| n_obs | i4 | | -9999 | | Total number of SWOT observations |
| facc_quality | i4 | | -9999 | 1=denoise_v3 | Flow accumulation correction flag |

## v17b Variables

Values are unchanged from v17b unless noted elsewhere in the release notes.
Rows marked "Not in v17b NetCDF; added in v17c" are database attributes that
v17c now includes in the NetCDF export; they were not variables in the v17b
NetCDF files.

**`edit_flag` note.** `edit_flag` is a comma-delimited tag list recording
edit provenance. Values include v17b numeric edit codes (e.g. `1`, `7`),
the literal string `NaN` (v17b's no-edit placeholder — present on ~126K
reaches; parsers must treat it as a string, not a float), and v17c tags:
`facc_denoise_v3`, `harp_lake`, `lake_sandwich`, `clf_reconcile`,
`clf_provisional_river`, `ghost_reclass`, `facc_suspect`, `facc_traced`.
Multiple tags combine comma-delimited (e.g. `harp_lake,clf_reconcile`).
In the NetCDF export, the reaches `edit_flag` variable carries the string
fill value `"-9999.0"` where empty.

### Reaches

| Variable | NetCDF Type | Units | Fill Value | Description |
|---|---|---|---|---|
| reach_id | i8 | | -9999 | Reach identifier (format: CBBBBBRRRRT) |
| x | f8 | degrees east | -9999.0 | Reach centroid longitude |
| x_min | f8 | degrees east | -9999.0 | Minimum longitude of reach extent |
| x_max | f8 | degrees east | -9999.0 | Maximum longitude of reach extent |
| y | f8 | degrees north | -9999.0 | Reach centroid latitude |
| y_min | f8 | degrees north | -9999.0 | Minimum latitude of reach extent |
| y_max | f8 | degrees north | -9999.0 | Maximum latitude of reach extent |
| reach_length | f8 | meters | -9999.0 | Length of reach centerline |
| n_nodes | i4 | | -9999 | Number of nodes in reach |
| wse | f8 | meters | -9999.0 | Water surface elevation (MERIT Hydro) |
| wse_var | f8 | meters^2 | -9999.0 | WSE variance |
| width | f8 | meters | -9999.0 | River width (GRWL) |
| width_var | f8 | meters^2 | -9999.0 | Width variance |
| facc | f8 | km^2 | -9999.0 | Flow accumulation (MERIT Hydro) |
| n_chan_max | i4 | | -9999 | Max number of channels at any node |
| n_chan_mod | i4 | | -9999 | Modal number of channels |
| obstr_type | i4 | | -9999 | Obstruction type (0=none, 1=dam, 2=lock, 3=low-head, 4=falls) |
| grod_id | i8 | | -9999 | GRanD/GROD dam ID |
| hfalls_id | i8 | | -9999 | High falls ID |
| slope | f8 | meters/kilometers | -9999.0 | Water surface slope from MERIT DEM |
| dist_out | f8 | meters | -9999.0 | Network outlet distance reported at the reach upstream endpoint (v17b original; outlet reach = reach_length, not 0). See "Distance Variable Conventions" for comparison with v17c distance variables |
| n_rch_up | i4 | | -9999 | Number of upstream neighbor reaches |
| n_rch_down | i4 | | -9999 | Number of downstream neighbor reaches |
| lakeflag | i4 | | -9999 | Water body type (0=river, 1=lake, 2=canal, 3=tidal) |
| type | i4 | | -9999 | Reach type (1=river, 3=lake_on_river, 4=dam, 5=unreliable, 6=ghost). Not in v17b NetCDF; added in v17c. Values from v17b database. |
| add_flag | string | | | Manually added reach flag. Not in v17b NetCDF; added in v17c. |
| swot_obs | i4 | | -9999 | Number of expected SWOT observations per cycle |
| swot_obs_source | string | | | Source of SWOT observation data. Not in v17b NetCDF; added in v17c. |
| max_width | f8 | meters | -9999.0 | Maximum width at any node |
| low_slope_flag | i4 | | -9999 | Low slope flag |
| trib_flag | i4 | | -9999 | MHV tributary enters (0=no, 1=yes, spatial proximity) |
| path_freq | i8 | | -9999 | Traversal count (increases toward outlets) |
| path_order | i8 | | -9999 | Path order from traversal |
| path_segs | i8 | | -9999 | Unique ID for (path_order, path_freq) combination |
| stream_order | i4 | | -9999 | Stream order: round(log(path_freq)) + 1 |
| main_side | i4 | | -9999 | Channel role (0=main, 1=side, 2=secondary outlet) |
| end_reach | i4 | | -9999 | Endpoint type (0=middle, 1=headwater, 2=outlet, 3=junction) |
| network | i4 | | -9999 | Connected component ID |
| dn_node_id | i8 | | -9999 | Downstream boundary node ID. Not in v17b NetCDF; added in v17c. |
| up_node_id | i8 | | -9999 | Upstream boundary node ID. Not in v17b NetCDF; added in v17c. |
| river_name | string | | | River name from GRWL |
| river_name_en | string | | | River name (English). Not in v17b NetCDF; added in v17c. |
| river_name_local | string | | | River name (local language). Not in v17b NetCDF; added in v17c. |
| edit_flag | string | | | Comma-delimited edit provenance tags; see the `edit_flag` note below |
| version | string | | | Data version identifier. Not in v17b NetCDF; added in v17c. |

### Nodes

| Variable | NetCDF Type | Units | Fill Value | Description |
|---|---|---|---|---|
| node_id | i8 | | -9999 | Node identifier (format: CBBBBBRRRRNNNT) |
| x | f8 | degrees east | -9999.0 | Node longitude |
| y | f8 | degrees north | -9999.0 | Node latitude |
| node_length | f8 | meters | -9999.0 | Length of river represented by this node |
| node_order | i4 | | -9999 | 1-based position within reach (1=downstream, n=upstream, by dist_out). Not in v17b NetCDF; added in v17c. |
| reach_id | i8 | | -9999 | Parent reach ID (format: CBBBBBRRRRT) |
| wse | f8 | meters | -9999.0 | Water surface elevation (MERIT Hydro) |
| wse_var | f8 | meters^2 | -9999.0 | WSE variance |
| width | f8 | meters | -9999.0 | River width (GRWL) |
| width_var | f8 | meters^2 | -9999.0 | Width variance |
| n_chan_max | i4 | | -9999 | Max number of channels |
| n_chan_mod | i4 | | -9999 | Modal number of channels |
| obstr_type | i4 | | -9999 | Obstruction type |
| grod_id | i8 | | -9999 | GRanD/GROD dam ID |
| hfalls_id | i8 | | -9999 | High falls ID |
| dist_out | f8 | meters | -9999.0 | Distance to network outlet. Interpolated from reach `dist_out` using node_length midpoint offsets (same formula as `hydro_dist_out` and `dist_out_dijkstra`). On single-path networks all three are exactly equal |
| wth_coef | f8 | | -9999.0 | Width coefficient |
| ext_dist_coef | f8 | | -9999.0 | Extraction distance coefficient |
| facc | f8 | km^2 | -9999.0 | Flow accumulation (MERIT Hydro) |
| lakeflag | i8 | | -9999 | Water body type (0=river, 1=lake, 2=canal, 3=tidal). Propagated from parent reach lakeflag per JPL request. |
| max_width | f8 | meters | -9999.0 | Maximum width |
| meander_length | f8 | | -9999.0 | Meander wavelength |
| sinuosity | f8 | | -9999.0 | Channel sinuosity |
| manual_add | i4 | | -9999 | Manually added node flag |
| trib_flag | i4 | | -9999 | MHV tributary enters (0=no, 1=yes) |
| path_freq | i8 | | -9999 | Traversal count |
| path_order | i8 | | -9999 | Path order |
| path_segs | i8 | | -9999 | Path segment ID |
| stream_order | i4 | | -9999 | Stream order |
| main_side | i4 | | -9999 | Channel role (0=main, 1=side, 2=secondary outlet) |
| end_reach | i4 | | -9999 | Endpoint type (0=middle, 1=headwater, 2=outlet, 3=junction) |
| network | i4 | | -9999 | Connected component ID |
| add_flag | string | | | Manually added node flag. Not in v17b NetCDF; added in v17c. |
| river_name | string | | | River name |
| edit_flag | string | | | Comma-delimited edit provenance tags; see the `edit_flag` note below |
| version | string | | | Data version identifier. Not in v17b NetCDF; added in v17c. |

### Multi-dimensional Arrays

| Variable | Group | Shape | Type | Description |
|---|---|---|---|---|
| cl_ids | reaches | [2, num_reaches] | i8 | Min (row 0) and max (row 1) centerline point IDs bounding each reach (v17b format) |
| rch_id_up | reaches | [4, num_reaches] | i8 | Up to 4 upstream neighbor reach IDs |
| rch_id_dn | reaches | [4, num_reaches] | i8 | Up to 4 downstream neighbor reach IDs |
| iceflag | reaches | [366, num_reaches] | i4 | Daily ice presence flag (Julian day 1-366) |
| swot_orbits | reaches | [75, num_reaches] | i8 | SWOT orbit IDs that observe this reach |
| cl_ids | nodes | [2, num_nodes] | i8 | Min (row 0) and max (row 1) centerline point IDs bounding each node (v17b format) |
| cl_id | centerlines | [num_points] | i8 | Centerline point ID |
| x | centerlines | [num_points] | f8 | Centerline point longitude (degrees east) |
| y | centerlines | [num_points] | f8 | Centerline point latitude (degrees north) |
| reach_id | centerlines | [4, num_points] | i8 | Reach ID at each centerline point (row 0); rows 1-3 hold neighboring reach IDs at reach boundaries. Copied from v17b |
| node_id | centerlines | [4, num_points] | i8 | Node ID at each centerline point (row 0); rows 1-3 hold neighboring node IDs. Copied from v17b |

### Subgroups (copied from v17b)

| Subgroup | Parent | Description |
|---|---|---|
| reaches/area_fits | reaches | Area-based rating curve fit coefficients |
| reaches/discharge_models | reaches | Discharge model parameters (organized by constraint/model) |
