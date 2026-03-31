# SWORD v17c Variable Reference

Quick-lookup for all variables in the v17c NetCDF export files (`{region}_sword_v17c_beta.nc`).

**Fill values:** i4 = `-9999`, i8 = `-9999`, f8 = `-9999.0`

---

## Distance Variable Conventions

SWORD contains four distance-to-outlet variables. They differ in routing
method, zero-point, and measurement anchor within a reach. The table below
summarizes these conventions; see "Node-Level Interpolation" for how each
extends to nodes.

| Variable | Routing | Reach-level anchor | Outlet value | Ghost reaches |
|---|---|---|---|---|
| `dist_out` (v17b) | v17b topology | upstream end of reach | `reach_length` | has value |
| `hydro_dist_out` | `rch_id_dn_main` chain | upstream end of reach | `reach_length` | has value |
| `dist_out_dijkstra` | Dijkstra shortest path | upstream end of reach | **0** | **NULL** |
| `hydro_dist_hw` | `rch_id_up_main` chain | downstream end of reach | headwater `reach_length` | has value |

**Anchor convention.** For `dist_out`, `hydro_dist_out`, and
`dist_out_dijkstra`, the reach-level value equals the distance from the
upstream end of the reach to the outlet. Each reach's value therefore
exceeds its downstream neighbor's value by exactly its own
`reach_length`. For `hydro_dist_hw` the convention is reversed: the
reach-level value equals the distance from the headwater to the
downstream end of the reach, so each reach's value exceeds its upstream
neighbor's value by exactly its own `reach_length`.

**Zero-point difference.** `dist_out` and `hydro_dist_out` assign the
outlet reach a value equal to its `reach_length` (the upstream end is
one reach_length from the outlet point). `dist_out_dijkstra` assigns the
outlet reach a value of 0 (the outlet point itself is the reference).
When comparing these variables, the offset at any reach equals the
outlet's `reach_length`.

**Ghost reaches.** `dist_out_dijkstra` is NULL for ghost reaches
(type=6). The other three variables retain values for ghost reaches.

### Node-Level Interpolation

Five v17c variables are interpolated per node using the node's position
within its parent reach. The offset from the reach's upstream end is
`reach.dist_out - node.dist_out` (where `node.dist_out` is the v17b
original). For flow-corrected reaches (660), the offset direction is
reversed.

| Variable | Node formula | node_order=n (upstream) | node_order=1 (downstream) |
|---|---|---|---|
| `hydro_dist_out` | `reach.hdo - offset` | `reach.hdo` | `reach.hdo - reach_length` |
| `dist_out_dijkstra` | `reach.dod - offset` | `reach.dod` | `reach.dod - reach_length` |
| `hydro_dist_hw` | `reach.hdh - reach_length + offset` | `reach.hdh - reach_length` | `reach.hdh` |
| `pathlen_hw` | `reach.plh - reach_length + offset` | `reach.plh - reach_length` | `reach.plh` |
| `pathlen_out` | `reach.plo + reach_length - offset` | `reach.plo + reach_length` | `reach.plo` |

For single-node reaches, the offset is `reach_length / 2` (midpoint).
Three additional variables (`subnetwork_id`, `best_headwater`,
`best_outlet`) are flat copies from the parent reach.

**Node `dist_out` is the v17b original value, not interpolated.** It
serves as the positional anchor for interpolation but is not itself
recomputed. At single-node outlet reaches, `node.dist_out` equals the
reach-level value (not the midpoint), while `node.hydro_dist_out` uses
the midpoint convention. This difference is by design: v17b `dist_out`
is preserved as-is for backward compatibility.

**Boundary continuity.** At reach boundaries, the downstream-end node of
the upstream reach and the upstream-end node of the downstream reach
share identical interpolated distance values (verified gap = 0.00 m
across all boundaries).

---

## New v17c Reach Variables

| Variable | NetCDF Type | Units | Fill Value | Encoding | Description |
|---|---|---|---|---|---|
| dist_out_dijkstra | f8 | meters | -9999.0 | | Dijkstra shortest-path distance from the upstream end of the reach to any network outlet; outlet reach = 0; NULL for ghost reaches (type=6) |
| hydro_dist_out | f8 | meters | -9999.0 | | Mainstem distance from the upstream end of the reach to best_outlet via rch_id_dn_main chain; outlet reach = reach_length |
| hydro_dist_hw | f8 | meters | -9999.0 | | Mainstem distance from best_headwater to the downstream end of the reach via rch_id_up_main chain; headwater reach = reach_length |
| rch_id_up_main | i8 | | -9999 | | Main upstream neighbor reach ID (mainstem-preferred) |
| rch_id_dn_main | i8 | | -9999 | | Main downstream neighbor reach ID (mainstem-preferred) |
| subnetwork_id | i4 | | -9999 | | Connected component ID (Pfafstetter-offset, globally unique; differs from v17b `network`) |
| main_path_id | i8 | | -9999 | | ID of the mainstem path this reach belongs to |
| is_mainstem | i4 | | -9999 | flag_values=[0,1], flag_meanings="not_mainstem mainstem" | Whether reach is on a mainstem path |
| best_headwater | i8 | | -9999 | | Width-prioritized upstream headwater reach ID |
| best_outlet | i8 | | -9999 | | Width-prioritized downstream outlet reach ID |
| pathlen_hw | f8 | meters | -9999.0 | | Cumulative reach_length sum from best_headwater to the downstream end of the reach (headwater = 0, increases downstream) |
| pathlen_out | f8 | meters | -9999.0 | | Cumulative reach_length sum from the upstream end of the reach to best_outlet (outlet = 0, increases upstream) |
| facc_quality | i4 | | -9999 | flag_values=[1], flag_meanings="denoise_v3" | Flow accumulation correction flag |
| dl_grod_id | i8 | | -9999 | | Dam/lake GROD ID (downstream lookup) |
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
| slope_obs_p10 | f8 | meters/kilometers | -9999.0 | | SWOT slope 10th percentile |
| slope_obs_p20 | f8 | meters/kilometers | -9999.0 | | SWOT slope 20th percentile |
| slope_obs_p30 | f8 | meters/kilometers | -9999.0 | | SWOT slope 30th percentile |
| slope_obs_p40 | f8 | meters/kilometers | -9999.0 | | SWOT slope 40th percentile |
| slope_obs_p50 | f8 | meters/kilometers | -9999.0 | | SWOT slope 50th percentile (median) |
| slope_obs_p60 | f8 | meters/kilometers | -9999.0 | | SWOT slope 60th percentile |
| slope_obs_p70 | f8 | meters/kilometers | -9999.0 | | SWOT slope 70th percentile |
| slope_obs_p80 | f8 | meters/kilometers | -9999.0 | | SWOT slope 80th percentile |
| slope_obs_p90 | f8 | meters/kilometers | -9999.0 | | SWOT slope 90th percentile |
| slope_obs_range | f8 | meters/kilometers | -9999.0 | | SWOT slope range (p90 - p10) |
| slope_obs_mad | f8 | meters/kilometers | -9999.0 | | SWOT slope median absolute deviation |
| slope_obs_adj | f8 | meters/kilometers | -9999.0 | | Adjusted SWOT slope (bias-corrected) |
| slope_obs_slopeF | f8 | | -9999.0 | | Slope quality F-statistic |
| slope_obs_reliable | i1 | | -9999 | BOOL->i1 (True=1, False=0) | Whether SWOT slope is reliable |
| slope_obs_quality | string | | | VARCHAR (reliable, small_negative, moderate_negative, large_negative, negative, below_ref_uncertainty, high_uncertainty, noise_high_nobs, flat_water_noise) | SWOT slope quality category |
| slope_obs_n | i8 | | -9999 | | Number of SWOT slope observations |
| slope_obs_n_passes | i8 | | -9999 | | Number of SWOT passes with slope |
| slope_obs_q | i8 | | -9999 | Integer bitfield (1=negative, 2=low_passes, 4=high_var, 8=extreme, 16=clipped) | SWOT slope quality bitfield |
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
| dist_out_dijkstra | f8 | meters | -9999.0 | | Dijkstra shortest-path distance to outlet, interpolated by node position within reach; single-node reaches use midpoint; NULL for ghost reaches |
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
| facc_quality | i4 | | -9999 | flag_values=[1], flag_meanings="denoise_v3" | Flow accumulation correction flag |

## v17b Variables (unchanged)

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
| dist_out | f8 | meters | -9999.0 | Distance from upstream end of reach to network outlet (v17b original; outlet reach = reach_length, not 0). See "Distance Variable Conventions" for comparison with v17c distance variables |
| n_rch_up | i4 | | -9999 | Number of upstream neighbor reaches |
| n_rch_down | i4 | | -9999 | Number of downstream neighbor reaches |
| lakeflag | i4 | | -9999 | Water body type (0=river, 1=lake, 2=canal, 3=tidal) |
| type | i8 | | -9999 | Reach type (1=river, 3=lake_on_river, 4=dam, 5=unreliable, 6=ghost). Not in v17b NetCDF; added in v17c. Values from v17b database. |
| add_flag | i8 | | -9999 | Manually added reach flag |
| swot_obs | i4 | | -9999 | Number of expected SWOT observations per cycle |
| swot_obs_source | string | | | Source of SWOT observation data |
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
| dn_node_id | i8 | | -9999 | Downstream boundary node ID |
| up_node_id | i8 | | -9999 | Upstream boundary node ID |
| river_name | string | | | River name from GRWL |
| river_name_en | string | | | River name (English) |
| river_name_local | string | | | River name (local language) |
| edit_flag | string | | | Edit provenance tag (e.g. lake_sandwich, harp_lake) |
| version | string | | | Data version identifier |

### Nodes

| Variable | NetCDF Type | Units | Fill Value | Description |
|---|---|---|---|---|
| node_id | i8 | | -9999 | Node identifier (format: CBBBBBRRRRNNNT) |
| x | f8 | degrees east | -9999.0 | Node longitude |
| y | f8 | degrees north | -9999.0 | Node latitude |
| node_length | f8 | meters | -9999.0 | Length of river represented by this node |
| node_order | i4 | | -9999 | 1-based position within reach (1=downstream, n=upstream, by dist_out) |
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
| dist_out | f8 | meters | -9999.0 | Distance to network outlet (v17b original, not interpolated; serves as positional anchor for v17c node interpolation). At single-node reaches, equals the reach-level value (not midpoint) |
| wth_coef | f8 | | -9999.0 | Width coefficient |
| ext_dist_coef | f8 | | -9999.0 | Extraction distance coefficient |
| facc | f8 | km^2 | -9999.0 | Flow accumulation (MERIT Hydro) |
| lakeflag | i8 | | -9999 | Water body type (0=river, 1=lake, 2=canal, 3=tidal) |
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
| add_flag | i8 | | -9999 | Manually added node flag |
| river_name | string | | | River name |
| edit_flag | string | | | Edit provenance tag (e.g. lake_sandwich, harp_lake) |
| version | string | | | Data version identifier |

### Multi-dimensional Arrays

| Variable | Group | Shape | Type | Description |
|---|---|---|---|---|
| cl_id_min | reaches | [num_reaches] | i8 | Minimum centerline ID bounding this reach |
| cl_id_max | reaches | [num_reaches] | i8 | Maximum centerline ID bounding this reach |
| rch_id_up | reaches | [4, num_reaches] | i8 | Up to 4 upstream neighbor reach IDs |
| rch_id_dn | reaches | [4, num_reaches] | i8 | Up to 4 downstream neighbor reach IDs |
| iceflag | reaches | [366, num_reaches] | i4 | Daily ice presence flag (Julian day 1-366) |
| swot_orbits | reaches | [75, num_reaches] | i8 | SWOT orbit IDs that observe this reach |
| cl_id_min | nodes | [num_nodes] | i8 | Minimum centerline ID bounding this node |
| cl_id_max | nodes | [num_nodes] | i8 | Maximum centerline ID bounding this node |
| cl_id | centerlines | [num_points] | i8 | Centerline point ID |
| x | centerlines | [num_points] | f8 | Centerline point longitude (degrees east) |
| y | centerlines | [num_points] | f8 | Centerline point latitude (degrees north) |
| reach_id | centerlines | [num_points] | i8 | Parent reach ID for this centerline point |
| node_id | centerlines | [num_points] | i8 | Parent node ID for this centerline point |
| version | centerlines | [num_points] | string | Data version identifier |

### Subgroups (copied from v17b)

| Subgroup | Parent | Description |
|---|---|---|
| reaches/area_fits | reaches | Area-based rating curve fit coefficients |
| reaches/discharge_models | reaches | Discharge model parameters (organized by constraint/model) |
