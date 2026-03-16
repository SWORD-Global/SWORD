# SWORD v17c Variable Reference

Quick-lookup for all variables in the v17c NetCDF export files (`{region}_sword_v17c_beta.nc`).

**Fill values:** i4 = `-9999`, i8 = `-9999`, f8 = `-9999.0`

---

## New v17c Reach Variables

| Variable | NetCDF Type | Units | Fill Value | Encoding | Description |
|---|---|---|---|---|---|
| dist_out_dijkstra | f8 | meters | -9999.0 | | Dijkstra shortest-path distance to any outlet |
| hydro_dist_out | f8 | meters | -9999.0 | | Mainstem distance to best_outlet via rch_id_dn_main |
| hydro_dist_hw | f8 | meters | -9999.0 | | Distance from best_headwater via rch_id_up_main chain walk |
| rch_id_up_main | i8 | | -9999 | | Main upstream neighbor reach ID (mainstem-preferred) |
| rch_id_dn_main | i8 | | -9999 | | Main downstream neighbor reach ID (mainstem-preferred) |
| subnetwork_id | i4 | | -9999 | | Connected component ID (Pfafstetter-offset, globally unique; differs from v17b `network`) |
| main_path_id | i8 | | -9999 | | ID of the mainstem path this reach belongs to |
| is_mainstem_edge | i4 | | -9999 | BOOL->i4 (True=1, False=0) | Whether reach is on a mainstem path |
| best_headwater | i8 | | -9999 | | Width-prioritized upstream headwater reach ID |
| best_outlet | i8 | | -9999 | | Width-prioritized downstream outlet reach ID |
| pathlen_hw | f8 | meters | -9999.0 | | Cumulative path length from headwater |
| pathlen_out | f8 | meters | -9999.0 | | Cumulative path length to outlet |
| facc_quality | i4 | | -9999 | VARCHAR->i4 (denoise_v3=1, else fill) | Flow accumulation correction flag |
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
| slope_obs_reliable | i4 | | -9999 | BOOL->i4 (True=1, False=0) | Whether SWOT slope is reliable |
| slope_obs_quality | i4 | | -9999 | VARCHAR->i4 (0=reliable, 1=small_negative, 2=moderate_negative, 3=large_negative, 4=negative, 5=below_ref_uncertainty, 6=high_uncertainty, 7=noise_high_nobs, 8=flat_water_noise) | SWOT slope quality category |
| slope_obs_n | i4 | | -9999 | | Number of SWOT slope observations |
| slope_obs_n_passes | i4 | | -9999 | | Number of SWOT passes with slope |
| slope_obs_q | i4 | | -9999 | Integer bitfield (1=negative, 2=low_passes, 4=high_var, 8=extreme, 16=clipped) | SWOT slope quality bitfield |
| n_obs | i4 | | -9999 | | Total number of SWOT observations |

## New v17c Node Variables

| Variable | NetCDF Type | Units | Fill Value | Encoding | Description |
|---|---|---|---|---|---|
| subnetwork_id | i4 | | -9999 | | Connected component ID (from parent reach) |
| best_headwater | i8 | | -9999 | | Width-prioritized upstream headwater reach ID |
| best_outlet | i8 | | -9999 | | Width-prioritized downstream outlet reach ID |
| pathlen_hw | f8 | meters | -9999.0 | | Cumulative path length from headwater |
| pathlen_out | f8 | meters | -9999.0 | | Cumulative path length to outlet |
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
| facc_quality | i4 | | -9999 | VARCHAR->i4 (denoise_v3=1, else fill) | Flow accumulation correction flag |

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
| dist_out | f8 | meters | -9999.0 | Distance to network outlet |
| n_rch_up | i4 | | -9999 | Number of upstream neighbor reaches |
| n_rch_down | i4 | | -9999 | Number of downstream neighbor reaches |
| lakeflag | i4 | | -9999 | Water body type (0=river, 1=lake, 2=canal, 3=tidal) |
| type | i4 | | -9999 | Reach type (1=river, 3=lake_on_river, 4=dam, 5=unreliable, 6=ghost). Not in v17b NetCDF; added in v17c. Values from v17b database. |
| swot_obs | i4 | | -9999 | Number of expected SWOT observations per cycle |
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
| edit_flag | string | | -9999.0 | Edit provenance tag (e.g. lake_sandwich) |

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
| dist_out | f8 | meters | -9999.0 | Distance to network outlet |
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
| river_name | string | | | River name |
| edit_flag | string | | | Edit provenance tag |

### Multi-dimensional Arrays

| Variable | Group | Shape | Type | Description |
|---|---|---|---|---|
| cl_ids | reaches | [2, num_reaches] | i8 | Min/max centerline IDs bounding this reach |
| rch_id_up | reaches | [4, num_reaches] | i8 | Up to 4 upstream neighbor reach IDs |
| rch_id_dn | reaches | [4, num_reaches] | i8 | Up to 4 downstream neighbor reach IDs |
| iceflag | reaches | [366, num_reaches] | i4 | Daily ice presence flag (Julian day 1-366) |
| swot_orbits | reaches | [75, num_reaches] | i8 | SWOT orbit IDs that observe this reach |
| cl_ids | nodes | [2, num_nodes] | i8 | Min/max centerline IDs bounding this node |
| cl_id | centerlines | [num_points] | i8 | Centerline point ID |
| x | centerlines | [num_points] | f8 | Centerline point longitude (degrees east) |
| y | centerlines | [num_points] | f8 | Centerline point latitude (degrees north) |
| reach_id | centerlines | [4, num_points] | i8 | Reach IDs associated with this centerline point |
| node_id | centerlines | [4, num_points] | i8 | Node IDs associated with this centerline point |

### Subgroups (copied from v17b)

| Subgroup | Parent | Description |
|---|---|---|
| reaches/area_fits | reaches | Area-based rating curve fit coefficients |
| reaches/discharge_models | reaches | Discharge model parameters (organized by constraint/model) |
