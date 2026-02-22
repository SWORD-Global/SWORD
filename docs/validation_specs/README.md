# SWORD Validation Specs Summary

28 deep-dive specifications documenting every SWORD variable: definitions, algorithms, valid ranges, failure modes, lint checks, and reconstruction rules.

## Spec Index

| Spec File | Variables | Source | Notes |
|-----------|-----------|--------|-------|
| `validation_spec_dist_out` | dist_out | Graph traversal from topology | BFS accumulation, outlets at 0m |
| `facc_validation_spec` | facc | MERIT Hydro D8 | ~3% downstream violations, outlier filtering needed |
| `wse_validation_spec` | wse, wse_var | MERIT DEM | Node reconstruction uses reach fallback |
| `width_slope_validation_spec` | width, width_var, slope, max_width | GRWL (width), WSE regression (slope) | Slope can be negative |
| `path_freq_validation_spec` | path_freq | Computed traversal count | Increases toward outlets |
| `stream_order_path_segs_validation_spec` | stream_order, path_segs, path_order | Derived from path_freq | `stream_order = round(log(path_freq)) + 1` |
| `end_reach_trib_flag_validation_spec` | end_reach, trib_flag | Topology (end_reach), MHV spatial proximity (trib_flag) | trib_flag is NOT a junction indicator |
| `main_side_network_validation_spec` | main_side, network | path_freq analysis, connected components | 0=main(95%), 1=side(3%), 2=secondary(2%) |
| `reach_length_neighbor_count_validation_spec` | reach_length, n_rch_up, n_rch_down | Euclidean from centerlines | <2% length corrections in v17b |
| `lakeflag_type_validation_spec` | lakeflag, type | GRWL (lakeflag), reach_id encoding (type) | Lake sandwiches ~1.55%, type/lakeflag mismatch |
| `obstruction_validation_spec` | obstr_type, grod_id, hfalls_id | GROD + HydroFALLS | 500m filtering for double-counting |
| `channel_count_validation_spec` | n_chan_max, n_chan_mod, n_nodes | GRWL nchan | Braiding detection |
| `flags_validation_spec` | swot_obs, iceflag, low_slope_flag, edit_flag, add_flag | Mixed sources | swot_obs max=31 passes/21-day cycle |
| `river_name_validation_spec` | river_name | GRWL + KDTree nearest neighbor | Multiple names joined with '; ' |
| `grades_discharge_validation_spec` | ~~h_variance, w_variance~~, fit_coeffs_*, h_break_*, w_break_* | GRADES/SWOT | **All GRADES columns removed from v17c** |
| `identifier_metadata_validation_spec` | reach_id, region, version | CBBBBBRRRRT Pfafstetter encoding | 11-digit reach_id, 6 regions |
| `geometry_metadata_validation_spec` | x, y, x_min/max, y_min/max, cl_id_min/max | Computed from geom + centerlines | Spatial indexing metadata |
| `geom_validation_spec` | geom (LINESTRING) | GRWL centerlines ordered by cl_id | WGS84 EPSG:4326 |
| `n_nodes_validation_spec` | n_nodes | Count from nodes table | Node-reach consistency |
| `subnetwork_id_validation_spec` | subnetwork_id | NetworkX weakly_connected_components | 855 subnetworks vs 247 networks |
| `reach_neighbor_ids_validation_spec` | rch_id_up_1-4, rch_id_dn_1-4 | Reconstructed from reach_topology | NetCDF backward compatibility |
| `topology_review_flags_validation_spec` | topology_suspect, topology_approved | Manual review workflow | topology_reviewer.py integration |
| `facc_quality_validation_spec` | facc_quality | v17c workflow metadata | traced/suspect/unfixable/manual_fix |
| `swot_observations_validation_spec` | *_obs_mean/median/std/range, n_obs | SWOT L2 RiverSP | v17c additions |
| `swot_slope_validation_spec` | swot_slope, swot_slope_se, swot_slope_confidence | SWOT LME model | **REMOVED** - 0% populated |
| `validation_spec_v17c_mainstem_variables` | hydro_dist_out/hw, best_headwater/outlet, pathlen_hw/out, is_mainstem_edge | v17c width-prioritized Dijkstra | Main channel identification |
| `v17c_path_topology_validation_spec` | main_path_id, dist_out_short, rch_id_up/dn_main | v17c path grouping | Mainstem routing |
| `v17c_sections_validation_spec` | v17c_sections, v17c_section_slope_validation tables | v17c junction-to-junction | Slope direction validation |

## Statistics

- **Total specs:** 28
- **v17b variables covered:** ~100
- **v17c additions:** ~20 new computed variables
- **Empty columns:** GRADES discharge (19/21), swot_slope (removed)
- **Reconstruction rules:** 35+ in `src/sword_duckdb/reconstruction.py`

## Lint Integration

Each spec maps to one or more of the 35 lint checks in `src/sword_duckdb/lint/`. See lint framework docs for check IDs (T001-T011, A002-A010, G001-G003, C001-C004, V001-V008).
