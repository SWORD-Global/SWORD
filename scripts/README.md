# Scripts

Standalone scripts organized by purpose.

## topology/

| Script | Description |
|--------|-------------|
| run_v17c_topology.py | Generate SWORD v17c by running topology recalculations on v17b |
| topology_investigator.py | Streamlit GUI to investigate topology issues using SWOT WSE data |
| topology_optimizer.py | Experimental MILP-based topology optimizer using phi (distance-to-outlet) |

## visualization/

| Script | Description |
|--------|-------------|
| visualize_all_pipelines.py | Generate 9-panel RivGraph pipeline view for each sampled reach |
| visualize_comparison_3panel.py | Three-panel comparison: RGB+lines, drift magnitude, water mask+lines |
| visualize_reach_maps.py | Generate maps showing SWORD vs observed centerlines per reach |
| visualize_samples.py | Visualize stratified sample results |
| presentation_hires.py | High-resolution lake context visualization with projected basemaps |
| presentation_lake_context.py | Lake misclassification context maps for presentations |
| presentation_materials.py | SWORD v17c data quality and topology improvement visualizations |

## analysis/

| Script | Description |
|--------|-------------|
| compare_v17b_v17c.py | Compare SWORD v17b vs v17c topology and attribute differences |
| find_descending_node_order_reaches.py | Diagnostic script identifying reaches whose node rows were historically stored upstream-first / descending dist_out |

## maintenance/

| Script | Description |
|--------|-------------|
| rebuild_v17b.py | Rebuild v17b DuckDB from source NetCDF files (use only if v17b corrupted) |
| reimport_fixes.py | Reimport lint fixes exported from topology_reviewer.py into another database |
| check_reviewer_setup.py | Verify all dependencies and data are ready for the SWORD QA Reviewer |
| load_from_duckdb.py | Export SWORD DuckDB data into PostgreSQL with PostGIS geometry support |
| check_roadmap_sync.sh | Validate roadmap milestone counts/due date against live GitHub issue + milestone data |
| revert_v17c_node_geometry_to_v17b.py | Restore v17b node geometry for D0-D2 continuity |
| repair_node_order_projection.py | Repair node_order on restored-coordinate reaches without moving nodes |
| recompute_swot_slopes_from_nodes.py | Recompute slope_obs_* from RiverSP node WSE after node_order repairs |
| build_node_correspondence_table.py | Build v17b-to-v17c node correspondence artifacts for coordinate-change tracking |

## sql/

| Script | Description |
|--------|-------------|
| create_postgres_schema.sql | PostgreSQL/PostGIS schema mirroring the DuckDB SWORD schema |
