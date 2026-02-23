-- -----------------------------------------------------------------------------
-- SWORD PostgreSQL Schema with PostGIS Support
-- -----------------------------------------------------------------------------
-- This schema mirrors the DuckDB schema for SWOT River Database (SWORD)
-- with PostGIS geometry columns for spatial operations in QGIS.
--
-- Usage:
--   psql -d sword_db -f create_postgres_schema.sql
--
-- Prerequisites:
--   CREATE EXTENSION IF NOT EXISTS postgis;
--
-- Tables:
--   centerlines       - Dense geospatial points forming river centerlines (66.9M rows)
--   centerline_neighbors - Normalized neighbor relationships
--   nodes             - Points at ~200m intervals with hydrologic attributes (11.1M rows)
--   reaches           - River segments between major junctions (248.7K rows)
--   reach_topology    - Upstream/downstream relationships
--   reach_swot_orbits - SWOT orbit data
--   reach_ice_flags   - Daily ice flags
--   sword_versions    - Version tracking
--   + provenance tables
--   + v17c tables
--   + operational tables
-- -----------------------------------------------------------------------------

-- Enable PostGIS extension (must be done by superuser or user with CREATE privilege)
CREATE EXTENSION IF NOT EXISTS postgis;

-- -----------------------------------------------------------------------------
-- CORE TABLES
-- -----------------------------------------------------------------------------

-- Centerlines: Dense geospatial points forming river centerlines
-- Primary Key: (cl_id, region) - cl_id is only unique within region
DROP TABLE IF EXISTS centerlines CASCADE;
CREATE TABLE centerlines (
    cl_id BIGINT NOT NULL,
    region VARCHAR(2) NOT NULL,

    -- Coordinates (stored separately for non-spatial queries)
    x DOUBLE PRECISION NOT NULL,
    y DOUBLE PRECISION NOT NULL,

    -- PostGIS geometry (POINT, SRID 4326 = WGS84)
    geom GEOMETRY(POINT, 4326),

    -- Primary associations
    reach_id BIGINT NOT NULL,
    node_id BIGINT NOT NULL,

    -- Metadata
    version VARCHAR(10) NOT NULL,

    PRIMARY KEY (cl_id, region)
);

-- Centerline neighbors: Normalized neighbor relationships (from [4,N] arrays)
DROP TABLE IF EXISTS centerline_neighbors CASCADE;
CREATE TABLE centerline_neighbors (
    cl_id BIGINT NOT NULL,
    region VARCHAR(2) NOT NULL,
    neighbor_rank SMALLINT NOT NULL,  -- 1, 2, or 3 (0 is in main table)

    -- Neighbor IDs
    reach_id BIGINT,
    node_id BIGINT,

    PRIMARY KEY (cl_id, region, neighbor_rank)
);

-- Nodes: Points at ~200m intervals with hydrologic attributes
-- Primary Key: (node_id, region) - node_id is only unique within region
DROP TABLE IF EXISTS nodes CASCADE;
CREATE TABLE nodes (
    node_id BIGINT NOT NULL,
    region VARCHAR(2) NOT NULL,

    -- Coordinates
    x DOUBLE PRECISION NOT NULL,
    y DOUBLE PRECISION NOT NULL,

    -- PostGIS geometry (POINT, SRID 4326)
    geom GEOMETRY(POINT, 4326),

    -- Centerline range
    cl_id_min BIGINT,
    cl_id_max BIGINT,

    -- Parent reach
    reach_id BIGINT NOT NULL,

    -- Core measurements
    node_length DOUBLE PRECISION,        -- len
    wse DOUBLE PRECISION,                -- water surface elevation (m)
    wse_var DOUBLE PRECISION,            -- wse variance (m^2)
    width DOUBLE PRECISION,              -- wth (m)
    width_var DOUBLE PRECISION,          -- wth_var (m^2)
    max_width DOUBLE PRECISION,          -- max_wth (m)

    -- Flow and hydrology
    facc DOUBLE PRECISION,               -- flow accumulation (km^2)
    dist_out DOUBLE PRECISION,           -- distance from outlet (m)
    lakeflag INTEGER,                    -- 0=river, 1=lake, 2=canal, 3=tidal

    -- Obstructions
    obstr_type INTEGER,                  -- 0=none, 1=dam, 2=lock, 3=low-perm, 4=waterfall
    grod_id BIGINT,                      -- GROD database ID
    hfalls_id BIGINT,                    -- HydroFALLS database ID

    -- Channel info
    n_chan_max INTEGER,
    n_chan_mod INTEGER,

    -- SWOT search parameters
    wth_coef DOUBLE PRECISION,
    ext_dist_coef DOUBLE PRECISION,

    -- Morphology
    meander_length DOUBLE PRECISION,
    sinuosity DOUBLE PRECISION,

    -- Metadata
    river_name VARCHAR,
    manual_add INTEGER,

    -- Quality flags
    edit_flag VARCHAR,
    trib_flag INTEGER,                   -- 0=no tributary, 1=tributary

    -- Network analysis
    path_freq BIGINT,
    path_order BIGINT,
    path_segs BIGINT,
    stream_order INTEGER,
    main_side INTEGER,                   -- 0=main, 1=side, 2=secondary outlet
    end_reach INTEGER,                   -- 0=main, 1=headwater, 2=outlet, 3=junction
    network INTEGER,

    -- Addition flag
    add_flag INTEGER,

    -- Metadata
    version VARCHAR(10) NOT NULL,

    -- v17c columns
    best_headwater BIGINT,
    best_outlet BIGINT,
    pathlen_hw DOUBLE PRECISION,
    pathlen_out DOUBLE PRECISION,

    -- SWOT observation statistics
    wse_obs_range DOUBLE PRECISION,
    width_obs_range DOUBLE PRECISION,
    n_obs INTEGER,

    -- Node ordering within reach (1=downstream, n=upstream)
    node_order INTEGER,

    -- SWOT percentile-based observations
    wse_obs_p10 DOUBLE PRECISION,
    wse_obs_p20 DOUBLE PRECISION,
    wse_obs_p30 DOUBLE PRECISION,
    wse_obs_p40 DOUBLE PRECISION,
    wse_obs_p50 DOUBLE PRECISION,
    wse_obs_p60 DOUBLE PRECISION,
    wse_obs_p70 DOUBLE PRECISION,
    wse_obs_p80 DOUBLE PRECISION,
    wse_obs_p90 DOUBLE PRECISION,
    wse_obs_mad DOUBLE PRECISION,
    width_obs_p10 DOUBLE PRECISION,
    width_obs_p20 DOUBLE PRECISION,
    width_obs_p30 DOUBLE PRECISION,
    width_obs_p40 DOUBLE PRECISION,
    width_obs_p50 DOUBLE PRECISION,
    width_obs_p60 DOUBLE PRECISION,
    width_obs_p70 DOUBLE PRECISION,
    width_obs_p80 DOUBLE PRECISION,
    width_obs_p90 DOUBLE PRECISION,
    width_obs_mad DOUBLE PRECISION,

    PRIMARY KEY (node_id, region)
);

-- Reaches: River segments between major junctions
-- Primary Key: (reach_id, region) - reach_id is globally unique but region included for consistency
DROP TABLE IF EXISTS reaches CASCADE;
CREATE TABLE reaches (
    reach_id BIGINT NOT NULL,
    region VARCHAR(2) NOT NULL,

    -- Centroid coordinates
    x DOUBLE PRECISION,
    y DOUBLE PRECISION,

    -- Bounding box
    x_min DOUBLE PRECISION,
    x_max DOUBLE PRECISION,
    y_min DOUBLE PRECISION,
    y_max DOUBLE PRECISION,

    -- PostGIS geometry (LINESTRING, SRID 4326)
    geom GEOMETRY(LINESTRING, 4326),

    -- Centerline range
    cl_id_min BIGINT,
    cl_id_max BIGINT,

    -- Core measurements
    reach_length DOUBLE PRECISION,       -- len (m)
    n_nodes INTEGER,
    wse DOUBLE PRECISION,                -- water surface elevation (m)
    wse_var DOUBLE PRECISION,
    width DOUBLE PRECISION,              -- wth (m)
    width_var DOUBLE PRECISION,
    slope DOUBLE PRECISION,              -- slope (m/km)
    max_width DOUBLE PRECISION,

    -- Flow and hydrology
    facc DOUBLE PRECISION,               -- flow accumulation (km^2)
    dist_out DOUBLE PRECISION,           -- distance from outlet (m)
    lakeflag INTEGER,                    -- 0=river, 1=lake, 2=canal, 3=tidal

    -- Obstructions
    obstr_type INTEGER,
    grod_id BIGINT,
    hfalls_id BIGINT,

    -- Channel info
    n_chan_max INTEGER,
    n_chan_mod INTEGER,

    -- Topology counts
    n_rch_up INTEGER,
    n_rch_down INTEGER,

    -- SWOT observations
    swot_obs INTEGER,                    -- max SWOT passes in 21-day cycle

    -- Flags
    iceflag INTEGER,
    low_slope_flag INTEGER,

    -- Metadata
    river_name VARCHAR,

    -- Quality flags
    edit_flag VARCHAR,
    trib_flag INTEGER,

    -- Network analysis
    path_freq BIGINT,
    path_order BIGINT,
    path_segs BIGINT,
    stream_order INTEGER,
    main_side INTEGER,                   -- 0=main, 1=side, 2=secondary outlet
    end_reach INTEGER,                   -- 0=main, 1=headwater, 2=outlet, 3=junction
    network INTEGER,

    -- Addition flag
    add_flag INTEGER,

    -- Metadata
    version VARCHAR(10) NOT NULL,

    -- Reconstructed neighbor IDs (denormalized for queries)
    rch_id_up_1 BIGINT,
    rch_id_dn_1 BIGINT,
    rch_id_up_2 BIGINT,
    rch_id_dn_2 BIGINT,
    rch_id_up_3 BIGINT,
    rch_id_dn_3 BIGINT,
    rch_id_up_4 BIGINT,
    rch_id_dn_4 BIGINT,

    -- v17c columns
    best_headwater BIGINT,
    best_outlet BIGINT,
    pathlen_hw DOUBLE PRECISION,
    pathlen_out DOUBLE PRECISION,
    main_path_id BIGINT,
    is_mainstem_edge BOOLEAN DEFAULT FALSE,
    dist_out_short DOUBLE PRECISION,
    hydro_dist_out DOUBLE PRECISION,
    hydro_dist_hw DOUBLE PRECISION,
    rch_id_up_main BIGINT,
    rch_id_dn_main BIGINT,

    -- SWOT observation statistics (range + n_obs)
    wse_obs_range DOUBLE PRECISION,
    width_obs_range DOUBLE PRECISION,
    slope_obs_range DOUBLE PRECISION,
    n_obs INTEGER,

    -- Quality flags
    facc_quality VARCHAR,
    subnetwork_id INTEGER,
    type INTEGER,

    -- SWOT slope observation quality
    slope_obs_adj DOUBLE PRECISION,
    slope_obs_reliable BOOLEAN,
    slope_obs_quality VARCHAR,
    slope_obs_n INTEGER,
    slope_obs_n_passes INTEGER,
    slope_obs_q INTEGER,
    slope_obs_slopeF DOUBLE PRECISION,
    swot_obs_source VARCHAR,

    -- River name variants
    river_name_local VARCHAR,
    river_name_en VARCHAR,

    -- Boundary node IDs
    dn_node_id BIGINT,
    up_node_id BIGINT,

    -- SWOT percentile-based observations
    wse_obs_p10 DOUBLE PRECISION,
    wse_obs_p20 DOUBLE PRECISION,
    wse_obs_p30 DOUBLE PRECISION,
    wse_obs_p40 DOUBLE PRECISION,
    wse_obs_p50 DOUBLE PRECISION,
    wse_obs_p60 DOUBLE PRECISION,
    wse_obs_p70 DOUBLE PRECISION,
    wse_obs_p80 DOUBLE PRECISION,
    wse_obs_p90 DOUBLE PRECISION,
    wse_obs_mad DOUBLE PRECISION,
    width_obs_p10 DOUBLE PRECISION,
    width_obs_p20 DOUBLE PRECISION,
    width_obs_p30 DOUBLE PRECISION,
    width_obs_p40 DOUBLE PRECISION,
    width_obs_p50 DOUBLE PRECISION,
    width_obs_p60 DOUBLE PRECISION,
    width_obs_p70 DOUBLE PRECISION,
    width_obs_p80 DOUBLE PRECISION,
    width_obs_p90 DOUBLE PRECISION,
    width_obs_mad DOUBLE PRECISION,
    slope_obs_p10 DOUBLE PRECISION,
    slope_obs_p20 DOUBLE PRECISION,
    slope_obs_p30 DOUBLE PRECISION,
    slope_obs_p40 DOUBLE PRECISION,
    slope_obs_p50 DOUBLE PRECISION,
    slope_obs_p60 DOUBLE PRECISION,
    slope_obs_p70 DOUBLE PRECISION,
    slope_obs_p80 DOUBLE PRECISION,
    slope_obs_p90 DOUBLE PRECISION,
    slope_obs_mad DOUBLE PRECISION,

    PRIMARY KEY (reach_id, region)
);

-- Reach topology: Normalized upstream/downstream relationships
DROP TABLE IF EXISTS reach_topology CASCADE;
CREATE TABLE reach_topology (
    reach_id BIGINT NOT NULL,
    region VARCHAR(2) NOT NULL,
    direction VARCHAR(4) NOT NULL,       -- 'up' or 'down'
    neighbor_rank SMALLINT NOT NULL,     -- 0-3

    neighbor_reach_id BIGINT NOT NULL,

    -- Review workflow flags
    topology_suspect BOOLEAN DEFAULT FALSE,
    topology_approved BOOLEAN DEFAULT FALSE,

    PRIMARY KEY (reach_id, region, direction, neighbor_rank)
);

-- Reach SWOT orbits: Normalized from [75,N] array
DROP TABLE IF EXISTS reach_swot_orbits CASCADE;
CREATE TABLE reach_swot_orbits (
    reach_id BIGINT NOT NULL,
    region VARCHAR(2) NOT NULL,
    orbit_rank SMALLINT NOT NULL,        -- 0-74

    orbit_id BIGINT NOT NULL,

    PRIMARY KEY (reach_id, region, orbit_rank)
);

-- Reach ice flags: Daily ice presence (366 days)
DROP TABLE IF EXISTS reach_ice_flags CASCADE;
CREATE TABLE reach_ice_flags (
    reach_id BIGINT NOT NULL,
    julian_day SMALLINT NOT NULL,        -- 1-366

    iceflag INTEGER NOT NULL,

    PRIMARY KEY (reach_id, julian_day)
);

-- SWORD versions: Version tracking metadata
DROP TABLE IF EXISTS sword_versions CASCADE;
CREATE TABLE sword_versions (
    version VARCHAR(10) PRIMARY KEY,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    production_date TIMESTAMP,
    schema_version VARCHAR(10),
    notes VARCHAR
);

-- -----------------------------------------------------------------------------
-- PROVENANCE TABLES
-- -----------------------------------------------------------------------------

-- Operations: Audit trail of all changes
DROP TABLE IF EXISTS sword_operations CASCADE;
CREATE TABLE sword_operations (
    operation_id SERIAL PRIMARY KEY,

    operation_type VARCHAR(50) NOT NULL,  -- CREATE, UPDATE, DELETE, RECALCULATE, etc.

    table_name VARCHAR(50),
    entity_ids BIGINT[],
    region VARCHAR(2),

    user_id VARCHAR(100),
    session_id VARCHAR(50),

    started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,

    operation_details JSONB,
    affected_columns VARCHAR[],

    reason VARCHAR(500),
    source_operation_id INTEGER,

    status VARCHAR(20) DEFAULT 'PENDING',
    error_message VARCHAR,

    before_checksum VARCHAR(64),
    after_checksum VARCHAR(64),

    synced_to_duckdb BOOLEAN DEFAULT FALSE
);

-- Value snapshots: Old/new values for rollback
DROP TABLE IF EXISTS sword_value_snapshots CASCADE;
CREATE TABLE sword_value_snapshots (
    snapshot_id SERIAL PRIMARY KEY,

    operation_id INTEGER NOT NULL REFERENCES sword_operations(operation_id),

    table_name VARCHAR(50) NOT NULL,
    entity_id BIGINT NOT NULL,
    column_name VARCHAR(50) NOT NULL,

    old_value JSONB,
    new_value JSONB,
    data_type VARCHAR(20)
);

-- Source lineage: Data provenance tracking
DROP TABLE IF EXISTS sword_source_lineage CASCADE;
CREATE TABLE sword_source_lineage (
    lineage_id SERIAL PRIMARY KEY,

    entity_type VARCHAR(20) NOT NULL,
    entity_id BIGINT NOT NULL,
    region VARCHAR(2) NOT NULL,

    source_dataset VARCHAR(50) NOT NULL,
    source_id VARCHAR(100),
    source_version VARCHAR(20),

    attribute_name VARCHAR(50),
    derivation_method VARCHAR(100),

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Reconstruction recipes: Scripts for rebuilding attributes
DROP TABLE IF EXISTS sword_reconstruction_recipes CASCADE;
CREATE TABLE sword_reconstruction_recipes (
    recipe_id SERIAL PRIMARY KEY,

    name VARCHAR(100) NOT NULL UNIQUE,
    description VARCHAR(500),

    target_attributes VARCHAR[],
    required_sources VARCHAR[],
    script_path VARCHAR(500),
    script_hash VARCHAR(64),

    parameters JSONB,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP
);

-- Snapshots: Named snapshots for versioning
DROP TABLE IF EXISTS sword_snapshots CASCADE;
CREATE TABLE sword_snapshots (
    snapshot_id SERIAL PRIMARY KEY,

    name VARCHAR(100) NOT NULL UNIQUE,
    description VARCHAR(500),

    operation_id_max INTEGER NOT NULL,

    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100),
    session_id VARCHAR(50),

    reach_count INTEGER,
    node_count INTEGER,
    centerline_count INTEGER,

    is_auto_snapshot BOOLEAN DEFAULT FALSE,
    tags VARCHAR[]
);

-- -----------------------------------------------------------------------------
-- V17C TABLES
-- -----------------------------------------------------------------------------

-- v17c sections: Junction-to-junction sections
DROP TABLE IF EXISTS v17c_sections CASCADE;
CREATE TABLE v17c_sections (
    section_id INTEGER NOT NULL,
    region VARCHAR(2) NOT NULL,
    upstream_junction BIGINT,
    downstream_junction BIGINT,
    reach_ids VARCHAR,                   -- Comma-separated list
    distance DOUBLE PRECISION,
    n_reaches INTEGER,

    PRIMARY KEY (section_id, region)
);

-- v17c section slope validation
DROP TABLE IF EXISTS v17c_section_slope_validation CASCADE;
CREATE TABLE v17c_section_slope_validation (
    section_id INTEGER NOT NULL,
    region VARCHAR(2) NOT NULL,
    slope_from_upstream DOUBLE PRECISION,
    slope_from_downstream DOUBLE PRECISION,
    direction_valid BOOLEAN,
    likely_cause VARCHAR,

    PRIMARY KEY (section_id, region)
);

-- facc corrections: Applied corrections
DROP TABLE IF EXISTS facc_corrections CASCADE;
CREATE TABLE facc_corrections (
    correction_id INTEGER PRIMARY KEY,
    reach_id BIGINT,
    old_facc DOUBLE PRECISION,
    new_facc DOUBLE PRECISION,
    predicted_facc DOUBLE PRECISION,
    correction_type VARCHAR,
    applied_at TIMESTAMP,
    batch_id INTEGER
);

-- v17c flow corrections: Section flow direction corrections
DROP TABLE IF EXISTS v17c_flow_corrections CASCADE;
CREATE TABLE v17c_flow_corrections (
    run_id VARCHAR,
    region VARCHAR(2),
    section_id INTEGER,
    iteration INTEGER,
    tier VARCHAR,
    action VARCHAR,
    slope_from_upstream DOUBLE PRECISION,
    slope_from_downstream DOUBLE PRECISION,
    n_reaches_flipped INTEGER,
    reach_ids_flipped VARCHAR,
    created_at TIMESTAMP
);

-- -----------------------------------------------------------------------------
-- OPERATIONAL TABLES
-- -----------------------------------------------------------------------------

-- facc fix log: Tracks flow accumulation corrections
DROP TABLE IF EXISTS facc_fix_log CASCADE;
CREATE TABLE facc_fix_log (
    fix_id INTEGER,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    reach_id BIGINT,
    region VARCHAR(2),
    fix_type VARCHAR(50),
    old_facc DOUBLE PRECISION,
    new_facc DOUBLE PRECISION,
    old_edit_flag VARCHAR,
    new_edit_flag VARCHAR,
    notes VARCHAR,
    source VARCHAR,
    batch_id VARCHAR,
    model_used VARCHAR,
    facc_corrected DOUBLE PRECISION
);

-- Lint fix log: Tracks lint corrections
DROP TABLE IF EXISTS lint_fix_log CASCADE;
CREATE TABLE lint_fix_log (
    fix_id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    check_id VARCHAR(20),
    reach_id BIGINT,
    region VARCHAR(2),
    action VARCHAR(50),
    column_changed VARCHAR(50),
    old_value VARCHAR,
    new_value VARCHAR,
    notes VARCHAR,
    undone BOOLEAN DEFAULT FALSE
);

-- Imagery acquisitions: Satellite imagery metadata
DROP TABLE IF EXISTS imagery_acquisitions CASCADE;
CREATE TABLE imagery_acquisitions (
    acquisition_id SERIAL PRIMARY KEY,
    stac_item_id VARCHAR,
    collection VARCHAR,
    bbox_wkt VARCHAR,
    acquired_at TIMESTAMP,
    cloud_cover DOUBLE PRECISION,
    sun_azimuth DOUBLE PRECISION,
    sun_elevation DOUBLE PRECISION,
    ndwi_computed BOOLEAN DEFAULT FALSE,
    water_mask_computed BOOLEAN DEFAULT FALSE,
    cached_at TIMESTAMP,
    cache_path VARCHAR,
    region VARCHAR(2),
    reach_ids BIGINT[],
    platform VARCHAR,
    processing_level VARCHAR,
    item_metadata JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Reach imagery: Per-reach imagery statistics
DROP TABLE IF EXISTS reach_imagery CASCADE;
CREATE TABLE reach_imagery (
    reach_id BIGINT NOT NULL,
    region VARCHAR(2) NOT NULL,
    acquisition_id INTEGER REFERENCES imagery_acquisitions(acquisition_id),
    ndwi_mean DOUBLE PRECISION,
    ndwi_std DOUBLE PRECISION,
    ndwi_min DOUBLE PRECISION,
    ndwi_max DOUBLE PRECISION,
    water_fraction DOUBLE PRECISION,
    water_pixel_count INTEGER,
    total_valid_pixels INTEGER,
    threshold_used DOUBLE PRECISION,
    computed_at TIMESTAMP,

    PRIMARY KEY (reach_id, region, acquisition_id)
);

-- Reach geometries: Separate geometry storage (for special cases)
DROP TABLE IF EXISTS reach_geometries CASCADE;
CREATE TABLE reach_geometries (
    reach_id BIGINT PRIMARY KEY,
    geom GEOMETRY(LINESTRING, 4326)
);

-- -----------------------------------------------------------------------------
-- SPATIAL INDEXES (GIST)
-- -----------------------------------------------------------------------------

CREATE INDEX idx_centerlines_geom_gist ON centerlines USING GIST (geom);
CREATE INDEX idx_nodes_geom_gist ON nodes USING GIST (geom);
CREATE INDEX idx_reaches_geom_gist ON reaches USING GIST (geom);
CREATE INDEX idx_reach_geometries_geom_gist ON reach_geometries USING GIST (geom);

-- -----------------------------------------------------------------------------
-- B-TREE INDEXES
-- -----------------------------------------------------------------------------

-- Foreign key lookups
CREATE INDEX idx_centerlines_reach ON centerlines(reach_id);
CREATE INDEX idx_centerlines_node ON centerlines(node_id);
CREATE INDEX idx_nodes_reach ON nodes(reach_id);

-- Regional partitioning queries
CREATE INDEX idx_centerlines_region ON centerlines(region);
CREATE INDEX idx_nodes_region ON nodes(region);
CREATE INDEX idx_reaches_region ON reaches(region);

-- Topology traversal
CREATE INDEX idx_topology_reach ON reach_topology(reach_id);
CREATE INDEX idx_topology_neighbor ON reach_topology(neighbor_reach_id);
CREATE INDEX idx_topology_direction ON reach_topology(reach_id, direction);

-- Common analytical queries
CREATE INDEX idx_reaches_dist_out ON reaches(dist_out);
CREATE INDEX idx_reaches_facc ON reaches(facc);
CREATE INDEX idx_reaches_stream_order ON reaches(stream_order);
CREATE INDEX idx_reaches_network ON reaches(network);
CREATE INDEX idx_nodes_stream_order ON nodes(stream_order);

-- SWOT orbits lookup
CREATE INDEX idx_swot_orbits_reach ON reach_swot_orbits(reach_id);

-- Provenance indexes
CREATE INDEX idx_operations_type ON sword_operations(operation_type);
CREATE INDEX idx_operations_table ON sword_operations(table_name);
CREATE INDEX idx_operations_region ON sword_operations(region);
CREATE INDEX idx_operations_session ON sword_operations(session_id);
CREATE INDEX idx_operations_started ON sword_operations(started_at);
CREATE INDEX idx_operations_status ON sword_operations(status);
CREATE INDEX idx_snapshots_operation_id ON sword_value_snapshots(operation_id);
CREATE INDEX idx_snapshots_entity ON sword_value_snapshots(table_name, entity_id);
CREATE INDEX idx_lineage_entity ON sword_source_lineage(entity_type, entity_id, region);
CREATE INDEX idx_lineage_source ON sword_source_lineage(source_dataset);

-- Snapshot versioning indexes
CREATE INDEX idx_sword_snapshots_name ON sword_snapshots(name);
CREATE INDEX idx_sword_snapshots_created ON sword_snapshots(created_at);
CREATE INDEX idx_sword_snapshots_operation ON sword_snapshots(operation_id_max);

-- v17c indexes
CREATE INDEX idx_v17c_sections_region ON v17c_sections(region);
CREATE INDEX idx_v17c_sections_junctions ON v17c_sections(upstream_junction, downstream_junction);
CREATE INDEX idx_v17c_validation_region ON v17c_section_slope_validation(region);
CREATE INDEX idx_v17c_validation_valid ON v17c_section_slope_validation(direction_valid);

-- Operational indexes
CREATE INDEX idx_facc_fix_log_reach ON facc_fix_log(reach_id);
CREATE INDEX idx_facc_fix_log_region ON facc_fix_log(region);
CREATE INDEX idx_lint_fix_log_reach ON lint_fix_log(reach_id);
CREATE INDEX idx_lint_fix_log_check ON lint_fix_log(check_id);
CREATE INDEX idx_reach_imagery_reach ON reach_imagery(reach_id);
CREATE INDEX idx_imagery_acquisitions_region ON imagery_acquisitions(region);

-- -----------------------------------------------------------------------------
-- REGIONAL VIEWS
-- -----------------------------------------------------------------------------

-- North America
CREATE OR REPLACE VIEW na_centerlines AS SELECT * FROM centerlines WHERE region = 'NA';
CREATE OR REPLACE VIEW na_nodes AS SELECT * FROM nodes WHERE region = 'NA';
CREATE OR REPLACE VIEW na_reaches AS SELECT * FROM reaches WHERE region = 'NA';

-- South America
CREATE OR REPLACE VIEW sa_centerlines AS SELECT * FROM centerlines WHERE region = 'SA';
CREATE OR REPLACE VIEW sa_nodes AS SELECT * FROM nodes WHERE region = 'SA';
CREATE OR REPLACE VIEW sa_reaches AS SELECT * FROM reaches WHERE region = 'SA';

-- Europe
CREATE OR REPLACE VIEW eu_centerlines AS SELECT * FROM centerlines WHERE region = 'EU';
CREATE OR REPLACE VIEW eu_nodes AS SELECT * FROM nodes WHERE region = 'EU';
CREATE OR REPLACE VIEW eu_reaches AS SELECT * FROM reaches WHERE region = 'EU';

-- Africa
CREATE OR REPLACE VIEW af_centerlines AS SELECT * FROM centerlines WHERE region = 'AF';
CREATE OR REPLACE VIEW af_nodes AS SELECT * FROM nodes WHERE region = 'AF';
CREATE OR REPLACE VIEW af_reaches AS SELECT * FROM reaches WHERE region = 'AF';

-- Oceania
CREATE OR REPLACE VIEW oc_centerlines AS SELECT * FROM centerlines WHERE region = 'OC';
CREATE OR REPLACE VIEW oc_nodes AS SELECT * FROM nodes WHERE region = 'OC';
CREATE OR REPLACE VIEW oc_reaches AS SELECT * FROM reaches WHERE region = 'OC';

-- Asia
CREATE OR REPLACE VIEW as_centerlines AS SELECT * FROM centerlines WHERE region = 'AS';
CREATE OR REPLACE VIEW as_nodes AS SELECT * FROM nodes WHERE region = 'AS';
CREATE OR REPLACE VIEW as_reaches AS SELECT * FROM reaches WHERE region = 'AS';

-- -----------------------------------------------------------------------------
-- SCHEMA VERSION
-- -----------------------------------------------------------------------------

INSERT INTO sword_versions (version, schema_version, notes)
VALUES ('schema', '1.7.0', 'Add percentile obs, node_order, dn/up_node_id, river_name variants')
ON CONFLICT (version) DO UPDATE
SET schema_version = EXCLUDED.schema_version,
    notes = EXCLUDED.notes,
    created_at = CURRENT_TIMESTAMP;

-- -----------------------------------------------------------------------------
-- DONE
-- -----------------------------------------------------------------------------
-- Schema created successfully. Run load_from_duckdb.py to populate data.
