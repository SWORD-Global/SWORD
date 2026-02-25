# -*- coding: utf-8 -*-
"""
SWORD DuckDB Schema Definitions
-------------------------------

This module defines the DuckDB schema for the SWOT River Database (SWORD).
It normalizes the multi-dimensional NetCDF arrays into proper relational tables.

Tables:
    centerlines - Dense geospatial points forming river centerlines
    centerline_neighbors - Normalized neighbor relationships (from [4,N] array)
    nodes - Points at ~200m intervals with hydrologic attributes
    reaches - River segments between major junctions
    reach_topology - Normalized upstream/downstream relationships (from [4,N] arrays)
    reach_swot_orbits - SWOT orbit data (normalized from [75,N] array)
    reach_ice_flags - Daily ice flags (normalized from [366,N] array)
    reach_discharge_params - Discharge algorithm parameters
    sword_versions - Version tracking metadata
"""

# Schema version for migration tracking
SCHEMA_VERSION = "1.7.0"  # SWOT obs: percentiles p10-p90, MAD, remove mean/std

# Valid region codes (uppercase)
VALID_REGIONS = frozenset(["NA", "SA", "EU", "AF", "AS", "OC"])


def normalize_region(region: str) -> str:
    """
    Normalize region code to uppercase and validate.

    Parameters
    ----------
    region : str
        Region code (case-insensitive)

    Returns
    -------
    str
        Uppercase region code

    Raises
    ------
    ValueError
        If region is not valid
    """
    r = region.upper()
    if r not in VALID_REGIONS:
        raise ValueError(
            f"Invalid region '{region}'. Must be one of: {sorted(VALID_REGIONS)}"
        )
    return r


# Core table definitions
# NOTE: cl_id and node_id are only unique within a region, so we use composite keys
CENTERLINES_TABLE = """
CREATE TABLE IF NOT EXISTS centerlines (
    -- Composite primary key (cl_id is only unique within region)
    cl_id BIGINT NOT NULL,
    region VARCHAR(2) NOT NULL,

    -- Coordinates
    x DOUBLE NOT NULL,
    y DOUBLE NOT NULL,

    -- Geometry (populated after insert via ST_Point)
    geom GEOMETRY,

    -- Primary associations (row 0 of the [4,N] arrays)
    reach_id BIGINT NOT NULL,
    node_id BIGINT NOT NULL,

    -- Metadata
    version VARCHAR(10) NOT NULL,

    PRIMARY KEY (cl_id, region)
);
"""

CENTERLINE_NEIGHBORS_TABLE = """
CREATE TABLE IF NOT EXISTS centerline_neighbors (
    -- Composite primary key (includes region for referential integrity)
    cl_id BIGINT NOT NULL,
    region VARCHAR(2) NOT NULL,
    neighbor_rank TINYINT NOT NULL,  -- 1, 2, or 3 (0 is in main table)

    -- Neighbor IDs (from rows 1-3 of the [4,N] arrays)
    reach_id BIGINT,
    node_id BIGINT,

    PRIMARY KEY (cl_id, region, neighbor_rank)
);
"""

NODES_TABLE = """
CREATE TABLE IF NOT EXISTS nodes (
    -- Identity
    node_id BIGINT NOT NULL,
    region VARCHAR(2) NOT NULL,

    -- Geometry
    x DOUBLE NOT NULL,
    y DOUBLE NOT NULL,
    geom GEOMETRY,

    -- Structure
    cl_id_min BIGINT,
    cl_id_max BIGINT,
    reach_id BIGINT NOT NULL,
    node_order INTEGER,          -- 1=downstream, n=upstream (by dist_out)
    node_length DOUBLE,          -- len

    -- WSE (prior)
    wse DOUBLE,                  -- water surface elevation (m)
    wse_var DOUBLE,              -- wse variance (m^2)

    -- Width (prior)
    width DOUBLE,                -- wth (m)
    width_var DOUBLE,            -- wth_var (m^2)
    max_width DOUBLE,            -- max_wth (m)

    -- Hydrology & Distance
    facc DOUBLE,                 -- flow accumulation (km^2)
    facc_quality VARCHAR,        -- facc correction status flag (v17c)
    dist_out DOUBLE,             -- distance from outlet (m)

    -- SWOT search parameters
    wth_coef DOUBLE,             -- width coefficient for search window
    ext_dist_coef DOUBLE,        -- max search window coefficient

    -- Morphology
    meander_length DOUBLE,       -- meand_len (m)
    sinuosity DOUBLE,            -- sinuosity ratio

    -- Network & Path Analysis
    network INTEGER,             -- connected network ID
    subnetwork_id INTEGER,       -- weakly connected component ID (v17c)
    stream_order INTEGER,        -- strm_order (log scale of path_freq)
    path_freq BIGINT,            -- traversal count
    path_order BIGINT,           -- 1=longest to N=shortest
    path_segs BIGINT,            -- segment ID between junctions
    main_side INTEGER,           -- 0=main, 1=side, 2=secondary outlet
    end_reach INTEGER,           -- end_rch: 0=main, 1=headwater, 2=outlet, 3=junction
    best_headwater BIGINT,       -- best upstream headwater node
    best_outlet BIGINT,          -- best downstream outlet node
    pathlen_hw DOUBLE,           -- cumulative path length to headwater
    pathlen_out DOUBLE,          -- cumulative path length to outlet

    -- SWOT observation statistics (computed from L2 RiverSP data)
    wse_obs_p10 DOUBLE,          -- 10th percentile observed WSE
    wse_obs_p20 DOUBLE,          -- 20th percentile observed WSE
    wse_obs_p30 DOUBLE,          -- 30th percentile observed WSE
    wse_obs_p40 DOUBLE,          -- 40th percentile observed WSE
    wse_obs_p50 DOUBLE,          -- 50th percentile (median) observed WSE
    wse_obs_p60 DOUBLE,          -- 60th percentile observed WSE
    wse_obs_p70 DOUBLE,          -- 70th percentile observed WSE
    wse_obs_p80 DOUBLE,          -- 80th percentile observed WSE
    wse_obs_p90 DOUBLE,          -- 90th percentile observed WSE
    wse_obs_range DOUBLE,        -- range (max-min) of observed WSE
    wse_obs_mad DOUBLE,          -- median absolute deviation of WSE
    width_obs_p10 DOUBLE,        -- 10th percentile observed width
    width_obs_p20 DOUBLE,        -- 20th percentile observed width
    width_obs_p30 DOUBLE,        -- 30th percentile observed width
    width_obs_p40 DOUBLE,        -- 40th percentile observed width
    width_obs_p50 DOUBLE,        -- 50th percentile (median) observed width
    width_obs_p60 DOUBLE,        -- 60th percentile observed width
    width_obs_p70 DOUBLE,        -- 70th percentile observed width
    width_obs_p80 DOUBLE,        -- 80th percentile observed width
    width_obs_p90 DOUBLE,        -- 90th percentile observed width
    width_obs_range DOUBLE,      -- range (max-min) of observed width
    width_obs_mad DOUBLE,        -- median absolute deviation of width
    n_obs INTEGER,               -- count of SWOT observations

    -- Classification & Flags
    lakeflag INTEGER,            -- 0=river, 1=lake, 2=canal, 3=tidal
    n_chan_max INTEGER,           -- nchan_max
    n_chan_mod INTEGER,           -- nchan_mod
    obstr_type INTEGER,          -- 0=none,1=dam,2=low_head_dam,3=lock,4=waterfall,5=partial_dam
    grod_id BIGINT,              -- GROD database ID (v17b source; sunset in v18)
    dl_grod_id BIGINT,           -- DL-GROD database ID (He et al. 2025)
    hfalls_id BIGINT,            -- HydroFALLS database ID
    trib_flag INTEGER,           -- 0=no tributary, 1=tributary
    manual_add INTEGER,          -- 0=not manual, 1=manual
    edit_flag VARCHAR,           -- comma-separated update codes
    add_flag INTEGER,            -- 0=not added, 1=added from MERIT Hydro

    -- Names
    river_name VARCHAR,          -- semicolon-separated if multiple

    -- Metadata
    version VARCHAR(10) NOT NULL,

    PRIMARY KEY (node_id, region)
);
"""

REACHES_TABLE = """
CREATE TABLE IF NOT EXISTS reaches (
    -- Identity
    reach_id BIGINT NOT NULL,
    region VARCHAR(2) NOT NULL,

    -- Geometry
    x DOUBLE,
    y DOUBLE,
    x_min DOUBLE,
    x_max DOUBLE,
    y_min DOUBLE,
    y_max DOUBLE,
    geom GEOMETRY,

    -- Structure
    reach_length DOUBLE,         -- len (m)
    n_nodes INTEGER,             -- rch_n_nodes
    cl_id_min BIGINT,
    cl_id_max BIGINT,
    dn_node_id BIGINT,           -- downstream boundary node ID
    up_node_id BIGINT,           -- upstream boundary node ID

    -- WSE (prior)
    wse DOUBLE,                  -- water surface elevation (m)
    wse_var DOUBLE,              -- wse variance (m^2)

    -- Width (prior)
    width DOUBLE,                -- wth (m)
    width_var DOUBLE,            -- wth_var (m^2)
    max_width DOUBLE,            -- max_wth (m)

    -- Slope (prior)
    slope DOUBLE,                -- slope (m/km)

    -- Hydrology & Distance
    facc DOUBLE,                 -- flow accumulation (km^2)
    facc_quality VARCHAR,        -- facc correction status flag (v17c)
    dist_out DOUBLE,             -- distance from outlet (m)
    hydro_dist_out DOUBLE,       -- hydrologic distance to outlet (via main channel)
    hydro_dist_hw DOUBLE,        -- hydrologic distance to headwater (via main channel)

    -- Topology
    n_rch_up INTEGER,            -- number of upstream neighbors
    n_rch_down INTEGER,          -- number of downstream neighbors
    rch_id_up_main BIGINT,       -- main upstream reach ID
    rch_id_dn_main BIGINT,       -- main downstream reach ID
    end_reach INTEGER,           -- end_rch: 0=main, 1=headwater, 2=outlet, 3=junction
    trib_flag INTEGER,           -- 0=no tributary, 1=tributary

    -- Network & Path Analysis
    network INTEGER,             -- connected network ID
    subnetwork_id INTEGER,       -- weakly connected component ID (v17c)
    stream_order INTEGER,        -- strm_order
    path_freq BIGINT,            -- traversal count
    path_order BIGINT,           -- 1=longest to N=shortest
    path_segs BIGINT,            -- segment ID between junctions
    main_side INTEGER,           -- 0=main, 1=side, 2=secondary outlet
    main_path_id BIGINT,         -- unique ID for headwater-outlet path
    is_mainstem_edge BOOLEAN DEFAULT FALSE,  -- whether edge is on mainstem
    best_headwater BIGINT,       -- best upstream headwater reach
    best_outlet BIGINT,          -- best downstream outlet reach
    pathlen_hw DOUBLE,           -- cumulative path length to headwater
    pathlen_out DOUBLE,          -- cumulative path length to outlet

    -- SWOT observation statistics (computed from L2 RiverSP data)
    wse_obs_p10 DOUBLE,              -- 10th percentile observed WSE
    wse_obs_p20 DOUBLE,              -- 20th percentile observed WSE
    wse_obs_p30 DOUBLE,              -- 30th percentile observed WSE
    wse_obs_p40 DOUBLE,              -- 40th percentile observed WSE
    wse_obs_p50 DOUBLE,              -- 50th percentile (median) observed WSE
    wse_obs_p60 DOUBLE,              -- 60th percentile observed WSE
    wse_obs_p70 DOUBLE,              -- 70th percentile observed WSE
    wse_obs_p80 DOUBLE,              -- 80th percentile observed WSE
    wse_obs_p90 DOUBLE,              -- 90th percentile observed WSE
    wse_obs_range DOUBLE,            -- range (max-min) of observed WSE
    wse_obs_mad DOUBLE,              -- median absolute deviation of WSE
    width_obs_p10 DOUBLE,            -- 10th percentile observed width
    width_obs_p20 DOUBLE,            -- 20th percentile observed width
    width_obs_p30 DOUBLE,            -- 30th percentile observed width
    width_obs_p40 DOUBLE,            -- 40th percentile observed width
    width_obs_p50 DOUBLE,            -- 50th percentile (median) observed width
    width_obs_p60 DOUBLE,            -- 60th percentile observed width
    width_obs_p70 DOUBLE,            -- 70th percentile observed width
    width_obs_p80 DOUBLE,            -- 80th percentile observed width
    width_obs_p90 DOUBLE,            -- 90th percentile observed width
    width_obs_range DOUBLE,          -- range (max-min) of observed width
    width_obs_mad DOUBLE,            -- median absolute deviation of width
    slope_obs_p10 DOUBLE,            -- 10th percentile observed slope
    slope_obs_p20 DOUBLE,            -- 20th percentile observed slope
    slope_obs_p30 DOUBLE,            -- 30th percentile observed slope
    slope_obs_p40 DOUBLE,            -- 40th percentile observed slope
    slope_obs_p50 DOUBLE,            -- 50th percentile (median) observed slope
    slope_obs_p60 DOUBLE,            -- 60th percentile observed slope
    slope_obs_p70 DOUBLE,            -- 70th percentile observed slope
    slope_obs_p80 DOUBLE,            -- 80th percentile observed slope
    slope_obs_p90 DOUBLE,            -- 90th percentile observed slope
    slope_obs_range DOUBLE,          -- range (max-min) of observed slope
    slope_obs_mad DOUBLE,            -- median absolute deviation of slope
    slope_obs_adj DOUBLE,            -- GREATEST(p50, 0) clipped slope
    slope_obs_slopeF DOUBLE,         -- weighted sign fraction (-1 to +1)
    slope_obs_reliable BOOLEAN,      -- |slopeF| > 0.5 AND |p50| > ref_uncertainty
    slope_obs_quality VARCHAR,       -- categorical: reliable, below_ref_uncertainty,
                                     -- high_uncertainty, negative
    n_obs INTEGER,                   -- count of SWOT observations

    -- Classification & Flags
    lakeflag INTEGER,            -- 0=river, 1=lake, 2=canal, 3=tidal
    n_chan_max INTEGER,           -- nchan_max
    n_chan_mod INTEGER,           -- nchan_mod
    obstr_type INTEGER,          -- 0=none,1=dam,2=low_head_dam,3=lock,4=waterfall,5=partial_dam
    grod_id BIGINT,              -- GROD database ID (v17b source; sunset in v18)
    dl_grod_id BIGINT,           -- DL-GROD database ID (He et al. 2025)
    hfalls_id BIGINT,            -- HydroFALLS database ID
    swot_obs INTEGER,            -- max_obs: max SWOT passes in 21-day cycle
    iceflag INTEGER,             -- ice/seasonal flag
    low_slope_flag INTEGER,      -- low_slope: 1=too low for discharge estimation
    edit_flag VARCHAR,           -- comma-separated update codes
    add_flag INTEGER,            -- 0=not added, 1=added from MERIT Hydro

    -- Names
    river_name VARCHAR,          -- semicolon-separated if multiple
    river_name_en VARCHAR,       -- standardized English name (from OSM)
    river_name_local VARCHAR,    -- local/native name (from OSM)

    -- Metadata
    version VARCHAR(10) NOT NULL,

    PRIMARY KEY (reach_id, region)
);
"""

# NOTE: reach_id IS globally unique in SWORD (first digits encode region/basin),
# but we include region in PK for consistency and query efficiency.
# Topology tables also include region for proper joins.

REACH_TOPOLOGY_TABLE = """
CREATE TABLE IF NOT EXISTS reach_topology (
    -- Composite primary key (includes region for efficient filtering)
    reach_id BIGINT NOT NULL,
    region VARCHAR(2) NOT NULL,
    direction VARCHAR(4) NOT NULL,  -- 'up' or 'down'
    neighbor_rank TINYINT NOT NULL,  -- 0-3

    -- Neighbor reach ID
    neighbor_reach_id BIGINT NOT NULL,

    PRIMARY KEY (reach_id, region, direction, neighbor_rank)
);
"""

REACH_SWOT_ORBITS_TABLE = """
CREATE TABLE IF NOT EXISTS reach_swot_orbits (
    -- Composite primary key (includes region for efficient filtering)
    reach_id BIGINT NOT NULL,
    region VARCHAR(2) NOT NULL,
    orbit_rank TINYINT NOT NULL,  -- 0-74

    -- SWOT orbit pass_tile ID
    orbit_id BIGINT NOT NULL,

    PRIMARY KEY (reach_id, region, orbit_rank)
);
"""

REACH_ICE_FLAGS_TABLE = """
CREATE TABLE IF NOT EXISTS reach_ice_flags (
    -- Composite primary key
    reach_id BIGINT NOT NULL,
    julian_day SMALLINT NOT NULL,  -- 1-366

    -- Ice flag value
    iceflag INTEGER NOT NULL,

    PRIMARY KEY (reach_id, julian_day)
);
"""

SWORD_VERSIONS_TABLE = """
CREATE TABLE IF NOT EXISTS sword_versions (
    version VARCHAR(10) PRIMARY KEY,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    production_date TIMESTAMP,
    schema_version VARCHAR(10),
    notes VARCHAR
);
"""

# =============================================================================
# PROVENANCE TABLES
# =============================================================================
# These tables track all operations on SWORD data with full provenance:
# - WHO made the change (user_id, session_id)
# - WHAT changed (table, entity_ids, columns, old/new values)
# - WHEN it happened (timestamps)
# - WHY it was done (reason)
# =============================================================================

SWORD_OPERATIONS_TABLE = """
CREATE TABLE IF NOT EXISTS sword_operations (
    -- Primary key
    operation_id INTEGER PRIMARY KEY,

    -- Operation type
    operation_type VARCHAR(50) NOT NULL,  -- CREATE, UPDATE, DELETE, RECALCULATE, IMPORT, EXPORT, RECONSTRUCT

    -- Target info
    table_name VARCHAR(50),               -- reaches, nodes, centerlines
    entity_ids BIGINT[],                  -- Affected entity IDs (array)
    region VARCHAR(2),

    -- Provenance: WHO
    user_id VARCHAR(100),                 -- System username or QGIS user
    session_id VARCHAR(50),               -- Workflow session ID

    -- Provenance: WHEN
    started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,

    -- Provenance: WHAT
    operation_details JSON,               -- Operation-specific parameters
    affected_columns VARCHAR[],           -- Which columns were modified

    -- Provenance: WHY
    reason VARCHAR(500),                  -- User-provided reason
    source_operation_id INTEGER,          -- Parent operation (for cascades)

    -- State tracking
    status VARCHAR(20) DEFAULT 'PENDING', -- PENDING, IN_PROGRESS, COMPLETED, FAILED, ROLLED_BACK
    error_message VARCHAR,

    -- Checksums for verification (optional, for auditing)
    before_checksum VARCHAR(64),          -- SHA256 of affected data before
    after_checksum VARCHAR(64),           -- SHA256 of affected data after

    -- Sync tracking (for PostgreSQL -> DuckDB sync)
    synced_to_duckdb BOOLEAN DEFAULT FALSE  -- Whether this operation has been synced to DuckDB backup
);
"""

SWORD_VALUE_SNAPSHOTS_TABLE = """
CREATE TABLE IF NOT EXISTS sword_value_snapshots (
    -- Primary key
    snapshot_id INTEGER PRIMARY KEY,

    -- Link to operation
    operation_id INTEGER NOT NULL,

    -- What changed
    table_name VARCHAR(50) NOT NULL,
    entity_id BIGINT NOT NULL,
    column_name VARCHAR(50) NOT NULL,

    -- Old and new values (stored as JSON for type flexibility)
    old_value JSON,
    new_value JSON,
    data_type VARCHAR(20)                 -- For proper type restoration on rollback
);
"""

SWORD_SOURCE_LINEAGE_TABLE = """
CREATE TABLE IF NOT EXISTS sword_source_lineage (
    -- Primary key
    lineage_id INTEGER PRIMARY KEY,

    -- Entity identification
    entity_type VARCHAR(20) NOT NULL,     -- reach, node, centerline
    entity_id BIGINT NOT NULL,
    region VARCHAR(2) NOT NULL,

    -- Source attribution
    source_dataset VARCHAR(50) NOT NULL,  -- GRWL, MERIT_HYDRO, HYDROBASINS, GRanD, GROD, etc.
    source_id VARCHAR(100),               -- ID in source dataset (if applicable)
    source_version VARCHAR(20),           -- Version of source dataset

    -- Attribute mapping
    attribute_name VARCHAR(50),           -- Which attribute came from this source
    derivation_method VARCHAR(100),       -- direct, interpolated, aggregated, computed, etc.

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

SWORD_RECONSTRUCTION_RECIPES_TABLE = """
CREATE TABLE IF NOT EXISTS sword_reconstruction_recipes (
    -- Primary key
    recipe_id INTEGER PRIMARY KEY,

    -- Recipe identification
    name VARCHAR(100) NOT NULL UNIQUE,
    description VARCHAR(500),

    -- Recipe definition
    target_attributes VARCHAR[],          -- Which attributes this recipe produces
    required_sources VARCHAR[],           -- Source datasets needed
    script_path VARCHAR(500),             -- Path to reconstruction script
    script_hash VARCHAR(64),              -- Hash for reproducibility verification

    -- Parameters
    parameters JSON,                      -- Default parameters

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP
);
"""

# =============================================================================
# VERSIONING TABLES
# =============================================================================
# Named snapshots for git-like versioning functionality.
# Snapshots reference a point in operation history (operation_id_max) rather
# than duplicating data, enabling restore by rolling back subsequent operations.
# =============================================================================

SWORD_SNAPSHOTS_TABLE = """
CREATE TABLE IF NOT EXISTS sword_snapshots (
    -- Primary key
    snapshot_id INTEGER PRIMARY KEY,

    -- Snapshot identification
    name VARCHAR(100) NOT NULL UNIQUE,    -- User-friendly name like "before-bulk-edit"
    description VARCHAR(500),             -- Optional description

    -- Reference point in operation history
    operation_id_max INTEGER NOT NULL,    -- Highest operation_id included in this snapshot

    -- Provenance: WHO/WHEN
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100),              -- User who created the snapshot
    session_id VARCHAR(50),               -- Session that created the snapshot

    -- Statistics at snapshot time (for quick reference)
    reach_count INTEGER,
    node_count INTEGER,
    centerline_count INTEGER,

    -- Flags
    is_auto_snapshot BOOLEAN DEFAULT FALSE,  -- Auto-created vs user-created
    tags VARCHAR[]                           -- Optional tags for categorization
);
"""

# Index definitions
INDEXES = [
    # Spatial indexes (for DuckDB Spatial extension)
    "CREATE INDEX IF NOT EXISTS idx_centerlines_geom ON centerlines USING RTREE (geom);",
    "CREATE INDEX IF NOT EXISTS idx_nodes_geom ON nodes USING RTREE (geom);",
    "CREATE INDEX IF NOT EXISTS idx_reaches_geom ON reaches USING RTREE (geom);",
    # Foreign key lookups (most common query patterns)
    "CREATE INDEX IF NOT EXISTS idx_centerlines_reach ON centerlines(reach_id);",
    "CREATE INDEX IF NOT EXISTS idx_centerlines_node ON centerlines(node_id);",
    "CREATE INDEX IF NOT EXISTS idx_nodes_reach ON nodes(reach_id);",
    # Regional partitioning queries
    "CREATE INDEX IF NOT EXISTS idx_centerlines_region ON centerlines(region);",
    "CREATE INDEX IF NOT EXISTS idx_nodes_region ON nodes(region);",
    "CREATE INDEX IF NOT EXISTS idx_reaches_region ON reaches(region);",
    # Topology traversal
    "CREATE INDEX IF NOT EXISTS idx_topology_reach ON reach_topology(reach_id);",
    "CREATE INDEX IF NOT EXISTS idx_topology_neighbor ON reach_topology(neighbor_reach_id);",
    "CREATE INDEX IF NOT EXISTS idx_topology_direction ON reach_topology(reach_id, direction);",
    # Common analytical queries
    "CREATE INDEX IF NOT EXISTS idx_reaches_dist_out ON reaches(dist_out);",
    "CREATE INDEX IF NOT EXISTS idx_reaches_facc ON reaches(facc);",
    "CREATE INDEX IF NOT EXISTS idx_reaches_stream_order ON reaches(stream_order);",
    "CREATE INDEX IF NOT EXISTS idx_reaches_network ON reaches(network);",
    "CREATE INDEX IF NOT EXISTS idx_nodes_stream_order ON nodes(stream_order);",
    # SWOT orbits lookup
    "CREATE INDEX IF NOT EXISTS idx_swot_orbits_reach ON reach_swot_orbits(reach_id);",
    # Provenance indexes
    "CREATE INDEX IF NOT EXISTS idx_operations_type ON sword_operations(operation_type);",
    "CREATE INDEX IF NOT EXISTS idx_operations_table ON sword_operations(table_name);",
    "CREATE INDEX IF NOT EXISTS idx_operations_region ON sword_operations(region);",
    "CREATE INDEX IF NOT EXISTS idx_operations_session ON sword_operations(session_id);",
    "CREATE INDEX IF NOT EXISTS idx_operations_started ON sword_operations(started_at);",
    "CREATE INDEX IF NOT EXISTS idx_operations_status ON sword_operations(status);",
    "CREATE INDEX IF NOT EXISTS idx_snapshots_operation ON sword_value_snapshots(operation_id);",
    "CREATE INDEX IF NOT EXISTS idx_snapshots_entity ON sword_value_snapshots(table_name, entity_id);",
    "CREATE INDEX IF NOT EXISTS idx_lineage_entity ON sword_source_lineage(entity_type, entity_id, region);",
    "CREATE INDEX IF NOT EXISTS idx_lineage_source ON sword_source_lineage(source_dataset);",
    # Snapshot versioning indexes
    "CREATE INDEX IF NOT EXISTS idx_snapshots_name ON sword_snapshots(name);",
    "CREATE INDEX IF NOT EXISTS idx_snapshots_created ON sword_snapshots(created_at);",
    "CREATE INDEX IF NOT EXISTS idx_snapshots_operation ON sword_snapshots(operation_id_max);",
]

# Regional views for backward compatibility
REGIONAL_VIEWS = """
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
"""


def get_schema_sql() -> str:
    """
    Returns the complete schema SQL as a single string.

    Returns
    -------
    str
        Complete SQL for creating all tables, indexes, and views.
    """
    core_tables = [
        CENTERLINES_TABLE,
        CENTERLINE_NEIGHBORS_TABLE,
        NODES_TABLE,
        REACHES_TABLE,
        REACH_TOPOLOGY_TABLE,
        REACH_SWOT_ORBITS_TABLE,
        REACH_ICE_FLAGS_TABLE,
        SWORD_VERSIONS_TABLE,
    ]

    provenance_tables = [
        SWORD_OPERATIONS_TABLE,
        SWORD_VALUE_SNAPSHOTS_TABLE,
        SWORD_SOURCE_LINEAGE_TABLE,
        SWORD_RECONSTRUCTION_RECIPES_TABLE,
        SWORD_SNAPSHOTS_TABLE,
    ]

    schema_parts = []
    schema_parts.append("-- SWORD DuckDB Schema")
    schema_parts.append(f"-- Schema Version: {SCHEMA_VERSION}")
    schema_parts.append("")

    # Core Tables
    schema_parts.append("-- Core Tables")
    schema_parts.extend(core_tables)

    # Provenance Tables
    schema_parts.append("\n-- Provenance Tables")
    schema_parts.extend(provenance_tables)

    # Indexes
    schema_parts.append("\n-- Indexes")
    schema_parts.extend(INDEXES)

    # Views
    schema_parts.append("\n-- Regional Views")
    schema_parts.append(REGIONAL_VIEWS)

    return "\n".join(schema_parts)


def create_schema(conn) -> None:
    """
    Creates all SWORD tables, indexes, and views in the database.

    Parameters
    ----------
    conn : duckdb.DuckDBPyConnection
        Active DuckDB connection.

    Raises
    ------
    Exception
        If schema creation fails.
    """
    # Create core tables
    core_tables = [
        CENTERLINES_TABLE,
        CENTERLINE_NEIGHBORS_TABLE,
        NODES_TABLE,
        REACHES_TABLE,
        REACH_TOPOLOGY_TABLE,
        REACH_SWOT_ORBITS_TABLE,
        REACH_ICE_FLAGS_TABLE,
        SWORD_VERSIONS_TABLE,
    ]

    for table_sql in core_tables:
        conn.execute(table_sql)

    # Create provenance tables
    provenance_tables = [
        SWORD_OPERATIONS_TABLE,
        SWORD_VALUE_SNAPSHOTS_TABLE,
        SWORD_SOURCE_LINEAGE_TABLE,
        SWORD_RECONSTRUCTION_RECIPES_TABLE,
        SWORD_SNAPSHOTS_TABLE,
    ]

    for table_sql in provenance_tables:
        conn.execute(table_sql)

    # Create indexes (may fail if spatial extension not loaded)
    for index_sql in INDEXES:
        try:
            conn.execute(index_sql)
        except Exception as e:
            # Skip spatial indexes if spatial extension not available
            if "RTREE" in index_sql and "spatial" in str(e).lower():
                continue
            raise

    # Create regional views
    for view_sql in REGIONAL_VIEWS.strip().split(";"):
        view_sql = view_sql.strip()
        if view_sql:
            conn.execute(view_sql)

    # Record schema version
    conn.execute(
        """
        INSERT OR REPLACE INTO sword_versions (version, schema_version, notes)
        VALUES ('schema', ?, 'Initial schema creation')
    """,
        [SCHEMA_VERSION],
    )


def drop_schema(conn) -> None:
    """
    Drops all SWORD tables from the database.

    WARNING: This will delete all data!

    Parameters
    ----------
    conn : duckdb.DuckDBPyConnection
        Active DuckDB connection.
    """
    # Drop views first
    views = [
        "na_centerlines",
        "na_nodes",
        "na_reaches",
        "sa_centerlines",
        "sa_nodes",
        "sa_reaches",
        "eu_centerlines",
        "eu_nodes",
        "eu_reaches",
        "af_centerlines",
        "af_nodes",
        "af_reaches",
        "oc_centerlines",
        "oc_nodes",
        "oc_reaches",
        "as_centerlines",
        "as_nodes",
        "as_reaches",
    ]

    for view in views:
        conn.execute(f"DROP VIEW IF EXISTS {view};")

    # Drop provenance tables first (they have no dependencies)
    provenance_tables = [
        "sword_snapshots",
        "sword_value_snapshots",
        "sword_operations",
        "sword_source_lineage",
        "sword_reconstruction_recipes",
    ]

    for table in provenance_tables:
        conn.execute(f"DROP TABLE IF EXISTS {table};")

    # Drop core tables in reverse dependency order
    core_tables = [
        "reach_ice_flags",
        "reach_swot_orbits",
        "reach_topology",
        "centerline_neighbors",
        "nodes",
        "centerlines",
        "reaches",
        "sword_versions",
    ]

    for table in core_tables:
        conn.execute(f"DROP TABLE IF EXISTS {table};")


def create_provenance_tables(db) -> None:
    """
    Creates only the provenance tables in an existing database.

    Use this to upgrade an existing SWORD database to include provenance tracking.

    Parameters
    ----------
    db : SWORDDatabase or duckdb.DuckDBPyConnection
        Database object with execute() method, or raw DuckDB connection.
    """
    provenance_tables = [
        SWORD_OPERATIONS_TABLE,
        SWORD_VALUE_SNAPSHOTS_TABLE,
        SWORD_SOURCE_LINEAGE_TABLE,
        SWORD_RECONSTRUCTION_RECIPES_TABLE,
        SWORD_SNAPSHOTS_TABLE,
    ]

    for table_sql in provenance_tables:
        db.execute(table_sql)

    # Create provenance indexes
    provenance_indexes = [
        "CREATE INDEX IF NOT EXISTS idx_operations_type ON sword_operations(operation_type);",
        "CREATE INDEX IF NOT EXISTS idx_operations_table ON sword_operations(table_name);",
        "CREATE INDEX IF NOT EXISTS idx_operations_region ON sword_operations(region);",
        "CREATE INDEX IF NOT EXISTS idx_operations_session ON sword_operations(session_id);",
        "CREATE INDEX IF NOT EXISTS idx_operations_started ON sword_operations(started_at);",
        "CREATE INDEX IF NOT EXISTS idx_operations_status ON sword_operations(status);",
        "CREATE INDEX IF NOT EXISTS idx_snapshots_operation ON sword_value_snapshots(operation_id);",
        "CREATE INDEX IF NOT EXISTS idx_snapshots_entity ON sword_value_snapshots(table_name, entity_id);",
        "CREATE INDEX IF NOT EXISTS idx_lineage_entity ON sword_source_lineage(entity_type, entity_id, region);",
        "CREATE INDEX IF NOT EXISTS idx_lineage_source ON sword_source_lineage(source_dataset);",
        # Snapshot versioning indexes
        "CREATE INDEX IF NOT EXISTS idx_snapshots_name ON sword_snapshots(name);",
        "CREATE INDEX IF NOT EXISTS idx_snapshots_created ON sword_snapshots(created_at);",
        "CREATE INDEX IF NOT EXISTS idx_snapshots_operation ON sword_snapshots(operation_id_max);",
    ]

    for index_sql in provenance_indexes:
        db.execute(index_sql)


def add_v17c_columns(db) -> bool:
    """
    Add v17c columns to existing nodes and reaches tables.

    This migration helper adds the new columns required for v17c topology
    without recreating the tables. Safe to call multiple times - checks
    if columns already exist.

    Parameters
    ----------
    db : SWORDDatabase or duckdb.DuckDBPyConnection
        Database object with execute() method, or raw DuckDB connection.

    Returns
    -------
    bool
        True if any columns were added, False if all already existed.
    """
    added = False

    # v17c columns for nodes table
    nodes_v17c_columns = [
        ("best_headwater", "BIGINT"),
        ("best_outlet", "BIGINT"),
        ("pathlen_hw", "DOUBLE"),
        ("pathlen_out", "DOUBLE"),
        ("subnetwork_id", "INTEGER"),  # weakly connected component ID
        ("facc_quality", "VARCHAR"),  # facc correction status flag
    ]

    # v17c columns for reaches table
    reaches_v17c_columns = [
        ("best_headwater", "BIGINT"),
        ("best_outlet", "BIGINT"),
        ("pathlen_hw", "DOUBLE"),
        ("pathlen_out", "DOUBLE"),
        ("main_path_id", "BIGINT"),
        ("is_mainstem_edge", "BOOLEAN DEFAULT FALSE"),
        ("hydro_dist_out", "DOUBLE"),
        ("hydro_dist_hw", "DOUBLE"),
        ("rch_id_up_main", "BIGINT"),
        ("rch_id_dn_main", "BIGINT"),
        # NOTE: swot_slope columns removed - pipeline incomplete (Issue #117)
        ("subnetwork_id", "INTEGER"),  # weakly connected component ID
        ("facc_quality", "VARCHAR"),  # facc correction status flag
    ]

    def _add_columns_to_table(table_name: str, columns: list) -> bool:
        """Add columns to a table if they don't exist."""
        nonlocal added
        # Get existing columns
        result = db.execute(
            f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'"
        ).fetchall()
        existing = {row[0].lower() for row in result}

        for col_name, col_type in columns:
            if col_name.lower() not in existing:
                db.execute(f"ALTER TABLE {table_name} ADD COLUMN {col_name} {col_type}")
                added = True

        return added

    # Add columns to nodes table
    try:
        _add_columns_to_table("nodes", nodes_v17c_columns)
    except Exception:
        # Table may not exist yet
        pass

    # Add columns to reaches table
    try:
        _add_columns_to_table("reaches", reaches_v17c_columns)
    except Exception:
        # Table may not exist yet
        pass

    # Propagate facc_quality from reaches to nodes (nodes inherit reach-level flag)
    db.execute("""
        UPDATE nodes
        SET facc_quality = r.facc_quality
        FROM reaches r
        WHERE nodes.reach_id = r.reach_id
          AND nodes.region = r.region
          AND r.facc_quality IS NOT NULL
    """)

    return added


def add_swot_obs_columns(conn) -> bool:
    """
    Add SWOT observation statistics columns to existing nodes and reaches tables.

    This migration helper adds columns for aggregated SWOT L2 RiverSP statistics
    (p10-p90 percentiles, range, MAD) for WSE, width, and slope observations.
    Safe to call multiple times - checks if columns already exist.

    Also drops legacy mean/median/std columns if they exist.

    Parameters
    ----------
    conn : duckdb.DuckDBPyConnection
        Active DuckDB connection.

    Returns
    -------
    bool
        True if any columns were added or dropped, False if schema unchanged.
    """
    changed = False

    _PERCENTILES = ["p10", "p20", "p30", "p40", "p50", "p60", "p70", "p80", "p90"]

    def _var_cols(var: str) -> list[tuple[str, str]]:
        cols = [(f"{var}_obs_{p}", "DOUBLE") for p in _PERCENTILES]
        cols.append((f"{var}_obs_range", "DOUBLE"))
        cols.append((f"{var}_obs_mad", "DOUBLE"))
        return cols

    # Nodes: WSE(11) + width(11) + n_obs(1) = 23
    nodes_swot_columns = _var_cols("wse") + _var_cols("width") + [("n_obs", "INTEGER")]

    # Reaches: WSE(11) + width(11) + slope(11) + derived(4) + n_obs(1) = 38
    reaches_swot_columns = (
        _var_cols("wse")
        + _var_cols("width")
        + _var_cols("slope")
        + [
            ("slope_obs_adj", "DOUBLE"),
            ("slope_obs_slopeF", "DOUBLE"),
            ("slope_obs_reliable", "BOOLEAN"),
            ("slope_obs_quality", "VARCHAR"),
            ("n_obs", "INTEGER"),
        ]
    )

    # Legacy columns to drop
    _LEGACY_SUFFIXES = ["_obs_mean", "_obs_median", "_obs_std"]
    _LEGACY_NODE_COLS = [f"{v}{s}" for v in ("wse", "width") for s in _LEGACY_SUFFIXES]
    _LEGACY_REACH_COLS = [
        f"{v}{s}" for v in ("wse", "width", "slope") for s in _LEGACY_SUFFIXES
    ]

    def _get_existing(table_name: str) -> set[str]:
        result = conn.execute(
            f"SELECT column_name FROM information_schema.columns "
            f"WHERE table_name = '{table_name}'"
        ).fetchall()
        return {row[0].lower() for row in result}

    def _table_exists(table_name: str) -> bool:
        result = conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            f"WHERE table_name = '{table_name}'"
        ).fetchone()
        return result[0] > 0

    def _add_columns_to_table(table_name: str, columns: list) -> None:
        nonlocal changed
        existing = _get_existing(table_name)
        for col_name, col_type in columns:
            if col_name.lower() not in existing:
                conn.execute(
                    f"ALTER TABLE {table_name} ADD COLUMN {col_name} {col_type}"
                )
                changed = True

    def _drop_legacy_columns(table_name: str, legacy_cols: list[str]) -> None:
        nonlocal changed
        existing = _get_existing(table_name)
        cols_to_drop = [c for c in legacy_cols if c.lower() in existing]
        if not cols_to_drop:
            return
        # Drop ALL dependent views and indexes before ALTER TABLE
        # Match views that reference this table via FROM/JOIN (not just substring)
        import re

        table_pat = re.compile(
            rf"\b(?:FROM|JOIN)\s+{re.escape(table_name)}\b", re.IGNORECASE
        )
        views = conn.execute(
            "SELECT view_name, sql FROM duckdb_views() WHERE NOT internal"
        ).fetchall()
        dropped_views = []
        for vname, vsql in views:
            if table_pat.search(vsql):
                conn.execute(f"DROP VIEW IF EXISTS {vname}")
                dropped_views.append((vname, vsql))
        indexes = conn.execute(
            f"SELECT index_name, sql FROM duckdb_indexes() "
            f"WHERE table_name = '{table_name}'"
        ).fetchall()
        dropped_indexes = []
        for iname, isql in indexes:
            if isql:
                conn.execute(f"DROP INDEX IF EXISTS {iname}")
                dropped_indexes.append((iname, isql))
        for col in cols_to_drop:
            conn.execute(f"ALTER TABLE {table_name} DROP COLUMN {col}")
            changed = True
        # Recreate indexes and views
        for iname, isql in dropped_indexes:
            conn.execute(isql)
        for vname, vsql in dropped_views:
            conn.execute(vsql)

    if _table_exists("nodes"):
        _add_columns_to_table("nodes", nodes_swot_columns)
        _drop_legacy_columns("nodes", _LEGACY_NODE_COLS)

    if _table_exists("reaches"):
        _add_columns_to_table("reaches", reaches_swot_columns)
        _drop_legacy_columns("reaches", _LEGACY_REACH_COLS)

    return changed


def add_osm_name_columns(conn) -> bool:
    """
    Add OSM river name columns to reaches table.

    Safe to call multiple times - checks if columns already exist.

    Parameters
    ----------
    conn : duckdb.DuckDBPyConnection
        Active DuckDB connection.

    Returns
    -------
    bool
        True if any columns were added, False if all already existed.
    """
    added = False

    osm_columns = [
        ("river_name_local", "VARCHAR"),
        ("river_name_en", "VARCHAR"),
    ]

    try:
        result = conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'reaches'"
        ).fetchall()
        existing = {row[0].lower() for row in result}

        for col_name, col_type in osm_columns:
            if col_name.lower() not in existing:
                conn.execute(f"ALTER TABLE reaches ADD COLUMN {col_name} {col_type}")
                added = True
    except Exception:
        pass

    return added


def add_node_boundary_columns(conn) -> bool:
    """
    Add node boundary and ordering columns to existing tables.

    Adds dn_node_id/up_node_id to reaches and node_order to nodes.
    Safe to call multiple times - checks if columns already exist.

    Parameters
    ----------
    conn : duckdb.DuckDBPyConnection
        Active DuckDB connection.

    Returns
    -------
    bool
        True if any columns were added, False if all already existed.
    """
    added = False

    reaches_columns = [
        ("dn_node_id", "BIGINT"),
        ("up_node_id", "BIGINT"),
    ]
    nodes_columns = [
        ("node_order", "INTEGER"),
    ]

    for table, columns in [("reaches", reaches_columns), ("nodes", nodes_columns)]:
        try:
            result = conn.execute(
                f"SELECT column_name FROM information_schema.columns "
                f"WHERE table_name = '{table}'"
            ).fetchall()
            existing = {row[0].lower() for row in result}
            for col_name, col_type in columns:
                if col_name.lower() not in existing:
                    conn.execute(
                        f"ALTER TABLE {table} ADD COLUMN {col_name} {col_type}"
                    )
                    added = True
        except Exception:
            pass

    return added


def create_v17c_tables(conn) -> None:
    """
    Create v17c-specific tables for section data and slope validation.

    These tables store:
    - v17c_sections: Junction-to-junction sections with reach lists
    - v17c_section_slope_validation: SWOT slope validation per section

    Safe to call multiple times - uses CREATE TABLE IF NOT EXISTS.

    Parameters
    ----------
    conn : duckdb.DuckDBPyConnection
        Active DuckDB connection.
    """
    # Section table - stores junction-to-junction sections
    conn.execute("""
        CREATE TABLE IF NOT EXISTS v17c_sections (
            section_id INTEGER NOT NULL,
            region VARCHAR(2) NOT NULL,
            upstream_junction BIGINT,
            downstream_junction BIGINT,
            reach_ids VARCHAR,
            distance DOUBLE,
            n_reaches INTEGER,
            PRIMARY KEY (section_id, region)
        )
    """)

    # Section slope validation table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS v17c_section_slope_validation (
            section_id INTEGER NOT NULL,
            region VARCHAR(2) NOT NULL,
            slope_from_upstream DOUBLE,
            slope_from_downstream DOUBLE,
            direction_valid BOOLEAN,
            likely_cause VARCHAR,
            PRIMARY KEY (section_id, region)
        )
    """)

    # Create indexes for efficient queries
    try:
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_v17c_sections_region
            ON v17c_sections(region)
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_v17c_sections_junctions
            ON v17c_sections(upstream_junction, downstream_junction)
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_v17c_validation_region
            ON v17c_section_slope_validation(region)
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_v17c_validation_valid
            ON v17c_section_slope_validation(direction_valid)
        """)
    except Exception:
        # Indexes may already exist
        pass


def add_sync_tracking_column(conn) -> bool:
    """
    Add synced_to_duckdb column to sword_operations table.

    This column tracks which operations have been synced from PostgreSQL
    to the DuckDB backup/cache. Safe to call multiple times.

    Parameters
    ----------
    conn : duckdb.DuckDBPyConnection or psycopg2.connection
        Active database connection.

    Returns
    -------
    bool
        True if column was added, False if it already existed.
    """
    try:
        # Check if column exists (works for both DuckDB and PostgreSQL)
        result = conn.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'sword_operations'
              AND column_name = 'synced_to_duckdb'
        """).fetchone()

        if result is None:
            conn.execute("""
                ALTER TABLE sword_operations
                ADD COLUMN synced_to_duckdb BOOLEAN DEFAULT FALSE
            """)
            return True
        return False

    except Exception:
        # Table may not exist yet
        return False
