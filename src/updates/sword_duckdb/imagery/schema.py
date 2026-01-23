# -*- coding: utf-8 -*-
"""
Imagery DuckDB Schema
=====================

Table definitions for satellite imagery metadata tracking.
"""

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import duckdb

logger = logging.getLogger(__name__)

# Sequence for acquisition IDs
IMAGERY_SEQUENCE = """
CREATE SEQUENCE IF NOT EXISTS imagery_acquisition_seq START 1;
"""

# Imagery acquisitions table - tracks STAC items fetched
IMAGERY_ACQUISITIONS_TABLE = """
CREATE TABLE IF NOT EXISTS imagery_acquisitions (
    acquisition_id INTEGER PRIMARY KEY DEFAULT nextval('imagery_acquisition_seq'),

    -- STAC item info
    stac_item_id VARCHAR NOT NULL,
    collection VARCHAR DEFAULT 'sentinel-2-l2a',

    -- Spatial/temporal
    bbox_wkt VARCHAR,
    acquired_at TIMESTAMP,

    -- Quality metrics
    cloud_cover DOUBLE,
    sun_azimuth DOUBLE,
    sun_elevation DOUBLE,

    -- Processing status
    ndwi_computed BOOLEAN DEFAULT FALSE,
    water_mask_computed BOOLEAN DEFAULT FALSE,

    -- Cache info
    cached_at TIMESTAMP,
    cache_path VARCHAR,

    -- Link to SWORD
    region VARCHAR(2),
    reach_ids BIGINT[],

    -- Metadata
    platform VARCHAR,
    processing_level VARCHAR,
    item_metadata JSON,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(stac_item_id, bbox_wkt)
);
"""

# Per-reach imagery statistics
REACH_IMAGERY_TABLE = """
CREATE TABLE IF NOT EXISTS reach_imagery (
    reach_id BIGINT NOT NULL,
    region VARCHAR(2) NOT NULL,
    acquisition_id INTEGER NOT NULL,

    -- NDWI statistics
    ndwi_mean DOUBLE,
    ndwi_std DOUBLE,
    ndwi_min DOUBLE,
    ndwi_max DOUBLE,

    -- Water classification
    water_fraction DOUBLE,
    water_pixel_count INTEGER,
    total_valid_pixels INTEGER,
    threshold_used DOUBLE,

    -- Processing info
    computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (reach_id, region, acquisition_id),
    FOREIGN KEY (acquisition_id) REFERENCES imagery_acquisitions(acquisition_id)
);
"""

# Indexes for imagery tables
IMAGERY_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_imagery_stac_item ON imagery_acquisitions(stac_item_id);
CREATE INDEX IF NOT EXISTS idx_imagery_region ON imagery_acquisitions(region);
CREATE INDEX IF NOT EXISTS idx_imagery_acquired ON imagery_acquisitions(acquired_at);
CREATE INDEX IF NOT EXISTS idx_reach_imagery_reach ON reach_imagery(reach_id, region);
CREATE INDEX IF NOT EXISTS idx_reach_imagery_acquisition ON reach_imagery(acquisition_id);
"""


def create_imagery_tables(conn: "duckdb.DuckDBPyConnection") -> None:
    """
    Create imagery tracking tables.

    Args:
        conn: DuckDB connection
    """
    logger.info("Creating imagery tables...")

    # Create sequence first (needed for auto-increment)
    conn.execute(IMAGERY_SEQUENCE)
    conn.execute(IMAGERY_ACQUISITIONS_TABLE)
    conn.execute(REACH_IMAGERY_TABLE)

    # Create indexes
    for stmt in IMAGERY_INDEXES.strip().split(";"):
        stmt = stmt.strip()
        if stmt:
            conn.execute(stmt)

    logger.info("Imagery tables created successfully")


def get_imagery_schema_sql() -> str:
    """Get full imagery schema SQL."""
    return "\n".join(
        [
            IMAGERY_ACQUISITIONS_TABLE,
            REACH_IMAGERY_TABLE,
            IMAGERY_INDEXES,
        ]
    )


def insert_acquisition(
    conn: "duckdb.DuckDBPyConnection",
    stac_item_id: str,
    bbox_wkt: str = None,
    acquired_at: str = None,
    cloud_cover: float = None,
    region: str = None,
    reach_ids: list = None,
    metadata: dict = None,
) -> int:
    """
    Insert a new imagery acquisition record.

    Args:
        conn: DuckDB connection
        stac_item_id: STAC item ID
        bbox_wkt: Bounding box as WKT
        acquired_at: Acquisition timestamp
        cloud_cover: Cloud cover percentage
        region: SWORD region code
        reach_ids: List of reach IDs covered
        metadata: Additional item metadata

    Returns:
        New acquisition ID
    """
    import json

    result = conn.execute(
        """
        INSERT INTO imagery_acquisitions
        (stac_item_id, bbox_wkt, acquired_at, cloud_cover, region, reach_ids, item_metadata)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        RETURNING acquisition_id
    """,
        [
            stac_item_id,
            bbox_wkt,
            acquired_at,
            cloud_cover,
            region,
            reach_ids,
            json.dumps(metadata) if metadata else None,
        ],
    ).fetchone()

    return result[0]


def insert_reach_imagery(
    conn: "duckdb.DuckDBPyConnection",
    reach_id: int,
    region: str,
    acquisition_id: int,
    ndwi_mean: float = None,
    ndwi_std: float = None,
    water_fraction: float = None,
    water_pixel_count: int = None,
    total_valid_pixels: int = None,
    threshold_used: float = None,
) -> None:
    """
    Insert reach-level imagery statistics.

    Args:
        conn: DuckDB connection
        reach_id: SWORD reach ID
        region: Region code
        acquisition_id: Acquisition ID (foreign key)
        ndwi_mean: Mean NDWI value
        ndwi_std: NDWI standard deviation
        water_fraction: Fraction of pixels classified as water
        water_pixel_count: Number of water pixels
        total_valid_pixels: Total valid pixels
        threshold_used: NDWI threshold used for classification
    """
    conn.execute(
        """
        INSERT OR REPLACE INTO reach_imagery
        (reach_id, region, acquisition_id, ndwi_mean, ndwi_std,
         water_fraction, water_pixel_count, total_valid_pixels, threshold_used)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """,
        [
            reach_id,
            region,
            acquisition_id,
            ndwi_mean,
            ndwi_std,
            water_fraction,
            water_pixel_count,
            total_valid_pixels,
            threshold_used,
        ],
    )


def get_reach_imagery_history(
    conn: "duckdb.DuckDBPyConnection",
    reach_id: int,
    region: str,
    limit: int = 10,
) -> list:
    """
    Get imagery history for a reach.

    Args:
        conn: DuckDB connection
        reach_id: SWORD reach ID
        region: Region code
        limit: Maximum number of records

    Returns:
        List of imagery records
    """
    result = conn.execute(
        """
        SELECT
            ri.*,
            ia.stac_item_id,
            ia.acquired_at,
            ia.cloud_cover
        FROM reach_imagery ri
        JOIN imagery_acquisitions ia ON ri.acquisition_id = ia.acquisition_id
        WHERE ri.reach_id = ? AND ri.region = ?
        ORDER BY ia.acquired_at DESC
        LIMIT ?
    """,
        [reach_id, region, limit],
    ).fetchall()

    columns = [
        "reach_id",
        "region",
        "acquisition_id",
        "ndwi_mean",
        "ndwi_std",
        "ndwi_min",
        "ndwi_max",
        "water_fraction",
        "water_pixel_count",
        "total_valid_pixels",
        "threshold_used",
        "computed_at",
        "stac_item_id",
        "acquired_at",
        "cloud_cover",
    ]

    return [dict(zip(columns, row)) for row in result]
