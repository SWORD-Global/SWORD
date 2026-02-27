#!/usr/bin/env python3
"""
A030 Violations Analysis Script
Investigate and categorize 4,816 reaches where WSE increases downstream.
"""

import duckdb

DB_PATH = "data/duckdb/sword_v17c.duckdb"


def main():
    con = duckdb.connect(DB_PATH)

    # Get total count of A030 violations
    query = """
    SELECT COUNT(*) as violation_count
    FROM reaches r1
    JOIN reach_topology rt ON r1.reach_id = rt.reach_id AND r1.region = rt.region
    JOIN reaches r2 ON rt.neighbor_reach_id = r2.reach_id AND rt.region = r2.region
    WHERE rt.direction = 'down'
      AND r1.wse IS NOT NULL AND r1.wse != -9999
      AND r2.wse IS NOT NULL AND r2.wse != -9999
      AND r2.wse > r1.wse
    """
    total_violations = con.execute(query).fetchone()[0]
    print(f"Total A030 violations: {total_violations}")

    # Categorize by magnitude
    print("\n" + "=" * 70)
    print("MAGNITUDE CATEGORIZATION")
    print("=" * 70)
    mag_query = """
    SELECT 
        CASE 
            WHEN (r2.wse - r1.wse) < 0.5 THEN 'small_jitter (<0.5m)'
            WHEN (r2.wse - r1.wse) < 1.0 THEN 'moderate (0.5-1.0m)'
            WHEN (r2.wse - r1.wse) < 5.0 THEN 'moderate_large (1-5m)'
            ELSE 'major_inversion (>5m)'
        END as magnitude,
        COUNT(*) as count
    FROM reaches r1
    JOIN reach_topology rt ON r1.reach_id = rt.reach_id AND r1.region = rt.region
    JOIN reaches r2 ON rt.neighbor_reach_id = r2.reach_id AND rt.region = r2.region
    WHERE rt.direction = 'down'
      AND r1.wse IS NOT NULL AND r1.wse != -9999
      AND r2.wse IS NOT NULL AND r2.wse != -9999
      AND r2.wse > r1.wse
    GROUP BY magnitude
    ORDER BY count DESC
    """
    magnitude_dist = con.execute(mag_query).fetchdf()
    print(magnitude_dist.to_string(index=false))


    # Regional distribution
    print("\n" + "=" * 70)
    print("REGIONAL DISTRIBUTION")
    print("=" * 70)
    region_query = """
    SELECT 
        r1.region,
        COUNT(*) as violations,
        ROUND(100.0 * COUNT(*) / 4816, 2) as pct
    FROM reaches r1
    JOIN reach_topology rt ON r1.reach_id = rt.reach_id AND r1.region = rt.region
    JOIN reaches r2 ON rt.neighbor_reach_id = r2.reach_id AND rt.region = r2.region
    WHERE rt.direction = 'down'
      AND r1.wse IS NOT NULL AND r1.wse != -9999
      AND r2.wse IS NOT NULL AND r2.wse != -9999
      AND r2.wse > r1.wse
    GROUP BY r1.region
    ORDER BY violations DESC
    """
    region_dist = con.execute(region_query).fetchdf()
    print(region_dist.to_string(index=false))


    # Check for dams/obstructions in violations
    print("\n" + "=" * 70)
    print("DAMS/OBSTRUCTIONS IN VIOLATIONS")
    print("=" * 70)
    dam_query = """
    SELECT 
        CASE 
            WHEN r1.obstr_type IS NOT NULL AND r1.obstr_type != 0 THEN 'has_obstruction'
            WHEN r2.obstr_type IS NOT NULL AND r2.obstr_type != 0 THEN 'has_obstruction_dn'
            ELSE 'no_obstruction'
        END as obstruction_flag,
        COUNT(*) as count
    FROM reaches r1
    JOIN reach_topology rt ON r1.reach_id = rt.reach_id AND r1.region = rt.region
    JOIN reaches r2 ON rt.neighbor_reach_id = r2.reach_id AND rt.region = r2.region
    WHERE rt.direction = 'down'
      AND r1.wse IS NOT NULL AND r1.wse != -9999
      AND r2.wse IS NOT NULL AND r2.wse != -9999
      AND r2.wse > r1.wse
    GROUP BY obstruction_flag
    ORDER BY count DESC
    """
    dam_dist = con.execute(dam_query).fetchdf()
    print(dam_dist.to_string(index=false))


    # Check lakeflag distribution in violations
    print("\n" + "=" * 70)
    print("LAKEFLAG DISTRIBUTION IN VIOLATIONS")
    print("=" * 70)
    lakeflag_query = """
    SELECT 
        r1.lakeflag,
        r1.type,
        r1.lakeflag || '_type_' || r1.type as combo,
        COUNT(*) as violations,
        ROUND(100.0 * COUNT(*) / 4816, 2) as pct
    FROM reaches r1
    JOIN reach_topology rt ON r1.reach_id = rt.reach_id AND r1.region = rt.region
    JOIN reaches r2 ON rt.neighbor_reach_id = r2.reach_id AND rt.region = r2.region
    WHERE rt.direction = 'down'
      AND r1.wse IS NOT NULL AND r1.wse != -9999
      AND r2.wse IS NOT NULL AND r2.wse != -9999
      AND r2.wse > r1.wse
    GROUP BY r1.lakeflag, r1.type, r1.lakeflag || '_type_' || r1.type
    ORDER BY violations DESC
    """
    lakeflag_dist = con.execute(lakeflag_query).fetchdf()
    print(lakeflag_dist.to_string(index=false))


    # Sample specific violations for inspection - top magnitude
    print("\n" + "=" * 70)
    print("SAMPLE VIOLATIONS (TOP 20 BY MAGNITUDE)")
    print("=" * 70)
    sample_query = """
    SELECT 
        r1.reach_id, r1.region, r1.river_name,
        r1.wse as wse_up,
        r2.wse as wse_down,
        ROUND((r2.wse - r1.wse), 3) as wse_increase,
        r1.lakeflag, r1.type, r1.obstr_type, r1.main_side,
        r1.stream_order, r1.end_reach, r1.hydro_dist_out
    FROM reaches r1
    JOIN reach_topology rt ON r1.reach_id = rt.reach_id AND r1.region = rt.region
    JOIN reaches r2 ON rt.neighbor_reach_id = r2.reach_id AND rt.region = r2.region
    WHERE rt.direction = 'down'
      AND r1.wse IS NOT NULL AND r1.wse != -9999
      AND r2.wse IS NOT NULL AND r2.wse != -9999
      AND r2.wse > r1.wse
    ORDER BY (r2.wse - r1.wse) DESC
    LIMIT 20
    """
    sample = con.execute(sample_query).fetchdf()
    print(sample.to_string())

    # Sample - small jitter cases
    print("\n" + "=" * 70)
    print("SAMPLE SMALL JITTER CASES (< 0.5m, TOP 15)")
    print("=" * 70)
    jitter_query = """
    SELECT 
        r1.reach_id, r1.region, r1.river_name,
        r1.wse as wse_up,
        r2.wse as wse_down,
        ROUND((r2.wse - r1.wse), 5) as wse_increase,
        r1.lakeflag, r1.type, r1.obstr_type,
        r1.stream_order, r1.dist_out
    FROM reaches r1
    JOIN reach_topology rt ON r1.reach_id = rt.reach_id AND r1.region = rt.region
    JOIN reaches r2 ON rt.neighbor_reach_id = r2.reach_id AND rt.region = r2.region
    WHERE rt.direction = 'down'
      AND r1.wse IS NOT NULL AND r1.wse != -9999
      AND r2.wse IS NOT NULL AND r2.wse != -9999
      AND r2.wse > r1.wse
      AND (r2.wse - r1.wse) < 0.5
    ORDER BY (r2.wse - r1.wse) ASC
    LIMIT 15
    """
    jitter = con.execute(jitter_query).fetchdf()
    print(jitter.to_string())

    # Statistical summary of WSE inversions
    print("\n" + "=" * 70)
    print("STATISTICAL SUMMARY OF WSE INVERSIONS")
    print("=" * 70)
    stats_query = """
    SELECT 
        COUNT(*) as total_violations,
        ROUND(AVG(r2.wse - r1.wse), 3) as avg_increase,
        ROUND(MIN(r2.wse - r1.wse), 5) as min_increase,
        ROUND(MAX(r2.wse - r1.wse), 3) as max_increase,
        ROUND(STDDEV(r2.wse - r1.wse), 3) as stddev_increase,
        ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY r2.wse - r1.wse), 3) as median_increase
    FROM reaches r1
    JOIN reach_topology rt ON r1.reach_id = rt.reach_id AND r1.region = rt.region
    JOIN reaches r2 ON rt.neighbor_reach_id = r2.reach_id AND rt.region = r2.region
    WHERE rt.direction = 'down'
      AND r1.wse IS NOT NULL AND r1.wse != -9999
      AND r2.wse IS NOT NULL AND r2.wse != -9999
      AND r2.wse > r1.wse
    """
    stats = con.execute(stats_query).fetchdf()
    print(stats.to_string())

    # Check if inversions cluster in specific stream orders
    print("\n" + "=" * 70)
    print("INVOLUTIONS BY STREAM ORDER")
    print("=" * 70)
    so_query = """
    SELECT 
        r1.stream_order,
        COUNT(*) as violations,
        ROUND(AVG(r2.wse - r1.wse), 3) as avg_increase,
        ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as pct
    FROM reaches r1
    JOIN reach_topology rt ON r1.reach_id = rt.reach_id AND r1.region = rt.region
    JOIN reaches r2 ON rt.neighbor_reach_id = r2.reach_id AND rt.region = r2.region
    WHERE rt.direction = 'down'
      AND r1.wse IS NOT NULL AND r1.wse != -9999
      AND r2.wse IS NOT NULL AND r2.wse != -9999
      AND r2.wse > r1.wse
      AND r1.stream_order IS NOT NULL
    GROUP BY r1.stream_order
    ORDER BY r1.stream_order
    """
    so_dist = con.execute(so_query).fetchdf()
    print(so_dist.to_string(index=false))


    print("\n" + "=" * 70)
    print("ANALYSIS COMPLETE")
    print("=" * 70)

    con.close()


if __name__ == "__main__":
    main()
