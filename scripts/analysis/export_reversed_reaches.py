#!/usr/bin/env python3
"""
Export suspected reversed reaches to GeoJSON for visual inspection.
Focuses on the multi-reach "uphill" chains identified in A030 analysis.
"""

import duckdb
import geopandas as gpd
from shapely import wkb

DB_PATH = "/Users/jakegearon/projects/SWORD/data/duckdb/sword_v17c.duckdb"


def export_reversed_geojson(db_path: str, output_path: str):
    print(f"Connecting to {db_path}...")
    conn = duckdb.connect(db_path)
    conn.execute("INSTALL spatial; LOAD spatial;")

    # 1. Find the inversions again
    query = """
    SELECT
        rt.reach_id as up_id,
        rt.neighbor_reach_id as dn_id,
        r1.region,
        r1.wse as wse_up,
        r2.wse as wse_dn,
        (r2.wse - r1.wse) as wse_delta
    FROM reach_topology rt
    JOIN reaches r1 ON rt.reach_id = r1.reach_id AND rt.region = r1.region
    JOIN reaches r2 ON rt.neighbor_reach_id = r2.reach_id AND rt.region = r2.region
    WHERE rt.direction = 'down'
      AND r1.wse IS NOT NULL AND r1.wse != -9999
      AND r2.wse IS NOT NULL AND r2.wse != -9999
      AND (r2.wse - r1.wse) > 0.1
    """

    print("Finding inversions...")
    inv = conn.execute(query).fetchdf()

    # 2. Identify chains (Topological path tracing)
    print(f"Found {len(inv)} inversion edges. Tracing chains...")
    visited = set()
    all_reversed_ids = set()

    for _, row in inv.iterrows():
        if row["up_id"] in visited:
            continue

        curr_chain = [row["up_id"], row["dn_id"]]
        visited.add(row["up_id"])

        # Trace downstream
        next_dn = row["dn_id"]
        while True:
            step = inv[inv["up_id"] == next_dn]
            if step.empty:
                break
            next_dn = step.iloc[0]["dn_id"]
            curr_chain.append(next_dn)
            visited.add(step.iloc[0]["up_id"])

        if len(curr_chain) >= 2:
            all_reversed_ids.update(curr_chain)

    print(f"Total reaches in reversed chains: {len(all_reversed_ids)}")

    # 3. Fetch geometries for these reaches
    if not all_reversed_ids:
        print("No reversed reaches found.")
        return

    ids_str = ",".join(str(rid) for rid in all_reversed_ids)

    geom_query = f"""
    SELECT reach_id, region, river_name, wse, facc, width,
           ST_AsWKB(geom) as geometry_wkb
    FROM reaches
    WHERE reach_id IN ({ids_str})
    """

    print("Fetching geometries...")
    df_geoms = conn.execute(geom_query).fetchdf()

    # Convert WKB to shapely
    df_geoms["geometry"] = df_geoms["geometry_wkb"].apply(lambda x: wkb.loads(bytes(x)))
    gdf = gpd.GeoDataFrame(df_geoms.drop(columns=["geometry_wkb"]), crs="EPSG:4326")

    # Add the wse_delta info back for context
    inv_subset = inv[["up_id", "wse_delta"]].rename(columns={"up_id": "reach_id"})
    gdf = gdf.merge(inv_subset, on="reach_id", how="left")

    print(f"Exporting to {output_path}...")
    gdf.to_file(output_path, driver="GeoJSON")
    print("Done.")


if __name__ == "__main__":
    export_reversed_geojson(DB_PATH, "suspected_reversed_reaches.geojson")
