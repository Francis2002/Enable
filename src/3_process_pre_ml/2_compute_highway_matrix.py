import os
import duckdb
import geopandas as gpd
import pandas as pd
import requests
import numpy as np
from shapely.geometry import Point
import time

# Configuration
PRE_ML_DB = "../../data/04_processed/pre_ml.db"
HIGHWAY_GPKG = "../../data/03_interim/highway_traffic.gpkg"
MAPPED_RAMPS_GPKG = "../../data/03_interim/mapped_ramps.gpkg"
VALHALLA_URL = "http://localhost:8002/sources_to_targets"

# Thresholds
IN_HIGHWAY_THRESHOLD_M = 50   # Max distance to be considered "inside" the service area
RAMP_SEARCH_RADIUS_M = 6000   # Euclidean search radius for on-ramps
VALHALLA_MAX_DRIVE_M = 5000   # Max driving distance to ramp

def haversine_vectorized(lon1, lat1, lon2, lat2):
    """Calculate haversine distance between a point and array of points in meters."""
    R = 6371000.0
    phi1, phi2 = np.radians(lat1), np.radians(lat2)
    dphi = np.radians(lat2 - lat1)
    dlambda = np.radians(lon2 - lon1)
    a = np.sin(dphi/2)**2 + np.cos(phi1)*np.cos(phi2)*np.sin(dlambda/2)**2
    return 2 * R * np.arctan2(np.sqrt(a), np.sqrt(1 - a))

def main():
    print("Starting Highway Matrix Enrichment...")

    # 1. Load Stations
    print(f"Loading stations from {PRE_ML_DB}...")
    con = duckdb.connect(PRE_ML_DB, read_only=True)
    df_stations = con.execute("SELECT station_id, LATITUDE as lat, LONGITUDE as lon FROM coordinates").df()
    con.close()
    
    df_stations = df_stations.drop_duplicates(subset=['station_id', 'lat', 'lon']).copy()
    
    # Create GeoDataFrame in EPSG:4326, then convert to EPSG:3035 for accurate metric distance
    stations_gdf = gpd.GeoDataFrame(
        df_stations, 
        geometry=gpd.points_from_xy(df_stations.lon, df_stations.lat),
        crs="EPSG:4326"
    )
    stations_3035 = stations_gdf.to_crs("EPSG:3035")
    print(f"Loaded {len(stations_gdf)} unique stations.")

    # 2. Load Highway Lines
    print(f"Loading highway line segments from {HIGHWAY_GPKG}...")
    import pyogrio
    layers = pyogrio.list_layers(HIGHWAY_GPKG)
    gdfs = [gpd.read_file(HIGHWAY_GPKG, layer=lyr[0]) for layer_name in layers for lyr in [layer_name]]
    # Flatten the list comprehension issue:
    gdfs = []
    for layer_name, _ in layers:
        gdf = gpd.read_file(HIGHWAY_GPKG, layer=layer_name)
        gdfs.append(gdf)
    highways_gdf = pd.concat(gdfs, ignore_index=True)
    if highways_gdf.crs != "EPSG:3035":
        highways_gdf = highways_gdf.to_crs("EPSG:3035")

    # 3. Load Ramps
    print(f"Loading mapped ramps from {MAPPED_RAMPS_GPKG}...")
    ramps_gdf = gpd.read_file(MAPPED_RAMPS_GPKG)
    # We need lat/lon for Valhalla, so let's pre-calculate them
    ramps_4326 = ramps_gdf.to_crs("EPSG:4326")
    ramps_gdf['ramp_lon'] = ramps_4326.geometry.x
    ramps_gdf['ramp_lat'] = ramps_4326.geometry.y

    # Get all unique highways to initialize matrix
    unique_highways = ramps_gdf['highway_right'].dropna().unique()
    print(f"Tracking features for {len(unique_highways)} highways: {', '.join(unique_highways)}")

    # Initialize results dictionary
    results = []

    # 4. Process Stations
    print("Processing stations...")
    for idx, station in stations_gdf.iterrows():
        station_id = station['station_id']
        st_lat = station['lat']
        st_lon = station['lon']
        
        # Get EPSG:3035 geometry for metric distance
        st_geom_3035 = stations_3035.loc[idx, 'geometry']
        
        # Initialize default values (0 traffic, -1 dist)
        station_result = {'station_id': station_id}
        for hw in unique_highways:
            station_result[f'{hw}_traffic'] = 0
            station_result[f'{hw}_dist_m'] = -1

        # --- STEP A: The 50m "In-Highway" Check ---
        # Calculate straight-line distance to all highway segments
        dists_to_lines = highways_gdf.geometry.distance(st_geom_3035)
        min_dist_line_idx = dists_to_lines.idxmin()
        min_dist_line_val = dists_to_lines.min()

        if min_dist_line_val <= IN_HIGHWAY_THRESHOLD_M:
            # It's an in-highway station!
            matched_segment = highways_gdf.loc[min_dist_line_idx]
            hw_name = matched_segment['highway']
            
            # Record it (Distance = 0, Traffic = segment traffic)
            station_result[f'{hw_name}_dist_m'] = 0
            station_result[f'{hw_name}_traffic'] = matched_segment['avg_tmdm_2024']
            
            # Fast-track complete for this station
            results.append(station_result)
            continue

        # --- STEP B: The Valhalla On-Ramp Check (City Stations) ---
        # 1. Euclidean Filter (Fast)
        dists_to_ramps = haversine_vectorized(st_lon, st_lat, ramps_gdf['ramp_lon'].values, ramps_gdf['ramp_lat'].values)
        nearby_ramps = ramps_gdf[dists_to_ramps <= RAMP_SEARCH_RADIUS_M].copy()

        if nearby_ramps.empty:
            results.append(station_result)
            continue

        nearby_ramps['euclidean_dist'] = dists_to_ramps[dists_to_ramps <= RAMP_SEARCH_RADIUS_M]

        # 2. Target Consolidation (One closest ramp per highway)
        # We group by highway name and take the one with the minimum euclidean distance
        closest_ramps_per_hw = nearby_ramps.loc[nearby_ramps.groupby('highway_right')['euclidean_dist'].idxmin()]

        # 3. Valhalla Request
        payload = {
            "sources": [{"lat": float(st_lat), "lon": float(st_lon)}],
            "targets": [{"lat": float(r['ramp_lat']), "lon": float(r['ramp_lon'])} for _, r in closest_ramps_per_hw.iterrows()],
            "costing": "auto",
            "max_matrix_distance": VALHALLA_MAX_DRIVE_M
        }

        try:
            resp = requests.post(VALHALLA_URL, json=payload, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                matrix = data.get('sources_to_targets', [[]])[0]
                
                # Parse results back to the respective highways
                for i, (_, row) in enumerate(closest_ramps_per_hw.iterrows()):
                    hw_name = row['highway_right']
                    v_dist_km = matrix[i].get('distance')
                    
                    if v_dist_km is not None:
                        v_dist_m = v_dist_km * 1000.0
                        if v_dist_m <= VALHALLA_MAX_DRIVE_M:
                            station_result[f'{hw_name}_dist_m'] = round(v_dist_m, 2)
                            station_result[f'{hw_name}_traffic'] = row['avg_tmdm_2024']
        except Exception as e:
            # If Valhalla fails or connection error, we silently default to -1/0 which is already set
            pass

        results.append(station_result)
        
        # Minimal sleep to avoid hammering the local Valhalla instance too aggressively
        if idx % 100 == 0 and idx > 0:
            print(f"Processed {idx}/{len(stations_gdf)} stations...")
            time.sleep(0.01)

    # 5. Save Results
    print("Enrichment complete. Preparing to save...")
    df_results = pd.DataFrame(results)
    
    print(f"Connecting to DuckDB: {PRE_ML_DB}")
    con_out = duckdb.connect(PRE_ML_DB)
    con_out.register("temp_results", df_results)
    con_out.execute("CREATE OR REPLACE TABLE station_highway_matrix AS SELECT * FROM temp_results")
    con_out.unregister("temp_results")
    con_out.close()
    
    print(f"Successfully saved 'station_highway_matrix' with {len(df_results)} rows and {len(df_results.columns)} columns.")

if __name__ == "__main__":
    main()
