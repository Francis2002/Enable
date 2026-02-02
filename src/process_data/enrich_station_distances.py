import duckdb
import pandas as pd
import requests
import json
import time
import os
import numpy as np

# Configuration
STATIONS_CSV = "../../data/MOBIe_Lista_de_postos_corrected.csv"
OSM_DB = "../../data/osm_analysis.db"
PRE_ML_DB = "../../data/pre_ml.db"
POI_LIST_TXT = "../../data/POI_FULL_LIST.txt"
VALHALLA_URL = "http://localhost:8002/sources_to_targets"

# Road Group Definitions
ROAD_GROUPS = {
    'highways_entry': {
        'types': ['motorway', 'trunk', 'motorway_link', 'trunk_link'],
        'threshold': 3000
    },
    'national_roads_entry': {
        'types': ['primary', 'secondary', 'primary_link', 'secondary_link'],
        'threshold': 500
    }
}

# POI Limits
WALKING_LIMIT_M = 350
FILTER_BUFFER_POI_M = 500  # Euclidean buffer

def haversine_vectorized(lon1, lat1, lon2, lat2):
    """Calculate haversine distance between a point and array of points in meters."""
    R = 6371000.0
    phi1, phi2 = np.radians(lat1), np.radians(lat2)
    dphi = np.radians(lat2 - lat1)
    dlambda = np.radians(lon2 - lon1)
    a = np.sin(dphi/2)**2 + np.cos(phi1)*np.cos(phi2)*np.sin(dlambda/2)**2
    return 2 * R * np.arctan2(np.sqrt(a), np.sqrt(1 - a))

def parse_poi_list(file_path):
    """Parse POI_FULL_LIST.txt to identify selected features."""
    features = {'amenity': [], 'shop': [], 'tourism': []}
    current_cat = None
    if not os.path.exists(file_path):
        print(f"Warning: {file_path} not found.")
        return features
    with open(file_path, 'r') as f:
        for line in f:
            line = line.strip()
            if not line: continue
            if "AMENITY" in line.upper(): current_cat = 'amenity'
            elif "SHOP" in line.upper(): current_cat = 'shop'
            elif "TOURISM" in line.upper(): current_cat = 'tourism'
            elif current_cat and ":" in line:
                name = line.split(':')[0].strip()
                features[current_cat].append(name)
    return features

def process_enrichment():
    print("Starting Grouped Station Enrichment...")
    
    # 1. Parse POIs
    poi_features = parse_poi_list(POI_LIST_TXT)
    print(f"Parsed {sum(len(v) for v in poi_features.values())} POI categories.")
    
    # 2. Load Stations (Group by ID and Coordinates to avoid redundant charger processing)
    df_stations = pd.read_csv(STATIONS_CSV, sep=';', encoding='utf-8', 
                              dtype={'LATITUDE': float, 'LONGITUDE': float},
                              converters={'ID': str})
    
    # Each row in the CSV is a charger, but we only care about unique station locations
    stations = df_stations[['ID', 'LATITUDE', 'LONGITUDE']].rename(
        columns={'ID': 'station_id', 'LATITUDE': 'lat', 'LONGITUDE': 'lon'}
    ).drop_duplicates(subset=['station_id', 'lat', 'lon'])
    
    print(f"Loaded {len(df_stations)} entries, identified {len(stations)} unique station locations.")

    results = stations[['station_id']].copy()
    con_osm = duckdb.connect(OSM_DB, read_only=True)
    con_osm.execute("INSTALL spatial; LOAD spatial;")

    # 3. Process POIs (Foot)
    for cat_type, names in poi_features.items():
        for name in names:
            print(f"Processing POI: {cat_type} -> {name}...")
            q_poi = f"SELECT lat, lon FROM pois_global WHERE {cat_type} = '{name}'"
            candidates = con_osm.execute(q_poi).df()
            if candidates.empty:
                results[f'{name}_count'] = 0
                results[f'{name}_dist'] = -1
                continue

            v_counts, v_dists = [], []
            for idx, station in stations.iterrows():
                dists = haversine_vectorized(station['lon'], station['lat'], candidates['lon'].values, candidates['lat'].values)
                nearby = candidates[dists <= FILTER_BUFFER_POI_M]
                if nearby.empty:
                    v_counts.append(0); v_dists.append(-1)
                    continue

                payload = {
                    "sources": [{"lat": float(station['lat']), "lon": float(station['lon'])}],
                    "targets": [{"lat": float(r['lat']), "lon": float(r['lon'])} for _, r in nearby.iterrows()],
                    "costing": "pedestrian", "max_matrix_distance": WALKING_LIMIT_M
                }
                try:
                    resp = requests.post(VALHALLA_URL, json=payload, timeout=10)
                    if resp.status_code == 200:
                        data = resp.json()
                        valid = [item['distance'] * 1000.0 for item in data['sources_to_targets'][0] if item.get('distance') is not None and item['distance'] * 1000.0 <= WALKING_LIMIT_M]
                        v_counts.append(len(valid))
                        v_dists.append(min(valid) if valid else -1)
                    else:
                        v_counts.append(0); v_dists.append(-1)
                except:
                    v_counts.append(0); v_dists.append(-1)
            results[f'{name}_count'] = v_counts
            results[f'{name}_dist'] = v_dists

    # 4. Process Grouped Roads (Car)
    for group_name, cfg in ROAD_GROUPS.items():
        print(f"Processing Road Group: {group_name}...")
        types_str = ", ".join([f"'{t}'" for t in cfg['types']])
        # We use EPSG:4326. In DuckDB's spatial transform from 3035 to 4326:
        # ST_X returns Latitude (~37-42) and ST_Y returns Longitude (~ -9 to -6)
        q_road = f"""
            SELECT ST_Y(ST_Transform(ST_Centroid(geometry), 'EPSG:3035', 'EPSG:4326')) as lon, 
                   ST_X(ST_Transform(ST_Centroid(geometry), 'EPSG:3035', 'EPSG:4326')) as lat 
            FROM roads_global 
            WHERE highway IN ({types_str})
        """
        # For highways (motorway/trunk), we target the points (nodes) of the links
        # This accurately represents the "entry/exit" access points.
        candidates = con_osm.execute(q_road).df()
        
        if candidates.empty:
            results[f'{group_name}_count'] = 0
            results[f'{group_name}_dist'] = -1
            continue

        v_counts, v_dists = [], []
        threshold = cfg['threshold']
        
        for idx, station in stations.iterrows():
            dists = haversine_vectorized(station['lon'], station['lat'], candidates['lon'].values, candidates['lat'].values)
            mask_val = threshold + 1000 # Safety buffer for Euclidean filter
            nearby = candidates[dists <= mask_val]
            
            if nearby.empty:
                v_counts.append(0); v_dists.append(-1)
                continue
            
            if len(nearby) > 50:
                nearby['d'] = haversine_vectorized(station['lon'], station['lat'], nearby['lon'], nearby['lat'])
                nearby = nearby.sort_values('d').head(50).drop(columns=['d'])

            payload = {
                "sources": [{"lat": float(station['lat']), "lon": float(station['lon'])}],
                "targets": [{"lat": float(r['lat']), "lon": float(r['lon'])} for _, r in nearby.iterrows()],
                "costing": "auto", "max_matrix_distance": threshold
            }
            try:
                resp = requests.post(VALHALLA_URL, json=payload, timeout=10)
                if resp.status_code == 200:
                    data = resp.json()
                    valid = [item['distance'] * 1000.0 for item in data['sources_to_targets'][0] if item.get('distance') is not None and item['distance'] * 1000.0 <= threshold]
                    v_counts.append(len(valid))
                    v_dists.append(min(valid) if valid else -1)
                else:
                    v_counts.append(0); v_dists.append(-1)
            except:
                v_counts.append(0); v_dists.append(-1)

        results[f'{group_name}_count'] = v_counts
        results[f'{group_name}_dist'] = v_dists

    con_osm.close()

    # 5. Save
    print("\nSaving results to database...")
    con_out = duckdb.connect(PRE_ML_DB)
    con_out.execute("CREATE OR REPLACE TABLE station_distances AS SELECT * FROM results")
    con_out.close()
    print(f"Success. Enriched data with {len(results.columns)-1} columns.")

if __name__ == "__main__":
    process_enrichment()
