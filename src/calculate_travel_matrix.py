import duckdb
import requests
import json
import pandas as pd
import numpy as np
import os
import time

OSM_DB = "../data/osm_analysis.db"
CHARGERS_DB = "../data/chargers.db"
MATRIX_DB = "../data/travel_matrix.db"
VALHALLA_URL = "http://localhost:8002/sources_to_targets"

# Optimization Thresholds
EUCLIDEAN_FILTER_KM = 60.0
TIME_THRESHOLD_MIN = 30.0
TIME_THRESHOLD_SEC = TIME_THRESHOLD_MIN * 60

def haversine(lon1, lat1, lon2, lat2):
    # Vectorized haversine distance in km
    R = 6371.0
    phi1, phi2 = np.radians(lat1), np.radians(lat2)
    dphi = np.radians(lat2 - lat1)
    dlambda = np.radians(lon2 - lon1)
    a = np.sin(dphi/2)**2 + np.cos(phi1)*np.cos(phi2)*np.sin(dlambda/2)**2
    return 2 * R * np.arctan2(np.sqrt(a), np.sqrt(1 - a))

def calculate_matrix():
    if not os.path.exists(OSM_DB) or not os.path.exists(CHARGERS_DB):
        print("Missing required databases.")
        return

    # 1. Load Data
    print("Loading origins and chargers...")
    conn_osm = duckdb.connect(OSM_DB)
    origins = conn_osm.execute("SELECT cell_id, lon, lat FROM cell_origins").df()
    conn_osm.close()

    conn_chg = duckdb.connect(CHARGERS_DB)
    chargers = conn_chg.execute("SELECT station_id, longitude as lon, latitude as lat FROM chargers").df()
    conn_chg.close()

    print(f"Loaded {len(origins)} origins and {len(chargers)} chargers.")

    # 2. Setup Travel Matrix DB
    conn_matrix = duckdb.connect(MATRIX_DB)
    conn_matrix.execute("CREATE TABLE IF NOT EXISTS travel_times (cell_id VARCHAR, station_id BIGINT, time_min DOUBLE, distance_km DOUBLE)")

    # 3. Process in Batches
    TARGET_CHUNK_SIZE = 200 # Process targets in smaller groups to prevent OOM
    
    start_time = time.time()
    total_processed = 0
    total_saved = 0

    for idx, origin in origins.iterrows():
        # Calculate Euclidean distances to all chargers
        dists = haversine(origin['lon'], origin['lat'], chargers['lon'], chargers['lat'])
        candidate_chargers = chargers[dists < EUCLIDEAN_FILTER_KM]
        
        if candidate_chargers.empty:
            total_processed += 1
            continue
        
        # Process targets in chunks for this origin
        for j in range(0, len(candidate_chargers), TARGET_CHUNK_SIZE):
            chunk = candidate_chargers.iloc[j : j + TARGET_CHUNK_SIZE]
            
            payload = {
                "sources": [{"lat": origin['lat'], "lon": origin['lon']}],
                "targets": [{"lat": c['lat'], "lon": c['lon']} for _, c in chunk.iterrows()],
                "costing": "auto"
            }
            
            try:
                response = requests.post(VALHALLA_URL, json=payload, timeout=30)
                response.raise_for_status()
                result = response.json()
                
                if 'sources_to_targets' not in result:
                    continue
                
                valid_edges = []
                for k, target_result in enumerate(result['sources_to_targets'][0]):
                    t_sec = target_result.get('time')
                    d_km = target_result.get('distance')
                    
                    if t_sec is not None and t_sec <= TIME_THRESHOLD_SEC:
                        valid_edges.append({
                            'cell_id': origin['cell_id'],
                            'station_id': chunk.iloc[k]['station_id'],
                            'time_min': t_sec / 60.0,
                            'distance_km': d_km
                        })
                
                if valid_edges:
                    df_edges = pd.DataFrame(valid_edges)
                    conn_matrix.execute("INSERT INTO travel_times SELECT * FROM df_edges")
                    total_saved += len(valid_edges)
                
                # Small pause to prevent overloading the local server
                time.sleep(0.1)
                
            except Exception as e:
                print(f"Request failed for cell {origin['cell_id']} chunk {j}: {e}")
                time.sleep(1) # Back off
            
        total_processed += 1
        if total_processed % 10 == 0:
            elapsed = time.time() - start_time
            print(f"Processed {total_processed}/{len(origins)} origins. Found {total_saved} edges. Elapsed: {elapsed:.1f}s")

    print(f"Calculation complete. Total edges saved: {total_saved}")
    conn_matrix.close()

if __name__ == "__main__":
    calculate_matrix()
