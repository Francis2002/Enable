import duckdb
import requests
import json
import pandas as pd
import numpy as np
import os
import time

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OSM_DB = os.path.join(BASE_DIR, "../data/osm_analysis.db")
MOBIE_DB = os.path.join(BASE_DIR, "../data/mobie_data.db")
MATRIX_DB = os.path.join(BASE_DIR, "../data/travel_matrix.db")
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
    if not os.path.exists(OSM_DB) or not os.path.exists(MOBIE_DB):
        print(f"Missing required databases. Checked:\n{OSM_DB}\n{MOBIE_DB}")
        return

    # 1. Load Data
    print("Loading origins (MICRO-TEST LIMIT 20) and Mobi.E chargers...")
    conn_osm = duckdb.connect(OSM_DB)
    # LIMIT 20 for a very small test as requested (and due to disk space constraints)
    origins = conn_osm.execute("SELECT cell_id, lon, lat FROM cell_origins LIMIT 20").df()
    conn_osm.close()

    conn_mobie = duckdb.connect(MOBIE_DB)
    chargers = conn_mobie.execute("SELECT ID as station_id, LONGITUDE as lon, LATITUDE as lat FROM stations").df()
    conn_mobie.close()

    print(f"Loaded {len(origins)} origins and {len(chargers)} chargers.")

    # 2. Setup Travel Matrix DB
    if os.path.exists(MATRIX_DB):
        print(f"Deleting existing {MATRIX_DB} to ensure clean state...")
        os.remove(MATRIX_DB) 
        
    conn_matrix = duckdb.connect(MATRIX_DB)
    conn_matrix.execute("CREATE TABLE travel_times (cell_id VARCHAR, station_id VARCHAR, time_min DOUBLE, distance_km DOUBLE)")

    # 3. Process in Batches
    TARGET_CHUNK_SIZE = 250
    
    start_time = time.time()
    total_processed = 0
    total_saved = 0

    print("\n--- Starting Batch Processing ---")
    for idx, origin in origins.iterrows():
        b_start = time.time()
        # Calculate Euclidean distances to all chargers
        dists = haversine(origin['lon'], origin['lat'], chargers['lon'], chargers['lat'])
        candidate_chargers = chargers[dists < EUCLIDEAN_FILTER_KM]
        
        print(f"Cell {origin['cell_id']} ({total_processed+1}/{len(origins)}): {len(candidate_chargers)} candidates in {EUCLIDEAN_FILTER_KM}km radius")
        
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
                response = requests.post(VALHALLA_URL, json=payload, timeout=60)
                response.raise_for_status()
                result = response.json()
                
                if 'sources_to_targets' in result:
                    valid_edges = []
                    for k, target_result in enumerate(result['sources_to_targets'][0]):
                        t_sec = target_result.get('time')
                        if t_sec is not None and t_sec <= TIME_THRESHOLD_SEC:
                            valid_edges.append({
                                'cell_id': origin['cell_id'],
                                'station_id': chunk.iloc[k]['station_id'],
                                'time_min': float(t_sec / 60.0),
                                'distance_km': float(target_result.get('distance'))
                            })
                    
                    if valid_edges:
                        df_edges = pd.DataFrame(valid_edges)
                        conn_matrix.execute("INSERT INTO travel_times SELECT * FROM df_edges")
                        total_saved += len(valid_edges)
                
            except Exception as e:
                print(f"  [ERROR] Chunk {j}-{j+len(chunk)}: {e}")
            
        b_elapsed = time.time() - b_start
        total_processed += 1
        print(f"  Done. Saved {total_saved - (total_saved - len(valid_edges) if 'valid_edges' in locals() else 0)} edges for this cell. ({b_elapsed:.2f}s)")

    elapsed = time.time() - start_time
    print(f"\n--- MICRO-TEST COMPLETE ---")
    print(f"Total origins processed: {total_processed}")
    print(f"Total reachable edges saved: {total_saved}")
    print(f"Average time per origin: {elapsed/total_processed:.2f}s")
    print(f"Database size: {os.path.getsize(MATRIX_DB)/1024:.1f} KB")
    conn_matrix.close()

if __name__ == "__main__":
    calculate_matrix()

if __name__ == "__main__":
    calculate_matrix()
