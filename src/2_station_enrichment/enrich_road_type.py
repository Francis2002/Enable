import duckdb
import pandas as pd
import requests
import builtins
import time

print = lambda *args, **kwargs: builtins.print(*args, **{**kwargs, 'flush': True})

def main():
    db_path = "/home/joao-martins/Desktop/code/Enable Mobility/code/Enable/data/04_processed/pre_ml.db"
    
    print("Waiting for database lock to fetch coordinates...")
    conn = None
    for attempt in range(600): # Wait up to 50 minutes
        try:
            conn = duckdb.connect(database=db_path, read_only=True)
            break
        except Exception as e:
            time.sleep(5)
            
    if not conn:
        print("Could not connect to database to read coordinates.")
        return
        
    stations_df = conn.execute("SELECT station_id, LATITUDE, LONGITUDE FROM coordinates").fetchdf()
    conn.close()
    
    print(f"Loaded {len(stations_df)} stations.")
    
    valhalla_url = "http://localhost:8002/locate"
    batch_size = 100
    results = []
    
    print("Querying Valhalla /locate API in batches...")
    for i in range(0, len(stations_df), batch_size):
        batch = stations_df.iloc[i:i+batch_size]
        
        locations = [{"lat": row.LATITUDE, "lon": row.LONGITUDE, "station_id": row.station_id} for _, row in batch.iterrows()]
        
        request_data = {
            "locations": [{"lat": loc["lat"], "lon": loc["lon"]} for loc in locations],
            "costing": "auto",
            "verbose": True
        }
        
        try:
            resp = requests.post(valhalla_url, json=request_data)
            if resp.status_code == 200:
                data = resp.json()
                for j, item in enumerate(data):
                    station_id = locations[j]["station_id"]
                    
                    road_classification = "unknown"
                    road_use = "unknown"
                    surface = "unknown"
                    
                    if "edges" in item and len(item["edges"]) > 0:
                        edge = item["edges"][0].get("edge", {})
                        classification = edge.get("classification", {})
                        
                        road_classification = classification.get("classification", "unknown")
                        road_use = classification.get("use", "unknown")
                        surface = classification.get("surface", "unknown")
                        
                    results.append({
                        "station_id": station_id,
                        "road_classification": road_classification,
                        "road_use": road_use,
                        "road_surface": surface
                    })
            else:
                for loc in locations:
                    results.append({
                        "station_id": loc["station_id"],
                        "road_classification": "error",
                        "road_use": "error",
                        "road_surface": "error"
                    })
        except Exception as e:
            for loc in locations:
                results.append({
                    "station_id": loc["station_id"],
                    "road_classification": "error",
                    "road_use": "error",
                    "road_surface": "error"
                })
                
        if (i + batch_size) % 1000 == 0 or (i + batch_size) >= len(stations_df):
            print(f"  Processed {min(i + batch_size, len(stations_df))} / {len(stations_df)} stations...")

    if results:
        print("Waiting for write access to DuckDB...")
        results_df = pd.DataFrame(results)
        
        write_conn = None
        for attempt in range(600):
            try:
                write_conn = duckdb.connect(database=db_path, read_only=False)
                break
            except Exception as e:
                time.sleep(5)
                
        if write_conn:
            print(f"Saving {len(results)} results to DuckDB table 'station_road_types'...")
            write_conn.execute("CREATE OR REPLACE TABLE station_road_types AS SELECT * FROM results_df")
            print("Table successfully created/updated!")
            
            sample = write_conn.execute("SELECT * FROM station_road_types LIMIT 5").fetchdf()
            print("\n--- First 5 rows of station_road_types ---")
            print(sample.to_markdown())
            print("------------------------------------------\n")
            write_conn.close()
        else:
            print("Failed to acquire write lock on DuckDB. Saved to 'station_road_types_backup.csv' instead.")
            results_df.to_csv("station_road_types_backup.csv", index=False)
            
    else:
        print("No results to save.")

if __name__ == "__main__":
    main()
