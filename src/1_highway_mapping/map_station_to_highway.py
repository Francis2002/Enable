import duckdb
import pandas as pd
import geopandas as gpd
import requests
import builtins
import time

# Force print to flush
print = lambda *args, **kwargs: builtins.print(*args, **{**kwargs, 'flush': True})

def get_db_connection(db_path):
    # Wait for lock if needed
    for attempt in range(600):
        try:
            return duckdb.connect(database=db_path, read_only=False)
        except Exception:
            time.sleep(1)
    raise Exception("Could not get write lock on DuckDB.")

def load_stations(conn):
    stations_df = conn.execute("SELECT station_id, LATITUDE, LONGITUDE FROM coordinates").fetchdf()
    return gpd.GeoDataFrame(
        stations_df,
        geometry=gpd.points_from_xy(stations_df.LONGITUDE, stations_df.LATITUDE),
        crs="EPSG:4326"
    )

def load_ramps(ramps_path):
    return gpd.read_file(ramps_path)

def get_route_fallback(station, targets):
    station_coords = {"lon": station.geometry.x, "lat": station.geometry.y}
    valhalla_url = "http://localhost:8002/route"
    
    results = {}
    for j, ramp in enumerate(targets.itertuples()):
        ramp_coords = {"lon": ramp.geometry.x, "lat": ramp.geometry.y}
        request_data = {
            "locations": [station_coords, ramp_coords],
            "costing": "auto"
        }
        try:
            resp = requests.post(valhalla_url, json=request_data)
            if resp.status_code == 200:
                data = resp.json()
                if 'trip' in data and 'summary' in data['trip']:
                    dist = data['trip']['summary']['length']
                    results[j] = {'distance': dist}
        except:
            pass
    return results

def get_road_distances(station, targets):
    if targets.empty:
        return None
        
    station_coords = {"lon": station.geometry.x, "lat": station.geometry.y}
    ramp_coords = [{"lon": ramp.geometry.x, "lat": ramp.geometry.y} for ramp in targets.itertuples()]
    
    valhalla_url = "http://localhost:8002/sources_to_targets"
    request_data = {
        "sources": [station_coords],
        "targets": ramp_coords,
        "costing": "auto"
    }
    
    try:
        response = requests.post(valhalla_url, json=request_data)
        if response.status_code == 200:
            return response.json()['sources_to_targets'][0]
        else:
            return "fallback"
    except:
        return "fallback"

def main():
    db_path = "/home/joao-martins/Desktop/code/Enable Mobility/code/Enable/data/04_processed/pre_ml.db"
    ramps_path = "/home/joao-martins/Desktop/code/Enable Mobility/code/Enable/data/03_interim/mapped_ramps.gpkg"
    
    print("Loading data...")
    conn = duckdb.connect(database=db_path, read_only=True)
    stations_gdf = load_stations(conn)
    conn.close()
    
    ramps_gdf = load_ramps(ramps_path)
    all_highways = ramps_gdf['highway_right'].unique()
    
    print(f"Total unique highways found: {len(all_highways)}")
    
    stations_wgs84 = stations_gdf.to_crs("EPSG:4326")
    ramps_wgs84 = ramps_gdf.to_crs("EPSG:4326")
    stations_proj = stations_gdf.to_crs("EPSG:3763")
    ramps_proj = ramps_gdf.to_crs("EPSG:3763")
    
    results = []
    fallback_count = 0
    
    print(f"Processing {len(stations_proj)} stations for matrix...")
    for i, station_proj in enumerate(stations_proj.itertuples()):
        if (i + 1) % 100 == 0:
            print(f"  Processed {i + 1}/{len(stations_proj)} stations")
            
        station = stations_wgs84.iloc[station_proj.Index]
        
        # Initialize dictionary with -1 for all highways
        row_data = {"station_id": station.station_id}
        for hw in all_highways:
            row_data[f"{hw}_dist_m"] = -1.0
            row_data[f"{hw}_traffic"] = -1.0
            
        # Ramps within 10km
        nearby_indices = ramps_proj.sindex.query(station_proj.geometry.buffer(10000))
        nearby_ramps_proj = ramps_proj.iloc[nearby_indices]
        
        if not nearby_ramps_proj.empty:
            distances_to_ramps = nearby_ramps_proj.geometry.distance(station_proj.geometry)
            nearby_ramps_proj_with_dist = nearby_ramps_proj.copy()
            nearby_ramps_proj_with_dist['euclidean_dist'] = distances_to_ramps
            
            # Group by highway, take 3 closest
            top_targets = nearby_ramps_proj_with_dist.sort_values('euclidean_dist').groupby('highway_right').head(3)
            targets_wgs84 = ramps_wgs84.loc[top_targets.index]
            
            valhalla_res = get_road_distances(station, targets_wgs84)
            
            distances_dict = {}
            if valhalla_res == "fallback":
                fallback_count += 1
                distances_dict = get_route_fallback(station, targets_wgs84)
            elif valhalla_res:
                for j, item in enumerate(valhalla_res):
                    if item and 'distance' in item and item['distance'] is not None:
                        distances_dict[j] = {'distance': item['distance']}
            
            # Aggregate the min distance for each highway
            best_hw_dists = {}
            for j, ramp in enumerate(targets_wgs84.itertuples()):
                if j in distances_dict:
                    hw = ramp.highway_right
                    dist_m = distances_dict[j]['distance'] * 1000.0
                    traffic = ramp.avg_tmdm_2024
                    
                    if hw not in best_hw_dists or dist_m < best_hw_dists[hw]['dist_m']:
                        best_hw_dists[hw] = {'dist_m': dist_m, 'traffic': traffic}
                        
            # Populate row_data
            for hw, data in best_hw_dists.items():
                row_data[f"{hw}_dist_m"] = data['dist_m']
                row_data[f"{hw}_traffic"] = data['traffic']
                
        results.append(row_data)

    print(f"Finished processing. Used fallback for {fallback_count} stations.")
    
    # Save to DuckDB
    print("Saving to database...")
    df = pd.DataFrame(results)
    
    conn = get_db_connection(db_path)
    
    # Drop legacy tables to be clean
    conn.execute("DROP TABLE IF EXISTS station_highway_matrix")
    conn.execute("DROP TABLE IF EXISTS station_to_highway")
    
    conn.execute("CREATE TABLE station_highway_matrix AS SELECT * FROM df")
    
    print(f"Successfully created station_highway_matrix with {len(df)} rows.")
    conn.close()

if __name__ == "__main__":
    main()
