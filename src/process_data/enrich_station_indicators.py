import duckdb
import pandas as pd
import os
import geopandas as gpd
from shapely.geometry import Point

# Configuration
PRE_ML_DB = "../../data/pre_ml.db"
OSM_DB = "../../data/osm_analysis.db"

def enrich_indicators():
    print("Starting Station Indicators Enrichment...")
    
    if not os.path.exists(PRE_ML_DB) or not os.path.exists(OSM_DB):
        print("Error: Required databases not found.")
        return

    # 1. Load Stations from pre_ml.db
    print("Loading station coordinates from pre_ml.db...")
    con_pre = duckdb.connect(PRE_ML_DB)
    df_stations = con_pre.execute("SELECT station_id, LATITUDE, LONGITUDE FROM coordinates").df()
    con_pre.close()
    
    if df_stations.empty:
        print("Error: No stations found in coordinates table.")
        return

    # 2. Load Grid Spine and Indicators from osm_analysis.db
    print("Loading grid spine and indicator tables from osm_analysis.db...")
    con_osm = duckdb.connect(OSM_DB)
    df_grid = con_osm.execute("SELECT cell_id, min_lon, min_lat, max_lon, max_lat FROM grid_spine").df()
    df_income = con_osm.execute("SELECT * FROM income").df()
    df_tourism = con_osm.execute("SELECT * FROM tourism").df()
    df_census = con_osm.execute("SELECT * FROM census_stats").df()
    con_osm.close()

    # 3. Spatial Matching: Assign cell_id to each station
    print("Mapping stations to grid cells...")
    
    # We'll use a vectorized approach to find which box contains each point
    # Since the grid is regular, we could calculate it, but checking boxes is safer if it's irregular.
    # For speed, we'll use geopandas join if the grid is large.
    
    # Create Station Points GDF
    stations_gdf = gpd.GeoDataFrame(
        df_stations, 
        geometry=[Point(lon, lat) for lon, lat in zip(df_stations.LONGITUDE, df_stations.LATITUDE)],
        crs="EPSG:4326"
    )

    # Create Grid Boxes GDF
    from shapely.geometry import box
    grid_boxes = [box(row.min_lon, row.min_lat, row.max_lon, row.max_lat) for _, row in df_grid.iterrows()]
    grid_gdf = gpd.GeoDataFrame(df_grid[['cell_id']], geometry=grid_boxes, crs="EPSG:4326")

    # Spatial Join
    joined = gpd.sjoin(stations_gdf, grid_gdf, how='left', predicate='within')
    
    # 4. Merge with Indicator Data
    print("Merging indicator metrics...")
    df_merged = joined[['station_id', 'cell_id']]
    df_merged = df_merged.merge(df_income, on='cell_id', how='left')
    df_merged = df_merged.merge(df_tourism, on='cell_id', how='left')
    df_merged = df_merged.merge(df_census, on='cell_id', how='left')

    # Drop cell_id for the final long-format indicators table
    df_merged = df_merged.drop(columns=['cell_id'])

    # 5. Melt into Long Format
    print("Melting into long format (station_id, indicator_name, value)...")
    df_long = df_merged.melt(
        id_vars=['station_id'], 
        var_name='indicator_name', 
        value_name='value'
    )
    
    # Clean up: remove rows with null values to keep the database lean
    df_long = df_long.dropna(subset=['value'])

    # 6. Save to pre_ml.db
    print(f"Saving {len(df_long)} records to indicators table in pre_ml.db...")
    try:
        con_pre = duckdb.connect(PRE_ML_DB)
        con_pre.execute("CREATE OR REPLACE TABLE indicators AS SELECT * FROM df_long")
        con_pre.close()
        print("Success: 'indicators' table created.")
    except Exception as e:
        print(f"Error saving to database: {e}")

if __name__ == "__main__":
    # Ensure we are in the script's directory for relative paths to work
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    enrich_indicators()
