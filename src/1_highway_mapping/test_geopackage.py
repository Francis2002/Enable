import geopandas as gpd
import os

GPKG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../data/highway_traffic.gpkg")

if not os.path.exists(GPKG_PATH):
    print(f"File not found: {GPKG_PATH}")
    exit(1)

gdf = gpd.read_file(GPKG_PATH, layer="A1")
print(f"Loaded GeoDataFrame with {len(gdf)} records.")
print("\nColumns:")
print(gdf.columns.tolist())

print("\nSample records:")
print(gdf[['sublanco', 'avg_tmdm_2025_q1']].head())
