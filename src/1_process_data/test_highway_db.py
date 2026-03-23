import geopandas as gpd
import pyogrio
import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
GPKG_PATH = os.path.join(SCRIPT_DIR, "../../data/highway_traffic.gpkg")

if not os.path.exists(GPKG_PATH):
    print(f"❌ DB not found: {GPKG_PATH}")
    exit(1)

layers = pyogrio.list_layers(GPKG_PATH)[:, 0].tolist()
print(f"==================================================")
print(f"📊 HIGHWAY TRAFFIC DATABASE SUMMARY")
print(f"==================================================")
print(f"Total Highways Processed: {len(layers)}")
print(f"{'HIGHWAY':<10} | {'TOTAL SEGMENTS MAPPED':<25}")
print(f"--------------------------------------------------")

total_segments = 0
for layer in sorted(layers):
    gdf = gpd.read_file(GPKG_PATH, layer=layer)
    count = len(gdf)
    total_segments += count
    print(f"{layer:<10} | {count:<25}")

print(f"==================================================")
print(f"Total Segments Across Portugal: {total_segments}")
print(f"==================================================")
