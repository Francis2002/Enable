import geopandas as gpd
import pyogrio
import os
import json

INPUT_DIR = "../../data/01_raw/highway_traffic"
GPKG_PATH = "../../data/03_interim/highway_traffic.gpkg"

layers = pyogrio.list_layers(GPKG_PATH)[:, 0].tolist()

for layer in layers:
    # Load JSON
    json_path = None
    for f in os.listdir(INPUT_DIR):
        if f.lower().startswith(layer.lower() + "_"):
            json_path = os.path.join(INPUT_DIR, f)
            break
            
    if not json_path:
        continue
        
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
        
    all_sublancos = data.get("sublancosNorte", []) + data.get("sublancosNorte2", []) + data.get("sublancosSul", []) + data.get("sublancosCentro", []) + data.get("sublancos", [])
    expected_sublancos = [item['sublanco'] for item in all_sublancos]
    
    # Load GPKG
    try:
        gdf = gpd.read_file(GPKG_PATH, layer=layer)
        actual_sublancos = gdf['sublanco'].tolist()
    except Exception as e:
        actual_sublancos = []
        gdf = None
        
    missing = set(expected_sublancos) - set(actual_sublancos)
    
    # Check for tiny lines (length < 0.001 degrees, approx 100m)
    tiny_lines = []
    if gdf is not None:
        for idx, row in gdf.iterrows():
            if row.geometry.length < 0.001:
                tiny_lines.append(row['sublanco'])
                
    if missing or tiny_lines:
        print(f"--- {layer} ---")
        if missing:
            print(f"  Missing (Not in GPKG): {missing}")
        if tiny_lines:
            print(f"  Tiny/Fallback lines (Geocoding failure?): {tiny_lines}")
            
print("Done analysis.")
