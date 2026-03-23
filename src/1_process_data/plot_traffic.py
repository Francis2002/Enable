import geopandas as gpd
import matplotlib.pyplot as plt
import contextily as cx
import os
import pyogrio
import pandas as pd

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
GPKG_PATH = os.path.join(SCRIPT_DIR, "../../data/highway_traffic.gpkg")
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "../../data/plots")

os.makedirs(OUTPUT_DIR, exist_ok=True)

if not os.path.exists(GPKG_PATH):
    print(f"❌ Could not find {GPKG_PATH}")
    exit(1)

# Get all layers (highways)
layers = pyogrio.list_layers(GPKG_PATH)[:, 0].tolist()
print(f"📦 Found {len(layers)} highways to plot: {layers}")

all_gdfs = []

for layer in layers:
    print(f"🎨 Plotting individual map for {layer}...")
    gdf = gpd.read_file(GPKG_PATH, layer=layer)
    
    if gdf.empty:
        print(f"  ⚠️ Layer {layer} is empty. Skipping.")
        continue
        
    all_gdfs.append(gdf)
    
    # Re-project to Web Mercator for contextily
    gdf_wm = gdf.to_crs(epsg=3857)
    
    # Create the plot
    fig, ax = plt.subplots(figsize=(10, 15))
    
    ax = gdf_wm.plot(
        ax=ax, 
        column='avg_tmdm_2025_q1', 
        cmap='RdYlGn_r', 
        linewidth=4, 
        legend=True,
        legend_kwds={'label': "Average Daily Traffic (Q1 2025)", 'orientation': "vertical", 'shrink': 0.6}
    )
    
    # Add basemap
    cx.add_basemap(ax, crs=gdf_wm.crs.to_string(), source=cx.providers.CartoDB.Positron)
    
    ax.set_axis_off()
    ax.set_title(f"{layer} Highway Traffic Volume (TMDM)", fontsize=16, fontweight='bold')
    
    plt.tight_layout()
    output_image = os.path.join(OUTPUT_DIR, f"{layer}_traffic_map.png")
    plt.savefig(output_image, dpi=300, bbox_inches='tight')
    plt.close()
    
# Now, master map
if all_gdfs:
    print("\n🌍 Generating master map with all highways...")
    master_gdf = pd.concat(all_gdfs, ignore_index=True)
    master_gdf = gpd.GeoDataFrame(master_gdf, geometry='geometry', crs="EPSG:4326")
    master_gdf_wm = master_gdf.to_crs(epsg=3857)
    
    fig, ax = plt.subplots(figsize=(15, 20))
    
    # We can plot all with a single colormap to compare traffic nationally
    ax = master_gdf_wm.plot(
        ax=ax, 
        column='avg_tmdm_2025_q1', 
        cmap='RdYlGn_r', 
        linewidth=3, 
        legend=True,
        legend_kwds={'label': "Average Daily Traffic (Q1 2025)", 'orientation': "vertical", 'shrink': 0.6}
    )
    
    cx.add_basemap(ax, crs=master_gdf_wm.crs.to_string(), source=cx.providers.CartoDB.Positron)
    
    ax.set_axis_off()
    ax.set_title("Portugal Highway Network Traffic Volume (TMDM)", fontsize=20, fontweight='bold')
    
    plt.tight_layout()
    master_image = os.path.join(OUTPUT_DIR, "all_highways_traffic_map.png")
    plt.savefig(master_image, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"✅ Master map saved to {master_image}")

