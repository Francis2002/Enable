import geopandas as gpd
import matplotlib.pyplot as plt
import contextily as cx
import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
GPKG_PATH = os.path.join(SCRIPT_DIR, "../../data/highway_traffic.gpkg")
OUTPUT_IMAGE = os.path.join(SCRIPT_DIR, "../../data/a1_traffic_map.png")

print(f"Loading data from {GPKG_PATH}...")
gdf = gpd.read_file(GPKG_PATH, layer="A1")

# Re-project to Web Mercator for contextily
gdf = gdf.to_crs(epsg=3857)

# Create the plot
fig, ax = plt.subplots(figsize=(10, 15))

# Plot the lines, color by traffic volume
# Use a color map from green (low) to red (high traffic)
ax = gdf.plot(
    ax=ax, 
    column='avg_tmdm_2025_q1', 
    cmap='RdYlGn_r', 
    linewidth=4, 
    legend=True,
    legend_kwds={'label': "Average Daily Traffic (Q1 2025)", 'orientation': "vertical", 'shrink': 0.6}
)

# Add basemap
cx.add_basemap(ax, crs=gdf.crs.to_string(), source=cx.providers.CartoDB.Positron)

# Add some text labels for a few major nodes (start of segments)
# Since the lines might overlap, we just get a few points
labeled_nodes = set()
for idx, row in gdf.iterrows():
    # Only label a subset to avoid clutter
    node = row['start_node']
    if node not in labeled_nodes and ('Coimbra' in node or 'Porto' in node or 'Lisboa' in node or 'Santarém' in node or 'Leiria' in node or 'Sacavém' in node):
        # get middle of the line to put label or start point
        pt = row.geometry.coords[0]
        ax.annotate(node, xy=(pt[0], pt[1]), xytext=(5, 5), textcoords="offset points", 
                    fontsize=8, fontweight='bold',
                    bbox=dict(boxstyle="round,pad=0.3", fc="white", ec="gray", alpha=0.8))
        labeled_nodes.add(node)

ax.set_axis_off()
ax.set_title("A1 Highway Traffic Volume (TMDM)", fontsize=16, fontweight='bold')

plt.tight_layout()
plt.savefig(OUTPUT_IMAGE, dpi=300, bbox_inches='tight')
print(f"✅ Map saved successfully to {OUTPUT_IMAGE}")
