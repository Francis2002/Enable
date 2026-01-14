from pyrosm import OSM
import matplotlib.pyplot as plt
import os

# Path to the PBF file
pbf_path = "../data/portugal-latest.osm.pbf"
output_plot = "lisbon_preview.png"

print(f"Loading data from {pbf_path}...")
osm = OSM(pbf_path)

# Extract a small area around Lisbon (approx bounding box)
# Min Lon, Min Lat, Max Lon, Max Lat
print("Filtering for Lisbon area...")
lisbon_bbox = [-9.23, 38.69, -9.09, 38.79]

# Set the bounding box on the OSM object
osm = OSM(pbf_path, bounding_box=lisbon_bbox)

# Get the road network for this area
print("Extracting road network (this might take a few seconds)...")
nodes, edges = osm.get_network(network_type="driving", nodes=True)

# Plot the network
print(f"Creating plot and saving to {output_plot}...")
fig, ax = plt.subplots(figsize=(12, 12))
edges.plot(ax=ax, linewidth=0.5, color="#1abc9c")

ax.set_title("Lisbon Road Network Preview", fontsize=15, color="white")
ax.set_axis_off()
fig.set_facecolor("#2c3e50")

plt.savefig(output_plot, dpi=300, bbox_inches='tight', facecolor=fig.get_facecolor())
print("Done!")
