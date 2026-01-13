import geopandas as gpd
import pandas as pd
import duckdb
from shapely.geometry import MultiPoint, Point
import os

db_path = "osm_analysis.db"

print("Connecting to DuckDB...")
con = duckdb.connect(db_path)
con.execute("LOAD spatial;")

# Load grid and roads (from the previous run or re-load)
# Actually, I'll re-extract roads for the Lisbon area to be sure
from pyrosm import OSM
pbf_path = "portugal-latest.osm.pbf"
min_lon, min_lat, max_lon, max_lat = -9.25, 38.65, -9.05, 38.80
osm = OSM(pbf_path, bounding_box=[min_lon, min_lat, max_lon, max_lat])
roads = osm.get_network(network_type="driving")
roads = roads.to_crs("EPSG:3035")

# Priority map for road classes (common to both plotting and filtering)
priority_map = {
    'motorway': 1, 'trunk': 2, 'primary': 3, 'secondary': 4, 
    'tertiary': 5, 'residential': 6, 'unclassified': 7, 'service': 8
}
roads['priority'] = roads['highway'].map(priority_map).fillna(99)

# Load grid from GDF (we already have it in analyze_lisbon.py memory but this is a separate script)
# For now, let's just use the same extent
from shapely.geometry import box
import numpy as np
extent_gdf = gpd.GeoDataFrame({"geometry": [box(min_lon, min_lat, max_lon, max_lat)]}, crs="EPSG:4326").to_crs("EPSG:3035")
bounds = extent_gdf.total_bounds
xmin, ymin, xmax, ymax = bounds
xmin, ymin = np.floor(xmin / 1000) * 1000, np.floor(ymin / 1000) * 1000
grid_cells = []
for i in range(int(np.ceil((xmax - xmin) / 1000))):
    for j in range(int(np.ceil((ymax - ymin) / 1000))):
        grid_cells.append(box(xmin + i * 1000, ymin + j * 1000, xmin + i * 1000 + 1000, ymin + j * 1000 + 1000))
grid = gpd.GeoDataFrame({"geometry": grid_cells}, crs="EPSG:3035")
grid["cell_id"] = [f"GRID_{int(c.centroid.x)}_{int(c.centroid.y)}" for c in grid.geometry]

print("Finding road-boundary intersections...")
# Get cell boundaries as LineStrings
grid_boundaries = grid.copy()
grid_boundaries.geometry = grid.geometry.boundary

# Intersect roads with grid boundaries to find entry points
# Set keep_geom_type=False to keep the Points (intersection of two LineStrings)
entry_points = gpd.overlay(roads[['highway', 'geometry']], grid_boundaries, how='intersection', keep_geom_type=False)

# Overlay might return various types; filter for Points only
entry_points = entry_points[entry_points.geometry.type == 'Point']

# Priority map for road classes
priority = {
    'motorway': 1, 'trunk': 2, 'primary': 3, 'secondary': 4, 
    'tertiary': 5, 'residential': 6, 'unclassified': 7, 'service': 8
}
# Map the priority, default to 99
entry_points['priority'] = entry_points['highway'].map(priority).fillna(99)

# Filter out very low priority roads (paths, tracks, etc. if needed)
entry_points = entry_points[entry_points['priority'] < 20]

# Sort by cell_id and priority
entry_points = entry_points.sort_values(['cell_id', 'priority'])

# For each cell, take the top 3 unique points (approximate unique by rounding coords)
entry_points['x'] = entry_points.geometry.x.round(1)
entry_points['y'] = entry_points.geometry.y.round(1)
top_3 = entry_points.drop_duplicates(subset=['cell_id', 'x', 'y']).groupby('cell_id').head(3)

print("Formatting entry points for storage...")
# Convert points to WGS84 for Valhalla travel time later
top_3_wgs84 = top_3.to_crs("EPSG:4326")
top_3_wgs84['lon'] = top_3_wgs84.geometry.x
top_3_wgs84['lat'] = top_3_wgs84.geometry.y

# Final table for storage
origins = top_3_wgs84[['cell_id', 'lon', 'lat', 'highway', 'priority']]

print(f"Storing {len(origins)} potential origins in DuckDB...")
origins_db = origins.copy()
con.execute("DROP TABLE IF EXISTS cell_origins")
con.execute("CREATE TABLE cell_origins AS SELECT * FROM origins_db")

print("Final 10 origins:")
print(origins.head(10))

# Visualization
import matplotlib.pyplot as plt
print("Saving visualization...")
fig, ax = plt.subplots(figsize=(15, 15))
grid.plot(ax=ax, facecolor='none', edgecolor='lightgray', linewidth=0.5)
roads[roads.priority < 10].plot(ax=ax, color='blue', alpha=0.3, linewidth=1)
top_3.plot(ax=ax, color='red', markersize=10, label='Entry Points')
ax.set_title("Grid Entry Points (Top 3 per cell boundary)")
plt.savefig("entry_points_preview.png", dpi=300)

con.close()
