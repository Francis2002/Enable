from pyrosm import OSM
import geopandas as gpd
from shapely.geometry import box
import numpy as np
import duckdb
import os

pbf_path = "portugal-latest.osm.pbf"
db_path = "osm_analysis2.db"

# LISBON AREA
min_lon, min_lat, max_lon, max_lat = -9.25, 38.65, -9.05, 38.80
print(f"Generating grid for Lisbon area: {min_lon, min_lat, max_lon, max_lat}")

# Create extent in EPSG:4326 and reproject to 3035
extent_gdf = gpd.GeoDataFrame({"geometry": [box(min_lon, min_lat, max_lon, max_lat)]}, crs="EPSG:4326").to_crs("EPSG:3035")
bounds = extent_gdf.total_bounds

print("Generating 1km grid cells...")
xmin, ymin, xmax, ymax = bounds
xmin = np.floor(xmin / 1000) * 1000
ymin = np.floor(ymin / 1000) * 1000

rows = int(np.ceil((ymax - ymin) / 1000))
cols = int(np.ceil((xmax - xmin) / 1000))

grid_cells = []
for i in range(cols):
    for j in range(rows):
        x = xmin + i * 1000
        y = ymin + j * 1000
        grid_cells.append(box(x, y, x + 1000, y + 1000))

grid = gpd.GeoDataFrame({"geometry": grid_cells, "cell_id": range(len(grid_cells))}, crs="EPSG:3035")
# Assign a more readable ID
grid["cell_x"] = [int(c.centroid.x) for c in grid.geometry]
grid["cell_y"] = [int(c.centroid.y) for c in grid.geometry]
grid["cell_id"] = [f"GRID_{x}_{y}" for x, y in zip(grid.cell_x, grid.cell_y)]

print(f"Generated {len(grid)} cells for Lisbon.")

# Store in DuckDB
print(f"Storing grid in {db_path}...")
con = duckdb.connect(db_path)
con.execute("INSTALL spatial; LOAD spatial;")
con.execute("DROP TABLE IF EXISTS grid")

# Convert geometry to WKB for DuckDB storage
grid_db = grid.copy()
grid_db['geometry'] = grid_db['geometry'].to_wkb()
con.execute("CREATE TABLE grid AS SELECT * FROM grid_db")

print("Extracting roads for the same area...")
osm = OSM(pbf_path, bounding_box=[min_lon, min_lat, max_lon, max_lat])
roads = osm.get_network(network_type="driving")
roads = roads.to_crs("EPSG:3035")

print(f"Extracted {len(roads)} road segments.")

print("Overlaying roads with grid (calculating lengths per class)...")
# We only need highway and geometry
roads_minimal = roads[['highway', 'geometry']].copy()
# Intersection cuts the roads by the grid boundaries
intersections = gpd.overlay(roads_minimal, grid, how='intersection')

# Calculate length in meters (EPSG:3035 is in meters)
intersections['length_m'] = intersections.geometry.length

# Aggregation: Group by cell_id and highway (road class)
agg = intersections.groupby(['cell_id', 'highway'])['length_m'].sum().reset_index()

# Pivot to have one row per cell
result = agg.pivot(index='cell_id', columns='highway', values='length_m').fillna(0)
result['total_road_len'] = result.sum(axis=1)

# Calculate shares
for col in result.columns:
    if col not in ['total_road_len', 'cell_id']:
        result[f"{col}_share"] = result[col] / result['total_road_len']

print("Success! First results:")
print(result.head())

# Store results in DuckDB
result_reset = result.reset_index()
con.execute("DROP TABLE IF EXISTS road_stats")
con.execute("CREATE TABLE road_stats AS SELECT * FROM result_reset")

print(f"Results stored in 'road_stats' table in {db_path}")
con.close()
