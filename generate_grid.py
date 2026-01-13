from pyrosm import OSM
import geopandas as gpd
from shapely.geometry import box
import numpy as np
import duckdb
import os

pbf_path = "portugal-latest.osm.pbf"
db_path = "osm_analysis.db"

print("Extracting Portugal boundary...")
osm = OSM(pbf_path)
boundaries = osm.get_boundaries()

# Filter for the national boundary (admin_level=2)
portugal = boundaries[boundaries["admin_level"] == "2"]

if portugal.empty:
    print("National boundary not found, using full PBF extent...")
    minx, miny, maxx, maxy = osm.bounding_box
    portugal = gpd.GeoDataFrame({"geometry": [box(minx, miny, maxx, maxy)]}, crs="EPSG:4326")
else:
    # Keep only the largest polygon (mainland) if needed or just take the whole thing
    portugal = portugal.dissolve()

# Reproject to EPSG:3035 (European Grid Standard)
print("Reprojecting to EPSG:3035...")
portugal_3035 = portugal.to_crs("EPSG:3035")
bounds = portugal_3035.total_bounds

# Create 1km grid
print("Generating 1km grid cells...")
xmin, ymin, xmax, ymax = bounds
# Align to 1000m multiples
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

grid = gpd.GeoDataFrame({"geometry": grid_cells}, crs="EPSG:3035")
grid["cell_id"] = [f"GRID_{int(c.centroid.x)}_{int(c.centroid.y)}" for c in grid.geometry]

# Clip grid to Portugal's shape
print("Clipping grid to Portugal boundary...")
grid_clipped = gpd.clip(grid, portugal_3035)

print(f"Generated {len(grid_clipped)} grid cells.")

# Store in DuckDB
print(f"Storing grid in {db_path}...")
con = duckdb.connect(db_path)
con.execute("INSTALL spatial; LOAD spatial;")

# Convert to WKT for DuckDB spatial if needed, or just use GeoArrow if con handles it
# DuckDB can read GeoPandas directly
con.execute("CREATE TABLE IF NOT EXISTS grid AS SELECT * FROM grid_clipped")

print("Done generating grid!")
con.close()
