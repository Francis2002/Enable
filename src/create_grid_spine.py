import geopandas as gpd
from shapely.geometry import box
import numpy as np
import duckdb
from pyproj import Transformer
import os

db_path = "../data/osm_analysis.db"

# 1. Define Mainland Portugal Bounding Box (Approx)
# Longitude: -9.5 to -6.2, Latitude: 36.9 to 42.2
min_lon, min_lat, max_lon, max_lat = -9.55, 36.95, -6.15, 42.20

print(f"Defining extent for Mainland Portugal: {min_lon, min_lat, max_lon, max_lat}")
extent_wgs84 = gpd.GeoDataFrame({"geometry": [box(min_lon, min_lat, max_lon, max_lat)]}, crs="EPSG:4326")

# 2. Project to Official EEA CRS (EPSG:3035)
print("Projecting to EPSG:3035 (ETRS89-LAEA)...")
extent_3035 = extent_wgs84.to_crs("EPSG:3035")
b = extent_3035.total_bounds

# 3. Align to 1000m Multiples (EEA Standard)
xmin = np.floor(b[0] / 1000) * 1000
ymin = np.floor(b[1] / 1000) * 1000
xmax = np.ceil(b[2] / 1000) * 1000
ymax = np.ceil(b[3] / 1000) * 1000

print(f"Grid Bounds (EPSG:3035): X[{xmin}:{xmax}], Y[{ymin}:{ymax}]")

# 4. Generate Cells
print("Generating 1km grid cells in EPSG:3035...")
xs = np.arange(xmin, xmax, 1000)
ys = np.arange(ymin, ymax, 1000)

grid_cells = []
cell_ids = []
x_coords = []
y_coords = []

for x in xs:
    for y in ys:
        grid_cells.append(box(x, y, x + 1000, y + 1000))
        cell_ids.append(f"RES1kmN{int(y/1000)}E{int(x/1000)}")
        x_coords.append(x)
        y_coords.append(y)

grid_gdf = gpd.GeoDataFrame({
    "cell_id": cell_ids,
    "x_3035": x_coords,
    "y_3035": y_coords,
    "geometry": grid_cells
}, crs="EPSG:3035")

print(f"Reprojecting {len(grid_gdf)} cells to WGS84...")
grid_wgs84 = grid_gdf.to_crs("EPSG:4326")

# Extract bounds for easy per-cell extraction in later steps
grid_wgs84['min_lon'] = grid_wgs84.geometry.bounds.minx
grid_wgs84['min_lat'] = grid_wgs84.geometry.bounds.miny
grid_wgs84['max_lon'] = grid_wgs84.geometry.bounds.maxx
grid_wgs84['max_lat'] = grid_wgs84.geometry.bounds.maxy

# Drop geometry to store as a clean "spine" table
df = grid_wgs84.drop(columns=['geometry'])

print(f"Generated {len(df)} cells.")

# 5. Store in DuckDB
print(f"Connecting to {db_path}...")
con = duckdb.connect(db_path)
con.execute("DROP TABLE IF EXISTS grid_spine")
con.execute("CREATE TABLE grid_spine AS SELECT * FROM df")

# 6. Verification
# Marquês de Pombal, Lisbon: 38.725, -9.15
v_lat, v_lon = 38.725, -9.15
print(f"\nVerification: Finding cell for Marquês de Pombal ({v_lat}, {v_lon})...")

# Find the cell that contains this point
# We can do this in DuckDB easily
verify_query = f"""
SELECT cell_id, x_3035, y_3035 
FROM grid_spine 
WHERE {v_lat} >= min_lat AND {v_lat} < max_lat 
  AND {v_lon} >= min_lon AND {v_lon} < max_lon
"""
v_result = con.execute(verify_query).fetchone()

if v_result:
    print(f"Result: Marquês de Pombal is in cell {v_result[0]}")
    print(f"Cell starts at X={v_result[1]}, Y={v_result[2]} in EPSG:3035")
else:
    print("Verification failed: Point not found in generated grid.")

con.close()
print("\nStep 1 Complete: Grid Spine created.")
