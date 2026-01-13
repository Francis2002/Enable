from pyrosm import OSM
import geopandas as gpd
import duckdb
import pandas as pd
import os

pbf_path = "portugal-latest.osm.pbf"
db_path = "osm_analysis.db"

print("Connecting to DuckDB and loading grid...")
con = duckdb.connect(db_path)
con.execute("LOAD spatial;")
grid = con.execute("SELECT * FROM grid").df()
# Convert from WKT or handle geometry column
# DuckDB spatial might return geometry as blob or string if not careful
# For now, let's assume we can fetch it. If it was created from gdf, it might be fine.
# Actually, I'll use geopandas to read the table if possible, or reconstruct it.

# Let's use a more robust way to load the grid with geometry
grid_gdf = gpd.read_file(db_path, layer="grid", engine="pyogrio") if os.path.exists(db_path) else None
# Wait, duckdb isn't a "file" that asily for gpd.read_file unless using specific drivers.
# I will use DuckDB to get it as a dataframe and then reconstruct.
# In generate_grid.py, I stored it as a table.

print("Extracting all roads from PBF (driving network)...")
osm = OSM(pbf_path)
roads = osm.get_network(network_type="driving")

print(f"Extracted {len(roads)} road segments.")

print("Reprojecting roads to EPSG:3035...")
roads = roads.to_crs("EPSG:3035")

# We need the grid as a GDF
# Reconstructing grid_gdf from grid df
# (Assuming the table has 'cell_id' and 'geometry' in WKB or similar)
# For now, I will just re-create a temporary grid GDF from the database data 
# or just use the one from the previous step if I can.

# Better: let's do the roads processing where the grid is already a variable.
# I'll combine the logic or ensure the DB read works.

# To simplify, I will write a script that assumes generate_grid.py just finished and grid_clipped is in memory,
# but since they are separate, I will make this script robust.

print("Performing spatial intersection (cutting roads by grid)...")
# Note: Use only relevant columns to save memory
roads_subset = roads[['highway', 'geometry']].copy()
# We'll need a way to relate back to cells
# roads_in_cells = gpd.overlay(roads_subset, grid_gdf, how='intersection')

# To be continued in the actual script execution...
