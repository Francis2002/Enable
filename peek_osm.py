from pyrosm import OSM
import os

# Path to the PBF file
pbf_path = "portugal-latest.osm.pbf"

if not os.path.exists(pbf_path):
    print(f"Error: {pbf_path} not found.")
else:
    print(f"Inspecting: {pbf_path}")
    print(f"File size: {os.path.getsize(pbf_path) / (1024*1024):.2f} MB")
    
    # Initialize the OSM parser
    osm = OSM(pbf_path)
    
    # Peek at the bounding box
    print(f"Bounding Box: {osm.bounding_box}")
    
    # Get some info about the file if possible (pyrosm doesn't have a direct 'summary' but we can check the conf)
    print("Parser initialized successfully.")
