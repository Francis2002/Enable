from pyrosm import OSM
import json

pbf_path = "portugal-latest.osm.pbf"
print(f"Extracting district boundaries from {pbf_path}...")

osm = OSM(pbf_path)
# Admin level 4 is usually districts in Portugal (Distritos)
boundaries = osm.get_boundaries()
districts = boundaries[boundaries["admin_level"] == "4"]

if districts.empty:
    print("No districts (admin_level=4) found. Trying admin_level=3 or 6...")
    districts = boundaries[boundaries["admin_level"].isin(["3", "6"])]

result = {}
for idx, row in districts.iterrows():
    name = row.get("name", f"District_{idx}")
    bbox = list(row.geometry.bounds) # (minx, miny, maxx, maxy)
    result[name] = bbox
    print(f"Found {name}: {bbox}")

with open("districts.json", "w") as f:
    json.dump(result, f, indent=4)

print(f"\nSaved {len(result)} districts to districts.json")
