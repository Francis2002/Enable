import json
import subprocess
import os

districts_file = "districts.json"
pbf_path = "portugal-latest.osm.pbf"
db_path = "osm_analysis.db"

# Clear DB if we want a fresh run (optional)
# if os.path.exists(db_path): os.remove(db_path)

def run_batch():
    if not os.path.exists(districts_file):
        print(f"Error: {districts_file} not found.")
        return

    with open(districts_file, "r") as f:
        districts = json.load(f)

    print(f"Starting batch process for {len(districts)} districts...")
    
    for name, bbox in districts.items():
        print(f"\n>>> PROCESSING: {name}")
        # Convert bbox to strings for subprocess
        bbox_str = [str(x) for x in bbox]
        
        cmd = [
            "python3", "analyze_district.py",
            "--district", name,
            "--bbox"
        ] + bbox_str + [
            "--pbf", pbf_path,
            "--db", db_path
        ]
        
        try:
            subprocess.run(cmd, check=True)
            print(f">>> COMPLETED: {name}")
        except subprocess.CalledProcessError as e:
            print(f">>> ERROR in {name}: {e}")

if __name__ == "__main__":
    run_batch()
