# Step-by-Step Execution Guide

This guide explains how to set up the Valhalla routing engine and run the station enrichment script.

## Core Prerequisites
1.  **Docker**: Must be installed and running on your system.
2.  **OSM Data**: Ensure `portugal-latest.osm.pbf` is inside your `Enable/data/` folder.
3.  **Virtual Environment**: Ensure your `.venv` is active or you have the required packages (`requests`, `pandas`, `duckdb`, `numpy`) installed.

---

## Step 1: Set up the Valhalla Server
Run these commands in your terminal. Pay close attention to the folders.

### A. Navigate to the `src` folder
```bash
cd "/home/joao-martins/Desktop/code/Enable Mobility/code/Enable/src"
```

### B. Create the data directory for Valhalla
This creates a folder inside `data` to store the routing tiles.
```bash
mkdir -p ../data/valhalla_data/valhalla_tiles
```

### C. Generate the Valhalla Configuration
This command asks Docker to create a `valhalla.json` file with all the settings.
```bash
docker run --rm ghcr.io/valhalla/valhalla:latest valhalla_build_config --mjolnir-tile-dir /data/valhalla_data/valhalla_tiles --mjolnir-traffic-extract /data/valhalla_data/traffic.tar --mjolnir-admin /data/valhalla_data/admin.sqlite > ../data/valhalla_data/valhalla.json
```

### D. Build the Routing Tiles
**This is the most important step.** It "reads" your map file (`portugal-latest.osm.pbf`) and creates the navigation graph. It may take 5–10 minutes.
```bash
docker run --rm -v "$(pwd)/../data:/data" ghcr.io/valhalla/valhalla:latest valhalla_build_tiles -c /data/valhalla_data/valhalla.json /data/portugal-latest.osm.pbf
```

### E. Start the Valhalla Server
This starts the server in the "background" (the `-d` flag).
```bash
docker run -d --name valhalla -p 8002:8002 -v "$(pwd)/../data:/data" ghcr.io/valhalla/valhalla:latest valhalla_service /data/valhalla_data/valhalla.json 1
```
*Note: You can verify it is running by typing `docker ps`.*

---

docker start valhalla

docker stop valhalla

Remove it entirely: docker rm -f valhalla (You would only do this if you wanted to change the configuration or update the OSM data).


## Troubleshooting
*   **"Connection Refused"**: The Valhalla server hasn't finished starting or Step 1E failed. Check `docker logs valhalla`.
*   **"File Not Found"**: Double-check that your `portugal-latest.osm.pbf` is exactly in the `data/` folder (not `data/valhalla_data/`).
*   **Updating POIs**: If you want to change the features, simply edit `data/POI_FULL_LIST.txt` and run Step 2B again.
