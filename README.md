# Portugal Spatial Analysis Pipeline

A decoupled, high-resolution spatial data analysis pipeline for Portugal. This project combines OpenStreetMap (OSM) infrastructure data with official INE Census 2021 statistics, aligned to a standardized European 1km x 1km grid (EPSG:3035).

## 📂 Project Structure

- `data/`: (Not in git) Store your `.osm.pbf`, `.gpkg`, and the resulting `.db` here.
- `src/`: Core analysis and processing scripts.
- `database_cleaning/`: Database sanitization and schema refinement tools.
- `images/`: Visualization scripts and analysis previews.
- `inspect_db.py`: Quick utility to check database status and row counts.

## 🚀 Getting Started

### 1. Requirements
Ensure you have Python 3.9+ installed. It is recommended to use a virtual environment:

```bash
python3 -m venv venv
source venv/bin/activate
pip install pyrosm duckdb geopandas matplotlib pyproj shapely requests
```

### 2. Data Acquisition
Due to file sizes, raw data is not included in the repository. You must place the following in the `data/` folder:
1.  **OSM Data**: Download `portugal-latest.osm.pbf` from [Geofabrik](https://download.geofabrik.de/europe/portugal.html).
2.  **Census Data**: Obtain `GRID1K21_CONT.gpkg` (INE Portugal - Censos 2021).

### 3. Execution Pipeline
Run the scripts in the following order to build your database from scratch:

#### Step A: Initialize the Grid
Generates the official EEA 1km grid spine for mainland Portugal.
```bash
cd src
python3 create_grid_spine.py
```

#### Step B: Process Census Data
Extracts high-resolution population and housing data from the INE GeoPackage.
```bash
python3 process_census.py
```

#### Step C: Process OSM Infrastructure
Extracts road networks, POIs, and land use features block-by-block (10x10km).
```bash
python3 orchestrate_blocks.py
```

#### Step D: Valhalla Routing (Phase 6)
To calculate real-world travel times, we use the Valhalla routing engine.

**1. Setup Valhalla (Docker)**
Ensure Docker is installed and running, then execute:
```bash
# 1. Prepare directory
mkdir -p ../data/valhalla_data/valhalla_tiles

# 2. Get Config
docker run --rm ghcr.io/valhalla/valhalla:latest valhalla_build_config --mjolnir-tile-dir /data/valhalla_data/valhalla_tiles --mjolnir-traffic-extract /data/valhalla_data/traffic.tar --mjolnir-admin-extract /data/valhalla_data/admin.sqlite > ../data/valhalla_data/valhalla.json

# 3. Build Tiles (This takes a few minutes)
docker run --rm -v "$(pwd)/../data:/data" ghcr.io/valhalla/valhalla:latest valhalla_build_tiles -c /data/valhalla_data/valhalla.json /data/portugal-latest.osm.pbf

# 4. Start Server
docker run -d --name valhalla -p 8002:8002 -v "$(pwd)/../data:/data" ghcr.io/valhalla/valhalla:latest valhalla_service /data/valhalla_data/valhalla.json 1
```

**2. Prepare Origins**
If you already ran Step C, update your database with internal centroids:
```bash
python3 backfill_internal_origins.py
```

**3. Calculate Travel Matrix**
Computes entries for `data/travel_matrix.db` (filtered to < 60km Euclidean, < 30min Travel).
```bash
python3 calculate_travel_matrix.py
```

## 📊 Viewing Results
From the `src` directory, run the inspection utilities:

**General Database Check:**
```bash
python3 ../inspect_db.py
```

**Travel Matrix & Coverage Check:**
```bash
python3 inspect_matrix.py
```

## 🛠️ Internal Logic
- **Decoupled Architecture**: Data from different sources is stored in separate tables joinable by a unique `cell_id`.
- **EEA Standard**: Grid cells are aligned to EPSG:3035.
- **Valhalla Integration**: Uses a local Dockerized instance for high-performance routing without external API costs.
- **Multi-Origin Strategy**: Routes from both boundary entry points and internal centroids for maximum accuracy.
