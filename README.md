# Portugal Spatial Analysis Pipeline

A decoupled, high-resolution spatial data analysis pipeline for Portugal. This project combines OpenStreetMap (OSM) infrastructure data with official INE Census 2021 statistics, aligned to a standardized European 1km x 1km grid (EPSG:3035).

## 📂 Project Structure

- `data/`: (Not in git) Store your `.osm.pbf`, `.gpkg`, and the resulting `.db` here.
- `src/`: Core analysis and processing scripts.
- `database_cleaning/`: Database sanitization and schema refinement tools.
- `inspect_db.py`: Quick utility to check database status and row counts.

## 🚀 Getting Started

### 1. Requirements
Ensure you have Python 3.9+ installed. It is recommended to use a virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Data Acquisition
Due to file sizes, raw data is not included in the repository. You must place the following in the `data/` folder:
1.  **OSM Data**: Download `portugal-latest.osm.pbf` from [Geofabrik](https://download.geofabrik.de/europe/portugal.html).
2.  **Census Data**: Obtain `GRID1K21_CONT.gpkg` (INE Portugal - Censos 2021).

### 3. Execution Pipeline
Run the scripts in the following order to build your database from scratch:

#### Step A: Initialize the Grid & OSM Base DB
Generates the official EEA 1km grid spine for mainland Portugal and processes OSM blocks.
```bash
cd src/0_generate_base_db
python3 create_grid_spine.py
python3 analyze_blocks.py
```

#### Step B: Highway Traffic Mapping (Phase 1)
Extracts highway coordinates from OSM and maps them to traffic values from IMT.
```bash
cd src/1_highway_mapping
./run_all_files.sh
```

#### Step C: Process Data Pipeline (Phase 2)
Navigate to `2_station_enrichment` to enrich the EV station information.
```bash
cd ../2_station_enrichment
python3 mobie_station_data_correction.py
python3 enrich_station_configuration.py
python3 enrich_station_indicators.py
```

#### Step D: Valhalla Routing (Phase 6)
To calculate real-world travel times, we use the Valhalla routing engine.

**1. Setup Valhalla (Docker)**
Ensure Docker is installed and running, then execute:
```bash
# 1. Prepare directory
mkdir -p ../../data/03_interim/valhalla_data/valhalla_tiles

# 2. Get Config
docker run --rm ghcr.io/valhalla/valhalla:latest valhalla_build_config --mjolnir-tile-dir /data/03_interim/valhalla_data/valhalla_tiles --mjolnir-traffic-extract /data/03_interim/valhalla_data/traffic.tar --mjolnir-admin /data/03_interim/valhalla_data/admin.sqlite > ../../data/03_interim/valhalla_data/valhalla.json

# 3. Build Tiles (This takes a few minutes)
docker run --rm -v "$(pwd)/../../data:/data" ghcr.io/valhalla/valhalla:latest valhalla_build_tiles -c /data/03_interim/valhalla_data/valhalla.json /data/01_raw/portugal-latest.osm.pbf

# 4. Start Server
docker run -d --name valhalla -p 8002:8002 -v "$(pwd)/../../data:/data" ghcr.io/valhalla/valhalla:latest valhalla_service /data/03_interim/valhalla_data/valhalla.json 1
```

#### Step E: Calculate Distances (Valhalla Required)
With the Valhalla server running, execute:
```bash
python3 enrich_station_distances.py
python3 clean_empty_features.py
python3 analyze_pre_ml.py
```

## 🤖 Mobi.E Labels Extraction (Cron Jobs)

This project requires real-time EV charging station status data (labels) from the Mobi.E map to estimate kWh consumption. To automatically extract this data, we have extraction scripts designed to run in the background via `cron`. Currently, only the production stealth extractor is active:

1. **Production Stealth Extractor (`src/stealth/mobie_label_extraction_cron.py`)**: 
   - **What it does:** Bypasses advanced bot detection (WAF) by running native Google Chrome directly on a physical Dummy HDMI display (`DISPLAY=:0`) to pass hardware acceleration and WebGL checks. It uses `ActionChains` for human-like mouse movements and zooms out to capture all ~15,900 sockets in mainland Portugal.
   - **Frequency:** Runs every 5 minutes.
   - **Output:** Saves CSV files to `data/production/mobie_labels/`.
   - **Manual Run:** `DISPLAY=:0 XAUTHORITY=~/.Xauthority .venv/bin/python src/stealth/mobie_label_extraction_cron.py`

2. **Legacy Extractor (`src/stealth/mobie_legacy_xvfb_extractor.py`)**: 
   - **What it does:** The original headless fallback script running inside an `xvfb` virtual frame buffer. *(Currently disabled as it triggers bot detection)*.
   - **Frequency:** Used to run every 9 minutes.
   - **Output:** Saved CSV files to `data/raw/mobie_labels/`.
   - **Manual Run:** `xvfb-run -a .venv/bin/python src/stealth/mobie_legacy_xvfb_extractor.py`

**How to check if they are running:**
Both scripts are fully automated via crontab. To view the active schedule, run:
```bash
crontab -l
```

You can monitor their activity by checking their respective log files:
```bash
tail -f data/production/mobie_labels/cron.log
tail -f data/production/mobie_labels/extraction.log
```

## 🔄 Database Synchronization (Supabase)

To facilitate collaboration, the processed datasets (like `pre_ml.db`) can be pushed to and pulled from a centralized Supabase PostgreSQL database. This allows team members to share up-to-date data without committing large binary files to Git.

**1. Setup Environment**
Create a `.env` file in the root directory (you can copy `.env.example`) and add the Supabase connection string:
```bash
SUPABASE_POSTGRES_URL=postgresql://postgres:[YOUR-PASSWORD]@db.[YOUR-PROJECT-REF].supabase.co:5432/postgres
```

**2. Pull Data from Supabase**
To download the latest tables from Supabase into your local `data/04_processed/pre_ml.db` file, run:
```bash
python3 src/5_db_sync/pull_from_supabase.py
```

**3. Push Data to Supabase**
If you have made local computations and want to update the shared database, run:
```bash
python3 src/5_db_sync/push_to_supabase.py
```

## 📊 Viewing Results
To explore the generated data and analysis, you can utilize the EDA notebooks and analysis scripts provided:

**Pre-Machine Learning Analysis:**
```bash
cd src/4_eda_pre_ml
jupyter notebook eda_features.ipynb
```

**General Exploratory Data Analysis (EDA):**
```bash
cd src/EDA
python3 generate_nb.py
jupyter notebook eda_basic.ipynb
```

## 🛠️ Internal Logic
- **Decoupled Architecture**: Data from different sources is stored in separate tables joinable by a unique `cell_id`.
- **EEA Standard**: Grid cells are aligned to EPSG:3035.
- **Valhalla Integration**: Uses a local Dockerized instance for high-performance routing without external API costs.
- **Multi-Origin Strategy**: Routes from both boundary entry points and internal centroids for maximum accuracy.
