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
pip install pyrosm duckdb geopandas matplotlib pyproj shapely
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
Extracts road networks, POIs, and land use features block-by-block (10x10km) to manage memory.
```bash
python3 orchestrate_blocks.py
```

## 📊 Viewing Results
From the root directory, run the inspection utility to verify table counts and previews:
```bash
python3 inspect_db.py
```

## 🛠️ Internal Logic
- **Decoupled Architecture**: Data from different sources is stored in separate tables joinable by a unique `cell_id`.
- **EEA Standard**: Grid cells are mathematically aligned to the official European reference (RES1000m).
- **Auto-Sanitization**: Scripts automatically exclude inaccessible cells (no roads) and default missing values to `0.0`.
