# Portugal Highway Traffic Pipeline Guide

This document explains the architecture, edge cases, and workflow for the geospatial pipeline that maps Portuguese highway traffic data (TMDM) to OpenStreetMap (OSM) geometries.

## 🏗️ Architecture

The pipeline reads JSON traffic data provided by IMT, geocodes the start and end nodes of every sublanço (segment), and maps them to physical coordinates using OpenStreetMap (PBF) data.

To ensure **100% accuracy** and prevent lines from detouring through secondary roads or doing massive U-turns, we use a specialized approach:
1. **Isolated Routing Graphs**: Instead of routing through the entire Portuguese road network, the code extracts *only* the specific highway requested (e.g., "A1") from the PBF file and builds an isolated mini-graph. This guarantees the route cannot leave the highway.
2. **KDTree Geometric Intersections**: When looking for highway intersections (e.g., "A4/A24"), the system pulls the geometries for both highways and uses a scipy `cKDTree` to instantly find the mathematical point where the two shapes overlap, circumventing API limits and Shapely hangs.
3. **Straight-line Gap Bridging**: OSM data often contains tiny topological gaps (e.g., a node missing a tag). The isolated graph logic detects these gaps and bridges them with a straight line, keeping the segment contiguous without failing.

## 🐛 Common Issues and How to Fix Them

When adding new highways, you might encounter visual bugs due to ambiguous naming in the IMT JSON data. 

### 1. The "Jump" (Line draws backward / Red below Green)
*   **Cause:** The geocoding API matched a town name to the wrong district. For example, "Monsanto" on the A5 matched to the historic village in Castelo Branco instead of the Lisbon exit. The router forces the path to stay on the A5, so it defaults to the start of the highway, causing overlapping lines.
*   **Fix:** Find the exact coordinates of the correct exit on Google Maps or OSM, and add it to the `EXTRA_FALLBACKS` dictionary in `src/1_highway_mapping/extra_fallbacks.py`.

### 2. The "Gap" (Missing Sublanços)
*   **Cause:** This happens when either a fallback coordinate is slightly off, causing a sublanço to be mathematically 0 meters long, or when an OSM tag ends prematurely (e.g., the "A6" OSM tag stopped 1km short of the Caia border).
*   **Fix:** Run `analyze_sublancos.py` to see which sublanços are missing or tiny. Adjust the fallback coordinates in `extra_fallbacks.py` to snap the start of the next segment directly to the end of the previous one.

## 🚀 Step-by-Step Workflow for a New Highway

1. **Add the Data:**
   Drop the new JSON file (e.g., `a11_traffic_data.json`) into the `data/01_raw/highway_traffic/` folder.
   *(Make sure the previously completed highways are safely tucked away in `data/01_raw/highway_traffic/processed/` so they don't get rerun!)*

2. **Run the Pipeline:**
   Navigate to the mapping folder and execute the bash script:
   ```bash
   cd src/1_highway_mapping
   ./run_all_files.sh
   ```
   *This will process only the new JSON files, insert them into `data/03_interim/highway_traffic.gpkg`, and regenerate the maps.*

3. **Verify Contiguity:**
   Run the mathematical checker to ensure no gaps or jumps exist:
   ```bash
   python check_contiguous.py
   ```
   *If it reports gaps over ~0.005 degrees, investigate those specific junctions.*

4. **Verify Missing Segments:**
   Run the sublanço analyzer to ensure all segments mapped properly:
   ```bash
   python analyze_sublancos.py
   ```

5. **Fix & Rerun (If needed):**
   If you found errors, update `extra_fallbacks.py`. 
   To regenerate just that highway without breaking the others, run:
   ```bash
   python create_highway_traffic_db.py a11_traffic_data.json
   ./run_all_files.sh  # to update the plots
   ```

6. **Archive:**
   Once the plot looks perfect, manually move the new JSON file to the `processed/` folder.

---
**Core Files (Do Not Delete):**
*   `create_highway_traffic_db.py`: The main ETL engine.
*   `extract_highway.py`: The OSM routing logic.
*   `local_geocoder.py`: The junction and intersection finder.
*   `extra_fallbacks.py`: The hardcoded coordinate dictionary.
*   `plot_traffic.py`: The mapping script.
*   `run_all_files.sh`: Safe execution loop.
*   `check_contiguous.py` & `analyze_sublancos.py`: Verification tools.

## 🛣️ Mapping EV Stations to Highways (Distance & Traffic Matrix)

Once the highway geometries and traffic volumes (TMDM) are established, the next step is mapping all EV charging stations to all nearby highways to build a complete distance and traffic matrix.

**The Script:** `map_station_to_highway.py`
This script computes the actual driving distance (not straight-line) from each station to **every** highway located within a 10km radius.

### 🧠 The Routing Architecture

1. **Valhalla Routing Engine:** We use a locally hosted Valhalla Docker container (`ghcr.io/valhalla/valhalla:latest` on port 8002) loaded with Portugal OSM tiles.
2. **Highway Ramps (`mapped_ramps.gpkg`):** Instead of routing to the massive highway lines, we route to the highway entry/exit ramps to calculate realistic road access.
3. **Euclidean Pre-Filtering (Performance Optimization):** A 10km radius can contain hundreds of ramps, which crashes the routing engine if tested at once. To optimize, the script uses a spatial buffer (EPSG:3763) to find ramps within 10km. It then groups the ramps by highway and selects only the **3 closest ramps per highway** (using straight-line distance).
4. **Primary Matrix API (`/sources_to_targets`):** The pre-filtered ramps are sent to Valhalla's high-speed matrix API to find the true driving distance. The script saves the minimum road distance found for each highway.
5. **The Fallback Route API (`/route`):** If the matrix API fails (e.g., due to route complexity), the script automatically falls back to querying the detailed `/route` API.

### 📊 The Matrix Format

The final output is saved to the DuckDB database (`pre_ml.db`) as the `station_highway_matrix` table. 

- **Columns:** For every highway in Portugal (e.g., A1, A2, A5), the matrix contains two columns:
  - `{highway}_dist_m`: The driving distance to the highway's nearest ramp in meters.
  - `{highway}_traffic`: The average daily traffic (TMDM) of that highway.
- **Threshold & Missing Values:** If a highway does not have a ramp within **10km** of the station, the distance and traffic columns are set to `-1.0`.

### 🚀 Execution

Because routing ~7,100 stations takes some time, always run this script in the background:
```bash
nohup python map_station_to_highway.py > run_valhalla.log 2>&1 &
```
