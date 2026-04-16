# Project TODOs

## High Priority
- [ ] **Spatial Join**: Associate EV stations (`data/04_processed/pre_ml.db`) with highway traffic data (`data/03_interim/highway_traffic.gpkg`). 
        Theres a branch with the idea implemented, understand how it works  
- [ ] **Push data to Supabase**
- [ ] **Label Extraction** How to extract data from mobie
- [ ] **Traffic data to station insights** How to convert the traffic on the highways to a table , idea: craete a table with 2 entries to the distance to each highway (if < threshold) and the traffic that goes by (if 1 segment that value if 2 average)

## To brainstorm 
- [ ] Check the last batch of traffic not all are good (go back to the beginning)
- [ ] How are we planning on collecting data from continente and tesla?
- [] How to deal with the fact that highways are 2 sided to measure the traffic to a station


- [ ] **Fix Highway Traffic Data Extraction**:
    *   Update `src/1_highway_mapping/create_highway_traffic_db.py` to extract and save individual monthly traffic data for 2024 and Q1 2025.
    *   **Crucial naming convention**: Use month abbreviations (e.g., `tmdm_2024_jan`, `tmdm_2024_fev`, `tmdm_2025_mar`).
    *   **Critical**: Keep the `avg_tmdm_2024` column so that the plotting scripts do not break.
    *   **Final Step**: Once highway corrections are complete, rebuild `highway_traffic.gpkg` and rerun `push_highway_traffic_to_supabase.py` to sync all detailed monthly data.

