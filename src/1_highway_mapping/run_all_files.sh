#!/bin/bash
echo "Starting Incremental ETL process..."

python_bin="../../.venv/bin/python"

# Only run if there are JSON files in the raw folder
shopt -s nullglob
files=(../../data/01_raw/highway_traffic/*_traffic_data.json)
if [ ${#files[@]} -eq 0 ]; then
    echo "No new JSON files found in ../../data/01_raw/highway_traffic/. Skipping extraction."
else
    for file in "${files[@]}"; do
        filename=$(basename "$file")
        echo "==================================="
        echo "Processing new highway: $filename"
        $python_bin -u create_highway_traffic_db.py "$filename"
    done
fi
shopt -u nullglob

echo "==================================="
echo "Generating fresh plots with all current data..."
$python_bin -u plot_traffic.py

echo "✅ Incremental ETL Process Finished!"
echo "Note: Once you verify the plots are correct, you can manually move the processed JSON files to ../../data/01_raw/highway_traffic/processed/"
