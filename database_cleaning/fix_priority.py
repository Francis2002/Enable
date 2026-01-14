import duckdb

db_path = "../data/osm_analysis.db"
con = duckdb.connect(db_path)

print("Checking cell_origins schema...")
cols = con.execute("DESCRIBE cell_origins").df()
if 'priority' not in cols['column_name'].values:
    print("Adding 'priority' column to cell_origins...")
    con.execute("ALTER TABLE cell_origins ADD COLUMN priority DOUBLE")

print("Backfilling priority values based on highway type...")
priority_query = """
UPDATE cell_origins
SET priority = CASE 
    WHEN highway = 'motorway' THEN 1.0
    WHEN highway = 'trunk' THEN 2.0
    WHEN highway = 'primary' THEN 3.0
    WHEN highway = 'secondary' THEN 4.0
    WHEN highway = 'tertiary' THEN 5.0
    WHEN highway = 'residential' THEN 6.0
    WHEN highway = 'unclassified' THEN 7.0
    WHEN highway = 'service' THEN 8.0
    ELSE 99.0
END
WHERE priority IS NULL
"""
con.execute(priority_query)

# Also ensure any other NULLs in cell_origins are handled (though lon/lat shouldn't be null)
con.execute("UPDATE cell_origins SET lon = COALESCE(lon, 0.0), lat = COALESCE(lat, 0.0) WHERE lon IS NULL OR lat IS NULL")

print("Verification of first 5 rows in cell_origins:")
print(con.execute("SELECT * FROM cell_origins LIMIT 5").df())

con.close()
print("Backfill complete.")
