import duckdb
import pandas as pd
import numpy as np
import os

# Configuration
PRE_ML_DB = "../../data/pre_ml.db"
OUTPUT_REPORT = "../../data/pre_ml_analysis_report.txt"

def analyze_pre_ml():
    print(f"Analyzing {PRE_ML_DB}...")
    
    if not os.path.exists(PRE_ML_DB):
        print("Error: Database not found.")
        return

    con = duckdb.connect(PRE_ML_DB, read_only=True)
    
    # Get all tables
    tables = con.execute("SHOW TABLES").df()
    if tables.empty:
        print("No tables found in database.")
        con.close()
        return

    report = []
    report.append("=== PRE-ML DATABASE ANALYSIS REPORT ===")
    report.append(f"Source: {PRE_ML_DB}\n")

    for table_name in tables['name']:
        report.append(f"\n--- TABLE: {table_name} ---")
        df = con.execute(f"SELECT * FROM {table_name}").df()
        
        total_rows = len(df)
        report.append(f"Total rows: {total_rows}")
        
        # ADDED: First 5 rows example
        report.append("\nDATA EXAMPLES (First 5 rows):")
        report.append(df.head(5).to_string(index=False))
        report.append("-" * 40)

        # Analysis logic
        empty_columns = []
        feature_stats = []
        
        for col in df.columns:
            if col == 'station_id': continue
            
            raw_data = df[col]
            if "_count" in col: null_val = 0
            elif "_dist" in col: null_val = -1
            else: null_val = None

            if null_val is not None:
                null_pct = (raw_data == null_val).mean() * 100
            else:
                null_pct = raw_data.isna().mean() * 100
                
            if null_pct == 100:
                empty_columns.append(col)
            else:
                if "_count" in col:
                    valid = raw_data[raw_data > 0]
                    stats = f"Avg: {valid.mean():.2f}, Max: {valid.max()}" if not valid.empty else "N/A"
                elif "_dist" in col:
                    valid = raw_data[raw_data > 0]
                    stats = f"Avg: {valid.mean():.1f}m, Min: {valid.min():.1f}m" if not valid.empty else "N/A"
                else:
                    unique_count = raw_data.nunique()
                    stats = f"Unique values: {unique_count}"
                
                feature_stats.append(f"{col:<35} | Population: {100-null_pct:>5.1f}% | {stats}")

        if empty_columns:
            report.append(f"\n[!] EMPTY COLUMNS ({len(empty_columns)} columns with 0% data):")
            for col in sorted(empty_columns)[:15]:
                report.append(f" - {col}")
            if len(empty_columns) > 15: report.append(f" ... and {len(empty_columns)-15} more.")
        else:
            report.append("\n[✓] All columns have at least some data.")

        report.append("\nDATA DISTRIBUTION SUMMARY:")
        report.append("-" * 85)
        display_limit = 40
        for stat in sorted(feature_stats)[:display_limit]:
            report.append(stat)

    con.close()

    final_report = "\n".join(report)
    with open(OUTPUT_REPORT, "w") as f:
        f.write(final_report)
    
    print(f"\nAnalysis complete. Full report saved to: {OUTPUT_REPORT}")

if __name__ == "__main__":
    analyze_pre_ml()
