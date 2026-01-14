import requests
import json
import os

API_KEY = "24cbf479-01f6-4345-838b-1512267cb9d5"
COUNTRY_CODE = "PT"
OUTPUT_FILE = "../data/ocm_portugal_raw.json"

def fetch_ocm_data():
    url = "https://api.openchargemap.io/v3/poi/"
    params = {
        "key": API_KEY,
        "countrycode": COUNTRY_CODE,
        "maxresults": 10000,
        "compact": "false",
        "verbose": "true",
        "output": "json"
    }
    
    print(f"Fetching OCM data for {COUNTRY_CODE}...")
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        # Ensure data directory exists
        os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
        
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)
        
        print(f"Successfully fetched {len(data)} entries. Saved to {OUTPUT_FILE}")
        return data
    except Exception as e:
        print(f"Error fetching data: {e}")
        return None

if __name__ == "__main__":
    fetch_ocm_data()
