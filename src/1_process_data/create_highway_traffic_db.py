import json
import os
import geopandas as gpd
from shapely.geometry import LineString, Point, MultiLineString
from shapely.ops import linemerge
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
import pandas as pd
import warnings
warnings.filterwarnings('ignore')

from extract_highway import get_highway_geometry_from_pbf

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_JSON = os.path.join(SCRIPT_DIR, "../../data/01_raw/a1_traffic_data.json")
OUTPUT_GPKG = os.path.join(SCRIPT_DIR, "../../data/highway_traffic.gpkg")
PBF_PATH = os.path.join(SCRIPT_DIR, "../../data/01_raw/portugal-latest.osm.pbf")


# Hardcoded A1 coordinates from your PoC script
A1_CENTRELINE = [
    (-9.1042, 38.7978), (-9.0850, 38.8125), (-9.0707, 38.8355), (-9.0580, 38.8470),
    (-9.0540, 38.8561), (-9.0400, 38.8720), (-9.0276, 38.8917), (-9.0100, 38.9120),
    (-8.9920, 38.9300), (-8.9880, 38.9401), (-8.9800, 38.9470), (-8.9760, 38.9530),
    (-8.9700, 38.9710), (-8.9640, 38.9950), (-8.9700, 39.0100), (-8.9757, 39.0225),
    (-8.9700, 39.0340), (-8.9680, 39.0458), (-8.9560, 39.0720), (-8.9475, 39.0968),
    (-8.9200, 39.1200), (-8.8800, 39.1400), (-8.8300, 39.1550), (-8.7880, 39.1690),
    (-8.7500, 39.1900), (-8.7100, 39.2150), (-8.6862, 39.2351), (-8.6600, 39.2650),
    (-8.6400, 39.2950), (-8.6250, 39.3200), (-8.6042, 39.3461), (-8.5800, 39.3800),
    (-8.5600, 39.4100), (-8.5400, 39.4450), (-8.5298, 39.4726), (-8.5450, 39.5050),
    (-8.5800, 39.5400), (-8.6100, 39.5750), (-8.6300, 39.5980), (-8.6730, 39.6180),
    (-8.6900, 39.6500), (-8.7100, 39.6800), (-8.7400, 39.7050), (-8.7700, 39.7200),
    (-8.8055, 39.7420), (-8.7900, 39.7700), (-8.7600, 39.8050), (-8.7100, 39.8400),
    (-8.6700, 39.8700), (-8.6400, 39.8950), (-8.6255, 39.9175), (-8.6200, 39.9500),
    (-8.6180, 39.9900), (-8.6200, 40.0200), (-8.6228, 40.0568), (-8.5900, 40.0700),
    (-8.5500, 40.0850), (-8.5200, 40.0920), (-8.5012, 40.0974), (-8.4800, 40.1100),
    (-8.4600, 40.1400), (-8.4400, 40.1680), (-8.4245, 40.1875), (-8.4340, 40.2030),
    (-8.4435, 40.2185), (-8.4480, 40.2500), (-8.4500, 40.2900), (-8.4520, 40.3300),
    (-8.4535, 40.3768), (-8.4600, 40.4200), (-8.4800, 40.4700), (-8.5000, 40.5200),
    (-8.5300, 40.5700), (-8.5640, 40.6215), (-8.5630, 40.9990)
]

FALLBACK_COORDS = {
    "Stª Iria de Azóia (A1/IC2)": (-9.0540, 38.8561),
    "Alverca (A1/A9)": (-9.0276, 38.8917),
    "Carregado Sul (A1/A10)": (-8.9757, 39.0225),
    "A1/A10": (-8.9757, 39.0225),
    "Santarém - A1/A15": (-8.6862, 39.2351), # Using Santarem coords
    "A1/A15 (Riachos)": (-8.6042, 39.3461),
    "A1/A15": (-8.6042, 39.3461),
    "Torres Novas (A1/A23)": (-8.5298, 39.4726),
    "Coimbra Norte (A1/A14)": (-8.4435, 40.2185),
    "Albergaria (A1/A25)": (-8.483, 40.697), # Approx
    "Espinho (A1/A41)": (-8.563, 40.999), # Approx
    "Vila Franca de Xira II": (-8.9880, 38.9401),
    "Vila Franca de Xira I":  (-8.9760, 38.9530),
    "Sacavém":                (-9.1042, 38.7978),
    "S. João da Talha":       (-9.0707, 38.8355),
    "Alverca":                (-9.0276, 38.8917),
    "Castanheira do Ribatejo":(-8.9640, 38.9950),
    "Carregado":              (-8.9680, 39.0458),
    "Aveiras de Cima":        (-8.9475, 39.0968),
    "Cartaxo":                (-8.7880, 39.1690),
    "Santarém":               (-8.6862, 39.2351),
    "Torres Novas":           (-8.5298, 39.4726),
    "Fátima":                 (-8.6730, 39.6180),
    "Leiria":                 (-8.8055, 39.7420),
    "Pombal":                 (-8.6255, 39.9175),
    "Soure":                  (-8.6228, 40.0568),
    "Condeixa":               (-8.5012, 40.0974),
    "Coimbra Sul":            (-8.4245, 40.1875),
    "Coimbra Norte":          (-8.4435, 40.2185),
    "Mealhada":               (-8.4535, 40.3768),
    "Aveiro Sul":             (-8.5640, 40.6215),
    "Feira":                  (-8.5445, 40.9258),
    "Estarreja":              (-8.5721, 40.7516),
    "Feiteira":               (-8.5312, 41.0267),
    "Carvalhos":              (-8.5833, 41.0711),
    "Jaca":                   (-8.6112, 41.1000),
    "Santo Ovídio":           (-8.6047, 41.1147),
    "Coimbrões":              (-8.6264, 41.1275),
    "Canidelo":               (-8.6419, 41.1350),
    "Afurada":                (-8.6472, 41.1417),
    "Arrábida":               (-8.6364, 41.1472),
}

def get_highway_geometry(highway_ref="A1"):
    print(f"📡 Extracting geometry dynamically from PBF for {highway_ref}...")
    geom = get_highway_geometry_from_pbf(PBF_PATH, highway_ref)
    if geom is None:
        raise ValueError(f"Could not extract geometry for {highway_ref}")
        
    if isinstance(geom, MultiLineString):
        # Force single LineString for easier slicing by taking the longest continuous segment
        geom = max(list(geom.geoms), key=lambda x: x.length)
        
    return geom

def geocode_junction(node_name, highway_ref="A1"):
    """Uses Nominatim to find the latitude/longitude of the junction with fallback."""
    # Check fallback first
    if node_name in FALLBACK_COORDS:
        lon, lat = FALLBACK_COORDS[node_name]
        return Point(lon, lat)
    
    geolocator = Nominatim(user_agent="ev_mobility_mapper")
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)
    
    # Try different query combinations
    queries = [
        f"{highway_ref} {node_name}, Portugal",
        f"Nó {node_name} {highway_ref}, Portugal",
        f"{node_name}, Portugal"
    ]
    
    for query in queries:
        location = geocode(query)
        if location:
            return Point(location.longitude, location.latitude)
            
    print(f"  ⚠️ Warning: Could not geocode '{node_name}'")
    return None

def slice_line(line, d0, d1):
    """Extract sub-linestring between two normalised distances."""
    if d0 > d1:
        d0, d1 = d1, d0
        
    coords = list(line.coords)
    start_pt = line.interpolate(d0, normalized=True)
    end_pt   = line.interpolate(d1, normalized=True)
    
    mid_pts  = [
        Point(c) for c in coords
        if d0 < line.project(Point(c), normalized=True) < d1
    ]
    all_pts = [start_pt] + mid_pts + [end_pt]
    return LineString([(p.x, p.y) for p in all_pts])

def process_traffic_data(input_json_path):
    if not os.path.exists(input_json_path):
        print(f"❌ Could not find {input_json_path}")
        return

    # 1. Load JSON data
    with open(input_json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
        
    highway_ref = data["autoestrada"] # "A1"
    all_sublancos = data.get("sublancosNorte", []) + data.get("sublancosNorte2", []) + data.get("sublancosSul", []) + data.get("sublancosCentro", []) + data.get("sublancos", [])
    
    # 2. Get the master line for the highway
    master_line = get_highway_geometry(highway_ref)
    
    segments = []
    print(f"\n🗺️ Processing {len(all_sublancos)} segments for {highway_ref}...")
    
    success_count = 0
    fail_count = 0
    
    for item in all_sublancos:
        # The JSON uses an en-dash " – " (not a regular hyphen)
        nodes = item['sublanco'].split(' – ')
        if len(nodes) != 2:
            print(f"  ⚠️ Could not parse sublanco: {item['sublanco']}")
            fail_count += 1
            continue
            
        start_node = nodes[0].strip()
        end_node = nodes[1].strip()
        
        # Calculate Q1 Average for 2025
        jan = item['2025']['Jan']
        fev = item['2025']['Fev']
        mar = item['2025']['Mar']
        avg_q1_tmdm = (jan + fev + mar) / 3.0
        
        print(f"📍 Geocoding: {start_node} -> {end_node}")
        
        # 3. Geocode junctions
        start_pt = geocode_junction(start_node, highway_ref)
        end_pt = geocode_junction(end_node, highway_ref)
        
        if not start_pt or not end_pt:
            fail_count += 1
            continue
            
        # 4. Snap to master line
        d0 = master_line.project(start_pt, normalized=True)
        d1 = master_line.project(end_pt, normalized=True)
        
        # Prevent zero-length slices if geocoder failed and returned same points
        if abs(d0 - d1) < 0.0001:
            print(f"  ⚠️ Warning: {start_node} and {end_node} snapped to same location.")
            fail_count += 1
            continue
            
        # 5. Slice geometry
        segment_geom = slice_line(master_line, d0, d1)
        
        segments.append({
            "highway": highway_ref,
            "sublanco": item['sublanco'],
            "start_node": start_node,
            "end_node": end_node,
            "avg_tmdm_2025_q1": round(avg_q1_tmdm),
            "geometry": segment_geom
        })
        success_count += 1

    # 6. Save to GeoPackage
    print(f"\n💾 Saving {len(segments)} segments to GeoPackage...")
    if len(segments) > 0:
        gdf = gpd.GeoDataFrame(segments, geometry="geometry", crs="EPSG:4326")
        
        # Append to GPKG if it exists, otherwise create new
        mode = 'a' if os.path.exists(OUTPUT_GPKG) else 'w'
        
        gdf.to_file(OUTPUT_GPKG, layer=highway_ref, driver="GPKG", mode=mode)
        print(f"✅ Success! Saved {success_count} segments. ({fail_count} failed) to {OUTPUT_GPKG}")
    else:
        print("❌ No segments were successfully processed. GeoPackage not created.")

if __name__ == "__main__":
    raw_dir = os.path.join(SCRIPT_DIR, "../../data/01_raw")
    json_files = [f for f in os.listdir(raw_dir) if f.endswith('_traffic_data.json')]
    
    # Delete the old database to prevent overlapping/duplicate lines (Fixing the "two lines" issue)
    if os.path.exists(OUTPUT_GPKG):
        print(f"🧹 Removing old database: {OUTPUT_GPKG} to prevent duplicates.")
        os.remove(OUTPUT_GPKG)

    for jf in json_files:
        json_path = os.path.join(raw_dir, jf)
        print(f"\n🚀 Processing file: {jf}")
        process_traffic_data(json_path)
