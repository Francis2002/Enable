import json
import os
import re
from shapely.geometry import Point
import time
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from geopy.exc import GeocoderUnavailable, GeocoderTimedOut

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CACHE_PATH = os.path.join(SCRIPT_DIR, "../../data/03_interim/geocoder_cache.json")

# Load local cache
if os.path.exists(CACHE_PATH):
    with open(CACHE_PATH, 'r', encoding='utf-8') as f:
        LOCAL_PLACES = json.load(f)
else:
    LOCAL_PLACES = {}

# Set up Nominatim fallback
geolocator = Nominatim(user_agent="ev_mobility_mapper_v2")

def clean_name(name):
    # Remove some common prefixes/suffixes that might not be in OSM
    n = name.lower().strip()
    n = re.sub(r'^nó\s+(de\s+)?', '', n)
    n = re.sub(r'\s*\(.*?\)$', '', n) # Remove anything in parens
    return n.strip()

def extract_highways(name):
    """Finds highway references like A2, A6 in string 'Nó A2/A6'"""
    return re.findall(r'A\d+(?:-\d+)?', name.upper())

def geocode_fallback(query):
    """Safe Nominatim request with backoff"""
    for attempt in range(3):
        try:
            time.sleep(2) # Prevent rate limiting
            loc = geolocator.geocode(query, timeout=10)
            if loc:
                return Point(loc.longitude, loc.latitude)
        except (GeocoderUnavailable, GeocoderTimedOut):
            time.sleep(5 * (attempt + 1))
    return None

def get_geometric_intersection(hw1, hw2):
    """Calculates the physical intersection of two highways"""
    try:
        print(f"    [Geoc] Importing get_highway_geometry_from_pbf...", flush=True)
        from extract_highway import get_highway_geometry_from_pbf
        pbf_path = os.path.join(SCRIPT_DIR, "../../data/01_raw/portugal-latest.osm.pbf")
        print(f"    [Geoc] Getting geom for {hw1}...", flush=True)
        geom1 = get_highway_geometry_from_pbf(pbf_path, hw1)
        print(f"    [Geoc] Getting geom for {hw2}...", flush=True)
        geom2 = get_highway_geometry_from_pbf(pbf_path, hw2)
        print(f"    [Geoc] Geoms loaded. Building KDTree...", flush=True)
        
        if geom1 and geom2:
            # Use cKDTree for fast intersection/nearest point finding to avoid Shapely hang
            import numpy as np
            from scipy.spatial import cKDTree
            
            coords1 = [pt for line in (geom1.geoms if hasattr(geom1, 'geoms') else [geom1]) for pt in line.coords]
            coords2 = [pt for line in (geom2.geoms if hasattr(geom2, 'geoms') else [geom2]) for pt in line.coords]
            
            if not coords1 or not coords2:
                return None
                
            tree = cKDTree(coords2)
            dists, idxs = tree.query(coords1)
            min_idx = np.argmin(dists)
            if dists[min_idx] < 0.01: # Within ~1km
                print(f"    [Geoc] Intersection found!", flush=True)
                return Point(coords1[min_idx])
    except Exception as e:
        print(f"    Failed to calculate intersection: {e}")
    return None

def geocode_junction(node_name, highway_ref, fallback_dict={}):
    """Main geocoding logic."""
    if node_name in fallback_dict:
        lon, lat = fallback_dict[node_name]
        return Point(lon, lat)
        
    cleaned = clean_name(node_name)
    
    # 1. Try exact match in local offline cache
    if cleaned in LOCAL_PLACES:
        lon, lat = LOCAL_PLACES[cleaned]
        return Point(lon, lat)
        
    # 2. Try partial match in local offline cache (e.g. 'Grândola' inside 'Grândola Norte')
    for cache_name in LOCAL_PLACES:
        if cleaned in cache_name and len(cleaned) > 4:
            lon, lat = LOCAL_PLACES[cache_name]
            return Point(lon, lat)
            
    # 3. Geometric intersection (e.g. "A2/A6/A13")
    hws = extract_highways(node_name)
    if len(hws) >= 2:
        hw1 = highway_ref if highway_ref in hws else hws[0]
        other_hws = [h for h in hws if h != hw1]
        if other_hws:
            hw2 = other_hws[0]
            print(f"    🧮 Calculating geometric intersection between {hw1} and {hw2}...")
            pt = get_geometric_intersection(hw1, hw2)
            if pt:
                return pt

    # 4. Fallback to Internet (Nominatim)
    print(f"    🌐 Local search failed for '{node_name}'. Falling back to internet API...")
    queries = [
        f"{node_name}, Portugal",
        f"{cleaned}, Portugal",
        f"Nó {node_name}, Portugal"
    ]
    
    for query in queries:
        pt = geocode_fallback(query)
        if pt:
            return pt
            
    print(f"  ⚠️ Warning: Could not geocode '{node_name}' anywhere.")
    return None
