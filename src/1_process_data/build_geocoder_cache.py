import osmium
import json
import os

PBF_PATH = os.path.join(os.path.dirname(__file__), "../../data/01_raw/portugal-latest.osm.pbf")
CACHE_PATH = os.path.join(os.path.dirname(__file__), "../../data/geocoder_cache.json")

class PlaceHandler(osmium.SimpleHandler):
    def __init__(self):
        super(PlaceHandler, self).__init__()
        self.places = {}

    def add_place(self, name, lon, lat):
        if not name: return
        name = name.lower().strip()
        # Keep the first encountered (or could collect all and average, but first is usually okay)
        if name not in self.places:
            self.places[name] = (lon, lat)

    def node(self, n):
        tags = n.tags
        if 'name' in tags:
            is_poi = ('place' in tags or 
                      tags.get('highway') in ['motorway_junction', 'toll_gantry'] or 
                      tags.get('barrier') == 'toll_booth' or
                      'railway' in tags)
            if is_poi:
                self.add_place(tags['name'], n.location.lon, n.location.lat)
                
        # Also store refs for junctions (e.g. ref="7" on a junction)
        if tags.get('highway') == 'motorway_junction':
            if 'name' in tags:
                self.add_place(tags['name'], n.location.lon, n.location.lat)
            # Sometimes name is not present but ref is, though hard to map to traffic text
            
        # Also store toll booths
        if tags.get('barrier') == 'toll_booth' and 'name' in tags:
            self.add_place(tags['name'], n.location.lon, n.location.lat)

if __name__ == '__main__':
    print("Building local geocoder cache from PBF...")
    handler = PlaceHandler()
    handler.apply_file(PBF_PATH)
    
    with open(CACHE_PATH, 'w', encoding='utf-8') as f:
        json.dump(handler.places, f, ensure_ascii=False, indent=2)
    print(f"✅ Cache built with {len(handler.places)} places and saved to {CACHE_PATH}")
