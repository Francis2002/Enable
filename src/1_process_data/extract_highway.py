import osmium
import sys
from shapely.geometry import LineString, MultiLineString
from shapely.ops import linemerge

class HighwayWayHandler(osmium.SimpleHandler):
    def __init__(self, target_ref):
        super(HighwayWayHandler, self).__init__()
        self.target_ref = target_ref
        self.node_ids = set()
        self.ways = []

    def way(self, w):
        if 'highway' in w.tags and w.tags['highway'] in ['motorway', 'motorway_link', 'trunk', 'trunk_link']:
            if 'ref' in w.tags:
                # Normalize tags by removing spaces and splitting by semicolon
                # OSM has 'A 1' but JSON has 'A1'
                raw_refs = w.tags['ref'].split(';')
                norm_refs = [r.replace(' ', '').strip() for r in raw_refs]
                
                if self.target_ref.replace(' ', '') in norm_refs:
                    nodes = [n.ref for n in w.nodes]
                    if len(nodes) >= 2:
                        self.ways.append(nodes)
                        self.node_ids.update(nodes)

class HighwayNodeHandler(osmium.SimpleHandler):
    def __init__(self, node_ids):
        super(HighwayNodeHandler, self).__init__()
        self.node_ids = node_ids
        self.nodes = {}

    def node(self, n):
        if n.id in self.node_ids:
            self.nodes[n.id] = (n.location.lon, n.location.lat)

def get_highway_geometry_from_pbf(pbf_path, highway_ref):
    """
    Extracts the combined LineString/MultiLineString for a given highway reference
    from an OSM PBF file using a two-pass approach.
    """
    print(f"📡 [1/2] Scanning PBF for ways matching ref='{highway_ref}'...")
    way_handler = HighwayWayHandler(highway_ref)
    way_handler.apply_file(pbf_path)
    
    if not way_handler.ways:
        print(f"❌ No ways found for {highway_ref}")
        return None
        
    print(f"📡 [2/2] Fetching coordinates for {len(way_handler.node_ids)} nodes...")
    node_handler = HighwayNodeHandler(way_handler.node_ids)
    node_handler.apply_file(pbf_path)
    
    # Reconstruct ways into Shapely LineStrings
    lines = []
    for way_nodes in way_handler.ways:
        coords = []
        for nid in way_nodes:
            if nid in node_handler.nodes:
                coords.append(node_handler.nodes[nid])
        if len(coords) >= 2:
            lines.append(LineString(coords))
            
    if not lines:
        return None
        
    # Merge all the disconnected segments into the longest possible continuous lines
    merged = linemerge(lines)
    
    # If it's a MultiLineString, we usually want the longest continuous segment 
    # to avoid random detached links, but sometimes highways have natural breaks.
    # We will return the full merged geometry and handle snapping globally.
    if isinstance(merged, MultiLineString):
        # We can extract the main backbone if there are many tiny disconnected ramps
        # Filter out very short segments (e.g. < 0.01 degrees)
        valid_geoms = [geom for geom in merged.geoms if geom.length > 0.005]
        if len(valid_geoms) == 1:
            return valid_geoms[0]
        elif len(valid_geoms) > 1:
            return MultiLineString(valid_geoms)
        else:
            # Fallback to longest
            return max(list(merged.geoms), key=lambda x: x.length)
            
    return merged

if __name__ == '__main__':
    # Quick test
    pbf = "../../data/01_raw/portugal-latest.osm.pbf"
    geom = get_highway_geometry_from_pbf(pbf, "A1")
    print("Result Geometry Type:", geom.geom_type)
    print("Length:", geom.length)
