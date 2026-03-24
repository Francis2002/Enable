import os
import sys
import numpy as np
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=UserWarning)
import pandas as pd
pd.options.mode.chained_assignment = None

from shapely.geometry import LineString, MultiLineString, Point
from shapely.ops import linemerge
import osmium

class HighwayWayHandler(osmium.SimpleHandler):
    def __init__(self, target_ref):
        super(HighwayWayHandler, self).__init__()
        self.target_ref = target_ref
        self.node_ids = set()
        self.ways = []

    def way(self, w):
        if 'highway' in w.tags and w.tags['highway'] in ['motorway', 'motorway_link', 'trunk', 'trunk_link']:
            if 'ref' in w.tags:
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

def get_routing_path(pbf_path, ptA, ptB):
    """Uses pyrosm to extract actual drivable road connecting ptA and ptB."""
    try:
        from pyrosm import OSM
        import networkx as nx
        import geopandas as gpd
        from scipy.spatial import cKDTree
    except ImportError:
        print("Missing routing dependencies (pyrosm, networkx, scipy). Returning straight line.")
        return LineString([ptA, ptB])
    
    # Create bounding box covering both points with padding (~1km)
    pad = 0.01
    lon1, lat1 = ptA
    lon2, lat2 = ptB
    bbox = [min(lon1, lon2)-pad, min(lat1, lat2)-pad, max(lon1, lon2)+pad, max(lat1, lat2)+pad]
    
    try:
        osm = OSM(pbf_path, bounding_box=bbox)
        nodes, edges = osm.get_network(network_type="driving", nodes=True)
        if nodes is None or edges is None or len(nodes) == 0:
            return LineString([ptA, ptB])
            
        node_coords = np.array([(geom.x, geom.y) for geom in nodes.geometry])
        tree = cKDTree(node_coords)
        
        _, idx1 = tree.query((lon1, lat1))
        _, idx2 = tree.query((lon2, lat2))
        
        start_node = nodes.iloc[idx1]['id']
        end_node = nodes.iloc[idx2]['id']
        
        G = osm.to_graph(nodes, edges, graph_type="networkx")
        
        if not nx.has_path(G, start_node, end_node):
            # Try undirected
            G_undir = G.to_undirected()
            if not nx.has_path(G_undir, start_node, end_node):
                return LineString([ptA, ptB])
            path = nx.shortest_path(G_undir, source=start_node, target=end_node, weight='length')
        else:
            path = nx.shortest_path(G, source=start_node, target=end_node, weight='length')
            
        edges_indexed = edges.set_index(['u', 'v'])
        path_geoms = []
        for i in range(len(path)-1):
            u = path[i]
            v = path[i+1]
            if (u, v) in edges_indexed.index:
                geom = edges_indexed.loc[(u, v)]['geometry']
                if isinstance(geom, gpd.GeoSeries):
                    geom = geom.iloc[0]
                path_geoms.append(geom)
            elif (v, u) in edges_indexed.index:
                geom = edges_indexed.loc[(v, u)]['geometry']
                if isinstance(geom, gpd.GeoSeries):
                    geom = geom.iloc[0]
                path_geoms.append(LineString(list(geom.coords)[::-1]))
                
        if not path_geoms:
            return LineString([ptA, ptB])
            
        merged_path = linemerge(path_geoms)
        if isinstance(merged_path, MultiLineString):
            # Fallback if somehow it's still disconnected
            return LineString([ptA, ptB])
        return merged_path
    except Exception as e:
        print(f"Routing failed ({e}). Using straight line fallback.")
        return LineString([ptA, ptB])

_GEOM_CACHE = {}

def get_highway_geometry_from_pbf(pbf_path, highway_ref):
    if highway_ref in _GEOM_CACHE:
        return _GEOM_CACHE[highway_ref]

    print(f"📡 [1/2] Scanning PBF for ways matching ref='{highway_ref}'...")
    way_handler = HighwayWayHandler(highway_ref)
    way_handler.apply_file(pbf_path)
    
    if not way_handler.ways:
        print(f"❌ No ways found for {highway_ref}")
        return None
        
    print(f"📡 [2/2] Fetching coordinates for {len(way_handler.node_ids)} nodes...")
    node_handler = HighwayNodeHandler(way_handler.node_ids)
    node_handler.apply_file(pbf_path)
    
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
        
    merged = linemerge(lines)
    
    if isinstance(merged, MultiLineString):
        print(f"⚠️ {highway_ref} is disconnected in OSM data. Attempting Deep Routing stitching...")
        
        valid_geoms = [geom for geom in merged.geoms if geom.length > 0.005]
        
        if len(valid_geoms) == 1:
            return valid_geoms[0]
        elif len(valid_geoms) > 1:
            # Greedy stitch: start with longest, append closest
            valid_geoms.sort(key=lambda x: x.length, reverse=True)
            
            master_coords = list(valid_geoms.pop(0).coords)
            
            while valid_geoms:
                master_start = master_coords[0]
                master_end = master_coords[-1]
                
                best_dist = float('inf')
                best_idx = -1
                best_action = None # (attach_to, flip_geom)
                
                for i, geom in enumerate(valid_geoms):
                    g_start = geom.coords[0]
                    g_end = geom.coords[-1]
                    
                    d_ss = Point(master_start).distance(Point(g_start))
                    d_se = Point(master_start).distance(Point(g_end))
                    d_es = Point(master_end).distance(Point(g_start))
                    d_ee = Point(master_end).distance(Point(g_end))
                    
                    min_d = min(d_ss, d_se, d_es, d_ee)
                    if min_d < best_dist:
                        best_dist = min_d
                        best_idx = i
                        if min_d == d_ss: best_action = ('start', True)
                        elif min_d == d_se: best_action = ('start', False)
                        elif min_d == d_es: best_action = ('end', False)
                        elif min_d == d_ee: best_action = ('end', True)
                        
                next_geom = valid_geoms.pop(best_idx)
                
                # If the closest chunk is more than ~5km away, it's likely a mislabelled road in OSM!
                # We skip it to avoid massive routing delays and incorrect geometries.
                if best_dist > 0.05:
                    print(f"  -> Gap is too large (~{best_dist*111:.2f} km). Ignoring this outlier chunk.")
                    continue
                    
                coords = list(next_geom.coords)
                if best_action[1]:
                    coords = coords[::-1]
                    
                print(f"  -> Routing gap of ~{best_dist*111:.2f} km. {len(valid_geoms)} chunks remaining...")
                
                if best_action[0] == 'start':
                    # Master start connects to coords end
                    bridge = get_routing_path(pbf_path, coords[-1], master_start)
                    master_coords = coords + list(bridge.coords)[1:-1] + master_coords
                else:
                    # Master end connects to coords start
                    bridge = get_routing_path(pbf_path, master_end, coords[0])
                    master_coords = master_coords + list(bridge.coords)[1:-1] + coords
                    
            print(f"✅ Deep Routing stitched all chunks into a single continuous master line.")
            res = LineString(master_coords)
            _GEOM_CACHE[highway_ref] = res
            return res
            
    _GEOM_CACHE[highway_ref] = merged
    return merged

if __name__ == '__main__':
    pbf = "../../data/01_raw/portugal-latest.osm.pbf"
    geom = get_highway_geometry_from_pbf(pbf, "A1")
    if geom:
        print("Result Geometry Type:", geom.geom_type)
        print("Length:", geom.length)
