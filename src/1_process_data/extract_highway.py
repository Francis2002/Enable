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
    
    _GEOM_CACHE[highway_ref] = merged
    return merged

class MotorwayHandler(osmium.SimpleHandler):
    def __init__(self):
        super(MotorwayHandler, self).__init__()
        self.nodes_to_keep = set()
        self.ways = []
        
    def way(self, w):
        if 'highway' in w.tags and w.tags['highway'] in ['motorway', 'motorway_link', 'trunk', 'trunk_link']:
            nodes = [n.ref for n in w.nodes]
            if len(nodes) >= 2:
                way_ref = w.tags.get('ref', '')
                self.ways.append({
                    'id': w.id,
                    'nodes': nodes,
                    'ref': way_ref
                })
                self.nodes_to_keep.update(nodes)

class MotorwayNodeHandler(osmium.SimpleHandler):
    def __init__(self, node_ids):
        super(MotorwayNodeHandler, self).__init__()
        self.node_ids = node_ids
        self.nodes = {}

    def node(self, n):
        if n.id in self.node_ids:
            self.nodes[n.id] = (n.location.lon, n.location.lat)

# Global variables to cache the national graph
_NATIONAL_GRAPH = None
_NATIONAL_NODES = None
_TREE = None
_COORDS_LIST = None
_NODE_IDS = None

def build_national_highway_graph(pbf_path):
    global _NATIONAL_GRAPH, _NATIONAL_NODES, _TREE, _COORDS_LIST, _NODE_IDS
    if _NATIONAL_GRAPH is not None:
        return _NATIONAL_GRAPH, _NATIONAL_NODES, _TREE, _COORDS_LIST, _NODE_IDS
        
    import time
    print("📡 [1/2] Scanning PBF for all motorways/trunks...")
    t0 = time.time()
    way_handler = MotorwayHandler()
    way_handler.apply_file(pbf_path)
    
    print("📡 [2/2] Extracting node coordinates...")
    node_handler = MotorwayNodeHandler(way_handler.nodes_to_keep)
    node_handler.apply_file(pbf_path)
    
    print("🕸️ Building National Highway NetworkX graph...")
    import networkx as nx
    from scipy.spatial import cKDTree
    G = nx.Graph()
    for way in way_handler.ways:
        nodes = way['nodes']
        ref = way['ref'].replace(' ', '')
        
        for i in range(len(nodes) - 1):
            u = nodes[i]
            v = nodes[i+1]
            if u in node_handler.nodes and v in node_handler.nodes:
                coord_u = node_handler.nodes[u]
                coord_v = node_handler.nodes[v]
                
                # Approx distance
                dx = coord_u[0] - coord_v[0]
                dy = coord_u[1] - coord_v[1]
                dist = (dx**2 + dy**2)**0.5
                
                G.add_edge(u, v, ref=ref, length=dist, coords=(coord_u, coord_v))
                
    # Build KDTree for quick nearest node lookup
    coords_list = []
    node_ids = []
    for nid, coord in node_handler.nodes.items():
        coords_list.append(coord)
        node_ids.append(nid)
        
    tree = cKDTree(coords_list)
    
    _NATIONAL_GRAPH = G
    _NATIONAL_NODES = node_handler.nodes
    _TREE = tree
    _COORDS_LIST = coords_list
    _NODE_IDS = node_ids
    
    print(f"✅ National Graph built in {time.time()-t0:.2f}s! {G.number_of_nodes()} nodes, {G.number_of_edges()} edges.")
    return _NATIONAL_GRAPH, _NATIONAL_NODES, _TREE, _COORDS_LIST, _NODE_IDS

def get_routing_path_for_segment(pbf_path, ptA, ptB, highway_ref):
    """Uses the cached national highway network to fast-route between A and B."""
    try:
        import networkx as nx
        from shapely.geometry import LineString
        G, nodes_dict, tree, coords_list, node_ids = build_national_highway_graph(pbf_path)
        
        # Determine target ref for weighting
        target_ref_clean = highway_ref.replace(' ', '')
        
        # Calculate dynamic weights based on the requested highway
        def weight_func(u, v, d):
            if target_ref_clean in d.get('ref', ''):
                return d['length'] * 1.0
            else:
                return d['length'] * 50.0
                
        _, idxA = tree.query(ptA)
        _, idxB = tree.query(ptB)
        
        start_node = node_ids[idxA]
        end_node = node_ids[idxB]
        
        path = nx.shortest_path(G, source=start_node, target=end_node, weight=weight_func)
        path_coords = [nodes_dict[n] for n in path]
        return LineString(path_coords)
    except Exception as e:
        print(f"Routing failed ({e}). Using straight line fallback.")
        return LineString([ptA, ptB])
