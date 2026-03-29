import os
import sys
import numpy as np
import warnings
import networkx as nx
from scipy.spatial import cKDTree
from shapely.geometry import LineString, MultiLineString, Point
from shapely.ops import linemerge
import osmium

warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=UserWarning)

class HighwayWayHandler(osmium.SimpleHandler):
    def __init__(self, target_ref):
        super(HighwayWayHandler, self).__init__()
        self.target_ref = target_ref
        self.node_ids = set()
        self.ways = []

    def way(self, w):
        # By excluding 'motorway_link' and 'trunk_link', we force snapping and routing 
        # to strictly use the mainline highway, preventing V-shaped artifacts.
        # Any small gaps created at interchanges will be bridged by build_isolated_highway_graph.
        if 'highway' in w.tags and w.tags['highway'] in ['motorway', 'trunk']:
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


_GEOM_CACHE = {}
_ISOLATED_GRAPH_CACHE = {}

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

def build_isolated_highway_graph(geom):
    """
    Builds a custom routing graph containing ONLY the coordinates 
    of the target highway, bridging any gaps caused by missing OSM tags.
    """
    if isinstance(geom, MultiLineString):
        lines = list(geom.geoms)
    elif isinstance(geom, LineString):
        lines = [geom]
    else:
        # Fallback if it's a Point or GeometryCollection (extremely rare)
        return None, None, None

    G = nx.Graph()
    nodes = []

    # 1. Build the base graph from the actual highway lines
    for i, line in enumerate(lines):
        coords = list(line.coords)
        for j in range(len(coords)):
            nodes.append(coords[j])
            if j < len(coords) - 1:
                p1 = coords[j]
                p2 = coords[j+1]
                # Simple euclidean distance since it's just relative weights for the same road
                dist = ((p1[0]-p2[0])**2 + (p1[1]-p2[1])**2)**0.5
                G.add_edge(p1, p2, weight=dist)

    if not nodes:
        return None, None, None

    # 2. Add crossovers between parallel carriageways (Northbound <-> Southbound)
    # This completely eliminates "U-turn" routing issues.
    tree = cKDTree(nodes)
    # Search within ~200m radius for parallel lanes
    pairs = tree.query_pairs(r=0.002)  
    for i, j in pairs:
        p1 = nodes[i]
        p2 = nodes[j]
        if not G.has_edge(p1, p2):
            dist = ((p1[0]-p2[0])**2 + (p1[1]-p2[1])**2)**0.5
            # Small penalty so it prefers staying on the same lane unless necessary
            G.add_edge(p1, p2, weight=dist * 1.5)

    # 3. Bridge disconnected pieces (the "gaps" in OSM mapping)
    comps = list(nx.connected_components(G))
    if len(comps) > 1:
        # Sort components by size (number of nodes)
        comps.sort(key=len, reverse=True)
        main_comp = list(comps[0])
        main_tree = cKDTree(main_comp)
        
        # Connect smaller orphaned pieces back to the main continuous highway
        for c in comps[1:]:
            c_nodes = list(c)
            # Find the closest point in the main component to any point in the small component
            dists, idxs = main_tree.query(c_nodes)
            min_idx = np.argmin(dists)
            
            # Bridge gaps up to ~15km (0.15 degrees)
            if dists[min_idx] < 0.15:  
                p1 = main_comp[idxs[min_idx]]
                p2 = c_nodes[min_idx]
                # Double penalty so it prefers mapped roads over jumping gaps if possible
                G.add_edge(p1, p2, weight=dists[min_idx] * 2.0)
                
    return G, nodes, tree

def get_routing_path_for_segment(pbf_path, ptA, ptB, highway_ref):
    """
    Routes between two points using ONLY the requested highway's geometry.
    Completely isolated from secondary roads, preventing any detours.
    """
    try:
        # 1. Fetch exactly the geometry for this specific highway
        geom = get_highway_geometry_from_pbf(pbf_path, highway_ref)
        if geom is None:
            print(f"  ⚠️ No geometry found for {highway_ref}. Using straight line fallback.")
            return LineString([ptA, ptB])

        # 2. Get or build the isolated routing graph for this highway
        if highway_ref not in _ISOLATED_GRAPH_CACHE:
            print(f"  🕸️ Building isolated routing graph for {highway_ref}...")
            G, nodes, tree = build_isolated_highway_graph(geom)
            _ISOLATED_GRAPH_CACHE[highway_ref] = (G, nodes, tree)
        else:
            G, nodes, tree = _ISOLATED_GRAPH_CACHE[highway_ref]

        if G is None:
            return LineString([ptA, ptB])

        # 3. Snap the start and end points to the closest node on the highway
        _, idxA = tree.query(ptA)
        _, idxB = tree.query(ptB)
        start_node = nodes[idxA]
        end_node = nodes[idxB]
        
        # 4. Calculate the path
        path = nx.shortest_path(G, source=start_node, target=end_node, weight='weight')
        path_coords = list(path)
        
        if len(path_coords) < 2:
            print("  ⚠️ Start and End map to the same node. Using straight line fallback.")
            return LineString([ptA, ptB])
            
        return LineString(path_coords)

    except nx.NetworkXNoPath:
        print("  ⚠️ Graph disconnected (gap too large). Using straight line fallback.")
        return LineString([ptA, ptB])
    except Exception as e:
        print(f"  ⚠️ Routing failed ({e}). Using straight line fallback.")
        return LineString([ptA, ptB])

