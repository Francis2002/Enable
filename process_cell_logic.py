import geopandas as gpd
import pandas as pd
from shapely.geometry import Point

def analyze_single_cell(cell_info, roads_in_block, pois_in_block, landuse_in_block, natural_in_block):
    """
    Processes a single cell using pre-loaded block data.
    cell_info: dict with 'cell_id', 'min_lon', 'min_lat', 'max_lon', 'max_lat'
    """
    cell_id = cell_info['cell_id']
    from shapely.geometry import box
    cell_poly = box(cell_info['x_3035'], cell_info['y_3035'], cell_info['x_3035'] + 1000, cell_info['y_3035'] + 1000)
    
    # 1. ROADS (Lengths & Shares)
    if roads_in_block is not None and not roads_in_block.empty:
        # Use spatial index for speed
        potential_roads = roads_in_block.iloc[roads_in_block.sindex.query(cell_poly, predicate="intersects")]
        # Fix potential topology issues
        potential_roads = potential_roads.copy()
        potential_roads.geometry = potential_roads.geometry.make_valid()
        roads_cell = gpd.clip(potential_roads, cell_poly)
    else:
        roads_cell = gpd.GeoDataFrame()

    if roads_cell.empty:
        return None # RULE: Ignore cells with no roads

    roads_cell['length_m'] = roads_cell.geometry.length
    road_agg = roads_cell.groupby('highway')['length_m'].sum()
    total_len = road_agg.sum()
    
    road_data = road_agg.to_dict()
    road_data['total_road_len'] = total_len
    road_data['cell_id'] = cell_id
    # Add shares
    shares = {f"{k}_share": v/total_len for k, v in road_agg.items()}
    road_data.update(shares)

    # 2. POIs (Counts)
    poi_data = {'cell_id': cell_id}
    if pois_in_block is not None and not pois_in_block.empty:
        # Just points within cell
        pois_cell = pois_in_block[pois_in_block.within(cell_poly)]
        if not pois_cell.empty:
            for key in ['amenity', 'shop', 'tourism']:
                if key in pois_cell.columns:
                    counts = pois_cell[key].value_counts().to_dict()
                    poi_data.update({f"poi_{k}": v for k, v in counts.items()})

    # 3. LAND USE & NATURAL (Area)
    poly_data = {'cell_id': cell_id}
    for df, key in [(landuse_in_block, 'landuse'), (natural_in_block, 'natural')]:
        if df is not None and not df.empty:
            potential_polys = df.iloc[df.sindex.query(cell_poly, predicate="intersects")]
            # Fix potential topology issues
            potential_polys = potential_polys.copy()
            potential_polys.geometry = potential_polys.geometry.make_valid()
            polys_cell = gpd.clip(potential_polys, cell_poly)
            if not polys_cell.empty:
                polys_cell['area_m2'] = polys_cell.geometry.area
                areas = polys_cell.groupby(key)['area_m2'].sum().to_dict()
                poly_data.update({f"area_{k}": v for k, v in areas.items()})

    # 4. ENTRY POINTS (Top 3)
    # Intersects road with grid cell boundary
    from pyproj import Transformer
    transformer = Transformer.from_crs("EPSG:3035", "EPSG:4326", always_xy=True)
    
    cell_boundary = cell_poly.boundary
    entry_potentials = roads_cell.geometry.intersection(cell_boundary)
    # Explode multi-points
    pts = []
    for geom, hway in zip(entry_potentials, roads_cell['highway']):
        if geom.is_empty: continue
        if geom.geom_type == 'Point':
            pts.append((geom, hway))
        elif geom.geom_type == 'MultiPoint':
            for p in geom.geoms:
                pts.append((p, hway))
    
    # Priority and sorting
    priority_map = {'motorway': 1, 'trunk': 2, 'primary': 3, 'secondary': 4, 'tertiary': 5, 'residential': 6}
    sorted_pts = sorted(pts, key=lambda x: priority_map.get(x[1], 99))
    
    origins = []
    unique_check = set()
    for p, hway in sorted_pts:
        coords = (round(p.x, 1), round(p.y, 1))
        if coords not in unique_check:
            lon, lat = transformer.transform(p.x, p.y)
            origins.append({'cell_id': cell_id, 'lon': lon, 'lat': lat, 'highway': hway})
            unique_check.add(coords)
            if len(origins) >= 3: break
    
    return {
        'road_stats': road_data,
        'poi_stats': poi_data,
        'poly_stats': poly_data,
        'origins': origins
    }
