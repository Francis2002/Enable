import geopandas as gpd
import pyogrio

gpkg = "../../data/03_interim/highway_traffic.gpkg"
layers = pyogrio.list_layers(gpkg)[:, 0].tolist()

for layer in layers:
    gdf = gpd.read_file(gpkg, layer=layer)
    prev_end = None
    prev_name = None
    for idx, row in gdf.iterrows():
        coords = list(row.geometry.coords)
        start = coords[0]
        end = coords[-1]
        
        if prev_end is not None:
            # Distance between previous end and current start
            dist = ((prev_end[0] - start[0])**2 + (prev_end[1] - start[1])**2)**0.5
            if dist > 0.005:  # more than ~500m gap or jump
                print(f"[{layer}] Gap/Jump between {prev_name} and {row['sublanco']}: dist {dist:.4f}")
        
        prev_end = end
        prev_name = row['sublanco']
