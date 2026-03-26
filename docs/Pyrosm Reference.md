Exhaustive Reference: pyrosm Data Extraction
This document lists every feature and tag that pyrosm can extract by default from an OSM PBF file.

1. Network Extraction (get_network)
Extracts routable segments (Ways). pyrosm filters these based on the network_type strategy.

Available Highway Classes (Found in highway column):
motorway, motorway_link, trunk, trunk_link, primary, primary_link, secondary, secondary_link, tertiary, tertiary_link, residential, living_street, service, pedestrian, track, bus_guideway, escape, raceway, road, footway, bridleway, steps, path, cycleway, proposed, construction, bus_stop, crossing, elevator, emergency_access_point, give_way, mini_roundabout, motorway_junction, passing_place, rest_area, speed_display, stop, traffic_signals, turning_circle, turning_loop.

2. Building Extraction (get_buildings)
Extracts polygon features representing structures.

Standard Building Attributes:
Geometry: Polygons.
Address: addr:city, addr:country, addr:full, addr:housenumber, addr:postcode, addr:street.
Metadata: building (type), height, levels, name, opening_hours, operator, phone, website.
3. Landuse & Natural Extraction (get_landuse, get_natural)
Used to categorize territories.

Landuse Tags:
allotments, basin, brownfield, cemetery, commercial, conservation, construction, depot, farmland, farmyard, forest, garages, grass, greenfield, greenhouse_horticulture, industrial, landfill, meadow, military, orchard, peat_cutting, plant_nursery, port, quarry, railway, recreation_ground, religious, reservoir, residential, retail, salt_pond, village_green, vineyard.

Natural Tags:
arete, bare_rock, bay, beach, cape, cave_entrance, cliff, coastline, dune, fell, geyser, glacier, grassland, heath, hill, hot_spring, isthmus, moor, mud, peak, peninsula, reef, ridge, rock, saddle, sand, scree, scrub, shingle, sinkhole, spring, stone, strait, tree, tree_row, valley, volcano, water, wetland, wood.

4. Points of Interest (POIs)
Parsed from amenity, shop, and tourism tags.

Amenity Tags:
bar, bbq, biergarten, cafe, drinking_water, fast_food, food_court, ice_cream, pub, restaurant, college, kindergarten, library, public_bookcase, school, university, bicycle_parking, bicycle_repair_station, bicycle_rental, boat_sharing, bus_station, car_breakdown, car_sharing, charging_station, ferry_terminal, fuel, grit_bin, motorcycle_parking, parking, parking_entrance, parking_space, taxi, atm, bank, bureau_de_change, baby_hatch, clinic, dentist, doctors, hospital, nursing_home, pharmacy, social_facility, veterinary, arts_centre, brothel, casino, cinema, community_centre, fountain, gambling, nightclub, planetarium, social_centre, stripclub, studio, theatre, animal_boarding, animal_shelter, baking_oven, bench, clock, courthouse, crematorium, dive_centre, embassy, fire_station, fire_hydrant, grave_yard, hunting_stand, internet_cafe, kitchen, marketplace, monastery, photo_booth, place_of_worship, police, post_box, post_office, prison, public_bath, public_building, public_facility, ranger_station, recycling, sanitary_dump_station, shelter, shower, telephone, toilets, townhall, vending_machine, waste_basket, waste_disposal, water_point, watering_place.

Analysis Workflow: The "Extract Once, Join Often" Strategy
To overlay 1km grid cells with OSM data, follow this architectural pattern:

1. Grid Generation (The Official Way)
The "Official" European 1km grid is provided by the EEA (European Environment Agency) using the EPSG:3035 projection.

You do NOT need to download it if you can generate it. I can provide a script that creates a 1km fishnet clipped to Portugal's boundary in EPSG:3035.
2. Spatial Aggregation Pattern
IMPORTANT

Do NOT call get_network repeatedly for each cell. This would be thousands of times slower because pyrosm has to parse the binary PBF file every single time.

Correct Approach:

Extract All: Load all roads once: roads = osm.get_network("driving").
Reproject: Convert roads to the grid's CRS (EPSG:3035): roads = roads.to_crs(3035).
Intersect: Use geopandas.overlay(roads, grid, how='intersection') to split road segments exactly where they cross grid lines.
Calculate: Compute segment.length for these new pieces.
Group: Aggregate by grid_id and highway_class.
3. Entry Points Logic
To find where people most likely "enter" a 1km cell:

Find the Boundary of each cell (Polygon -> LineString).
Intersect with the Road Network.
Filter for the 3 points where the highest-ranking roads (Motorway, Primary) cross the boundary.
OSM Topology vs. Geometry: Understanding Nodes and Segments
To perform accurate spatial analysis, it is critical to understand how OSM data is physically structured:

1. The "Way" (Geometry)
In OSM, a road is a Way. A Way is an ordered list of Nodes.

Curves: To represent a curve, a Way will have many nodes close together. Each node has a Lat/Lon.
Segmentation: A long highway is rarely a single Way. It is usually split into many segments by contributors whenever:
The road name changes.
The number of lanes changes.
A bridge or tunnel starts.
It intersects with another major road (though not ALWAYS).
2. Nodes: Shaping vs. Connectivity
There are two "roles" nodes play when you extract them with pyrosm:

Geometry Nodes (Vertices): These define the shape/curves of the road. Every time a road turns, there is a node.
Topological Nodes (Intersections): These are nodes shared by two or more Ways. This is how routing engines (like Valhalla) know you can turn from one road onto another.
3. The Grid Split (Intersections)
When you overlay a 1km grid:

A single OSM Way might be 5km long and cross 5 different cells.
If you just "assign" the Way to the cell where its center is, your length calculations will be wrong.
The Solution: geopandas.overlay(how='intersection') literally cuts the Way at the exact point it crosses the grid boundary. It creates new "artificial" nodes at the grid boundary to ensure the road piece fits perfectly inside the cell.
NOTE

When you use osm.get_network(nodes=True), pyrosm returns:

Edges: A GeoDataFrame of LineStrings (the segments).
Nodes: A GeoDataFrame of Points (the vertices).