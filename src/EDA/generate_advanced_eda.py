import nbformat as nbf
import os

def create_advanced_eda_notebook():
    nb = nbf.v4.new_notebook()

    cells = []

    # Title and Introduction
    cells.append(nbf.v4.new_markdown_cell("# Advanced Exploratory Data Analysis: MOBIE Datasets\n"
                                         "This notebook performs hierarchical analysis, pricing segmentation, and geospatial mapping for EV charging stations in Portugal."))

    # Imports
    cells.append(nbf.v4.new_code_cell("import pandas as pd\n"
                                     "import matplotlib.pyplot as plt\n"
                                     "import seaborn as sns\n"
                                     "import os\n"
                                     "import re\n"
                                     "import numpy as np\n"
                                     "try:\n"
                                     "    import geopandas as gpd\n"
                                     "    from shapely.geometry import Point\n"
                                     "    import duckdb\n"
                                     "    HAS_GEOPANDAS = True\n"
                                     "except ImportError:\n"
                                     "    HAS_GEOPANDAS = False\n"
                                     "\n"
                                     "# Set plot style\n"
                                     "sns.set_theme(style='whitegrid')\n"
                                     "plt.rcParams['figure.figsize'] = (12, 6)"))

    # Data Path Configuration
    cells.append(nbf.v4.new_code_cell("DATA_DIR = '../../data'\n"
                                     "POSTOS_FILE = os.path.join(DATA_DIR, 'MOBIe_Lista_de_postos.csv')\n"
                                     "TARIFAS_FILE = os.path.join(DATA_DIR, 'MOBIE_Tarifas.csv')\n"
                                     "MAP_PATH = os.path.join(DATA_DIR, 'GRID1K21_CONT.gpkg')\n"
                                     "OSM_DB_PATH = os.path.join(DATA_DIR, 'osm_analysis.db')"))

    # Loading Datasets
    cells.append(nbf.v4.new_code_cell("# Load datasets\n"
                                     "df_postos = pd.read_csv(POSTOS_FILE, sep=';', encoding='utf-8-sig')\n"
                                     "df_tarifas = pd.read_csv(TARIFAS_FILE, sep=';', encoding='utf-8-sig')\n"
                                     "\n"
                                     "# Clean power column (convert comma to dot if string)\n"
                                     "def clean_power(val):\n"
                                     "    if isinstance(val, str):\n"
                                     "        return float(val.replace(',', '.'))\n"
                                     "    return val\n"
                                     "\n"
                                     "df_postos['POTÊNCIA DA TOMADA (kW)'] = df_postos['POTÊNCIA DA TOMADA (kW)'].apply(clean_power)\n"
                                     "\n"
                                     "print('Datasets loaded successfully.')"))

    # Hierarchy Analysis
    cells.append(nbf.v4.new_markdown_cell("## 1. Station-Charger Hierarchy\n"
                                         "Analyzing the distribution of chargers per station and their characteristics."))

    cells.append(nbf.v4.new_code_cell("# Chargers per Station\n"
                                     "chargers_per_station = df_postos.groupby('ID')['UID DA TOMADA'].count().reset_index()\n"
                                     "chargers_per_station.columns = ['Station ID', 'Charger Count']\n"
                                     "\n"
                                     "plt.figure(figsize=(12, 6))\n"
                                     "# Using countplot for discrete data to avoid KDE issues with integers\n"
                                     "sns.countplot(data=chargers_per_station, x='Charger Count', palette='Blues_d')\n"
                                     "plt.title('Distribution of Chargers per Station (Counts)')\n"
                                     "plt.xlabel('Number of Chargers')\n"
                                     "plt.ylabel('Number of Stations')\n"
                                     "plt.show()\n"
                                     "\n"
                                     "print(f'Max chargers in a single station: {chargers_per_station[\"Charger Count\"].max()}')\n"
                                     "print(f'Average chargers per station: {chargers_per_station[\"Charger Count\"].mean():.2f}')"))

    cells.append(nbf.v4.new_code_cell("# Segmentation by Socket Type and Power\n"
                                     "socket_power = df_postos.groupby(['TIPO DE TOMADA', 'FORMATO DA TOMADA']).size().reset_index(name='count')\n"
                                     "display(socket_power.sort_values('count', ascending=False))\n"
                                     "\n"
                                     "plt.figure(figsize=(12, 6))\n"
                                     "sns.boxplot(data=df_postos, x='TIPO DE TOMADA', y='POTÊNCIA DA TOMADA (kW)', palette='muted')\n"
                                     "plt.title('Power Distribution by Socket Type')\n"
                                     "plt.xticks(rotation=0)\n"
                                     "\n"
                                     "# Increase Y-axis granularity for better visibility\n"
                                     "max_p = df_postos['POTÊNCIA DA TOMADA (kW)'].max()\n"
                                     "plt.yticks(np.arange(0, max_p + 50, 25))\n"
                                     "plt.grid(True, axis='y', linestyle='--', alpha=0.4)\n"
                                     "\n"
                                     "plt.show()"))

    cells.append(nbf.v4.new_code_cell("# Connector Composition by Station Size\n"
                                     "station_sizes = df_postos.groupby('ID')['UID DA TOMADA'].count().reset_index()\n"
                                     "station_sizes.columns = ['ID', 'Station Size']\n"
                                     "df_merged = df_postos.merge(station_sizes, on='ID')\n"
                                     "\n"
                                     "plt.figure(figsize=(12, 6))\n"
                                     "ax = sns.histplot(data=df_merged, x='Station Size', hue='TIPO DE TOMADA', \n"
                                     "                  multiple='stack', discrete=True, palette='viridis')\n"
                                     "plt.title('Connector Composition by Station Size')\n"
                                     "plt.xlabel('Number of Chargers per Station')\n"
                                     "plt.ylabel('Total Count of Connectors')\n"
                                     "sns.move_legend(ax, 'upper left', bbox_to_anchor=(1, 1), title='Connector Type')\n"
                                     "plt.tight_layout()\n"
                                     "plt.show()"))

    # Pricing Segmentation
    cells.append(nbf.v4.new_markdown_cell("## 2. Pricing Segmentation\n"
                                         "Extracting numeric tariffs and analyzing by tariff type."))

    cells.append(nbf.v4.new_code_cell("def extract_price(tarifa_str):\n"
                                     "    if pd.isna(tarifa_str): return np.nan\n"
                                     "    # Find patterns like € 0.261 or 0,1\n"
                                     "    match = re.search(r'(\\d+[.,]\\d+)', str(tarifa_str))\n"
                                     "    if match:\n"
                                     "        return float(match.group(1).replace(',', '.'))\n"
                                     "    return np.nan\n"
                                     "\n"
                                     "df_tarifas['tarifa_num'] = df_tarifas['TARIFA'].apply(extract_price)"))

    cells.append(nbf.v4.new_code_cell("for t_type in df_tarifas['TIPO_TARIFA'].unique():\n"
                                     "    subset = df_tarifas[df_tarifas['TIPO_TARIFA'] == t_type].dropna(subset=['tarifa_num'])\n"
                                     "    if len(subset) == 0: continue\n"
                                     "    \n"
                                     "    plt.figure(figsize=(12, 5))\n"
                                     "    # Standard distribution plot with KDE for better visibility of trends\n"
                                     "    sns.histplot(subset['tarifa_num'], kde=True, bins=30, color='green')\n"
                                     "    plt.title(f'Tariff Distribution: {t_type}')\n"
                                     "    plt.xlabel('Price (€)')\n"
                                     "    plt.ylabel('Frequency')\n"
                                     "    plt.show()"))

    cells.append(nbf.v4.new_code_cell("# Segmented Energy Tariff by Connector Type\n"
                                     "energy_subset = df_tarifas[df_tarifas['TIPO_TARIFA'] == 'ENERGY'].dropna(subset=['tarifa_num'])\n"
                                     "if len(energy_subset) > 0:\n"
                                     "    plt.figure(figsize=(12, 7))\n"
                                     "    ax = sns.histplot(data=energy_subset, x='tarifa_num', hue='TIPO_TOMADA', \n"
                                     "                      kde=True, multiple='stack', bins=30, palette='Set2')\n"
                                     "    plt.title('Energy Tariff Distribution Segmented by Connector Type')\n"
                                     "    plt.xlabel('Price (€)')\n"
                                     "    plt.ylabel('Frequency')\n"
                                     "    sns.move_legend(ax, 'upper left', bbox_to_anchor=(1, 1), title='Connector Type')\n"
                                     "    plt.tight_layout()\n"
                                     "    plt.show()"))

    # Geographic Mapping
    cells.append(nbf.v4.new_markdown_cell("## 3. Geospatial Mapping\n"
                                         "Integrated visualization of charging stations and road networks.\n\n"
                                         "### 3.1 National Highway Network"))

    cells.append(nbf.v4.new_code_cell("# Advanced Mapping with Classification\n"
                                     "if HAS_GEOPANDAS:\n"
                                     "    # 1. Create Station GeoDataFrame (EPSG:4326 -> EPSG:3763)\n"
                                     "    gdf_stations = gpd.GeoDataFrame(df_postos, geometry=gpd.points_from_xy(df_postos.LONGITUDE, df_postos.LATITUDE))\n"
                                     "    gdf_stations.crs = 'EPSG:4326'\n"
                                     "    gdf_stations = gdf_stations.to_crs('EPSG:3763')\n"
                                     "    \n"
                                     "    # 2. Fetch Broad Road Network with Links\n"
                                     "    print('Fetching comprehensive road network...')\n"
                                     "    all_roads = gpd.GeoDataFrame()\n"
                                     "    try:\n"
                                     "        con = duckdb.connect(OSM_DB_PATH, read_only=True)\n"
                                     "        con.execute('INSTALL spatial; LOAD spatial;')\n"
                                     "        \n"
                                     "        if 'roads_global' in [t[0] for t in con.execute('SHOW TABLES').fetchall()]:\n"
                                     "             # Expanded query to include links and tertiary roads for detail\n"
                                     "             # We map them to simpler categories for plotting\n"
                                     "             target_types = [\n"
                                     "                 'motorway', 'motorway_link', \n"
                                     "                 'trunk', 'trunk_link', \n"
                                     "                 'primary', 'primary_link',\n"
                                     "                 'secondary', 'secondary_link',\n"
                                     "                 'tertiary', 'tertiary_link',\n"
                                     "                 'residential', 'unclassified', 'living_street',\n"
                                     "                 'service', 'pedestrian', 'footway', 'track', 'path', 'cycleway'\n"
                                     "             ]\n"
                                     "             formatted_types = \", \".join([f\"'{t}'\" for t in target_types])\n"
                                     "             \n"
                                     "             query = f\"SELECT highway, ST_AsWKB(geometry) as geometry FROM roads_global WHERE highway IN ({formatted_types})\"\n"
                                     "             \n"
                                     "             df_roads = con.execute(query).fetchdf()\n"
                                     "             if not df_roads.empty:\n"
                                     "                 df_roads['geometry'] = gpd.GeoSeries.from_wkb(df_roads['geometry'].apply(bytes))\n"
                                     "                 raw_roads = gpd.GeoDataFrame(df_roads, geometry='geometry', crs='EPSG:3035')\n"
                                     "                 all_roads = raw_roads.to_crs('EPSG:3763')\n"
                                     "                 \n"
                                     "                 # Create simplified 'Road Class' for coloring\n"
                                     "                 def classify_road(h):\n"
                                     "                     if 'motorway' in h or 'trunk' in h: return 'Highway' # Fast\n"
                                     "                     if 'primary' in h: return 'Primary' # Main\n"
                                     "                     if 'secondary' in h: return 'Secondary'\n"
                                     "                     return 'Local'\n"
                                     "                 all_roads['road_class'] = all_roads['highway'].apply(classify_road)\n"
                                     "\n"
                                     "    except Exception as e:\n"
                                     "        print(f'DB Error: {e}')\n"
                                     "    \n"
                                     "\n"
                                     "    # --- PLOT 1: National Overview (Highways Only) ---\n"
                                     "    fig, ax = plt.subplots(figsize=(12, 18))\n"
                                     "    \n"
                                     "    # Define color scheme for importance\n"
                                     "    road_colors = {'Highway': '#d95f02', 'Primary': '#7570b3', 'Secondary': '#999999', 'Local': '#d9d9d9'}\n"
                                     "    road_widths = {'Highway': 1.0, 'Primary': 0.7, 'Secondary': 0.5, 'Local': 0.3}\n"
                                     "    \n"
                                     "    if not all_roads.empty:\n"
                                     "        # Plot National Backbone (Highways + Primary)\n"
                                     "        national_roads = all_roads[all_roads['road_class'].isin(['Highway', 'Primary'])]\n"
                                     "        \n"
                                     "        # Plot by category for legend\n"
                                     "        for rtype in ['Highway', 'Primary']:\n"
                                     "            subset = national_roads[national_roads['road_class'] == rtype]\n"
                                     "            if not subset.empty:\n"
                                     "                subset.plot(ax=ax, color=road_colors[rtype], linewidth=road_widths[rtype], label=f'{rtype} Roads', alpha=0.7)\n"
                                     "\n"
                                     "    gdf_stations.plot(ax=ax, markersize=3, color='black', alpha=0.5, label='Stations')\n"
                                     "    \n"
                                     "    plt.title('Portuguese Charging Network & Highway System')\n"
                                     "    plt.axis('off')\n"
                                     "    plt.legend(loc='upper right')\n"
                                     "    plt.show()\n"
                                     "    \n"
                                     "\n"
                                     "    # --- HELPER FUNCTION: City Zoom ---\n"
                                     "    def plot_city_context(city_name, color_column=None, title_suffix=''):\n"
                                     "        city_stations = gdf_stations[gdf_stations['CIDADE'] == city_name]\n"
                                     "        if city_stations.empty: return\n"
                                     "        \n"
                                     "        minx, miny, maxx, maxy = city_stations.total_bounds\n"
                                     "        buff = 2500 # 2.5km buffer\n"
                                     "        \n"
                                     "        fig, ax = plt.subplots(figsize=(10, 10))\n"
                                     "        \n"
                                     "        # Plot Roads (All types relevant for city)\n"
                                     "        if not all_roads.empty:\n"
                                     "            try:\n"
                                     "                local_roads = all_roads.cx[minx-buff:maxx+buff, miny-buff:maxy+buff]\n"
                                     "                if not local_roads.empty:\n"
                                     "                     for rtype in ['Highway', 'Primary', 'Secondary', 'Local']:\n"
                                     "                         subset = local_roads[local_roads['road_class'] == rtype]\n"
                                     "                         if not subset.empty:\n"
                                     "                            subset.plot(ax=ax, color=road_colors.get(rtype, 'grey'), \n"
                                     "                                      linewidth=road_widths.get(rtype, 0.5), alpha=0.6, zorder=1)\n"
                                     "            except: pass\n"
                                     "        \n"
                                     "        # Plot Stations\n"
                                     "        if color_column:\n"
                                     "            # Categorical Plot\n"
                                     "            city_stations.plot(ax=ax, column=color_column, markersize=35, \n"
                                     "                             legend=True, cmap='viridis', zorder=2, alpha=0.9)\n"
                                     "        else:\n"
                                     "            # Simple Plot\n"
                                     "            city_stations.plot(ax=ax, color='red', markersize=35, label='Stations', zorder=2)\n"
                                     "            \n"
                                     "        plt.title(f'{city_name}: {title_suffix}')\n"
                                     "        plt.xlim(minx-buff, maxx+buff)\n"
                                     "        plt.ylim(miny-buff, maxy+buff)\n"
                                     "        plt.axis('off')\n"
                                     "        plt.show()\n"
                                     "    \n"
                                     "    top_cities = df_postos['CIDADE'].value_counts().head(3).index.tolist()\n"
                                     "else:\n"
                                     "    print('Geopandas required')\n"
                                     "    top_cities = []"))

    cells.append(nbf.v4.new_markdown_cell("### 3.2 Network Density by City\n"
                                         "Zoomed views showing station placement relative to local road infrastructure."))
    
    cells.append(nbf.v4.new_code_cell("# Plot Basic City Zooms\n"
                                     "if HAS_GEOPANDAS:\n"
                                     "    for city in top_cities:\n"
                                     "        plot_city_context(city, title_suffix='Network Density')"))

    cells.append(nbf.v4.new_markdown_cell("### 3.3 Charging Types by City\n"
                                         "Distribution of charging categories (TIPO DE CARREGAMENTO) within major urban centers."))

    cells.append(nbf.v4.new_code_cell("# Plot Charging Type Distribution\n"
                                     "if HAS_GEOPANDAS:\n"
                                     "    for city in top_cities:\n"
                                     "        plot_city_context(city, color_column='TIPO DE CARREGAMENTO', \n"
                                     "                         title_suffix='Distribution by Charging Type')"))

    nb['cells'] = cells

    with open('/home/joao-martins/Desktop/code/Enable Mobility/code/Enable/src/EDA/eda_advanced.ipynb', 'w') as f:
        nbf.write(nb, f)

if __name__ == '__main__':
    create_advanced_eda_notebook()
