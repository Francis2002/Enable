# Data Enrichment Documentation

This document tracks the enrichment phases applied to the charging station dataset.

## Phase 0: Base Location Metadata
**Script**: `initialize_pre_ml.py`
**Description**: Initializes the `pre_ml.db` with station identification and coordinate metadata.

### Output Table: `coordinates`
Location: `data/pre_ml.db`

| Column | Description |
| :--- | :--- |
| `station_id` | Unique station identifier (matches CSV `ID`) |
| `CIDADE` | City name |
| `MORADA` | Physical address |
| `LATITUDE` | WGS84 Latitude |
| `LONGITUDE` | WGS84 Longitude |

---

## Phase 1: Geospatial Distance Enrichment
**Script**: `enrich_station_distances.py`  
**Description**: Computes counts and distances to various POIs and Road systems using the Valhalla routing engine.

### Output Table: `station_distances`
Location: `data/pre_ml.db`

| Feature Group | Column | Description | Threshold | Mode |
| :--- | :--- | :--- | :--- | :--- |
| **Identification** | `station_id` | Primary Key (matches CSV `ID`) | - | - |
| **Highways** | `highways_entry_count` | Number of motorway/trunk access points | 3000m | Drive |
| | `highways_entry_dist` | Min distance to motorway/trunk entrance | 3000m | Drive |
| **National Roads** | `national_roads_entry_count` | Number of primary/secondary road segments | 500m | Drive |
| | `national_roads_entry_dist` | Min distance to primary/secondary road | 500m | Drive |
| **POIs** | `{poi_name}_count` | Count of specific POI (from `POI_FULL_LIST.txt`) | 350m | Walk |
| | `{poi_name}_dist` | Min distance to specific POI | 350m | Walk |

> [!NOTE]
> Distances are in **meters**. If no features are found within the threshold, `count` will be `0` and `dist` will be `-1`.

---

## Phase 2: Station Price Enrichment
**Script**: `enrich_station_prices.py`
**Description**: Ingests station-level and socket-level tariff information from `MOBIE_Tarifas.csv`.

### Output Table: `prices`
Location: `data/pre_ml.db`

| Column | Description |
| :--- | :--- |
| `station_ID` | Station identifier (matches CSV `ID`) |
| `UID_TOMADA` | Unique socket identifier |
| `TIPO_TARIFARIO` | Tariff model type |
| `TIPO_TARIFA` | Type of fee (ENERGY, TIME, FLAT) |
| `TARIFA` | Current value of the tariff |

---

## Phase 3: Station Indicator Enrichment
**Script**: `enrich_station_indicators.py`
**Description**: Maps each station to its corresponding 1km grid cell and extracts socioeconomic metrics (income, census, tourism).

### Output Table: `indicators`
Location: `data/pre_ml.db`

| Column | Description |
| :--- | :--- |
| `station_id` | Station identifier (matches CSV `ID`) |
| `indicator_name` | Name of the metric (e.g., `avg_income`, `tourism_pressure`, `pop_total`) |
| `value` | Numerical value of the indicator |

---

## Phase 4: Station Configuration
**Script**: `enrich_station_configuration.py`
**Description**: Extracts hardware-specific configuration for each socket from the corrected MOBIe station list.

### Output Table: `station_configuration`
Location: `data/pre_ml.db`

| Column | Description |
| :--- | :--- |
| `station_ID` | Station identifier (matches CSV `ID`) |
| `UID DA TOMADA` | Unique socket identifier |
| `TIPO DE CARREGAMENTO` | Charging speed category (e.g., Ultra-Rápido) |
| `NIVEL DE TENSÃO` | Voltage level (e.g., BC, BT, MT) |
| `TIPO DE TOMADA` | Physical connector type (e.g., CHAdeMO, Type 2) |
| `FORMATO DE TOMADA` | Socket or tethered cable |
| `POTÊNCIA DA TOMADA` | Maximum power output in kW |

---
*Future enrichment phases will be documented here.*
