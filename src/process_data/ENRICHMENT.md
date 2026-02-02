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
*Future enrichment phases will be documented here.*
