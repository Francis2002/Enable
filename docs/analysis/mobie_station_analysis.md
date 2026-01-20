# Analysis Report: MOBIe_Lista_de_postos.csv

This document provides a field-by-field description of the Mobi.E Station Master List, containing static technical features and real-time status for the entire Portuguese charging network.

## Field Descriptions

### 1. ID
*   **Type**: String (Identifier)
*   **Description**: The unique identifier for a charging station (e.g., `ABF-00008`).
*   **Notes**: This corresponds to `idChargingStation` in the session data. A single ID represents a physical station which can contain multiple sockets.

### 2. UID DA TOMADA
*   **Type**: String (Identifier)
*   **Description**: Unique identifier for a specific socket/connector (EVSE) (e.g., `PT-EDP-EABF-00008-1`).
*   **Notes**: Corresponds to `idEVSE` in the session data. Essential for granular analysis of capacity and availability.

### 3. TIPO DE CARREGAMENTO
*   **Type**: Categorical
*   **Description**: Broad classification of the charging speed/technology.
*   **Common Values**:
    *   `Potência Normal Médio` (AC, usually 11-22kW)
    *   `Alta Potência Rápido` (DC, usually 50kW+)
    *   `Alta Potência Ultrarrápido` (DC, usually 150kW+)
    *   `Potência Normal Lento` (AC, usually 3.7-7.4kW)

### 4. ESTADO DO POSTO
*   **Type**: Categorical (Status)
*   **Description**: Real-time operational status of the entire station.
*   **Common Values**: `Disponível`, `Offline`, `Em uso`.

### 5. CIDADE
*   **Type**: String (Categorical)
*   **Description**: The name of the city or locality where the station is located.

### 6. MORADA
*   **Type**: String (Text)
*   **Description**: The physical address of the station.

### 7. LATITUDE
*   **Type**: Numeric (Coordinate)
*   **Description**: Geographic latitude in WGS84 format.
*   **Usage**: Crucial for spatial join with the 1km grid.

### 8. LONGITUDE
*   **Type**: Numeric (Coordinate)
*   **Description**: Geographic longitude in WGS84 format.

### 9. OPERADOR
*   **Type**: String (Categorical)
*   **Description**: The name of the Charging Point Operator (CPO) (e.g., `Galp`, `EDP`).

### 10. MOBICHARGER
*   **Type**: Boolean/Flag
*   **Description**: Likely a proprietary feature flag or specific program (found as 'Não' in all samples).

### 11. NÍVEL DE TENSÃO
*   **Type**: Categorical
*   **Description**: The electrical grid voltage level provided to the station.
*   **Values**: `Média Tensão` (MT), `Baixa Tensão Especial` (BTE), `Baixa Tensão Normal` (BTN).

### 12. TIPO DE TOMADA
*   **Type**: Categorical
*   **Description**: The physical connector type.
*   **Common Values**: `Mennekes` (Type 2), `CHAdeMO`, `CCS`, `Industrial`.

### 13. FORMATO DA TOMADA
*   **Type**: Categorical
*   **Description**: Specifies if the station provides a socket (`SOCKET`) or a fixed connector (`CABLE`).

### 14. POTÊNCIA DA TOMADA (kW)
*   **Type**: Numeric
*   **Description**: Nominal power output capability of the specific socket in kilowatts.

### 15. ESTADO DA TOMADA
*   **Type**: Categorical (Status)
*   **Description**: Real-time status of the individual connector.
*   **Values**: `Disponível`, `Offline`, `Em uso`, `INOPERATIVE`, `Fora de Serviço`, `BLOCKED`.
*   **Usage**: Used to calculate "Real-time Capacity" vs "Nominal Capacity".

### 16. ÚLTIMA ATUALIZAÇÃO
*   **Type**: Timestamp
*   **Description**: The last time the data for this specific row was synced/updated.

---

## Proposal for Feature Engineering (Static Station Description)

To describe a station for a demand capture model, we need to capture physical attractiveness and capacity.

### Key Engineered Features:

1.  **Capacity (Total Stalls)**: Sum of unique `UID DA TOMADA` per `ID`. This is the fundamental "supply" volume.
2.  **Peak Performance**: `max(POTÊNCIA DA TOMADA (kW))` across all sockets. Users often choose stations based on the fastest available speed, even if they share it.
3.  **Connector Mix (Counts)**:
    *   `cnt_ccs`: Number of CCS connectors.
    *   `cnt_chademo`: Number of CHAdeMO connectors.
    *   `cnt_type2`: Number of Mennekes/Type 2 connectors.
    *   *Rationale*: This is more expressive than a simple "diversity count" as it tells the model exactly which car fleets can charge there.
4.  **Voltage Level (Ordinal Encoding)**:
    *   Encode `NÍVEL DE TENSÃO` as `BTN=1, BTE=2, MT=3`. 
    *   *Rationale*: This captures the "seniority" of the grid connection. MT (Medium Voltage) stations typically support higher power stability and faster charging clusters.
5.  **CPO Portfolio Logic**:
    *   Dummy-code the top 5-10 operators (e.g., `is_edp`, `is_galp`, `is_ionity`).
    *   Calculate `Operator_Global_Share`: The % of the total Mobi.E network owned by this operator.
    *   *Rationale*: This captures brand loyalty and "network effects" (users with an EDP card prefer EDP stations).
6.  **Ownership Hierarchy**: Distinction between high-speed networks (OPCs like Ionity/Brisa) vs. municipal/retail clusters.
