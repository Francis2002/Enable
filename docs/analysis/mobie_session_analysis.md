# Analysis Report: detailed-20260116.csv

This document analyzes the detailed Session Data (CDR - Charging Data Records) for the Mobi.E network. 

## Context: Session Fragmentation
A session spanning midnight (e.g., starts at 22:00 Jan 15, ends at 02:00 Jan 16) is split into TWO rows. Both rows share the same `idCdr` but have different `idDay` and `energia_total_periodo`.

## Field Descriptions

### 1. idCdr
*   **Type**: UUID/String
*   **Description**: The unique session identifier. Used to group fragments belonging to the same physical charge event.

### 2. idUsage
*   **Type**: String
*   **Description**: Usage event identifier. Often matches or is derived from station code + sequence.

### 3. idServiceProvider
*   **Type**: String (CEME)
*   **Description**: The Electric Mobility Energy Supplier (CEME) (e.g., `PRIO`, `MUVE`, `GLPP`). This is the entity that bills the user.

### 4. idInternalNumber
*   **Type**: String
*   **Description**: Internal provider-specific identifier.

### 5. type
*   **Type**: Categorical
*   **Description**: The authentication method. e.g., `RFID`, `AD_HOC_USER`.

### 6. idNetworkOperator
*   **Type**: String
*   **Description**: The Network Operator identifier (usually `ENBL` for Entidade Gestora de Mobilidade Elétrica).

### 7. idChargingStation
*   **Type**: String
*   **Description**: The Station ID (e.g., `ODV-00044`). Matches `ID` in the Station Master List.

### 8. idEVSE
*   **Type**: String
*   **Description**: The specific socket/connector ID. Matches `UID DA TOMADA` in the Station Master List.

### 9. evse_max_power
*   **Type**: Numeric
*   **Description**: The maximum power (kW) available at that specific socket during that session.

### 10. startTimestamp
*   **Type**: Timestamp (YYYYMMDDHHMMSS)
*   **Description**: The moment the session began.

### 11. stopTimestamp
*   **Type**: Timestamp (YYYYMMDDHHMMSS)
*   **Description**: The moment the session ended.

### 12. totalDuration
*   **Type**: Numeric
*   **Description**: The TOTAL session duration in minutes (constant across fragments of the same `idCdr`).

### 13. energia_total_transacao
*   **Type**: Numeric
*   **Description**: The TOTAL energy (kWh) consumed in the session (constant across fragments of the same `idCdr`).

### 14. nivel_tensao_ponto_entrega
*   **Type**: Categorical
*   **Description**: Voltage level at the delivery point (e.g., `BTE`, `MT`).

### 15. nivel_tensao_transacao
*   **Type**: Categorical
*   **Description**: Voltage level used for the specific transaction.

### 16. idORD
*   **Type**: String
*   **Description**: Redes Energéticas Nacionais (REN) or Distributor entity identifier.

### 17. nuts_1
*   **Type**: String
*   **Description**: Nomenclature of Territorial Units for Statistics (Level 1).

### 18. idSubUsage
*   **Type**: String
*   **Description**: Sub-event identifier.

### 19. idDay
*   **Type**: Numeric (YYYYMMDD)
*   **Description**: The specific calendar day this record/fragment pertains to.

### 20. periodDuration
*   **Type**: Numeric
*   **Description**: Minutes consumed *within this specific day*.

### 21 - 31. Price Fields (`preco_opc`, `preco_unitario_opc_energia`, etc.)
*   **Type**: Numeric
*   **Description**: Detailed breakdown of the cost. 
*   **Key Distinction**: `preco_opc` refers to the CPO fee, while `preco_adhoc` is the transient fee.

### 32. energia_total_periodo
*   **Type**: Numeric
*   **Description**: Energy (kWh) consumed *within this specific day*.
*   **Usage**: Summing this per day gives accurate daily demand.

### 33 - 38. Time Partitioning Fields (`duracao_ponta`, `duracao_vazio`, etc.)
*   **Type**: Numeric
*   **Description**: Minutes spent in different electricity tariff periods (Peak, Off-Peak, Empty, etc.).

---

## Proposal for Feature & Label Engineering (Realized Demand)

This data provides the "Ground Truth" for our model.

### Label Engineering (Model Targets):

1.  **Daily Realized Energy (kWh)**: `sum(energia_total_periodo)` per Station-Day. This is the primary target for a demand model.
2.  **Daily Session Count**: `count(distinct idCdr)` where `start_day == target_day`.
    *   **Decision: Why `start_day`?** We use the start day because a demand model predicts the user's *decision* to pick a station. That decision occurs when the session begins. Using `end_day` would lag the demand signal for overnight sessions.

### Engineered Features (Dynamic Context):
*These features describe the "behavior" or "type" of demand the station attracts.*

1.  **Saturation Fraction (Proxy)**:
    *   **Max Concurrent**: `max(concurrent_sessions)` observed during the day.
    *   **Saturation Ratio**: `% of the day` where `concurrent_sessions == capacity`. This is a critical feature for predicting when a station is at its limit.
2.  **CEME Portfolio**: Ratio of sessions from different `idServiceProvider` (e.g., % PRIO, % MUVE). High concentration suggests loyalty-driven demand or commercial fleet use.
3.  **Auth Type (Ad-Hoc vs RFID)**: % of `AD_HOC_USER`. High values indicate transient/tourist demand vs. regular resident demand.
4.  **Time-of-Day Profile**: Ratio of night/vazio duration vs. daytime duration. Helps distinguish "home-proxy" stations from "errand" stations.
