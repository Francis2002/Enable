# Analysis Report: MOBIE_Tarifas.csv

This document analyzes the Mobi.E Tariff dataset, which describes the pricing structure for every socket in the network.

## Field Descriptions

### 1. ID
*   **Type**: String
*   **Description**: Station identifier (e.g., `ABF-00008`). Matches the `ID` in the Station List.

### 2. UID_TOMADA
*   **Type**: String
*   **Description**: Socket/Tariff identifier (e.g., `PT-EDP-EABF-00008-1-1`). 
*   **Notes**: This ID is more specific than the one in the Station List. Multiple rows in the Tariff file can share the same socket prefix if that socket has multiple price components (e.g., Energy price + Time price + Activation fee).

### 3. TIPO_POSTO
*   **Type**: Categorical
*   **Description**: Simplified speed category. 
*   **Values**: `Médio`, `Rápido`, `Ultrarrápido`, `Lento`.

### 4. MUNICIPIO
*   **Type**: String (Categorical)
*   **Description**: Municipality name. Useful for regional demand analysis and local tax environment proxy.

### 5. MORADA
*   **Type**: String (Text)
*   **Description**: Address. Matches Station List.

### 6. OPERADOR
*   **Type**: String (Categorical - Abbreviation)
*   **Description**: Short code for the CPO (e.g., `EDP`, `GLP`).

### 7. MOBICHARGER
*   **Type**: Boolean
*   **Description**: Matches Station List flag.

### 8. NIVELTENSAO
*   **Type**: Categorical (Abbreviation)
*   **Description**: Grid connection voltage.
*   **Values**: `MT` (Medium), `BTE` (Special Low), `BTN` (Normal Low).

### 9. TIPO_TARIFARIO
*   **Type**: Categorical
*   **Description**: The contractual framework for the tariff.
*   **Values**: 
    *   `REGULAR`: Standard contract rates.
    *   `AD_HOC_PAYMENT`: Rates for walk-up users without a specific CEME contract.

### 10. TIPO_TARIFA
*   **Type**: Categorical
*   **Description**: The dimension being charged.
*   **Values**:
    *   `FLAT`: A one-time activation or fixed fee per session.
    *   `ENERGY`: Price per kWh consumed.
    *   `TIME`: Price per minute spent charging.
    *   `PARKING_TIME`: Price per minute spent at the station (often after charging is complete).

### 11. TARIFA
*   **Type**: String/Numeric
*   **Description**: The specific cost and unit (e.g., `€ 0.1 /kWh`, `€ 0.261 /charge`).
*   **Notes**: Requires parsing to extract numeric values for computational use.

### 12. TIPO_TOMADA
*   **Type**: Categorical
*   **Description**: Connector type (Mennekes, CSS, etc.). Matches Station List.

### 13. FORMATO_TOMADA
*   **Type**: Categorical
*   **Description**: Socket vs Cable. Matches Station List.

### 14. POTENCIA_TOMADA
*   **Type**: Numeric
*   **Description**: Power rating in kW. Matches Station List.

---

## Proposal for Feature Engineering (Pricing & Demand Sensitivity)

Price is a primary driver in demand capture models. We distinguish between two data sources:
*   **Static Price (Tarifas.csv)**: This is the "Scheduled Price" a user sees on an app *before* choosing. This is a **Predictive Feature**.
*   **Realized Price (Detailed.csv)**: This is what was actually paid. This is a **Historical Fact**.

For the model, we use the **Static Price** as the primary feature.

### Engineered Features:

1.  **Effective kWh Price**: Calculate a "Typical Session Cost" to normalize across Energy, Time, and Activation fees.
    *   *Reference Session*: 20kWh charge over 1 hour.
    *   `Price = (Activation Fee) + (20 * Energy Fee) + (60 * Time Fee)`.
    *   `Normalized_Price_per_kWh = Price / 20`.
2.  **Operator Price Rank**: Global percentile of the station's `Normalized_Price_per_kWh` within its speed category (`TIPO_POSTO`). Captures how competitive a station is vs. its neighbors.
3.  **Ad-Hoc Premium**: Ratio between `AD_HOC_PAYMENT` and `REGULAR` tariffs. High premiums discourage walk-up demand.
4.  **Parking Fee Flag**: Binary indicator (`0/1`) if `TIPO_TARIFA == PARKING_TIME`. Even without duration data, the *threat* of a parking fee affects demand behavior (favors higher turnover).
