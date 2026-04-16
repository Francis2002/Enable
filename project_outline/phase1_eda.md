# Phase 1: Exploratory Data Analysis (EDA)

## 📌 Objectives
Understand what drives the performance of Electric Vehicle (EV) charging stations—specifically Fast vs. Ultra-Fast charging. The goal is to determine the impact of Point of Interest (POI) density, pricing, and overall location characteristics on station utilization, translating these findings into clear, client-friendly visualizations.

## 📊 Target Variables (Labels)
- **Energy Dispensed:** Total kWh sold.
- **Utilization:** Number of charging sessions.

## 🔍 Key Areas of Investigation
1. **Charging Speeds (Fast vs. Ultra-Fast):** Do different speeds require completely different environments to succeed?
2. **POI Impact:** Which categories (e.g., retail, restaurants, highways, urban centers) correlate most strongly with high kWh and session counts?
3. **Pricing Elasticity:** How does the price of charging influence utilization rates compared to sheer convenience (location)?
4. **Geospatial Hotspots:** Identifying geographic areas that inherently perform better based on historical data.

## 📈 Deliverables & Visualizations (Client-Facing)
- **Correlation Matrices:** Easy-to-read heatmaps showing relationships between POIs, pricing, and the target labels.
- **Geospatial Heatmaps:** Maps displaying high-performing stations and density of surrounding amenities.
- **Categorical Comparisons:** Bar charts or box plots comparing Fast vs. Ultra-Fast station performance across different location types (Urban vs. Highway).
- **Executive Summary:** A set of concrete "rules of thumb" or insights derived from the data to present to stakeholders.

## 🛠️ Tech Stack & Requirements
- **Libraries:** `pandas`, `geopandas`, `matplotlib`, `seaborn` / `plotly` (for interactive client plots).