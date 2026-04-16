# Phase 3: Web Application & Automated Reporting

## 📌 Objectives
Build an interactive web interface where clients or internal users can drop a pin (or input a location) and instantly receive a prediction for EV charging viability, accompanied by a downloadable, explanatory PDF report.

## 💻 Core Features
1. **Interactive Map UI:** A clean interface to search for addresses, drop pins, and define the type of station (Fast vs. Ultra-Fast).
2. **Backend Inference Trigger:** Seamless connection to the Phase 2 modeling pipeline to extract features for the new location on-the-fly and run model predictions.
3. **Automated PDF Report Generation:**
   - **Prediction Scores:** Estimated kWh sold and sessions.
   - **Feature Importance (The "Why"):** Explainable AI output (e.g., SHAP plots) showing *why* a location scored high or low (e.g., "High score driven by proximity to retail and a major highway").
   - **Visuals:** Map snapshot of the area and nearby POIs.

## 🏗️ Architecture & Tech Stack (TBD)
*Note: The exact stack is left open for now and will be decided as the project evolves.*
- **Frontend / Data App Options:** `Streamlit`, `Dash`, or a custom `React` / `Vue` frontend.
- **Backend / API Options:** `FastAPI`, `Flask`, or integrated directly into a framework like Streamlit.
- **Mapping Libraries:** `Leaflet`, `Mapbox`, or `Folium`.
- **PDF Generation:** `ReportLab`, `WeasyPrint`, or HTML-to-PDF conversion tools.