# Phase 2: Machine Learning Modeling & Prediction

## 📌 Objectives
Develop, train, and evaluate predictive models capable of scoring potential new EV charging locations. By dropping a pin on a map, the pipeline must automatically extract spatial features (POIs, road networks) and predict the expected kWh sold and number of sessions.

## 🧠 Modeling Methodologies
- **Algorithms to Test:** Random Forest, XGBoost, LightGBM, Spatial Regressors (e.g., Spatial Lag Models), and Neural Networks.
- **Target Segmentation:** Train models capable of adapting to specific client segments (e.g., a client strictly focused on highway ultra-fast charging vs. inner-city fast charging).

## ⚙️ Feature Engineering Pipeline
- **Input:** Raw GPS Coordinates (Latitude, Longitude).
- **Extraction:**
  - Query surrounding POIs within specific radii (e.g., 1km, 5km).
  - Calculate distance to major road networks/highways.
  - Assess competitor density or existing charging infrastructure in the vicinity.
- **Transformation:** Normalize and encode features to match the training dataset schema.

## 📐 Evaluation Metrics
- **Continuous Predictions (kWh, Sessions):** Root Mean Squared Error (RMSE), Mean Absolute Error (MAE), R-squared ($R^2$).
- **Cross-Validation:** Implement *Spatial Cross-Validation* to prevent data leakage (since nearby stations are highly correlated).

## 🛠️ Tech Stack & Requirements
- **Libraries:** `scikit-learn`, `xgboost`, `osmnx` (or similar for dynamic map feature extraction), `shap` (for model interpretability and explaining predictions).