# utils.py

import numpy as np


def aggregate_station_flows(
    D: np.ndarray,
    P: np.ndarray,
    J: np.ndarray,
    n_station_days: int,
) -> np.ndarray:
    """
    Aggregates cell-to-station flows into station-day predictions.

    Args:
        D: (C,) latent demand per cell-day.
        P: (C, K) capture probabilities.
        J: (C, K) local station-day indices.
        n_station_days: total number of station-day labels in this split.

    Returns:
        yhat: (n_station_days,) predicted station-day kWh.
    """

    flow = D[:, None] * P
    yhat = np.zeros(n_station_days, dtype=np.float64)
    np.add.at(yhat, J.ravel(), flow.ravel())
    return yhat


def regression_metrics(y_true: np.ndarray, y_pred: np.ndarray) -> dict:
    eps = 1e-8

    err = y_pred - y_true
    abs_err = np.abs(err)

    return {
        "mae": float(abs_err.mean()),
        "rmse": float(np.sqrt(np.mean(err ** 2))),
        "wape": float(abs_err.sum() / np.clip(np.abs(y_true).sum(), eps, None)),
        "log_mse": float(np.mean((np.log1p(y_pred) - np.log1p(y_true)) ** 2)),
    }