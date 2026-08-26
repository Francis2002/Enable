# src/train.py

from __future__ import annotations

import numpy as np
import pandas as pd

from .utils import aggregate_station_flows, regression_metrics


def _as_demand_df(X: np.ndarray, feature_names: list[str]) -> pd.DataFrame:
    return pd.DataFrame(X, columns=feature_names)


def _is_lgbm_fitted(model) -> bool:
    return hasattr(model, "booster_")


def _predict_demand_or_initial(
    demand_model,
    X: np.ndarray,
    D_initial: np.ndarray,
    feature_names: list[str],
    is_fitted: bool,
) -> np.ndarray:
    if not is_fitted:
        return D_initial.copy()

    X_df = _as_demand_df(X, feature_names)
    pred_log1p = demand_model.predict(X_df)
    return np.expm1(pred_log1p).clip(min=0.0)


def _predict_demand(
    demand_model,
    X: np.ndarray,
    feature_names: list[str],
) -> np.ndarray:
    X_df = _as_demand_df(X, feature_names)
    pred_log1p = demand_model.predict(X_df)
    return np.expm1(pred_log1p).clip(min=0.0)


def train_model(
    demand_model,
    capture_model,
    data,
    n_em_iters: int,
    start_iter: int = 0,
    history: list[dict] | None = None,
):
    """
    EM-style training loop.

    This function supports checkpoint resume:
      - if demand_model already has booster_, it is treated as fitted
      - if capture_model already has beta_, it is treated as fitted
      - history can be passed from a previous checkpoint
    """

    if history is None:
        history = []

    if capture_model.beta_ is None:
        capture_model.initialize(data.beta_init)

    demand_is_fitted = _is_lgbm_fitted(demand_model)

    X_train_df = _as_demand_df(data.X_train, data.demand_feature_names)

    for local_iter in range(n_em_iters):
        em_iter = start_iter + local_iter

        # ------------------------------------------------------------
        # 1) Current demand prediction
        # ------------------------------------------------------------
        D_train = _predict_demand_or_initial(
            demand_model=demand_model,
            X=data.X_train,
            D_initial=data.D0_train,
            feature_names=data.demand_feature_names,
            is_fitted=demand_is_fitted,
        )

        # ------------------------------------------------------------
        # 2) Current capture probabilities
        # ------------------------------------------------------------
        P_train = capture_model.predict_proba(data.Z_train)

        # ------------------------------------------------------------
        # 3) Station aggregation
        # ------------------------------------------------------------
        yhat_train = aggregate_station_flows(
            D=D_train,
            P=P_train,
            J=data.J_train,
            n_station_days=data.n_station_days_train,
        )

        # ------------------------------------------------------------
        # 4) Rescale flows to observed station-day labels
        # ------------------------------------------------------------
        gamma_train = data.y_train / np.clip(yhat_train, a_min=1e-8, a_max=None)
        F_train = D_train[:, None] * P_train * gamma_train[data.J_train]

        # ------------------------------------------------------------
        # 5) Pseudo-demand target
        # ------------------------------------------------------------
        D_tilde_train = np.sum(F_train, axis=1)

        # ------------------------------------------------------------
        # 6) Fit LightGBM on pseudo-demand
        # ------------------------------------------------------------
        demand_model.fit(
            X_train_df,
            np.log1p(D_tilde_train),
        )
        demand_is_fitted = True

        # ------------------------------------------------------------
        # 7) Recompute demand with fitted LightGBM
        # ------------------------------------------------------------
        D_train = _predict_demand(
            demand_model=demand_model,
            X=data.X_train,
            feature_names=data.demand_feature_names,
        )

        # ------------------------------------------------------------
        # 8) Recompute pseudo-flows with updated demand
        # ------------------------------------------------------------
        P_train = capture_model.predict_proba(data.Z_train)

        yhat_train = aggregate_station_flows(
            D=D_train,
            P=P_train,
            J=data.J_train,
            n_station_days=data.n_station_days_train,
        )

        gamma_train = data.y_train / np.clip(yhat_train, a_min=1e-8, a_max=None)
        F_train = D_train[:, None] * P_train * gamma_train[data.J_train]

        # ------------------------------------------------------------
        # 9) Fit capture beta using weighted conditional logit
        # ------------------------------------------------------------
        capture_model.fit(
            data.Z_train,
            F_train,
            beta0=capture_model.beta_,
        )

        # ------------------------------------------------------------
        # 10) Evaluate train and validation
        # ------------------------------------------------------------
        D_train_eval = _predict_demand(
            demand_model=demand_model,
            X=data.X_train,
            feature_names=data.demand_feature_names,
        )

        P_train_eval = capture_model.predict_proba(data.Z_train)

        yhat_train_eval = aggregate_station_flows(
            D=D_train_eval,
            P=P_train_eval,
            J=data.J_train,
            n_station_days=data.n_station_days_train,
        )

        D_val = _predict_demand(
            demand_model=demand_model,
            X=data.X_val,
            feature_names=data.demand_feature_names,
        )

        P_val = capture_model.predict_proba(data.Z_val)

        yhat_val = aggregate_station_flows(
            D=D_val,
            P=P_val,
            J=data.J_val,
            n_station_days=data.n_station_days_val,
        )

        train_metrics = regression_metrics(data.y_train, yhat_train_eval)
        val_metrics = regression_metrics(data.y_val, yhat_val)

        row = {
            "em_iter": em_iter + 1,
            "train": train_metrics,
            "val": val_metrics,
            "capture_success": bool(capture_model.opt_result_.success),
            "capture_loss": float(capture_model.opt_result_.fun),
        }
        history.append(row)

        print(
            f"EM {em_iter + 1:02d} | "
            f"train WAPE={train_metrics['wape']:.4f}, "
            f"train logMSE={train_metrics['log_mse']:.4f} | "
            f"val WAPE={val_metrics['wape']:.4f}, "
            f"val logMSE={val_metrics['log_mse']:.4f} | "
            f"capture success={capture_model.opt_result_.success}"
        )

    return {
        "demand_model": demand_model,
        "capture_model": capture_model,
        "history": history,
    }