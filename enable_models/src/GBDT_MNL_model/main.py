# src/main.py

from pathlib import Path
from time import time

from ..data.data import load_data
from .train import train_model
from .capture_model import WeightedConditionalLogit
from .demand_model import LGBMRegressor
from .model_io import save_model_artifacts, load_model_artifacts


def build_fresh_models():
    demand_model = LGBMRegressor(
        objective="regression",
        n_estimators=800,
        learning_rate=0.04,
        num_leaves=31,
        max_depth=-1,
        min_child_samples=50,
        subsample=0.85,
        colsample_bytree=0.85,
        reg_alpha=0.0,
        reg_lambda=1.0,
        random_state=42,
        n_jobs=-1,
        verbose=-1,
    )

    capture_model = WeightedConditionalLogit(
        l2=1e-4,
        maxiter=300,
    )

    return demand_model, capture_model


def main():
    data = load_data(data_dir="data/synthetic")

    print("Dataset loaded from disk")
    print(f"X_train: {data.X_train.shape}")
    print(f"Z_train: {data.Z_train.shape}")
    print(f"J_train: {data.J_train.shape}")
    print(f"y_train: {data.y_train.shape}")
    print(f"X_val:   {data.X_val.shape}")
    print(f"Z_val:   {data.Z_val.shape}")
    print(f"J_val:   {data.J_val.shape}")
    print(f"y_val:   {data.y_val.shape}")
    print()

    current_date = time.strftime("%Y-%m-%d")
    current_time = time.strftime("%H-%M-%S")

    model_dir = Path("artifacts/GBDT_MNL_model") / f"GBDT_MNL_model_{current_date}_{current_time}"
    resume = model_dir.exists() and (model_dir / "demand_model.joblib").exists()

    if resume:
        print(f"Resuming from checkpoint: {model_dir}")
        demand_model, capture_model, metadata = load_model_artifacts(model_dir)
        history = metadata.get("history", [])
        start_iter = len(history)
    else:
        print("Starting fresh training run")
        demand_model, capture_model = build_fresh_models()
        history = []
        start_iter = 0

    result = train_model(
        demand_model=demand_model,
        capture_model=capture_model,
        data=data,
        n_em_iters=10,          # number of extra EM iterations
        start_iter=start_iter,
        history=history,
    )

    save_model_artifacts(
        demand_model=result["demand_model"],
        capture_model=result["capture_model"],
        output_dir=model_dir,
        metadata={
            "demand_feature_names": data.demand_feature_names,
            "capture_feature_names": data.capture_feature_names,
            "K": data.K,
            "n_cells": data.n_cells,
            "n_stations": data.n_stations,
            "history": result["history"],
        },
    )

    print()
    print(f"Saved trained models to {model_dir}")

    if data.true_beta is not None:
        print()
        print("True beta vs learned beta:")
        learned_beta = result["capture_model"].beta_

        for name, b_true, b_hat in zip(
            data.capture_feature_names,
            data.true_beta,
            learned_beta,
        ):
            print(f"{name:25s} true={b_true:+.4f} learned={b_hat:+.4f}")


if __name__ == "__main__":
    main()