# src/main_neural.py

from pathlib import Path
import json

import torch

from ..data.data import load_data
from .train_neural import train_neural_model, NeuralTrainConfig

import time

import argparse


def main():
    data = load_data("data/synthetic")

    device = "cuda" if torch.cuda.is_available() else "cpu"
    print(f"Using device: {device}")

    parser = argparse.ArgumentParser()
    parser.add_argument("--neural-type", default="better_mlp", choices=["mlp", "better_mlp"])
    args = parser.parse_args()

    config = NeuralTrainConfig(
        epochs=1000,
        lr=3e-4,
        weight_decay=1e-4,
        batch_days=data.n_train_days,
        demand_hidden=(128, 128),
        capture_hidden=(128, 128),
        dropout=0.01,
        patience=80,
        neural_type=args.neural_type,
        device=device,
    )

    result = train_neural_model(data, config)


    current_date = time.strftime("%Y-%m-%d")
    current_time = time.strftime("%H-%M-%S")

    output_dir = Path("artifacts/neural/") / f"neural_{current_date}_{current_time}"
    output_dir.mkdir(parents=True, exist_ok=True)

    torch.save(
        {
            "model_state_dict": result["model"].state_dict(),
        },
        output_dir / f"flow_mlp_{current_date}_{current_time}.pt",
    )

    metadata = {
        "method": "end_to_end_flow_mlp",
        "demand_feature_names": data.demand_feature_names,
        "capture_feature_names": data.capture_feature_names,
        "K": data.K,
        "n_cells": data.n_cells,
        "n_stations": data.n_stations,
        "history": result["history"],
        "best_epoch": result["best_epoch"],
        "best_val_log_mse": result["best_val_log_mse"],
    }

    with open(output_dir / f"training_metadata_{current_date}_{current_time}.json", "w") as f:
        json.dump(metadata, f, indent=2)

    with open(output_dir / f"config_{current_date}_{current_time}.json", "w") as f:
        json.dump(config.__dict__, f, indent=2)

    print()
    print(f"Saved neural model to {output_dir}")


if __name__ == "__main__":
    main()