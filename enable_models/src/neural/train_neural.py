# src/train_neural.py

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass

import numpy as np
import torch

from .neural_model import EndToEndFlowMLP


@dataclass
class NeuralTrainConfig:
    epochs: int = 1000
    lr: float = 1e-3
    weight_decay: float = 1e-4
    batch_days: int = 7
    demand_hidden: tuple[int, int] = (128, 128)
    capture_hidden: tuple[int, int] = (128, 128)
    dropout: float = 0.05
    patience: int = 80
    neural_type: str = "better_mlp"
    device: str = "cpu"


def _iter_day_batches(
    X: np.ndarray,
    Z: np.ndarray,
    J: np.ndarray,
    y: np.ndarray,
    n_cells: int,
    n_stations: int,
    n_days: int,
    batch_days: int,
):
    """
    Yields batches containing complete days.

    This is necessary because the loss is defined after aggregating
    cell-to-station flows into station-day labels.
    """

    for day_start in range(0, n_days, batch_days):
        day_end = min(day_start + batch_days, n_days)

        c0 = day_start * n_cells
        c1 = day_end * n_cells

        s0 = day_start * n_stations
        s1 = day_end * n_stations

        X_b = X[c0:c1]
        Z_b = Z[c0:c1]
        J_b = J[c0:c1] - s0
        y_b = y[s0:s1]

        n_station_days_b = s1 - s0

        yield X_b, Z_b, J_b, y_b, n_station_days_b


def _log_mse_loss(yhat: torch.Tensor, y: torch.Tensor) -> torch.Tensor:
    return torch.mean((torch.log1p(yhat) - torch.log1p(y)) ** 2)


def _evaluate(model, data, config: NeuralTrainConfig):
    model.eval()

    total_abs_err = 0.0
    total_y = 0.0
    total_log_mse_sum = 0.0
    total_count = 0

    with torch.no_grad():
        for X_b, Z_b, J_b, y_b, n_station_days_b in _iter_day_batches(
            X=data.X_val,
            Z=data.Z_val,
            J=data.J_val,
            y=data.y_val,
            n_cells=data.n_cells,
            n_stations=data.n_stations,
            n_days=data.n_val_days,
            batch_days=config.batch_days,
        ):
            X_t = torch.tensor(X_b, dtype=torch.float32, device=config.device)
            Z_t = torch.tensor(Z_b, dtype=torch.float32, device=config.device)
            J_t = torch.tensor(J_b, dtype=torch.long, device=config.device)
            y_t = torch.tensor(y_b, dtype=torch.float32, device=config.device)

            yhat_t, _, _ = model(X_t, Z_t, J_t, n_station_days_b)

            abs_err = torch.abs(yhat_t - y_t)
            log_mse = (torch.log1p(yhat_t) - torch.log1p(y_t)) ** 2

            total_abs_err += float(abs_err.sum().cpu())
            total_y += float(torch.abs(y_t).sum().cpu())
            total_log_mse_sum += float(log_mse.sum().cpu())
            total_count += y_t.numel()

    wape = total_abs_err / max(total_y, 1e-8)
    log_mse = total_log_mse_sum / max(total_count, 1)

    return {
        "wape": wape,
        "log_mse": log_mse,
    }


def train_neural_model(data, config: NeuralTrainConfig):
    model = EndToEndFlowMLP(
        demand_in_dim=data.X_train.shape[1],
        capture_in_dim=data.Z_train.shape[2],
        demand_hidden=config.demand_hidden,
        capture_hidden=config.capture_hidden,
        dropout=config.dropout,
        neural_type=config.neural_type
    ).to(config.device)

    optimizer = torch.optim.AdamW(
        model.parameters(),
        lr=config.lr,
        weight_decay=config.weight_decay,
    )

    scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(
        optimizer,
        mode="min",
        factor=0.5,
        patience=20,
        min_lr=1e-5,
    )

    best_val = float("inf")
    best_state = None
    best_epoch = -1
    bad_epochs = 0

    history = []

    for epoch in range(config.epochs):
        model.train()

        train_loss_sum = 0.0
        train_count = 0

        for X_b, Z_b, J_b, y_b, n_station_days_b in _iter_day_batches(
            X=data.X_train,
            Z=data.Z_train,
            J=data.J_train,
            y=data.y_train,
            n_cells=data.n_cells,
            n_stations=data.n_stations,
            n_days=data.n_train_days,
            batch_days=config.batch_days,
        ):
            X_t = torch.tensor(X_b, dtype=torch.float32, device=config.device)
            Z_t = torch.tensor(Z_b, dtype=torch.float32, device=config.device)
            J_t = torch.tensor(J_b, dtype=torch.long, device=config.device)
            y_t = torch.tensor(y_b, dtype=torch.float32, device=config.device)

            optimizer.zero_grad(set_to_none=True)

            yhat_t, _, _ = model(X_t, Z_t, J_t, n_station_days_b)

            loss = _log_mse_loss(yhat_t, y_t)

            loss.backward()
            torch.nn.utils.clip_grad_norm_(model.parameters(), max_norm=5.0)
            optimizer.step()

            train_loss_sum += float(loss.detach().cpu()) * y_t.numel()
            train_count += y_t.numel()

        train_log_mse = train_loss_sum / max(train_count, 1)

        val_metrics = _evaluate(model, data, config)
        val_log_mse = val_metrics["log_mse"]

        scheduler.step(val_log_mse)

        current_lr = optimizer.param_groups[0]["lr"]

        row = {
            "epoch": epoch + 1,
            "train_log_mse": train_log_mse,
            "val_log_mse": val_metrics["log_mse"],
            "val_wape": val_metrics["wape"],
            "best_val_log_mse": best_val,
            "best_epoch": best_epoch,
            "lr": current_lr,
        }
        history.append(row)

        print(
            f"Epoch {epoch + 1:04d}/{config.epochs} | "
            f"train logMSE={train_log_mse:.4f} | "
            f"val WAPE={val_metrics['wape']:.4f}, "
            f"val logMSE={val_metrics['log_mse']:.4f} | "
            f"best val logMSE={best_val:.4f} @ {best_epoch} | "
            f"lr={current_lr:.2e}"
        )

        if val_log_mse < best_val:
            best_val = val_log_mse
            best_state = deepcopy(model.state_dict())
            best_epoch = epoch + 1
            bad_epochs = 0
        else:
            bad_epochs += 1

        if bad_epochs >= config.patience:
            print(f"Early stopping at epoch {epoch + 1}. Best epoch: {best_epoch}")
            break

    if best_state is not None:
        model.load_state_dict(best_state)

    return {
        "model": model,
        "history": history,
        "best_epoch": best_epoch,
        "best_val_log_mse": best_val,
    }