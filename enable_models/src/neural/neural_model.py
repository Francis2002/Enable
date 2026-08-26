# src/neural_model.py

from __future__ import annotations

import torch
import torch.nn as nn
import torch.nn.functional as F


class MLP(nn.Module):
    def __init__(
        self,
        in_dim: int,
        hidden_dims: tuple[int, ...],
        out_dim: int,
        dropout: float = 0.0,
    ):
        super().__init__()

        layers = []
        prev = in_dim

        for h in hidden_dims:
            layers.append(nn.Linear(prev, h))
            layers.append(nn.ReLU())
            if dropout > 0:
                layers.append(nn.Dropout(dropout))
            prev = h

        layers.append(nn.Linear(prev, out_dim))

        self.net = nn.Sequential(*layers)

    def forward(self, x):
        return self.net(x)
    
class ResidualBlock(nn.Module):
    def __init__(
        self,
        block_dim: int,
        hidden_dim: int,
        dropout: float = 0.0,
    ):
        super().__init__()
        self.net = nn.Sequential(
            nn.LayerNorm(block_dim),
            nn.Linear(block_dim, 2 * hidden_dim),
            nn.GELU(),
            nn.Dropout(dropout),
            nn.Linear(2 * hidden_dim, block_dim),
        )

    def forward(self, x):
        return x + self.net(x)
    
class BetterMLP(nn.Module):
    def __init__(
        self,
        in_dim: int,
        hidden_dims: tuple[int, ...],
        out_dim: int,
        dropout: float = 0.0,
    ):
        super().__init__()

        layers = []

        layers.append(nn.Linear(in_dim, hidden_dims[0]))

        prev = hidden_dims[0]
        for h in hidden_dims:
            layers.append(ResidualBlock(prev, h, dropout))
            prev = h

        layers.append(nn.Linear(prev, out_dim))

        self.net = nn.Sequential(*layers)

        # Print layers
        print("BetterMLP architecture:")
        for layer in self.net:
            print(f"  {layer}")

    def forward(self, x):
        return self.net(x)


class EndToEndFlowMLP(nn.Module):
    """
    End-to-end neural version of:

        D_c = positive demand MLP(X_c)
        U_ck = capture MLP(Z_ck)
        P_ck = softmax_k(U_ck)
        yhat_s = sum_{c,k: J_ck=s} D_c * P_ck
    """

    def __init__(
        self,
        demand_in_dim: int,
        capture_in_dim: int,
        demand_hidden: tuple[int, ...] = (128, 128),
        capture_hidden: tuple[int, ...] = (128, 128),
        dropout: float = 0.05,
        neural_type: str = "mlp",
    ):
        super().__init__()

        if neural_type == "mlp":
            MLPClass = MLP
        elif neural_type == "better_mlp":
            MLPClass = BetterMLP
        else:
            raise ValueError(f"Unsupported neural_type: {neural_type}")
        
        self.demand_net = MLPClass(
            in_dim=demand_in_dim,
            hidden_dims=demand_hidden,
            out_dim=1,
            dropout=dropout,
        )

        self.capture_net = MLPClass(
            in_dim=capture_in_dim,
            hidden_dims=capture_hidden,
            out_dim=1,
            dropout=dropout,
        )

    def forward(
        self,
        X: torch.Tensor,       # (C, P)
        Z: torch.Tensor,       # (C, K, R)
        J: torch.Tensor,       # (C, K)
        n_station_days: int,
    ):
        C, K, R = Z.shape

        # Demand must be positive.
        # Softplus is smoother and safer than exp at the beginning.
        D = F.softplus(self.demand_net(X)).squeeze(-1) + 1e-6  # (C,)

        # Capture logits per alternative.
        U = self.capture_net(Z.reshape(C * K, R)).reshape(C, K)  # (C, K)

        # Softmax over alternatives inside each choice set.
        P = torch.softmax(U, dim=1)  # (C, K)

        # Flow per edge.
        flow = D[:, None] * P  # (C, K)

        # Aggregate to station-day predictions.
        yhat = torch.zeros(
            n_station_days,
            dtype=flow.dtype,
            device=flow.device,
        )

        yhat.scatter_add_(
            dim=0,
            index=J.reshape(-1),
            src=flow.reshape(-1),
        )

        return yhat, D, P