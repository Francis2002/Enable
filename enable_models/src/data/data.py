# data.py

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional

import numpy as np

# src/data.py

import json
from pathlib import Path
from typing import Literal

import numpy as np
import pandas as pd

# Keep your existing imports/classes/functions:
# - DataBundle
# - generate_synthetic_data


@dataclass
class DataBundle:
    # Train arrays
    X_train: np.ndarray          # (C_train, P)
    Z_train: np.ndarray          # (C_train, K, R)
    J_train: np.ndarray          # (C_train, K)
    y_train: np.ndarray          # (S_train,)
    D0_train: np.ndarray         # (C_train,)

    # Validation arrays
    X_val: np.ndarray            # (C_val, P)
    Z_val: np.ndarray            # (C_val, K, R)
    J_val: np.ndarray            # (C_val, K)
    y_val: np.ndarray            # (S_val,)
    D0_val: np.ndarray           # (C_val,)

    # Sizes
    n_cells: int
    n_stations: int
    n_train_days: int
    n_val_days: int
    n_station_days_train: int
    n_station_days_val: int
    K: int

    # Metadata
    demand_feature_names: List[str]
    capture_feature_names: List[str]

    # Synthetic truth, useful for debugging
    true_beta: np.ndarray
    beta_init: np.ndarray

    # Optional diagnostic arrays
    y_train_clean: Optional[np.ndarray] = None
    y_val_clean: Optional[np.ndarray] = None


def _standardize(x: np.ndarray, eps: float = 1e-8) -> np.ndarray:
    return (x - x.mean()) / (x.std() + eps)


def _softmax(U: np.ndarray, axis: int = 1) -> np.ndarray:
    U = U - U.max(axis=axis, keepdims=True)
    E = np.exp(U)
    return E / E.sum(axis=axis, keepdims=True)


def _aggregate_station_flows(
    D: np.ndarray,
    P: np.ndarray,
    J: np.ndarray,
    n_station_days: int,
) -> np.ndarray:
    flow = D[:, None] * P
    yhat = np.zeros(n_station_days, dtype=np.float64)
    np.add.at(yhat, J.ravel(), flow.ravel())
    return yhat


def _make_cell_static_features(
    rng: np.random.Generator,
    n_cells: int,
) -> Tuple[np.ndarray, List[str], Dict[str, np.ndarray]]:
    """
    Creates synthetic cell-level features similar in spirit to:
    census_stats, road_stats, poi_stats.
    """

    # Raw synthetic quantities
    pop = rng.lognormal(mean=7.0, sigma=1.0, size=n_cells)
    road_total = rng.gamma(shape=3.0, scale=2.0, size=n_cells)
    road_primary = road_total * rng.beta(2.0, 5.0, size=n_cells)
    road_residential = road_total * rng.beta(4.0, 2.0, size=n_cells)

    poi_food = rng.poisson(lam=np.clip(pop / pop.mean() * 4.0, 0.1, 25.0))
    poi_retail = rng.poisson(lam=np.clip(pop / pop.mean() * 3.0, 0.1, 20.0))
    poi_fuel = rng.poisson(lam=np.clip(road_primary / (road_primary.mean() + 1e-8), 0.1, 10.0))
    poi_parking = rng.poisson(lam=np.clip(pop / pop.mean() * 2.5, 0.1, 18.0))

    # Model-ready standardized features
    features = np.column_stack(
        [
            _standardize(np.log1p(pop)),
            _standardize(np.log1p(road_total)),
            _standardize(np.log1p(road_primary)),
            _standardize(np.log1p(road_residential)),
            _standardize(np.log1p(poi_food)),
            _standardize(np.log1p(poi_retail)),
            _standardize(np.log1p(poi_fuel)),
            _standardize(np.log1p(poi_parking)),
        ]
    )

    names = [
        "log_pop_z",
        "log_road_total_z",
        "log_road_primary_z",
        "log_road_residential_z",
        "log_poi_food_z",
        "log_poi_retail_z",
        "log_poi_fuel_z",
        "log_poi_parking_z",
    ]

    raw = {
        "pop": pop,
        "road_total": road_total,
        "road_primary": road_primary,
        "road_residential": road_residential,
        "poi_food": poi_food,
        "poi_retail": poi_retail,
        "poi_fuel": poi_fuel,
        "poi_parking": poi_parking,
    }

    return features.astype(np.float64), names, raw


def _make_station_static_features(
    rng: np.random.Generator,
    n_stations: int,
) -> Tuple[np.ndarray, List[str], Dict[str, np.ndarray]]:
    """
    Creates station-level features similar to:
    stalls, power, connector mix, operator, price.
    """

    stalls = rng.choice([1, 2, 3, 4, 6], size=n_stations, p=[0.20, 0.35, 0.20, 0.15, 0.10])

    station_class = rng.choice(
        ["slow", "normal", "fast", "ultrafast"],
        size=n_stations,
        p=[0.10, 0.25, 0.45, 0.20],
    )

    power_max = np.zeros(n_stations)
    power_max[station_class == "slow"] = rng.choice([3.7, 7.4], size=(station_class == "slow").sum())
    power_max[station_class == "normal"] = rng.choice([11, 22], size=(station_class == "normal").sum())
    power_max[station_class == "fast"] = rng.choice([50, 60, 90, 120], size=(station_class == "fast").sum())
    power_max[station_class == "ultrafast"] = rng.choice([150, 180, 250, 350], size=(station_class == "ultrafast").sum())

    power_sum = power_max * stalls * rng.uniform(0.75, 1.05, size=n_stations)

    n_ccs = np.maximum(0, stalls - rng.binomial(stalls, 0.20))
    n_chademo = rng.binomial(stalls, 0.25)
    n_type2 = rng.binomial(stalls, 0.45)

    is_fast = (power_max >= 43).astype(float)
    is_ultrafast = (power_max >= 150).astype(float)

    # Prices: faster stations tend to be slightly more expensive.
    price_ref_regular = (
        0.28
        + 0.05 * is_fast
        + 0.08 * is_ultrafast
        + rng.normal(0.0, 0.025, size=n_stations)
    )
    price_ref_regular = np.clip(price_ref_regular, 0.18, 0.75)

    adhoc_premium = rng.uniform(0.0, 0.35, size=n_stations)
    price_ref_adhoc = price_ref_regular * (1.0 + adhoc_premium)

    # Operators: 4 synthetic operators.
    operator = rng.integers(0, 4, size=n_stations)
    operator_1 = (operator == 1).astype(float)
    operator_2 = (operator == 2).astype(float)
    operator_3 = (operator == 3).astype(float)
    # operator 0 is baseline

    features = np.column_stack(
        [
            _standardize(np.log1p(stalls)),
            _standardize(np.log1p(power_max)),
            _standardize(np.log1p(power_sum)),
            _standardize(n_ccs),
            _standardize(n_chademo),
            _standardize(n_type2),
            is_fast,
            is_ultrafast,
            _standardize(price_ref_regular),
            _standardize(price_ref_adhoc),
            _standardize(adhoc_premium),
            operator_1,
            operator_2,
            operator_3,
        ]
    )

    names = [
        "log_stalls_z",
        "log_power_max_z",
        "log_power_sum_z",
        "n_ccs_z",
        "n_chademo_z",
        "n_type2_z",
        "is_fast",
        "is_ultrafast",
        "price_ref_regular_z",
        "price_ref_adhoc_z",
        "adhoc_premium_z",
        "operator_1",
        "operator_2",
        "operator_3",
    ]

    raw = {
        "stalls": stalls,
        "station_class": station_class,
        "power_max": power_max,
        "power_sum": power_sum,
        "n_ccs": n_ccs,
        "n_chademo": n_chademo,
        "n_type2": n_type2,
        "is_fast": is_fast,
        "is_ultrafast": is_ultrafast,
        "price_ref_regular": price_ref_regular,
        "price_ref_adhoc": price_ref_adhoc,
        "adhoc_premium": adhoc_premium,
        "operator": operator,
    }

    return features.astype(np.float64), names, raw


def _make_day_features(n_days: int) -> Tuple[np.ndarray, List[str]]:
    day_idx = np.arange(n_days)
    dow = day_idx % 7

    dow_sin = np.sin(2 * np.pi * dow / 7.0)
    dow_cos = np.cos(2 * np.pi * dow / 7.0)
    weekend = ((dow == 5) | (dow == 6)).astype(float)

    # A smooth synthetic tourism/seasonality factor.
    tourism = 0.5 + 0.5 * np.sin(2 * np.pi * (day_idx / max(n_days, 1)))

    # Sparse synthetic holidays.
    holiday = np.zeros(n_days)
    holiday[::17] = 1.0

    features = np.column_stack(
        [
            dow_sin,
            dow_cos,
            weekend,
            _standardize(tourism),
            holiday,
        ]
    )

    names = [
        "dow_sin",
        "dow_cos",
        "weekend",
        "tourism_z",
        "holiday",
    ]

    return features.astype(np.float64), names


def _make_choice_sets(
    rng: np.random.Generator,
    n_cells: int,
    n_stations: int,
    K: int,
) -> Tuple[np.ndarray, np.ndarray]:
    """
    Creates synthetic cell/station coordinates and computes top-K station choices
    by synthetic travel time.
    """

    cell_xy = rng.uniform(0, 100, size=(n_cells, 2))

    # Place stations near random cells, with noise, so geography matters.
    anchor_cells = rng.choice(n_cells, size=n_stations, replace=True)
    station_xy = cell_xy[anchor_cells] + rng.normal(0, 8.0, size=(n_stations, 2))
    station_xy = np.clip(station_xy, 0, 100)

    diff = cell_xy[:, None, :] - station_xy[None, :, :]
    euclidean_km = np.sqrt((diff**2).sum(axis=2))

    # Synthetic travel time in minutes.
    # Not exactly distance: add road-network-like noise.
    tt_all = 3.0 + 1.3 * euclidean_km + rng.normal(0.0, 2.0, size=euclidean_km.shape)
    tt_all = np.clip(tt_all, 1.0, None)

    topk_idx = np.argsort(tt_all, axis=1)[:, :K]
    topk_tt = np.take_along_axis(tt_all, topk_idx, axis=1)

    return topk_idx.astype(np.int64), topk_tt.astype(np.float64)


def _build_X_for_days(
    cell_features: np.ndarray,
    day_features: np.ndarray,
    day_indices: np.ndarray,
) -> Tuple[np.ndarray, np.ndarray]:
    """
    Builds X for all cell-day choice situations.

    Output:
        X: (len(day_indices) * n_cells, P)
        day_local_index_for_c: (C,)
    """

    n_cells = cell_features.shape[0]
    rows = []
    day_local_index_for_c = []

    for local_d, global_d in enumerate(day_indices):
        day_block = np.repeat(day_features[global_d][None, :], n_cells, axis=0)
        X_d = np.concatenate([cell_features, day_block], axis=1)
        rows.append(X_d)
        day_local_index_for_c.append(np.full(n_cells, local_d, dtype=np.int64))

    X = np.vstack(rows).astype(np.float64)
    day_local_index_for_c = np.concatenate(day_local_index_for_c)

    return X, day_local_index_for_c


def _build_Z_and_J_for_days(
    station_features: np.ndarray,
    station_raw: Dict[str, np.ndarray],
    day_features: np.ndarray,
    day_indices: np.ndarray,
    choice_station_idx: np.ndarray,
    choice_tt_min: np.ndarray,
) -> Tuple[np.ndarray, np.ndarray, List[str]]:
    """
    Builds Z and J.

    Z[c, k, :] contains alternative-specific capture features.
    J[c, k] maps to the local station-day label index.
    """

    n_cells, K = choice_station_idx.shape
    n_stations = station_features.shape[0]
    n_days_split = len(day_indices)
    C = n_cells * n_days_split

    # Travel-time scaling based on all top-K TT values.
    tt_z_all = _standardize(choice_tt_min.ravel()).reshape(n_cells, K)

    # Extract useful station raw features for interactions.
    is_ultrafast = station_raw["is_ultrafast"].astype(float)
    is_fast = station_raw["is_fast"].astype(float)

    # We use only selected station features, not all station features.
    # This keeps the synthetic model easy to debug.
    station_name_to_col = {
        "log_stalls_z": 0,
        "log_power_max_z": 1,
        "log_power_sum_z": 2,
        "is_fast": 6,
        "is_ultrafast": 7,
        "price_ref_regular_z": 8,
        "price_ref_adhoc_z": 9,
        "adhoc_premium_z": 10,
        "operator_1": 11,
        "operator_2": 12,
        "operator_3": 13,
    }

    capture_names = [
        "tt_z",
        "log_stalls_z",
        "log_power_max_z",
        "log_power_sum_z",
        "is_fast",
        "is_ultrafast",
        "price_ref_regular_z",
        "price_ref_adhoc_z",
        "adhoc_premium_z",
        "operator_1",
        "operator_2",
        "operator_3",
        "tt_x_is_ultrafast",
        "tt_x_price_regular",
        "weekend_x_is_ultrafast",
    ]

    R = len(capture_names)
    Z = np.zeros((C, K, R), dtype=np.float64)
    J = np.zeros((C, K), dtype=np.int64)

    c_offset = 0
    for local_d, global_d in enumerate(day_indices):
        weekend = day_features[global_d, 2]

        for i in range(n_cells):
            c = c_offset + i
            alts = choice_station_idx[i]
            tt_z = tt_z_all[i]

            J[c, :] = local_d * n_stations + alts

            # Base alternative-specific features.
            Z[c, :, 0] = tt_z
            Z[c, :, 1] = station_features[alts, station_name_to_col["log_stalls_z"]]
            Z[c, :, 2] = station_features[alts, station_name_to_col["log_power_max_z"]]
            Z[c, :, 3] = station_features[alts, station_name_to_col["log_power_sum_z"]]
            Z[c, :, 4] = station_features[alts, station_name_to_col["is_fast"]]
            Z[c, :, 5] = station_features[alts, station_name_to_col["is_ultrafast"]]
            Z[c, :, 6] = station_features[alts, station_name_to_col["price_ref_regular_z"]]
            Z[c, :, 7] = station_features[alts, station_name_to_col["price_ref_adhoc_z"]]
            Z[c, :, 8] = station_features[alts, station_name_to_col["adhoc_premium_z"]]
            Z[c, :, 9] = station_features[alts, station_name_to_col["operator_1"]]
            Z[c, :, 10] = station_features[alts, station_name_to_col["operator_2"]]
            Z[c, :, 11] = station_features[alts, station_name_to_col["operator_3"]]

            # Interaction features.
            Z[c, :, 12] = tt_z * is_ultrafast[alts]
            Z[c, :, 13] = tt_z * station_features[alts, station_name_to_col["price_ref_regular_z"]]
            Z[c, :, 14] = weekend * is_ultrafast[alts]

        c_offset += n_cells

    return Z, J, capture_names


def _true_demand_function(X: np.ndarray) -> np.ndarray:
    """
    Synthetic ground-truth latent demand function.

    X columns:
      0 log_pop_z
      1 log_road_total_z
      2 log_road_primary_z
      3 log_road_residential_z
      4 log_poi_food_z
      5 log_poi_retail_z
      6 log_poi_fuel_z
      7 log_poi_parking_z
      8 dow_sin
      9 dow_cos
      10 weekend
      11 tourism_z
      12 holiday
    """

    log_pop = X[:, 0]
    road_total = X[:, 1]
    road_primary = X[:, 2]
    poi_food = X[:, 4]
    poi_retail = X[:, 5]
    poi_fuel = X[:, 6]
    weekend = X[:, 10]
    tourism = X[:, 11]
    holiday = X[:, 12]

    # Nonlinear but learnable by GBDT.
    score = (
        2.0
        + 0.75 * log_pop
        + 0.35 * road_total
        + 0.30 * road_primary
        + 0.22 * poi_food
        + 0.20 * poi_retail
        + 0.28 * poi_fuel
        + 0.25 * weekend
        + 0.22 * tourism
        + 0.35 * holiday
        + 0.18 * np.maximum(log_pop, 0.0) * np.maximum(poi_retail, 0.0)
    )

    D = np.exp(score)

    # Scale to convenient kWh-like magnitudes.
    D = D / np.mean(D) * 25.0

    return D.astype(np.float64)


def _initial_demand_prior(X: np.ndarray, y_station_day: np.ndarray, n_cells: int, n_days_split: int) -> np.ndarray:
    """
    Initial D0 for EM.

    Uses a simple population/road/POI prior and rescales it so that each day total
    matches the observed total station kWh for that split.
    """

    log_pop = X[:, 0]
    road_total = X[:, 1]
    poi_fuel = X[:, 6]
    weekend = X[:, 10]

    score = 0.7 * log_pop + 0.25 * road_total + 0.15 * poi_fuel + 0.1 * weekend
    base = np.exp(score)
    base = np.clip(base, 1e-8, None)

    D0 = np.zeros_like(base, dtype=np.float64)

    for local_d in range(n_days_split):
        start = local_d * n_cells
        end = (local_d + 1) * n_cells

        total_y_day = y_station_day[local_d].sum()
        weights = base[start:end]
        D0[start:end] = total_y_day * weights / np.clip(weights.sum(), 1e-8, None)

    return D0


def generate_synthetic_data(
    n_cells: int = 300,
    n_stations: int = 40,
    n_days: int = 70,
    n_val_days: int = 14,
    K: int = 12,
    seed: int = 42,
    noise: str = "gamma",
) -> DataBundle:
    """
    Generates a synthetic dataset from the same model family used by training.

    The labels are learnable because they are produced by:
      latent demand from X
      softmax capture from Z
      aggregation into station-day labels
    """

    if n_val_days >= n_days:
        raise ValueError("n_val_days must be smaller than n_days.")
    if K > n_stations:
        raise ValueError("K cannot be larger than n_stations.")

    rng = np.random.default_rng(seed)

    cell_features, cell_names, cell_raw = _make_cell_static_features(rng, n_cells)
    station_features, station_names, station_raw = _make_station_static_features(rng, n_stations)
    day_features, day_names = _make_day_features(n_days)
    choice_station_idx, choice_tt_min = _make_choice_sets(rng, n_cells, n_stations, K)

    train_days = np.arange(0, n_days - n_val_days)
    val_days = np.arange(n_days - n_val_days, n_days)

    # Build full data first so labels are generated consistently.
    all_days = np.arange(n_days)
    X_all, _ = _build_X_for_days(cell_features, day_features, all_days)
    Z_all, J_all, capture_names = _build_Z_and_J_for_days(
        station_features=station_features,
        station_raw=station_raw,
        day_features=day_features,
        day_indices=all_days,
        choice_station_idx=choice_station_idx,
        choice_tt_min=choice_tt_min,
    )

    # True beta used to generate capture probabilities.
    true_beta = np.array(
        [
            -1.35,  # tt_z
            0.35,   # log_stalls_z
            0.42,   # log_power_max_z
            0.25,   # log_power_sum_z
            0.25,   # is_fast
            0.70,   # is_ultrafast
            -0.45,  # price_ref_regular_z
            -0.15,  # price_ref_adhoc_z
            -0.08,  # adhoc_premium_z
            0.15,   # operator_1
            -0.10,  # operator_2
            0.25,   # operator_3
            0.55,   # tt_x_is_ultrafast: ultrafast weakens the TT penalty
            -0.25,  # tt_x_price_regular
            0.25,   # weekend_x_is_ultrafast
        ],
        dtype=np.float64,
    )

    if len(true_beta) != Z_all.shape[2]:
        raise RuntimeError("true_beta length does not match capture feature dimension.")

    D_true_all = _true_demand_function(X_all)
    U_all = np.einsum("ckr,r->ck", Z_all, true_beta)
    P_true_all = _softmax(U_all, axis=1)

    y_all_clean_flat = _aggregate_station_flows(
        D_true_all,
        P_true_all,
        J_all,
        n_station_days=n_days * n_stations,
    )
    y_all_clean = y_all_clean_flat.reshape(n_days, n_stations)

    if noise == "gamma":
        # Gamma noise with mean equal to clean label.
        # Higher shape means less noise.
        shape = 30.0
        scale = np.clip(y_all_clean, 1e-8, None) / shape
        y_all = rng.gamma(shape=shape, scale=scale)
    elif noise == "normal":
        y_all = y_all_clean + rng.normal(0.0, 0.08 * np.maximum(y_all_clean, 1.0))
        y_all = np.clip(y_all, 0.0, None)
    elif noise == "none":
        y_all = y_all_clean.copy()
    else:
        raise ValueError(f"Unknown noise type: {noise}")

    # Build train split.
    X_train, _ = _build_X_for_days(cell_features, day_features, train_days)
    Z_train, J_train, _ = _build_Z_and_J_for_days(
        station_features=station_features,
        station_raw=station_raw,
        day_features=day_features,
        day_indices=train_days,
        choice_station_idx=choice_station_idx,
        choice_tt_min=choice_tt_min,
    )
    y_train_2d = y_all[train_days]
    y_train = y_train_2d.reshape(-1)

    # Build val split.
    X_val, _ = _build_X_for_days(cell_features, day_features, val_days)
    Z_val, J_val, _ = _build_Z_and_J_for_days(
        station_features=station_features,
        station_raw=station_raw,
        day_features=day_features,
        day_indices=val_days,
        choice_station_idx=choice_station_idx,
        choice_tt_min=choice_tt_min,
    )
    y_val_2d = y_all[val_days]
    y_val = y_val_2d.reshape(-1)

    D0_train = _initial_demand_prior(
        X_train,
        y_train_2d,
        n_cells=n_cells,
        n_days_split=len(train_days),
    )
    D0_val = _initial_demand_prior(
        X_val,
        y_val_2d,
        n_cells=n_cells,
        n_days_split=len(val_days),
    )

    # Initial beta: mostly zero, but TT starts negative to break symmetry.
    beta_init = np.zeros_like(true_beta)
    beta_init[0] = -0.30

    demand_names = cell_names + day_names

    return DataBundle(
        X_train=X_train,
        Z_train=Z_train,
        J_train=J_train,
        y_train=y_train,
        D0_train=D0_train,
        X_val=X_val,
        Z_val=Z_val,
        J_val=J_val,
        y_val=y_val,
        D0_val=D0_val,
        n_cells=n_cells,
        n_stations=n_stations,
        n_train_days=len(train_days),
        n_val_days=len(val_days),
        n_station_days_train=len(train_days) * n_stations,
        n_station_days_val=len(val_days) * n_stations,
        K=K,
        demand_feature_names=demand_names,
        capture_feature_names=capture_names,
        true_beta=true_beta,
        beta_init=beta_init,
        y_train_clean=y_all_clean[train_days].reshape(-1),
        y_val_clean=y_all_clean[val_days].reshape(-1),
    )


def _write_parquet(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


def _read_parquet(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path)


def materialize_synthetic_dataset(
    output_dir: str | Path = "data/synthetic",
    overwrite: bool = False,
) -> None:
    """
    Generates synthetic data once, writes it to disk as realistic tables,
    and then future runs can load from disk.

    This simulates the future Supabase flow:
      DB tables -> pandas DataFrames -> model arrays.
    """

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    demand_path = output_dir / "demand_features.parquet"
    edges_path = output_dir / "capture_edges.parquet"
    labels_path = output_dir / "station_day_labels.parquet"
    metadata_path = output_dir / "metadata.json"

    if not overwrite and demand_path.exists() and edges_path.exists() and labels_path.exists() and metadata_path.exists():
        print(f"Synthetic dataset already exists at {output_dir}. Use overwrite=True to regenerate.")
        return

    data = generate_synthetic_data(
        n_cells=300,
        n_stations=40,
        n_days=70,
        n_val_days=14,
        K=12,
        seed=42,
        noise="gamma",
    )

    demand_rows = []
    label_rows = []
    edge_rows = []

    # ----------------------------
    # Demand feature table
    # ----------------------------
    def add_demand_rows(split: str, X: np.ndarray, D0: np.ndarray, n_days_split: int):
        rows = []
        for choice_id in range(X.shape[0]):
            local_day_idx = choice_id // data.n_cells
            cell_idx = choice_id % data.n_cells

            row = {
                "split": split,
                "choice_id": choice_id,
                "day_idx": local_day_idx,
                "cell_idx": cell_idx,
                "D0": float(D0[choice_id]),
            }

            for col_idx, name in enumerate(data.demand_feature_names):
                row[name] = float(X[choice_id, col_idx])

            rows.append(row)

        return rows

    demand_rows.extend(
        add_demand_rows(
            split="train",
            X=data.X_train,
            D0=data.D0_train,
            n_days_split=data.n_train_days,
        )
    )

    demand_rows.extend(
        add_demand_rows(
            split="val",
            X=data.X_val,
            D0=data.D0_val,
            n_days_split=data.n_val_days,
        )
    )

    demand_df = pd.DataFrame(demand_rows)

    # ----------------------------
    # Station-day label table
    # ----------------------------
    def add_label_rows(
        split: str,
        y: np.ndarray,
        y_clean: np.ndarray | None,
        n_days_split: int,
    ):
        rows = []
        for station_day_idx in range(y.shape[0]):
            local_day_idx = station_day_idx // data.n_stations
            station_idx = station_day_idx % data.n_stations

            row = {
                "split": split,
                "station_day_idx": station_day_idx,
                "day_idx": local_day_idx,
                "station_idx": station_idx,
                "y_kwh": float(y[station_day_idx]),
            }

            if y_clean is not None:
                row["y_kwh_clean"] = float(y_clean[station_day_idx])

            rows.append(row)

        return rows

    label_rows.extend(
        add_label_rows(
            split="train",
            y=data.y_train,
            y_clean=data.y_train_clean,
            n_days_split=data.n_train_days,
        )
    )

    label_rows.extend(
        add_label_rows(
            split="val",
            y=data.y_val,
            y_clean=data.y_val_clean,
            n_days_split=data.n_val_days,
        )
    )

    labels_df = pd.DataFrame(label_rows)

    # ----------------------------
    # Capture edge table
    # ----------------------------
    def add_edge_rows(split: str, Z: np.ndarray, J: np.ndarray):
        rows = []
        C, K, R = Z.shape

        for choice_id in range(C):
            local_day_idx = choice_id // data.n_cells
            cell_idx = choice_id % data.n_cells

            for alt_rank in range(K):
                station_day_idx = int(J[choice_id, alt_rank])
                station_idx = station_day_idx % data.n_stations

                row = {
                    "split": split,
                    "choice_id": choice_id,
                    "day_idx": local_day_idx,
                    "cell_idx": cell_idx,
                    "alt_rank": alt_rank,
                    "station_day_idx": station_day_idx,
                    "station_idx": station_idx,
                }

                for feat_idx, name in enumerate(data.capture_feature_names):
                    row[name] = float(Z[choice_id, alt_rank, feat_idx])

                rows.append(row)

        return rows

    edge_rows.extend(add_edge_rows("train", data.Z_train, data.J_train))
    edge_rows.extend(add_edge_rows("val", data.Z_val, data.J_val))

    edges_df = pd.DataFrame(edge_rows)

    # ----------------------------
    # Metadata
    # ----------------------------
    metadata = {
        "n_cells": data.n_cells,
        "n_stations": data.n_stations,
        "n_train_days": data.n_train_days,
        "n_val_days": data.n_val_days,
        "n_station_days_train": data.n_station_days_train,
        "n_station_days_val": data.n_station_days_val,
        "K": data.K,
        "demand_feature_names": data.demand_feature_names,
        "capture_feature_names": data.capture_feature_names,
        "true_beta": data.true_beta.tolist(),
        "beta_init": data.beta_init.tolist(),
    }

    _write_parquet(demand_df, demand_path)
    _write_parquet(edges_df, edges_path)
    _write_parquet(labels_df, labels_path)

    with open(metadata_path, "w") as f:
        json.dump(metadata, f, indent=2)

    print(f"Wrote synthetic dataset to {output_dir}")
    print(f"  {demand_path}")
    print(f"  {edges_path}")
    print(f"  {labels_path}")
    print(f"  {metadata_path}")


def _assemble_split_from_tables(
    split: Literal["train", "val"],
    demand_df: pd.DataFrame,
    edges_df: pd.DataFrame,
    labels_df: pd.DataFrame,
    metadata: dict,
):
    """
    Converts disk tables into model arrays for one split.
    """

    demand_feature_names = metadata["demand_feature_names"]
    capture_feature_names = metadata["capture_feature_names"]

    ddf = (
        demand_df[demand_df["split"] == split]
        .sort_values("choice_id")
        .reset_index(drop=True)
    )

    ldf = (
        labels_df[labels_df["split"] == split]
        .sort_values("station_day_idx")
        .reset_index(drop=True)
    )

    edf = (
        edges_df[edges_df["split"] == split]
        .sort_values(["choice_id", "alt_rank"])
        .reset_index(drop=True)
    )

    C = len(ddf)
    K = metadata["K"]
    R = len(capture_feature_names)

    expected_edge_rows = C * K
    if len(edf) != expected_edge_rows:
        raise ValueError(
            f"Expected {expected_edge_rows} edge rows for split={split}, got {len(edf)}."
        )

    X = ddf[demand_feature_names].to_numpy(dtype=np.float64)
    D0 = ddf["D0"].to_numpy(dtype=np.float64)

    Z = edf[capture_feature_names].to_numpy(dtype=np.float64).reshape(C, K, R)
    J = edf["station_day_idx"].to_numpy(dtype=np.int64).reshape(C, K)

    y = ldf["y_kwh"].to_numpy(dtype=np.float64)

    y_clean = None
    if "y_kwh_clean" in ldf.columns:
        y_clean = ldf["y_kwh_clean"].to_numpy(dtype=np.float64)

    return X, Z, J, y, D0, y_clean


def load_data_from_disk(
    data_dir: str | Path = "data/synthetic",
) -> DataBundle:
    """
    Loads materialized synthetic tables from disk and assembles DataBundle.
    """

    data_dir = Path(data_dir)

    demand_df = _read_parquet(data_dir / "demand_features.parquet")
    edges_df = _read_parquet(data_dir / "capture_edges.parquet")
    labels_df = _read_parquet(data_dir / "station_day_labels.parquet")

    with open(data_dir / "metadata.json", "r") as f:
        metadata = json.load(f)

    X_train, Z_train, J_train, y_train, D0_train, y_train_clean = _assemble_split_from_tables(
        split="train",
        demand_df=demand_df,
        edges_df=edges_df,
        labels_df=labels_df,
        metadata=metadata,
    )

    X_val, Z_val, J_val, y_val, D0_val, y_val_clean = _assemble_split_from_tables(
        split="val",
        demand_df=demand_df,
        edges_df=edges_df,
        labels_df=labels_df,
        metadata=metadata,
    )

    return DataBundle(
        X_train=X_train,
        Z_train=Z_train,
        J_train=J_train,
        y_train=y_train,
        D0_train=D0_train,
        X_val=X_val,
        Z_val=Z_val,
        J_val=J_val,
        y_val=y_val,
        D0_val=D0_val,
        n_cells=metadata["n_cells"],
        n_stations=metadata["n_stations"],
        n_train_days=metadata["n_train_days"],
        n_val_days=metadata["n_val_days"],
        n_station_days_train=metadata["n_station_days_train"],
        n_station_days_val=metadata["n_station_days_val"],
        K=metadata["K"],
        demand_feature_names=metadata["demand_feature_names"],
        capture_feature_names=metadata["capture_feature_names"],
        true_beta=np.array(metadata["true_beta"], dtype=np.float64),
        beta_init=np.array(metadata["beta_init"], dtype=np.float64),
        y_train_clean=y_train_clean,
        y_val_clean=y_val_clean,
    )


def load_data(
    data_dir: str | Path = "data/synthetic",
    regenerate: bool = False,
) -> DataBundle:
    """
    Public loader used by main.py.

    If the synthetic dataset does not exist yet, create it.
    """

    data_dir = Path(data_dir)

    required_files = [
        data_dir / "demand_features.parquet",
        data_dir / "capture_edges.parquet",
        data_dir / "station_day_labels.parquet",
        data_dir / "metadata.json",
    ]

    if regenerate or not all(path.exists() for path in required_files):
        materialize_synthetic_dataset(data_dir, overwrite=True)

    return load_data_from_disk(data_dir)