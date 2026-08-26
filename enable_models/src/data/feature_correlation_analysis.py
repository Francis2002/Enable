from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Literal

import numpy as np
import pandas as pd
from scipy import stats

try:
    from sklearn.feature_selection import mutual_info_regression, f_regression
    SKLEARN_AVAILABLE = True
except Exception:
    SKLEARN_AVAILABLE = False

try:
    # Works when placed inside src/ and run as: python -m src.feature_correlation_analysis
    from .data import load_data, generate_synthetic_data, _true_demand_function
except Exception:
    # Works when placed next to data.py and run as: python feature_correlation_analysis.py
    from data import load_data, generate_synthetic_data, _true_demand_function


EPS = 1e-12
COMPUTE_MI = False


@dataclass(frozen=True)
class SplitArrays:
    split: str
    X: np.ndarray
    Z: np.ndarray
    J: np.ndarray
    y: np.ndarray
    D0: np.ndarray
    y_clean: np.ndarray | None
    n_station_days: int
    n_days: int


def _safe_log1p(x: np.ndarray) -> np.ndarray:
    return np.log1p(np.clip(np.asarray(x, dtype=float), 0.0, None))


def _is_constant(x: np.ndarray) -> bool:
    x = np.asarray(x, dtype=float)
    x = x[np.isfinite(x)]
    return x.size < 2 or np.nanstd(x) < EPS


def _pearson(x: np.ndarray, y: np.ndarray) -> tuple[float, float]:
    mask = np.isfinite(x) & np.isfinite(y)
    x = x[mask]
    y = y[mask]
    if _is_constant(x) or _is_constant(y):
        return np.nan, np.nan
    r, p = stats.pearsonr(x, y)
    return float(r), float(p)


def _spearman(x: np.ndarray, y: np.ndarray) -> tuple[float, float]:
    mask = np.isfinite(x) & np.isfinite(y)
    x = x[mask]
    y = y[mask]
    if _is_constant(x) or _is_constant(y):
        return np.nan, np.nan
    r, p = stats.spearmanr(x, y)
    return float(r), float(p)


def _fdr_bh(p_values: Iterable[float]) -> np.ndarray:
    """Benjamini-Hochberg q-values for a vector of p-values."""
    p = np.asarray(list(p_values), dtype=float)
    q = np.full_like(p, np.nan, dtype=float)
    valid = np.isfinite(p)
    pv = p[valid]
    m = pv.size
    if m == 0:
        return q

    order = np.argsort(pv)
    ranked = pv[order]
    raw_q = ranked * m / np.arange(1, m + 1)
    monotone_q = np.minimum.accumulate(raw_q[::-1])[::-1]
    monotone_q = np.clip(monotone_q, 0.0, 1.0)

    out = np.empty_like(pv)
    out[order] = monotone_q
    q[valid] = out
    return q


def _mutual_information_scores(X: np.ndarray, y: np.ndarray, seed: int, max_samples: int = 20000) -> np.ndarray:
    if (not COMPUTE_MI) or (not SKLEARN_AVAILABLE):
        return np.full(X.shape[1], np.nan)

    mask = np.isfinite(y)
    X = np.asarray(X, dtype=float)
    for j in range(X.shape[1]):
        mask &= np.isfinite(X[:, j])

    if mask.sum() < 5 or _is_constant(y[mask]):
        return np.full(X.shape[1], np.nan)

    X_valid = X[mask]
    y_valid = y[mask]

    # MI can be expensive on edge-level rows. Subsampling keeps the diagnostic usable.
    if X_valid.shape[0] > max_samples:
        rng = np.random.default_rng(seed)
        idx = rng.choice(X_valid.shape[0], size=max_samples, replace=False)
        X_valid = X_valid[idx]
        y_valid = y_valid[idx]

    # mutual_info_regression is model-free but estimated with k-nearest-neighbor entropy estimators.
    return mutual_info_regression(
        X_valid,
        y_valid,
        random_state=seed,
        n_neighbors=5,
    )


def _f_regression_scores(X: np.ndarray, y: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    # Pearson p-values are already computed above with scipy.stats.pearsonr.
    # Keeping these columns as NaN avoids doing redundant work on large edge-level diagnostics.
    return np.full(X.shape[1], np.nan), np.full(X.shape[1], np.nan)


def score_feature_matrix(
    X: np.ndarray,
    y: np.ndarray,
    feature_names: list[str],
    *,
    split: str,
    feature_family: str,
    target_name: str,
    unit: str,
    aggregation: str,
    seed: int,
) -> pd.DataFrame:
    """Compute several univariate, model-free dependency scores feature-by-feature."""
    X = np.asarray(X, dtype=float)
    y = np.asarray(y, dtype=float)
    if X.ndim != 2:
        raise ValueError(f"X must be 2D, got shape {X.shape}")
    if y.ndim != 1:
        raise ValueError(f"y must be 1D, got shape {y.shape}")
    if X.shape[0] != y.shape[0]:
        raise ValueError(f"X and y row mismatch: {X.shape[0]} versus {y.shape[0]}")
    if X.shape[1] != len(feature_names):
        raise ValueError("Number of columns does not match feature_names.")

    mi = _mutual_information_scores(X, y, seed=seed)
    f_stat, f_p = _f_regression_scores(X, y)

    rows = []
    for j, name in enumerate(feature_names):
        pearson_r, pearson_p = _pearson(X[:, j], y)
        spearman_r, spearman_p = _spearman(X[:, j], y)
        rows.append(
            {
                "split": split,
                "feature_family": feature_family,
                "unit": unit,
                "aggregation": aggregation,
                "target": target_name,
                "feature": name,
                "pearson_r": pearson_r,
                "pearson_p": pearson_p,
                "spearman_r": spearman_r,
                "spearman_p": spearman_p,
                "mutual_info": float(mi[j]) if np.isfinite(mi[j]) else np.nan,
                "f_stat": float(f_stat[j]) if np.isfinite(f_stat[j]) else np.nan,
                "f_p": float(f_p[j]) if np.isfinite(f_p[j]) else np.nan,
                "n": int(np.isfinite(X[:, j]).sum()),
            }
        )

    out = pd.DataFrame(rows)
    out["pearson_q"] = _fdr_bh(out["pearson_p"])
    out["spearman_q"] = _fdr_bh(out["spearman_p"])
    out["f_q"] = _fdr_bh(out["f_p"])
    out["abs_pearson_r"] = out["pearson_r"].abs()
    out["abs_spearman_r"] = out["spearman_r"].abs()

    # A practical combined rank. It is not a statistical test; it is a dashboard sorting score.
    out["rank_abs_pearson"] = out["abs_pearson_r"].rank(ascending=False, method="min")
    out["rank_abs_spearman"] = out["abs_spearman_r"].rank(ascending=False, method="min")
    out["rank_mi"] = out["mutual_info"].rank(ascending=False, method="min")
    out["mean_rank"] = out[["rank_abs_pearson", "rank_abs_spearman", "rank_mi"]].mean(axis=1)
    out = out.sort_values(["mean_rank", "abs_spearman_r", "abs_pearson_r"], ascending=[True, False, False])
    return out


def _scatter_mean(values: np.ndarray, indices: np.ndarray, n_out: int) -> np.ndarray:
    """Mean aggregation of rows in values according to integer indices."""
    values = np.asarray(values, dtype=float)
    indices = np.asarray(indices, dtype=np.int64)
    out = np.zeros((n_out, values.shape[1]), dtype=float)
    counts = np.zeros(n_out, dtype=float)
    np.add.at(out, indices, values)
    np.add.at(counts, indices, 1.0)
    return out / np.clip(counts[:, None], 1.0, None)


def _scatter_weighted_mean(values: np.ndarray, indices: np.ndarray, weights: np.ndarray, n_out: int) -> np.ndarray:
    """Weighted mean aggregation of rows in values according to integer indices."""
    values = np.asarray(values, dtype=float)
    indices = np.asarray(indices, dtype=np.int64)
    weights = np.asarray(weights, dtype=float)
    out = np.zeros((n_out, values.shape[1]), dtype=float)
    denom = np.zeros(n_out, dtype=float)
    np.add.at(out, indices, values * weights[:, None])
    np.add.at(denom, indices, weights)
    return out / np.clip(denom[:, None], EPS, None)


def build_stationday_capture_exposures(split: SplitArrays, capture_feature_names: list[str]) -> dict[str, tuple[np.ndarray, list[str]]]:
    """
    Convert alternative-level Z[c,k,r] into station-day-level features.

    Each station-day label y[s] receives many incoming edges (c,k) where J[c,k] = s.
    This function summarizes those edges into one row per station-day.
    """
    Z_flat = split.Z.reshape(-1, split.Z.shape[2])
    J_flat = split.J.reshape(-1)

    # Lower tt_z means closer. exp(-tt_z) gives more weight to nearby cells.
    tt_z = Z_flat[:, 0]
    near_weights = np.exp(-np.clip(tt_z, -8.0, 8.0))

    mean = _scatter_mean(Z_flat, J_flat, split.n_station_days)
    near_mean = _scatter_weighted_mean(Z_flat, J_flat, near_weights, split.n_station_days)

    return {
        "edge_mean_by_station_day": (mean, [f"mean_{n}" for n in capture_feature_names]),
        "near_weighted_edge_mean_by_station_day": (near_mean, [f"near_wmean_{n}" for n in capture_feature_names]),
    }


def build_stationday_demand_exposures(split: SplitArrays, demand_feature_names: list[str]) -> dict[str, tuple[np.ndarray, list[str]]]:
    """
    Convert cell-day demand features X[c,p] into station-day-level exposure features.

    For each edge (c,k) pointing to station-day s=J[c,k], attach X[c]. Then summarize
    all cells that contain that station in their choice set. This asks: what kind of
    demand environment surrounds each station-day?
    """
    C, K = split.J.shape
    X_repeated = np.repeat(split.X, repeats=K, axis=0)
    J_flat = split.J.reshape(-1)

    tt_z_flat = split.Z[:, :, 0].reshape(-1)
    near_weights = np.exp(-np.clip(tt_z_flat, -8.0, 8.0))

    mean = _scatter_mean(X_repeated, J_flat, split.n_station_days)
    near_mean = _scatter_weighted_mean(X_repeated, J_flat, near_weights, split.n_station_days)

    return {
        "cell_mean_by_station_day": (mean, [f"mean_cell_{n}" for n in demand_feature_names]),
        "near_weighted_cell_mean_by_station_day": (near_mean, [f"near_wmean_cell_{n}" for n in demand_feature_names]),
    }


def edge_level_capture_matrix(split: SplitArrays) -> tuple[np.ndarray, np.ndarray]:
    """
    Alternative-level diagnostic: one row per (cell-day, candidate station) edge.

    The target for an edge is the observed station-day y[J[c,k]]. This is useful as a
    descriptive diagnostic, but it is not an independent-row dataset because the same
    station-day label is repeated many times.
    """
    Z_edge = split.Z.reshape(-1, split.Z.shape[2])
    y_edge = split.y[split.J.reshape(-1)]
    return Z_edge, y_edge


def make_split(data, split_name: Literal["train", "val"]) -> SplitArrays:
    if split_name == "train":
        return SplitArrays(
            split="train",
            X=data.X_train,
            Z=data.Z_train,
            J=data.J_train,
            y=data.y_train,
            D0=data.D0_train,
            y_clean=data.y_train_clean,
            n_station_days=data.n_station_days_train,
            n_days=data.n_train_days,
        )
    if split_name == "val":
        return SplitArrays(
            split="val",
            X=data.X_val,
            Z=data.Z_val,
            J=data.J_val,
            y=data.y_val,
            D0=data.D0_val,
            y_clean=data.y_val_clean,
            n_station_days=data.n_station_days_val,
            n_days=data.n_val_days,
        )
    raise ValueError(split_name)


def analyze_split(split: SplitArrays, data, *, seed: int, include_edge_level: bool) -> pd.DataFrame:
    all_tables: list[pd.DataFrame] = []

    station_targets: dict[str, np.ndarray] = {
        "y_kwh": split.y,
        "log1p_y_kwh": _safe_log1p(split.y),
    }
    if split.y_clean is not None:
        station_targets["y_kwh_clean"] = split.y_clean
        station_targets["log1p_y_kwh_clean"] = _safe_log1p(split.y_clean)

    # 1) Station-day capture exposure correlations.
    for aggregation, (X_station, names) in build_stationday_capture_exposures(split, data.capture_feature_names).items():
        for target_name, target in station_targets.items():
            all_tables.append(
                score_feature_matrix(
                    X_station,
                    target,
                    names,
                    split=split.split,
                    feature_family="capture_features_Z",
                    target_name=target_name,
                    unit="station_day",
                    aggregation=aggregation,
                    seed=seed,
                )
            )

    # 2) Station-day demand exposure correlations.
    for aggregation, (X_station, names) in build_stationday_demand_exposures(split, data.demand_feature_names).items():
        for target_name, target in station_targets.items():
            all_tables.append(
                score_feature_matrix(
                    X_station,
                    target,
                    names,
                    split=split.split,
                    feature_family="demand_features_X_as_station_environment",
                    target_name=target_name,
                    unit="station_day",
                    aggregation=aggregation,
                    seed=seed,
                )
            )

    if include_edge_level:
        # 3) Edge-level capture diagnostic.
        Z_edge, y_edge = edge_level_capture_matrix(split)
        for target_name, target in {
            "edge_y_kwh_of_station_day_J": y_edge,
            "edge_log1p_y_kwh_of_station_day_J": _safe_log1p(y_edge),
        }.items():
            all_tables.append(
                score_feature_matrix(
                    Z_edge,
                    target,
                    data.capture_feature_names,
                    split=split.split,
                    feature_family="capture_features_Z",
                    target_name=target_name,
                    unit="edge_cell_day_station_alt",
                    aggregation="none_edge_level_repeated_labels",
                    seed=seed,
                )
            )

    # 4) Cell-day demand-side diagnostics.
    cell_targets = {
        "D0_initial_target_derived_prior": split.D0,
        "log1p_D0_initial_target_derived_prior": _safe_log1p(split.D0),
    }

    # This is only valid for synthetic data because _true_demand_function is the hidden generator.
    try:
        D_true = _true_demand_function(split.X)
        cell_targets["D_true_synthetic_only"] = D_true
        cell_targets["log1p_D_true_synthetic_only"] = _safe_log1p(D_true)
    except Exception:
        pass

    for target_name, target in cell_targets.items():
        all_tables.append(
            score_feature_matrix(
                split.X,
                target,
                data.demand_feature_names,
                split=split.split,
                feature_family="demand_features_X",
                target_name=target_name,
                unit="cell_day",
                aggregation="none_cell_day",
                seed=seed,
            )
        )

    return pd.concat(all_tables, ignore_index=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Model-free feature correlation/dependence report for the EV synthetic dataset.")
    parser.add_argument("--data-dir", default="data/synthetic", help="Folder containing synthetic parquet tables and metadata.")
    parser.add_argument("--output-dir", default="artifacts/feature_analysis", help="Where CSV outputs are written.")
    parser.add_argument("--regenerate", action="store_true", help="Regenerate synthetic data before loading.")
    parser.add_argument("--synthetic-direct", action="store_true", help="Bypass parquet files and generate the in-memory synthetic dataset directly.")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for mutual information estimator.")
    parser.add_argument("--compute-mi", action="store_true", help="Also compute mutual information. Slower, especially on edge-level diagnostics.")
    parser.add_argument("--top-k", type=int, default=30, help="Number of top rows printed per selected view.")
    parser.add_argument("--include-edge-level", action="store_true", help="Also compute the large repeated-label edge-level diagnostic.")
    args = parser.parse_args()

    global COMPUTE_MI
    COMPUTE_MI = args.compute_mi

    if args.synthetic_direct:
        data = generate_synthetic_data()
    else:
        try:
            data = load_data(data_dir=args.data_dir, regenerate=args.regenerate)
        except ImportError as e:
            # Some minimal environments do not have pyarrow/fastparquet installed.
            # For the synthetic experiment, we can still run the full analysis in memory.
            print(f"Could not read/write parquet via load_data: {e}")
            print("Falling back to generate_synthetic_data() in memory.")
            data = generate_synthetic_data()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    results = []
    for split_name in ["train", "val"]:
        split = make_split(data, split_name)
        results.append(analyze_split(split, data, seed=args.seed, include_edge_level=args.include_edge_level))

    results_df = pd.concat(results, ignore_index=True)
    results_df.to_csv(output_dir / "feature_dependency_scores.csv", index=False)

    # A compact default report focused on the most business-relevant unit: station-day + observed log kWh.
    stationday_logy = results_df[
        (results_df["unit"] == "station_day")
        & (results_df["target"] == "log1p_y_kwh")
    ].copy()
    stationday_logy = stationday_logy.sort_values(["split", "mean_rank", "abs_spearman_r"], ascending=[True, True, False])
    stationday_logy.to_csv(output_dir / "stationday_logkwh_top_features.csv", index=False)

    # Synthetic-only sanity report: does the method recover the hidden generator's demand-side features?
    synthetic_demand = results_df[
        (results_df["feature_family"] == "demand_features_X")
        & (results_df["target"] == "log1p_D_true_synthetic_only")
    ].copy()
    synthetic_demand = synthetic_demand.sort_values(["split", "mean_rank", "abs_spearman_r"], ascending=[True, True, False])
    synthetic_demand.to_csv(output_dir / "synthetic_true_demand_feature_scores.csv", index=False)

    metadata = {
        "n_cells": data.n_cells,
        "n_stations": data.n_stations,
        "n_train_days": data.n_train_days,
        "n_val_days": data.n_val_days,
        "K": data.K,
        "demand_feature_names": data.demand_feature_names,
        "capture_feature_names": data.capture_feature_names,
        "outputs": [
            "feature_dependency_scores.csv",
            "stationday_logkwh_top_features.csv",
            "synthetic_true_demand_feature_scores.csv",
        ],
        "notes": [
            "Pearson measures linear association.",
            "Spearman measures monotonic association by ranks.",
            "Mutual information is non-negative and captures general dependence but has no sign.",
            "Edge-level rows repeat station-day labels and should be treated as diagnostic only.",
            "D_true_synthetic_only is available only because the current dataset is synthetic; do not use it for real data.",
        ],
    }
    with open(output_dir / "feature_analysis_metadata.json", "w") as f:
        json.dump(metadata, f, indent=2)

    pd.set_option("display.width", 200)
    pd.set_option("display.max_columns", 30)

    print(f"\nWrote outputs to: {output_dir}\n")

    print("Top station-day observed log-kWh associations:")
    cols = [
        "split", "feature_family", "aggregation", "feature", "pearson_r", "spearman_r", "mutual_info", "mean_rank"
    ]
    print(stationday_logy[cols].head(args.top_k).to_string(index=False))

    if not synthetic_demand.empty:
        print("\nSynthetic sanity check: top demand features against hidden true demand:")
        print(synthetic_demand[cols].head(args.top_k).to_string(index=False))


if __name__ == "__main__":
    main()
