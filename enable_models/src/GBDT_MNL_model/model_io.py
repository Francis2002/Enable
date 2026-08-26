# src/model_io.py

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

import joblib
import numpy as np


def save_model_artifacts(
    demand_model,
    capture_model,
    output_dir: str | Path,
    metadata: Dict[str, Any],
) -> None:
    """
    Saves trained models and metadata.

    The LightGBM demand model is saved with joblib.
    The capture model is also saved with joblib because it only contains numpy arrays
    and scipy optimization metadata.
    """

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    joblib.dump(demand_model, output_dir / "demand_model.joblib")
    joblib.dump(capture_model, output_dir / "capture_model.joblib")

    metadata_to_save = dict(metadata)

    # Make JSON-safe.
    for key, value in list(metadata_to_save.items()):
        if isinstance(value, np.ndarray):
            metadata_to_save[key] = value.tolist()

    with open(output_dir / "training_metadata.json", "w") as f:
        json.dump(metadata_to_save, f, indent=2)


def load_model_artifacts(model_dir: str | Path):
    """
    Loads trained demand/capture models and metadata.
    """

    model_dir = Path(model_dir)

    demand_model = joblib.load(model_dir / "demand_model.joblib")
    capture_model = joblib.load(model_dir / "capture_model.joblib")

    with open(model_dir / "training_metadata.json", "r") as f:
        metadata = json.load(f)

    return demand_model, capture_model, metadata