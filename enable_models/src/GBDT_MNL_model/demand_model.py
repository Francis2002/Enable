# demand_model.py

from lightgbm import LGBMRegressor as _LightGBMRegressor


class LGBMRegressor(_LightGBMRegressor):
    """
    Thin wrapper so the rest of the project can import:

        from .demand_model import LGBMRegressor

    This keeps the full LightGBM sklearn API intact.
    """

    pass