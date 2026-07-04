# src/targets.py

from __future__ import annotations

import numpy as np
import pandas as pd


def add_directional_targets(
    panel: pd.DataFrame,
    horizons: tuple[int, ...] = (1, 3, 6, 12, 24),
    theta: float = 0.01,
    market_col: str = "market_slug",
    time_col: str = "timestamp",
    price_col: str = "price",
) -> pd.DataFrame:
    """
    Add future price-change and direction-label targets.

    For each market m and time t:

        ret_fwd_hh = price_{t+h} - price_t

    Direction label:

        +1 if future return > theta
        -1 if future return < -theta
         0 if move is small/noisy

    The 0 class can be dropped before training.
    """
    out = panel.sort_values([market_col, time_col]).copy()

    # Ensure timestamps are actual datetimes
    out[time_col] = pd.to_datetime(out[time_col], utc=True)

    # Important: group by market so future prices do not cross markets
    g = out.groupby(market_col, group_keys=False)

    for h in horizons:
        future_price = g[price_col].shift(-h)
        ret = future_price - out[price_col]

        out[f"price_fwd_{h}h"] = future_price
        out[f"ret_fwd_{h}h"] = ret
        out[f"dir_fwd_{h}h"] = np.select(
            [ret > theta, ret < -theta],
            [1, -1],
            default=0,
        )
        returns = out.groupby(market_col)[price_col].diff()

        out[f"future_volatility_{h}h"] = returns.groupby(out[market_col]).transform(
            lambda s: s.shift(-h + 1).rolling(h).std()
        )

        # If future price is missing, target should also be missing
        out.loc[future_price.isna(), f"dir_fwd_{h}h"] = np.nan

    return out
