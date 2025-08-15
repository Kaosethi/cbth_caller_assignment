# source/filters.py
from __future__ import annotations

from typing import Optional
import pandas as pd


def filter_by_last_login_window(
    df: pd.DataFrame,
    asof_date,
    day_min: int,
    day_max: Optional[int],
) -> pd.DataFrame:
    """
    Keep rows whose last_login_date is within [day_min, day_max] days ago (inclusive).
    If day_max is None, treat as open-ended: [day_min, +inf).

    Expects a column 'last_login_date' parseable by pandas (YYYY-MM-DD recommended).
    Returns a copy with an added 'window_label' column (e.g., '3-7' or '15+').
    """
    if "last_login_date" not in df.columns:
        raise KeyError("Missing required column 'last_login_date'")

    asof_ts = pd.Timestamp(asof_date)
    last_login_ts = pd.to_datetime(df["last_login_date"], errors="coerce")

    # days since last login; invalid dates become NaT -> NaN days -> will be excluded by masks
    days = (asof_ts - last_login_ts).dt.days

    mask_min = days >= day_min
    mask_max = (days <= day_max) if day_max is not None else pd.Series(True, index=df.index)

    out = df.loc[mask_min & mask_max].copy()

    label = f"{day_min}-{day_max}" if day_max is not None else f"{day_min}+"
    out["window_label"] = label
    return out
