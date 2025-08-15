# allocator/pool.py
from __future__ import annotations

from datetime import date as Date
from typing import List

import pandas as pd

from config.settings import WindowRule


def _require_columns(df: pd.DataFrame, cols: List[str]) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise KeyError(f"Missing required columns: {missing}. "
                       f"Got columns: {list(df.columns)}")


def _norm_phone(s: object) -> str:
    """
    Normalize phone:
      - keep digits only
      - if 9 digits, left-pad with '0' (common Thai formatting quirk)
    """
    digits = "".join(ch for ch in str(s) if ch.isdigit())
    if len(digits) == 9:
        digits = "0" + digits
    return digits


def _days_since_last_login(df: pd.DataFrame, asof: Date) -> pd.Series:
    """
    Compute days since last login.
    Expects 'last_login_date' to be parseable by pandas (e.g., 'YYYY-MM-DD').
    Unparseable rows become NaT -> days = NaN -> will be excluded by window masks.
    """
    # Convert asof (date/datetime) to pandas Timestamp
    asof_ts = pd.Timestamp(asof)

    # Parse last_login_date; coerce errors to NaT
    last_login_ts = pd.to_datetime(df["last_login_date"], errors="coerce")

    # Subtract to get timedeltas, then days
    return (asof_ts - last_login_ts).dt.days


def filter_by_window_rule(df: pd.DataFrame, asof: Date, rule: WindowRule) -> pd.DataFrame:
    """
    Return rows within [day_min, day_max] (inclusive); if day_max is None, it's open-ended (e.g., 15+).
    Adds a 'window_label' column with rule.label.
    """
    _require_columns(df, ["last_login_date"])

    days = _days_since_last_login(df, asof)

    mask_min = days >= rule.day_min
    mask_max = (days <= rule.day_max) if rule.day_max is not None else pd.Series(True, index=df.index)

    out = df.loc[mask_min & mask_max].copy()
    out["window_label"] = rule.label
    return out


def build_windowed_pool(
    df_all: pd.DataFrame,
    asof: Date,
    windows: List[WindowRule],
    target_rows: int,
) -> pd.DataFrame:
    """
    Iterate windows in the given order (already priority-sorted: Hot -> Cold -> Hibernated),
    append results while deduping by normalized phone, stop at target_rows.

    Requirements:
      - df_all has columns: 'last_login_date', 'phone'
      - windows is a non-empty list of WindowRule
      - target_rows >= 1
    """
    if target_rows <= 0:
        return pd.DataFrame(columns=list(df_all.columns) + ["window_label"])

    _require_columns(df_all, ["last_login_date", "phone"])

    pool = pd.DataFrame(columns=list(df_all.columns) + ["window_label"])
    seen_phones: set[str] = set()

    for rule in windows:
        dfw = filter_by_window_rule(df_all, asof, rule)

        # Deduplicate by normalized phone, preferring earlier windows (already in pool)
        phones_norm = dfw["phone"].map(_norm_phone)
        keep_mask = ~phones_norm.isin(seen_phones)
        dfw = dfw.loc[keep_mask].copy()

        # Append and update seen set
        pool = pd.concat([pool, dfw], ignore_index=True)
        seen_phones.update(phones_norm[keep_mask].tolist())

        if len(pool) >= target_rows:
            break

    # Trim deterministically in case we exceeded target on the last batch
    if len(pool) > target_rows:
        pool = pool.head(target_rows)

    return pool


__all__ = [
    "filter_by_window_rule",
    "build_windowed_pool",
]
