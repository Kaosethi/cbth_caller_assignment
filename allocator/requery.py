# allocator/requery.py
from __future__ import annotations

from typing import Callable, List
import pandas as pd

from config.settings import WindowRule


def _norm_phone(s: object) -> str:
    d = "".join(ch for ch in str(s) if ch.isdigit())
    return ("0" + d) if len(d) == 9 else d


def _require_columns(df: pd.DataFrame, cols: List[str]) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise KeyError(f"Missing required columns: {missing}. "
                       f"Got columns: {list(df.columns)}")


def _days_since_last_login(df: pd.DataFrame, asof) -> pd.Series:
    asof_ts = pd.Timestamp(asof)
    last_login_ts = pd.to_datetime(df["last_login_date"], errors="coerce")
    return (asof_ts - last_login_ts).dt.days


def _filter_by_window_rule(df: pd.DataFrame, asof, rule: WindowRule) -> pd.DataFrame:
    _require_columns(df, ["last_login_date"])
    days = _days_since_last_login(df, asof)
    mask_min = days >= rule.day_min
    mask_max = (days <= rule.day_max) if rule.day_max is not None else pd.Series(True, index=df.index)
    out = df.loc[mask_min & mask_max].copy()
    out["window_label"] = rule.label
    return out


def build_candidate_pool(
    df_all: pd.DataFrame,
    asof_date,
    windows: List[WindowRule],
    target_rows: int,
    apply_all_filters: Callable[[pd.DataFrame], pd.DataFrame],
    phone_col: str = "phone",
) -> pd.DataFrame:
    """
    Escalate across windows (already priority-sorted), applying `apply_all_filters`
    INSIDE EACH WINDOW, and merge while deduping by phone so earlier windows win.

    Args:
      df_all: full candidate DF (must include last_login_date and phone)
      asof_date: date used to compute window day ranges
      windows: list[WindowRule] sorted by priority
      target_rows: stop once pool reaches this size
      apply_all_filters: function that applies your gates (repeat, cooldown, outcomes, etc.)
      phone_col: column to dedupe by (default 'phone')

    Returns:
      Pooled DataFrame up to target_rows with a 'window_label' column.
    """
    if df_all is None or df_all.empty or not windows or target_rows <= 0:
        return pd.DataFrame(columns=(list(df_all.columns) if df_all is not None else []) + ["window_label"])

    _require_columns(df_all, ["last_login_date", phone_col])

    pool = pd.DataFrame(columns=list(df_all.columns) + ["window_label"])
    seen_phones: set[str] = set()

    for rule in windows:
        # 1) base window slice
        df_win = _filter_by_window_rule(df_all, asof_date, rule)
        if df_win.empty:
            continue

        # 2) per-window filter stack (same logic for every window)
        df_win = apply_all_filters(df_win)
        if df_win.empty:
            continue

        # 3) merge with dedupe by phone (earlier window wins)
        phones_norm = df_win[phone_col].map(_norm_phone)
        keep_mask = ~phones_norm.isin(seen_phones)
        df_win = df_win.loc[keep_mask].copy()

        pool = pd.concat([pool, df_win], ignore_index=True)
        seen_phones.update(phones_norm[keep_mask].tolist())

        if len(pool) >= target_rows:
            break

    if len(pool) > target_rows:
        pool = pool.head(target_rows)

    return pool
