# utils/merge.py
from __future__ import annotations

from typing import Optional
import pandas as pd


def _norm_phone(x: object) -> str:
    """Keep digits only; if 9 digits, left-pad with '0' (Thai local quirk)."""
    d = "".join(ch for ch in str(x) if ch.isdigit())
    return ("0" + d) if len(d) == 9 else d


def prefer_earlier_window(
    existing: pd.DataFrame,
    incoming: pd.DataFrame,
    phone_col: str = "phone",
    normalize_phone: bool = True,
) -> pd.DataFrame:
    """
    Drop rows from `incoming` that collide (by phone) with `existing`,
    preferring the rows already in `existing` (earlier window wins).

    Returns a COPY of the filtered `incoming`.
    """
    if phone_col not in incoming.columns:
        raise KeyError(f"Missing required column {phone_col!r} in incoming DataFrame")

    if existing is None or existing.empty or phone_col not in existing.columns:
        return incoming.copy()

    if normalize_phone:
        seen = set(existing[phone_col].map(_norm_phone))
        keep_mask = ~incoming[phone_col].map(_norm_phone).isin(seen)
    else:
        seen = set(existing[phone_col].astype(str))
        keep_mask = ~incoming[phone_col].astype(str).isin(seen)

    return incoming.loc[keep_mask].copy()
