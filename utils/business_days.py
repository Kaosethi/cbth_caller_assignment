# utils/business_days.py
from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from typing import Iterable, Set

import pandas as pd


# ---- Pure business-day logic (no external I/O) -----------------------------

def is_weekend(d: date) -> bool:
    return d.weekday() >= 5  # 5=Sat, 6=Sun


def is_holiday(d: date, holidays: Set[date]) -> bool:
    return d in holidays


def is_business_day(d: date, holidays: Set[date]) -> bool:
    return not is_weekend(d) and not is_holiday(d, holidays)


def next_business_day(start: date, holidays: Set[date], forward: bool = True) -> date:
    """
    Return the first business day on/after (forward=True) or on/before (forward=False) `start`.
    If `start` is already a business day, `start` is returned.
    """
    step = timedelta(days=1 if forward else -1)
    current = start
    while not is_business_day(current, holidays):
        current += step
    return current


# ---- Lightweight helpers for local CSV-based QA (optional) ----------------

def parse_holiday_strings(rows: Iterable[str]) -> Set[date]:
    """
    Robust date parsing for holiday strings:
      - Prefer explicit YYYY-MM-DD.
      - Fallback to generic parse with dayfirst=False.
      - Ignore blanks/unparseable rows.
    """
    out: Set[date] = set()
    for raw in rows:
        s = str(raw).strip()
        if not s:
            continue

        dt = None
        if len(s) == 10 and s[4] == "-" and s[7] == "-":  # fast path YYYY-MM-DD
            dt = pd.to_datetime(s, format="%Y-%m-%d", errors="coerce")
        if dt is None or pd.isna(dt):
            dt = pd.to_datetime(s, dayfirst=False, errors="coerce")

        if not pd.isna(dt):
            out.add(dt.date())
    return out


def load_holidays_csv(source: str | Path) -> Set[date]:
    """
    Convenience for QA scripts that keep holidays in a CSV with a 'date' column.
    This is NOT used by the main pipeline (which loads via config.holidays_loader).
    """
    try:
        df = pd.read_csv(source, dtype=str)
    except Exception:
        return set()
    if "date" not in df.columns:
        return set()
    return parse_holiday_strings(df["date"].dropna().astype(str))


# Note:
# - Google Sheets reading has been moved to config.holidays_loader.load_holidays.
# - Keep this module I/O-free (apart from optional CSV helper) so it’s easy to unit test.
