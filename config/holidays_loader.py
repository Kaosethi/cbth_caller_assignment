# config/holidays_loader.py
from __future__ import annotations

from datetime import date as Date
from typing import Dict, Set, List

import pandas as pd
from utils.gsheets import read_range


def _as_dates(values: List[List[str]]) -> Set[Date]:
    """
    Accepts raw values from Sheets (first row may be header).
    Parses robustly:
      - If first row looks like a header (non-date), skip it.
      - Accepts YYYY-MM-DD explicitly; otherwise tries generic parse with dayfirst=False.
      - Ignores blanks / unparseable rows.
    """
    if not values:
        return set()

    rows = values
    # Detect & drop header if the first cell isn't parseable as a date
    first = (values[0][0] if values[0] else "").strip()
    try_first = pd.to_datetime(first, format="%Y-%m-%d", errors="coerce")
    if pd.isna(try_first):
        # maybe generic parse (still header-like?), try without strict format
        try_first2 = pd.to_datetime(first, dayfirst=False, errors="coerce")
        if pd.isna(try_first2):
            rows = values[1:]  # treat first row as header

    out: Set[Date] = set()
    for r in rows:
        cell = (r[0] if r else "")
        raw = str(cell).strip()
        if not raw:
            continue

        dt = None
        # Fast path for canonical format
        if len(raw) == 10 and raw[4] == "-" and raw[7] == "-":
            dt = pd.to_datetime(raw, format="%Y-%m-%d", errors="coerce")
        if dt is None or pd.isna(dt):
            dt = pd.to_datetime(raw, dayfirst=False, errors="coerce")

        if pd.isna(dt):
            continue

        out.add(dt.date())

    return out


def load_holidays(gs_cfg: Dict) -> Set[Date]:
    """
    Read holiday dates from the configured sheet/range and return a set of datetime.date.
    """
    vals = read_range(
        gs_cfg["service_account_file"],
        gs_cfg["config_sheet_id"],
        gs_cfg["holiday_range"],
    )
    return _as_dates(vals)
