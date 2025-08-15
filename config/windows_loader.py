# config/windows_loader.py
from __future__ import annotations

from typing import List, Dict
import logging

import pandas as pd

from utils.gsheets import read_range
from .settings import WindowRule

log = logging.getLogger(__name__)

EXPECTED = ["label", "day_min", "day_max", "priority"]


def _to_df(values) -> pd.DataFrame:
    """
    Convert raw Sheets values to a DataFrame with lowercase headers,
    padding ragged rows and ensuring all expected columns exist.
    """
    if not values or len(values) < 1:
        return pd.DataFrame(columns=EXPECTED)

    headers = [str(h).strip().lower() for h in values[0]]
    rows = values[1:]
    width = len(headers)
    safe_rows = [(r + [""] * (width - len(r)))[:width] for r in rows]
    df = pd.DataFrame(safe_rows, columns=headers)

    for col in EXPECTED:
        if col not in df.columns:
            df[col] = ""

    return df[EXPECTED].copy()


def _parse_int(s, default=None) -> int | None:
    """Parse to non-negative int; return default if empty/NaN; raise on invalid negatives."""
    if s is None:
        return default
    txt = str(s).strip()
    if txt == "" or txt.lower() in {"none", "nan"}:
        return default
    v = pd.to_numeric(txt, errors="coerce")
    if pd.isna(v):
        raise ValueError(f"Invalid integer: {s!r}")
    iv = int(v)
    if iv < 0:
        raise ValueError(f"Negative not allowed: {iv}")
    return iv


def load_windows(gs_cfg: Dict) -> List[WindowRule]:
    """
    Read Windows!A:D with columns: label | day_min | day_max | priority
    Returns a priority-sorted list[WindowRule] (ascending: 1 = highest).
    """
    vals = read_range(
        gs_cfg["service_account_file"],
        gs_cfg["config_sheet_id"],
        gs_cfg["ranges"]["windows"],
    )
    df = _to_df(vals)
    if df.empty:
        raise RuntimeError("Windows tab is empty. Expect headers: label, day_min, day_max, priority")

    rules: List[WindowRule] = []
    errors: List[str] = []

    for i, r in df.iterrows():
        label = str(r["label"]).strip()
        try:
            dmin = _parse_int(r["day_min"], default=None)
            dmax = _parse_int(r["day_max"], default=None)
            prio = _parse_int(r["priority"], default=None)
        except ValueError as e:
            errors.append(f"row {i+2}: {e}")  # +2 for header + 1-based row
            continue

        # Required fields
        if not label:
            errors.append(f"row {i+2}: label is required")
            continue
        if dmin is None:
            errors.append(f"row {i+2}: day_min is required")
            continue
        if prio is None:
            errors.append(f"row {i+2}: priority is required")
            continue
        if dmax is not None and dmax < dmin:
            errors.append(f"row {i+2}: day_max ({dmax}) < day_min ({dmin})")
            continue

        rules.append(WindowRule(label=label, day_min=dmin, day_max=dmax, priority=prio))

    if errors:
        msg = "Invalid Windows rows:\n  - " + "\n  - ".join(errors)
        # log and still fail to avoid silent misconfiguration
        log.error(msg)
        raise ValueError(msg)

    rules.sort(key=lambda x: x.priority)
    return rules
