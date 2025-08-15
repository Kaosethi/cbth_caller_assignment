# config/callers_loader.py
from __future__ import annotations

from typing import Dict, List
import pandas as pd
from utils.gsheets import read_range


TRUTHY = {"TRUE", "1", "YES", "Y", "AVAILABLE"}


def _to_df(values) -> pd.DataFrame:
    """
    Convert raw Sheets values to a DataFrame with lowercase headers,
    padding ragged rows to header width.
    """
    if not values or len(values) < 1:
        return pd.DataFrame(columns=["caller_id"])

    headers = [str(h).strip().lower() for h in values[0]]
    rows = values[1:]
    width = len(headers)
    safe_rows = [(r + [""] * (width - len(r)))[:width] for r in rows]
    return pd.DataFrame(safe_rows, columns=headers)


def _is_truthy(x: object) -> bool:
    return str(x).strip().upper() in TRUTHY


def load_callers(gs_cfg: Dict) -> List[str]:
    """
    Supports either schema:
      - caller_id | status         (AVAILABLE marks active)
      - caller_id | available      (TRUE/YES/1 marks active)
    Returns list of active caller IDs (unique, order preserved).
    """
    vals = read_range(
        gs_cfg["service_account_file"],
        gs_cfg["config_sheet_id"],
        gs_cfg["ranges"]["callers"],
    )
    df = _to_df(vals)
    if df.empty:
        return []

    # Ensure caller_id column exists (fallback to first col if unnamed/misnamed)
    if "caller_id" not in df.columns:
        first_col = df.columns[0] if len(df.columns) else None
        if first_col:
            df = df.rename(columns={first_col: "caller_id"})
        else:
            return []

    # Normalize caller_id
    df["caller_id"] = df["caller_id"].astype(str).str.strip()
    df = df[df["caller_id"] != ""]
    if df.empty:
        return []

    # Build availability mask from either/both columns
    available_mask = pd.Series(False, index=df.index)
    if "status" in df.columns:
        available_mask = available_mask | df["status"].map(_is_truthy)
    if "available" in df.columns:
        available_mask = available_mask | df["available"].map(_is_truthy)

    # If neither column exists, assume none available
    if "status" not in df.columns and "available" not in df.columns:
        return []

    active = df.loc[available_mask, "caller_id"].tolist()

    # De-duplicate while preserving order
    seen = set()
    unique_active = []
    for c in active:
        if c not in seen:
            seen.add(c)
            unique_active.append(c)

    return unique_active
