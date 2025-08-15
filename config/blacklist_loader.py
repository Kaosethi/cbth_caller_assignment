# config/blacklist_loader.py
from __future__ import annotations

from typing import Dict, List
import pandas as pd
from utils.gsheets import read_range


EXPECTED: List[str] = ["username", "phone", "source_key"]


def _to_df(values, expected=EXPECTED) -> pd.DataFrame:
    """
    Convert raw Sheets values to a DataFrame with normalized lowercase headers,
    padding ragged rows and ensuring all expected columns exist.
    """
    if not values or len(values) < 1:
        return pd.DataFrame(columns=expected)

    # Normalize headers to lowercase & strip
    headers_actual = [str(h).strip().lower() for h in values[0]]
    rows = values[1:]

    # Pad ragged rows to the header width
    width = len(headers_actual)
    safe_rows = [(r + [""] * (width - len(r)))[:width] for r in rows]

    df = pd.DataFrame(safe_rows, columns=headers_actual)

    # Ensure expected columns exist
    for col in expected:
        if col not in df.columns:
            df[col] = ""

    return df[expected].copy()


def _norm_user(s: object) -> str:
    return str(s).strip().lower()


def _norm_phone(s: object) -> str:
    digits = "".join(ch for ch in str(s) if ch.isdigit())
    if len(digits) == 9:  # TH local quirk: pad to 10
        digits = "0" + digits
    return digits


def _nonempty(x: object) -> bool:
    xs = str(x).strip()
    return bool(xs) and xs.lower() != "nan"


def load_blacklist(gs_cfg: Dict) -> Dict:
    """
    Reads Blacklist!A:C (username | phone | source_key)
    Returns:
      {"triples": set[(source_key, username_norm, phone_norm)]}
    Only rows with ALL THREE non-empty are included (strict triple).
    """
    vals = read_range(
        gs_cfg["service_account_file"],
        gs_cfg["config_sheet_id"],
        gs_cfg["ranges"]["blacklist"],
    )
    df = _to_df(vals)

    triples = set()
    for _, r in df.iterrows():
        src = str(r.get("source_key", "")).strip()
        usr = _norm_user(r.get("username", ""))
        phn = _norm_phone(r.get("phone", ""))

        if _nonempty(src) and _nonempty(usr) and _nonempty(phn):
            triples.add((src, usr, phn))

    return {"triples": triples}
