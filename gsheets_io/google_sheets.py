# gsheets_io/google_sheets.py
from __future__ import annotations

from pathlib import Path
from typing import List, Union
import time
import logging

import gspread
from google.oauth2.service_account import Credentials
import pandas as pd

log = logging.getLogger(__name__)

# Read-only scope is enough for our loaders
SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

# simple in-process client cache
_CLIENTS: dict[str, gspread.Client] = {}


def get_client(sa_json_path: Union[str, Path]) -> gspread.Client:
    """Authorize and return a cached gspread client for the given SA JSON."""
    key = str(sa_json_path)
    if key in _CLIENTS:
        return _CLIENTS[key]
    creds = Credentials.from_service_account_file(key, scopes=SCOPES)
    client = gspread.authorize(creds)
    _CLIENTS[key] = client
    return client


def _with_retries(fn, retries: int = 3, delay: float = 0.6):
    last = None
    for i in range(retries):
        try:
            return fn()
        except Exception as e:
            last = e
            if i < retries - 1:
                time.sleep(delay)
            else:
                raise
    raise last  # for type checkers


def read_range(client: gspread.Client, spreadsheet_id: str, a1_range: str) -> List[List[str]]:
    """
    Low-level fetch that returns a list-of-lists (rows). First row is typically headers.
    Uses UNFORMATTED_VALUE so numbers/dates come back as raw text.
    """
    def _do():
        sh = client.open_by_key(spreadsheet_id)
        if "!" in a1_range:
            tab, rng = a1_range.split("!", 1)
            ws = sh.worksheet(tab)
            return ws.get(rng, value_render_option="UNFORMATTED_VALUE")
        else:
            ws = sh.sheet1
            return ws.get_all_values()

    values = _with_retries(_do)
    if not values:
        return []
    # normalize to list[str]
    out: List[List[str]] = []
    for row in values:
        if isinstance(row, list):
            out.append([("" if v is None else str(v)) for v in row])
        else:
            out.append([("" if row is None else str(row))])
    return out


def read_range_as_df(
    client: gspread.Client,
    spreadsheet_id: str,
    a1_range: str,
    header: bool = True,
) -> pd.DataFrame:
    """
    Convenience wrapper that returns a pandas DataFrame.
    Pads ragged rows to the header width (if header=True) or to the longest row otherwise.
    """
    values = read_range(client, spreadsheet_id, a1_range)
    if not values:
        return pd.DataFrame()

    if header:
        headers = [str(h).strip() for h in values[0]]
        rows = values[1:]
        width = len(headers)
        safe_rows = [(r + [""] * (width - len(r)))[:width] for r in rows]
        return pd.DataFrame(safe_rows, columns=headers)

    width = max((len(r) for r in values), default=0)
    padded = [(r + [""] * (width - len(r)))[:width] for r in values]
    return pd.DataFrame(padded)
