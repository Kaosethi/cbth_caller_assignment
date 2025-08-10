from __future__ import annotations
from pathlib import Path
import gspread
from google.oauth2.service_account import Credentials
import pandas as pd

# Read-only scope is enough for the smoke test
SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

def get_client(sa_json_path: str | Path) -> gspread.Client:
    creds = Credentials.from_service_account_file(str(sa_json_path), scopes=SCOPES)
    return gspread.authorize(creds)

def read_range_as_df(client: gspread.Client, spreadsheet_id: str, a1_range: str, header: bool = True) -> pd.DataFrame:
    sh = client.open_by_key(spreadsheet_id)
    if "!" in a1_range:
        tab, rng = a1_range.split("!", 1)
        ws = sh.worksheet(tab)
        values = ws.get(rng)
    else:
        ws = sh.sheet1
        values = ws.get_all_values()

    if not values:
        return pd.DataFrame()

    if header:
        cols = values[0]
        rows = values[1:]
        return pd.DataFrame(rows, columns=cols)
    return pd.DataFrame(values)
