from datetime import date, timedelta
from pathlib import Path
import pandas as pd

def load_holidays(source: str | Path) -> set[date]:
    df = pd.read_csv(source, dtype=str)
    if "date" not in df.columns:
        return set()
    holidays = set()
    for raw in df["date"].dropna().astype(str):
        try:
            holidays.add(pd.to_datetime(raw, dayfirst=True).date())
        except Exception:
            continue
    return holidays

def is_weekend(d: date) -> bool:
    return d.weekday() >= 5

def is_holiday(d: date, holidays: set[date]) -> bool:
    return d in holidays

def is_business_day(d: date, holidays: set[date]) -> bool:
    return not is_weekend(d) and not is_holiday(d, holidays)

def next_business_day(start: date, holidays: set[date], forward: bool = True) -> date:
    step = timedelta(days=1) if forward else timedelta(days=-1)
    current = start
    while not is_business_day(current, holidays):
        current += step
    return current

# ---- Google Sheets loader ----
from gsheets_io.google_sheets import get_client, read_range_as_df

def load_holidays_from_gsheet(sa_json_path: str | Path, spreadsheet_id: str, a1_range: str) -> set[date]:
    client = get_client(sa_json_path)
    df = read_range_as_df(client, spreadsheet_id, a1_range, header=True)
    if df.empty or "date" not in df.columns:
        return set()
    out = set()
    for raw in df["date"].dropna().astype(str):
        try:
            out.add(pd.to_datetime(raw, dayfirst=True).date())
        except Exception:
            continue
    return out
