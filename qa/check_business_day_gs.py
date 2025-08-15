# qa/check_business_day_gs.py
from __future__ import annotations

from datetime import date
from config.settings import GOOGLE_SHEETS_CONFIG
from config.holidays_loader import load_holidays
from utils.business_days import is_business_day, next_business_day


def main():
    holidays = load_holidays(GOOGLE_SHEETS_CONFIG)  # reads config_sheet_id + holiday_range
    today = date.today()
    ok = is_business_day(today, holidays)

    print(f"Today: {today} | Business day? {ok}")
    if not ok:
        print("Next business day:", next_business_day(today, holidays))
    print(f"Holidays loaded: {len(holidays)} date(s)")

if __name__ == "__main__":
    main()
