# scheduler/runner.py
from datetime import date
from config.settings import GOOGLE_SHEETS_CONFIG
from utils.business_days import is_business_day, next_business_day, load_holidays_from_gsheet

def _load_holidays():
    return load_holidays_from_gsheet(
        GOOGLE_SHEETS_CONFIG['service_account_file'],
        GOOGLE_SHEETS_CONFIG['holiday_sheet_id'],
        GOOGLE_SHEETS_CONFIG['holiday_range'],
    )

def main():
    holidays = _load_holidays()
    today = date.today()

    if not is_business_day(today, holidays):
        print(f"Skip: {today} is not a business day. Next business day = {next_business_day(today, holidays)}")
        return

    # --- Placeholder for the pipeline ---
    print(f"Proceed: {today} is a business day. (Runner stub OK)")

if __name__ == "__main__":
    main()
