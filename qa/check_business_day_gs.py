from datetime import date
from config.settings import GOOGLE_SHEETS_CONFIG
from utils.business_days import is_business_day, next_business_day, load_holidays_from_gsheet

def main():
    holidays = load_holidays_from_gsheet(
        GOOGLE_SHEETS_CONFIG['service_account_file'],
        GOOGLE_SHEETS_CONFIG['holiday_sheet_id'],
        GOOGLE_SHEETS_CONFIG['holiday_range'],
    )
    today = date.today()
    ok = is_business_day(today, holidays)
    print(f"Today: {today} | Business day? {ok}")
    if not ok:
        print("Next business day:", next_business_day(today, holidays))

if __name__ == "__main__":
    main()
