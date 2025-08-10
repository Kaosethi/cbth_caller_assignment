"""
Configuration settings for CBTH Caller Assignment System
"""
import os
from datetime import datetime

# === BUSINESS RULES ===
TARGET_PER_CALLER = 80  # Daily call target per caller
MIN_REPEAT_COUNT = 2    # Minimum repeat count in current month

# No-login windows (days) - progressively expand to fill capacity
NO_LOGIN_WINDOWS = [
    (3, 7),    # Initial window: 3-7 days no login
    (8, 10),   # Expansion 1: 8-10 days
    (11, 14),  # Expansion 2: 11-14 days  
    (15, 21),  # Expansion 3: 15-21 days
    (22, 30),  # Final expansion: 22-30 days
]

# === GOOGLE SHEETS CONFIGURATION ===
GOOGLE_SHEETS_CONFIG = {
    'holiday_sheet_id': '1fKLpkGZ6UiWfTeEV9iH7J3AFOq3cfU5gCWPdm2fpa8I',
    'holiday_range': 'Holidays!A:A',                 # column A with header 'date'
    'service_account_file': 'secrets/service_account.json',

    # (placeholders for later features; OK to leave as-is for now)
    'caller_availability_sheet_id': 'YOUR_CALLER_AVAILABILITY_SHEET_ID',
    'tier_a_template': 'CBTH-Tier A - {month_year}',
    'non_tier_a_template': 'CBTH-Non A - {month_year}',
}

# === DATABASE CONFIGURATION ===
# TODO: Replace with actual DB connection details
DATABASE_CONFIG = {
    'pc_db': {
        'host': 'YOUR_PC_DB_HOST',
        'database': 'YOUR_PC_DB_NAME',
        'user': 'YOUR_DB_USER',
        'password': 'YOUR_DB_PASSWORD',
        'port': 3306
    },
    'mobile_db': {
        'host': 'YOUR_MOBILE_DB_HOST', 
        'database': 'YOUR_MOBILE_DB_NAME',
        'user': 'YOUR_DB_USER',
        'password': 'YOUR_DB_PASSWORD',
        'port': 3306
    }
}

# === DISCORD CONFIGURATION ===
DISCORD_CONFIG = {
    'tier_a_webhook_url': 'YOUR_TIER_A_DISCORD_WEBHOOK_URL',
    'non_tier_a_webhook_url': 'YOUR_NON_TIER_A_DISCORD_WEBHOOK_URL',
    'tier_a_mention': '<@P_AIMMIE_USER_ID>',  # Direct mention for P'Aimmie
    'non_tier_a_mention': '<@P_PUINOON_USER_ID>',  # Direct mention for P'Puinoon
}

# === APPSHEET CONFIGURATION ===
APPSHEET_CONFIG = {
    'app_id': 'YOUR_APPSHEET_APP_ID',
    'api_key': 'YOUR_APPSHEET_API_KEY',
    'table_name': 'NonTierA_Queue'
}

# === MOCK DATA CONFIGURATION (for development) ===
MOCK_DATA_CONFIG = {
    'use_mock_data': True,  # Set to False when DB credentials are available
    'pc_data_file': 'data/mock/pc_data.csv',
    'mobile_data_file': 'data/mock/mobile_data.csv',
    'holiday_data_file': 'data/mock/holiday_data.csv',
    'caller_availability_file': 'data/mock/caller_availability.csv'
}

# === LOGGING CONFIGURATION ===
LOGGING_CONFIG = {
    'level': 'INFO',
    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    'file': 'logs/cbth_automation.log',
    'max_bytes': 10 * 1024 * 1024,  # 10MB
    'backup_count': 5
}

# === TIMEZONE CONFIGURATION ===
TIMEZONE = 'Asia/Bangkok'

# === AUDIT CONFIGURATION ===
AUDIT_CONFIG = {
    'exclusions_file': 'audit/exclusions_{date}.csv',
    'unassigned_file': 'audit/unassigned_{date}.csv',
    'assignments_file': 'audit/assignments_{date}.csv'
}

# === HELPER FUNCTIONS ===
def get_current_month_year():
    """Get current month/year in MM/YYYY format"""
    return datetime.now().strftime('%m/%Y')

def get_current_date():
    """Get current date in dd/mm/yyyy format"""
    return datetime.now().strftime('%d/%m/%Y')

def get_audit_date():
    """Get current date in ddmmyy format for audit files"""
    return datetime.now().strftime('%d%m%y')

# === ENVIRONMENT OVERRIDES ===
# Allow environment variables to override config values
if os.getenv('TARGET_PER_CALLER'):
    TARGET_PER_CALLER = int(os.getenv('TARGET_PER_CALLER'))

if os.getenv('USE_MOCK_DATA'):
    MOCK_DATA_CONFIG['use_mock_data'] = os.getenv('USE_MOCK_DATA').lower() == 'true'
