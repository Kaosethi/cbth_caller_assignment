# config/settings.py
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional, Dict, Any

# === Business rules (constants) ===
PER_CALLER_TARGET: int = int(os.getenv("PER_CALLER_TARGET", "80"))
MIN_REPEAT_COUNT: int = 2
TIMEZONE: str = "Asia/Bangkok"

# === Runtime bag (filled at startup by runner) ===
CONFIG: Dict[str, Any] = {}

# === MOCK DATA CONFIGURATION (imported by adapters) ===
MOCK_DATA_CONFIG: Dict[str, Any] = {
    "use_mock_data": str(os.getenv("USE_MOCK_DATA", "true")).strip().lower() in ("1", "true", "yes", "y"),
    "pc_data_file": os.getenv("MOCK_PC_DATA_FILE", "data/mock/pc_data.csv"),
    "mobile_data_file": os.getenv("MOCK_MOBILE_DATA_FILE", "data/mock/mobile_data.csv"),
}

# === Google Sheets config (central) ===
GOOGLE_SHEETS_CONFIG: Dict[str, Any] = {
    "service_account_file": os.getenv("GOOGLE_SA_FILE", "secrets/service_account.json"),
    "config_sheet_id": os.getenv("CONFIG_SHEET_ID", "1fKLpkGZ6UiWfTeEV9iH7J3AFOq3cfU5gCWPdm2fpa8I"),
    "holiday_range": os.getenv("HOLIDAY_RANGE", "Holidays!A:A"),
    "ranges": {
        "sources": "Config!A:E",
        "callers": "Callers!A:B",
        "blacklist": "Blacklist!A:C",
        "cabal_tiers": "Cabal_Tiers!A:B",
        "windows": "Windows!A:D",
        # "caller_availability": "CallerAvailability!A:B",
    },
    "tier_a_template": "CBTH-Tier A - {month_year}",
    "non_tier_a_template": "CBTH-Non A - {month_year}",
}

# === Shared dataclasses ===
@dataclass(frozen=True)
class WindowRule:
    label: str
    day_min: int
    day_max: Optional[int]   # None = open-ended (e.g., 15+)
    priority: int            # 1 = highest priority

# === Orchestration ===
def load_runtime_config() -> Dict[str, Any]:
    """Collect sheet-driven runtime config into CONFIG."""
    from .windows_loader import load_windows
    CONFIG["windows"] = load_windows(GOOGLE_SHEETS_CONFIG)
    return CONFIG

__all__ = [
    "PER_CALLER_TARGET",
    "MIN_REPEAT_COUNT",
    "TIMEZONE",
    "CONFIG",
    "MOCK_DATA_CONFIG",
    "GOOGLE_SHEETS_CONFIG",
    "WindowRule",
    "load_runtime_config",
]
