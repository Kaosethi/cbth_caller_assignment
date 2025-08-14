# utils/gsheets.py
from __future__ import annotations

import logging
import time
from typing import List

try:
    from google.oauth2.service_account import Credentials
    from googleapiclient.discovery import build
except Exception as e:
    # If deps are missing, importing read_range will still work, but calls will log an error.
    Credentials = None
    build = None
    _import_err = e
else:
    _import_err = None

__all__ = ["read_range"]

def read_range(service_account_file: str, spreadsheet_id: str, a1_range: str, retries: int = 2) -> List[List[str]]:
    """
    Read a range from Google Sheets using a service account.
    Returns: list of rows (each row is a list of cell strings). If the range is empty, returns [].
    """
    if _import_err is not None:
        logging.error("Google API deps missing: %s", _import_err)
        return []

    scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    last_err = None
    for attempt in range(retries + 1):
        try:
            creds = Credentials.from_service_account_file(service_account_file, scopes=scopes)
            service = build("sheets", "v4", credentials=creds, cache_discovery=False)
            result = service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=a1_range,
            ).execute()
            values = result.get("values", [])
            logging.info(
                "Sheets read OK: %s %s (%d row(s) including header)",
                spreadsheet_id, a1_range, max(0, len(values))
            )
            return values
        except FileNotFoundError:
            logging.error("Service account file not found: %s", service_account_file)
            logging.info("Ensure the Sheet is shared with the service account’s client_email.")
            return []
        except Exception as e:
            last_err = e
            logging.warning(
                "Sheets read failed (attempt %d/%d) for %s %s: %s",
                attempt + 1, retries + 1, spreadsheet_id, a1_range, e
            )
            time.sleep(0.8 * (attempt + 1))

    logging.error("Google Sheets read error for range %s: %s", a1_range, last_err)
    return []
