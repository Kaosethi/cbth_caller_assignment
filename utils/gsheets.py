# utils/gsheets.py
from __future__ import annotations

import logging
import time
from typing import List, Optional

try:
    from google.oauth2.service_account import Credentials
    from googleapiclient.discovery import build
except Exception as e:
    # If deps are missing, importing read_range will still work; calls will log an error and return [].
    Credentials = None  # type: ignore[assignment]
    build = None        # type: ignore[assignment]
    _import_err: Optional[Exception] = e
else:
    _import_err = None

__all__ = ["read_range"]


def read_range(
    service_account_file: str,
    spreadsheet_id: str,
    a1_range: str,
    retries: int = 2,
) -> List[List[str]]:
    """
    Read a range from Google Sheets using a service account.
    Returns a list of rows (each row is a list of strings). [] if empty or on failure.

    Notes:
      - Uses UNFORMATTED_VALUE to avoid locale/format surprises (we normalize in loaders).
      - Retries with simple linear backoff.
    """
    if _import_err is not None or Credentials is None or build is None:
        logging.error("Google API dependencies are missing: %s", _import_err)
        return []

    scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    last_err: Optional[Exception] = None

    for attempt in range(retries + 1):
        try:
            creds = Credentials.from_service_account_file(service_account_file, scopes=scopes)
            service = build("sheets", "v4", credentials=creds, cache_discovery=False)

            result = (
                service.spreadsheets()
                .values()
                .get(
                    spreadsheetId=spreadsheet_id,
                    range=a1_range,
                    valueRenderOption="UNFORMATTED_VALUE",
                    dateTimeRenderOption="SERIAL_NUMBER",
                )
                .execute()
            )
            values = result.get("values", []) or []
            logging.info(
                "Sheets read OK: %s %s (%d row(s) including header)",
                spreadsheet_id,
                a1_range,
                len(values),
            )
            # Normalize to list[list[str]]
            out: List[List[str]] = []
            for row in values:
                if isinstance(row, list):
                    out.append([("" if v is None else str(v)) for v in row])
                else:
                    out.append([("" if row is None else str(row))])
            return out

        except FileNotFoundError:
            logging.error("Service account file not found: %s", service_account_file)
            logging.info("Ensure the Sheet is shared with the service account's client_email.")
            return []
        except Exception as e:
            last_err = e
            logging.warning(
                "Sheets read failed (attempt %d/%d) for %s %s: %s",
                attempt + 1,
                retries + 1,
                spreadsheet_id,
                a1_range,
                e,
            )
            time.sleep(0.8 * (attempt + 1))

    logging.error("Google Sheets read error for range %s: %s", a1_range, last_err)
    return []
