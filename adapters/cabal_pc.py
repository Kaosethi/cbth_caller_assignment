# adapters/cabal_pc.py
from __future__ import annotations

import os
import logging
from datetime import timedelta
from typing import List

import pandas as pd
import config.settings as settings

log = logging.getLogger(__name__)

REQUIRED_COLUMNS: List[str] = [
    "platform",
    "game",
    "username",
    "phone",
    "calling_code",
    "last_login_date",  # required by windowing
]


def _empty_df() -> pd.DataFrame:
    return pd.DataFrame(columns=REQUIRED_COLUMNS)


def _norm_phone(s: object) -> str:
    digits = "".join(ch for ch in str(s) if ch.isdigit())
    if len(digits) == 9:
        digits = "0" + digits
    return digits


def _ensure_last_login_date(df: pd.DataFrame, run_date: pd.Timestamp) -> pd.DataFrame:
    if "last_login_date" in df.columns:
        s = pd.to_datetime(df["last_login_date"], errors="coerce")
        df["last_login_date"] = s.dt.strftime("%Y-%m-%d").fillna("")
        return df

    if "last_login_days_ago" in df.columns:
        days = pd.to_numeric(df["last_login_days_ago"], errors="coerce")
        derived = (run_date - days.fillna(0).astype(int).map(lambda d: timedelta(days=d)))
        df["last_login_date"] = pd.to_datetime(derived, errors="coerce").dt.strftime("%Y-%m-%d").fillna("")
        return df

    if not getattr(_ensure_last_login_date, "_warned", False):
        log.warning("[pc] last_login_date not found and cannot be derived; affected rows will be skipped by windowing")
        _ensure_last_login_date._warned = True  # type: ignore[attr-defined]
    df["last_login_date"] = ""
    return df


def fetch_candidates(run_date) -> pd.DataFrame:
    """
    Returns a DataFrame with columns at least:
      platform, game, username, phone, calling_code, last_login_date
    'source_key' is added upstream.
    """
    path = settings.MOCK_DATA_CONFIG.get("pc_data_file", "data/mock/pc_data.csv")
    if not os.path.exists(path):
        log.warning("[pc] mock file not found: %s", path)
        return _empty_df()

    df = pd.read_csv(path, dtype=str).fillna("")

    # Ensure required columns exist
    for col in REQUIRED_COLUMNS:
        if col not in df.columns:
            df[col] = ""

    # Normalize basics
    df["platform"] = df["platform"].astype(str).str.strip()
    df["game"] = df["game"].astype(str).str.strip()
    df["username"] = df["username"].astype(str).str.strip()
    df["phone"] = df["phone"].map(_norm_phone)
    df["calling_code"] = df["calling_code"].astype(str).str.strip()

    # Ensure/derive last_login_date
    df = _ensure_last_login_date(df, pd.Timestamp(run_date))

    return df[REQUIRED_COLUMNS].reset_index(drop=True)
