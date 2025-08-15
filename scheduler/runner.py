# scheduler/runner.py
from __future__ import annotations

from datetime import date
import logging
import pandas as pd

# Settings and runtime config
import config.settings as settings
from config.settings import (
    GOOGLE_SHEETS_CONFIG,
    PER_CALLER_TARGET,
    load_runtime_config,
)

# Business-day utilities (pure logic only)
from utils.business_days import is_business_day, next_business_day

# Modular loaders (these take gs_cfg)
from config.sources_loader import load_sources_and_mix
from config.callers_loader import load_callers
from config.blacklist_loader import load_blacklist
from config.holidays_loader import load_holidays

# Windowed pool builder (import explicitly from module)
from allocator.pool import build_windowed_pool

# Configure root logger once
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def _load_holidays():
    """Fetch the list of holidays from Google Sheets (same workbook as other tabs)."""
    return load_holidays(GOOGLE_SHEETS_CONFIG)


def run_assignment_flow(run_date: date) -> pd.DataFrame:
    """
    Full assignment process:
      - Load sources/mix, callers, blacklist from Google Sheets
      - Fetch candidates from each source
      - Apply blacklist (strict triple: source_key+username+phone)
      - Build windowed pool (Hot -> Cold -> Hibernated) up to target rows
      - Allocate to callers according to mix and per-caller target
    Returns: assigned DataFrame (in memory)
    """
    logger.info("Loading configuration from Google Sheet...")

    # Load live config
    SOURCES, SOURCE_MIX = load_sources_and_mix(GOOGLE_SHEETS_CONFIG)
    caller_ids = load_callers(GOOGLE_SHEETS_CONFIG)
    bl = load_blacklist(GOOGLE_SHEETS_CONFIG)

    if not SOURCES:
        logger.warning("No sources configured or enabled in the sheet. Stopping.")
        return pd.DataFrame()
    if not SOURCE_MIX:
        logger.warning("No source mix derived from sheet (no enabled sources or zero weights). Stopping.")
        return pd.DataFrame()
    if not caller_ids:
        logger.warning("No available callers found in sheet. Skipping assignment.")
        return pd.DataFrame()

    bl_count = len(bl.get("triples", set()))
    logger.info("Loaded %d sources, %d callers, %d blacklist entries.", len(SOURCES), len(caller_ids), bl_count)

    # 1) Load raw candidates from all enabled sources
    from adapters.registry import load_all_candidates  # local import to avoid cycles
    pool_raw = load_all_candidates(run_date, SOURCES)

    # 2) Apply blacklist (STRICT triple: (source_key, username, phone))
    if not pool_raw.empty:
        before = len(pool_raw)

        def norm_user(s):  # same normalization as loader
            return str(s).strip().lower()

        def norm_phone(s):
            digits = "".join(ch for ch in str(s) if ch.isdigit())
            if len(digits) == 9:
                digits = "0" + digits
            return digits

        pool_raw["_u"] = pool_raw.get("username", "").map(norm_user)
        pool_raw["_p"] = pool_raw.get("phone", "").map(norm_phone)
        pool_raw["_s"] = pool_raw.get("source_key", "").astype(str).str.strip()

        triples = bl.get("triples", set())
        if triples:
            mask_keep = ~pool_raw.apply(lambda r: (r["_s"], r["_u"], r["_p"]) in triples, axis=1)
            pool_raw = pool_raw[mask_keep]

        removed = before - len(pool_raw)
        logger.info("Blacklist removed %d record(s) (strict triple rows).", removed)

        pool_raw = pool_raw.drop(columns=["_u", "_p", "_s"], errors="ignore")

    if pool_raw.empty:
        logger.info("No candidates available to assign after blacklist.")
        return pd.DataFrame()

    # 3) Build windowed pool up to TARGET ROWS using CONFIG["windows"]
    target_rows = len(caller_ids) * PER_CALLER_TARGET
    windows = settings.CONFIG.get("windows", [])

    if not windows:
        logger.warning("No window rules found in CONFIG; skipping window filtering.")
        pool = pool_raw.copy()
    else:
        # Guard: adapters must supply last_login_date & phone
        missing_cols = [c for c in ("last_login_date", "phone") if c not in pool_raw.columns]
        if missing_cols:
            logger.warning(
                "Adapters did not provide required columns %s; skipping window filtering.",
                missing_cols,
            )
            pool = pool_raw.copy()
        else:
            pool = build_windowed_pool(pool_raw, run_date, windows, target_rows)
            logger.info(
                "Windowed pool built: %d selected / %d available (target=%d).",
                len(pool), len(pool_raw), target_rows
            )
            if "window_label" in pool.columns and not pool.empty:
                logger.info("By window:\n%s", pool["window_label"].value_counts())

    if pool.empty:
        logger.info("No candidates after window filtering.")
        return pd.DataFrame()

    # 4) Allocate to callers according to mix and per-caller target
    from allocator.mix_allocator import allocate  # local import avoids early side-effects
    assigned = allocate(
        candidates=pool,
        caller_ids=caller_ids,
        per_caller_target=PER_CALLER_TARGET,
        source_mix=SOURCE_MIX,
    )

    logger.info("[OK] Assignments generated: %d rows", len(assigned))
    if not assigned.empty and "caller_id" in assigned.columns:
        logger.info("Assignments per caller:\n%s", assigned["caller_id"].value_counts())

    return assigned


def main():
    # 0) Load runtime config (pull Windows from Sheets into settings.CONFIG)
    load_runtime_config()

    # 1) Holiday check
    holidays = _load_holidays()
    today = date.today()

    if not is_business_day(today, holidays):
        logger.info(
            "Skip: %s is not a business day. Next business day = %s",
            today,
            next_business_day(today, holidays),
        )
        return

    logger.info("Proceed: %s is a business day. Running assignment flow...", today)

    # 2) Run the assignment pipeline
    assigned_df = run_assignment_flow(today)

    # 3) Post-processing hook (replace with your downstream action)
    if not assigned_df.empty:
        logger.info("Processing %d assigned rows...", len(assigned_df))
        # TODO: send_to_api(assigned_df) or write to Sheets/DB
    else:
        logger.info("No assignments to process today.")


if __name__ == "__main__":
    main()
