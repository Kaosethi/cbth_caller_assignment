# scheduler/runner.py
from datetime import date
import pandas as pd
import logging

from config.settings import GOOGLE_SHEETS_CONFIG, PER_CALLER_TARGET
from utils.business_days import is_business_day, next_business_day, load_holidays_from_gsheet
from config.loader import load_sources_and_mix, load_callers, load_blacklist
from allocator.mix_allocator import allocate
from adapters.registry import load_all_candidates

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def _load_holidays():
    """Fetch the list of holidays from Google Sheets."""
    return load_holidays_from_gsheet(
        GOOGLE_SHEETS_CONFIG['service_account_file'],
        GOOGLE_SHEETS_CONFIG['holiday_sheet_id'],
        GOOGLE_SHEETS_CONFIG['holiday_range'],
    )


def run_assignment_flow(run_date: date) -> pd.DataFrame:
    """
    Full assignment process:
      - Load config from Google Sheets
      - Load caller availability
      - Load blacklist
      - Fetch candidates from each source
      - Apply blacklist filters
      - Allocate to callers according to mix_weight
    Returns: assigned DataFrame (in memory, no CSV saved)
    """
    logging.info("Loading configuration from Google Sheet...")
    SOURCES, SOURCE_MIX = load_sources_and_mix()
    caller_ids = load_callers()
    bl = load_blacklist()

    if not SOURCES:
        logging.warning("No sources configured or enabled in the sheet. Stopping.")
        return pd.DataFrame()
    if not SOURCE_MIX:
        logging.warning("No source mix derived from sheet (no enabled sources or zero weights). Stopping.")
        return pd.DataFrame()
    if not caller_ids:
        logging.warning("No available callers found in sheet. Skipping assignment.")
        return pd.DataFrame()

    bl_count = len(bl.get('triples', set()))
    logging.info(
        "Loaded %d sources, %d callers, %d blacklist entries.",
        len(SOURCES), len(caller_ids), bl_count
    )

    # Load candidates from all enabled sources
    pool = load_all_candidates(run_date, SOURCES)

    # Apply blacklist
    if not pool.empty:
        before = len(pool)

        def norm_user(s):  # same normalization as loader
            return str(s).strip().lower()

        def norm_phone(s):
            digits = "".join(ch for ch in str(s) if ch.isdigit())
            if len(digits) == 9:
                digits = "0" + digits
            return digits

        pool["_u"] = pool.get("username", "").map(norm_user)
        pool["_p"] = pool.get("phone", "").map(norm_phone)
        pool["_s"] = pool.get("source_key", "").astype(str).str.strip()

        triples = bl.get("triples", set())
        if triples:
            mask_keep = ~pool.apply(lambda r: (r["_s"], r["_u"], r["_p"]) in triples, axis=1)
            pool = pool[mask_keep]

        removed = before - len(pool)
        logging.info("Blacklist removed %d record(s) (strict triple rows).", removed)

        pool = pool.drop(columns=["_u", "_p", "_s"], errors="ignore")

    if pool.empty:
        logging.info("No candidates available to assign.")
        return pd.DataFrame()

    # Allocate to callers
    assigned = allocate(
        candidates=pool,
        caller_ids=caller_ids,
        per_caller_target=PER_CALLER_TARGET,
        source_mix=SOURCE_MIX,
    )

    logging.info("[OK] Assignments generated: %d rows", len(assigned))
    if not assigned.empty and "caller_id" in assigned.columns:
        logging.info("Assignments per caller:\n%s", assigned["caller_id"].value_counts())

    return assigned


def main():
    holidays = _load_holidays()
    today = date.today()

    if not is_business_day(today, holidays):
        logging.info(
            "Skip: %s is not a business day. Next business day = %s",
            today,
            next_business_day(today, holidays),
        )
        return

    logging.info("Proceed: %s is a business day. Running assignment flow...", today)
    assigned_df = run_assignment_flow(today)

    # Example: process assigned_df without saving to CSV
    if not assigned_df.empty:
        logging.info("Processing %d assigned rows...", len(assigned_df))
        # TODO: Replace with your next step (API call, DB insert, etc.)
        # send_to_api(assigned_df)
    else:
        logging.info("No assignments to process today.")


if __name__ == "__main__":
    main()
