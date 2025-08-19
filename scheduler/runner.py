# scheduler/runner.py
from __future__ import annotations

from datetime import date, datetime
import logging
import os
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

# Google Sheets low-level util for reading Compile tab
from utils.gsheets import read_range

# Configure root logger once
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# -------------------------------
# Inline legacy filter helpers
# -------------------------------

UNREACHABLE_STATUSES = ["ไม่รับสาย", "ติดต่อไม่ได้", "กดตัดสาย", "รับสายไม่สะดวกคุย"]

def _load_holidays():
    """Fetch the list of holidays from Google Sheets (same workbook as other tabs)."""
    return load_holidays(GOOGLE_SHEETS_CONFIG)

def _load_compile_df() -> pd.DataFrame:
    """Load the Compile tab for the CURRENT workbook; expects at least Date, Username, Phone, Answer Status, Call Status, Result, History, History Date, Telesale."""
    rng = GOOGLE_SHEETS_CONFIG["ranges"].get("compile")
    if not rng:
        logger.warning("No 'compile' range configured in GOOGLE_SHEETS_CONFIG['ranges']. Skipping compile-based filters.")
        return pd.DataFrame(columns=["Date","Username","Phone","Answer Status","Call Status","Result","History","History Date","Telesale"])
    vals = read_range(GOOGLE_SHEETS_CONFIG["service_account_file"], GOOGLE_SHEETS_CONFIG["config_sheet_id"], rng)
    if not vals or len(vals) < 2:
        return pd.DataFrame(columns=["Date","Username","Phone","Answer Status","Call Status","Result","History","History Date","Telesale"])
    headers = [str(h).strip() for h in vals[0]]
    df = pd.DataFrame(vals[1:], columns=headers)
    # normalize expected cols even if missing
    for col in ["Date","Username","Phone","Answer Status","Call Status","Result","History","History Date","Telesale"]:
        if col not in df.columns:
            df[col] = None
    # types
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df["Username"] = df["Username"].astype(str).str.strip()
    df["Phone"] = df["Phone"].astype(str).str.replace(" ", "").str.strip()
    return df[["Date","Username","Phone","Answer Status","Call Status","Result","History","History Date","Telesale"]]

def _month_slice(df: pd.DataFrame, today: date) -> pd.DataFrame:
    if df.empty:
        return df
    start = datetime(today.year, today.month, 1)
    mask = (df["Date"] >= pd.Timestamp(start)) & (df["Date"] <= pd.Timestamp(today))
    return df.loc[mask]

def _block_unreachable_repeat_this_month(compile_df: pd.DataFrame, today: date, min_cnt: int = 2) -> set[str]:
    mdf = _month_slice(compile_df, today)
    if mdf.empty:
        return set()
    grp = (mdf["Answer Status"].isin(UNREACHABLE_STATUSES)).groupby(mdf["Username"]).sum()
    return set(grp[grp >= min_cnt].index.astype(str))

def _block_answered_this_month(compile_df: pd.DataFrame, today: date) -> set[str]:
    mdf = _month_slice(compile_df, today)
    if mdf.empty:
        return set()
    return set(mdf.loc[mdf["Answer Status"] == "รับสาย", "Username"].astype(str).unique())

def _block_invalid_number_ever(compile_df: pd.DataFrame) -> set[str]:
    if compile_df.empty:
        return set()
    return set(compile_df.loc[compile_df["Result"] == "เบอร์เสีย", "Username"].astype(str).unique())

def _block_not_interested_this_month(compile_df: pd.DataFrame, today: date) -> set[str]:
    mdf = _month_slice(compile_df, today)
    if mdf.empty:
        return set()
    return set(mdf.loc[mdf["Result"] == "ไม่สนใจ", "Username"].astype(str).unique())

def _block_not_owner_hard(compile_df: pd.DataFrame) -> set[str]:
    if compile_df.empty:
        return set()
    return set(compile_df.loc[compile_df["Result"] == "ไม่ใช่เจ้าของไอดี", "Username"].astype(str).unique())

def _get_redeemed_usernames_today() -> set[str]:
    """Query Grafana datasource DB for usernames redeemed today. Safe no-op if DB not configured."""
    url = os.getenv("DATABASE_URL_GRAFANA")
    use_db = os.getenv("USE_REAL_DB", "false").lower() in ("1","true","yes")
    if not url or not use_db:
        return set()
    try:
        from sqlalchemy import create_engine, text
        eng = create_engine(url)
        sql = text("SELECT DISTINCT username FROM redemption_logs WHERE DATE(redeem_time) = CURRENT_DATE")
        with eng.connect() as c:
            rows = c.execute(sql).fetchall()
        return {r[0] for r in rows if r and r[0]}
    except Exception as e:
        logger.warning("Redeemed-today query skipped (error: %s).", e)
        return set()

def _normalize_reward_rank(df: pd.DataFrame) -> pd.DataFrame:
    if "Reward Rank" in df.columns:
        df["Reward Rank"] = df["Reward Rank"].fillna("SILVER").replace("#N/A", "SILVER")
    return df

def _apply_legacy_filters(pool_df: pd.DataFrame, compile_df: pd.DataFrame, run_date: date) -> pd.DataFrame:
    """Apply agreed legacy filters. Uses env/config toggles when present; sane defaults otherwise."""
    if pool_df.empty:
        return pool_df.copy()

    cfg = getattr(settings, "CONFIG", {}) or {}

    def _cfg(name: str, default):
        # Allow both settings.CONFIG and environment overrides
        return cfg.get(name, os.getenv(name, str(default))).__str__().lower() if isinstance(default, bool) else cfg.get(name, default)

    # toggles / params
    drop_unreachable = (_cfg("DROP_UNREACHABLE_REPEAT", True) in ("true","1","yes"))
    unreachable_min = int(_cfg("UNREACHABLE_MIN_COUNT", 2))
    drop_answered = (_cfg("DROP_ANSWERED_THIS_MONTH", False) in ("true","1","yes"))
    drop_invalid = (_cfg("DROP_INVALID_NUMBER", True) in ("true","1","yes"))
    drop_not_interested = (_cfg("DROP_NOT_INTERESTED_THIS_MONTH", True) in ("true","1","yes"))
    drop_not_owner = (_cfg("DROP_NOT_OWNER_AS_BLACKLIST", True) in ("true","1","yes"))
    drop_redeemed_today = (_cfg("DROP_REDEEMED_TODAY", True) in ("true","1","yes"))

    before = len(pool_df)
    df = pool_df.copy()

    # make sure Username column exists for matching (our pool uses lowercase 'username')
    if "Username" not in df.columns and "username" in df.columns:
        df["Username"] = df["username"].astype(str)
    if "Phone" not in df.columns and "phone" in df.columns:
        df["Phone"] = df["phone"].astype(str)

    removed_total = 0

    if drop_unreachable:
        block = _block_unreachable_repeat_this_month(compile_df, run_date, unreachable_min)
        pre = len(df); df = df[~df["Username"].astype(str).isin(block)]; removed_total += (pre - len(df))
        logger.info("Filter unreachable(≥%d this month): removed %d", unreachable_min, pre - len(df))

    if drop_answered:
        block = _block_answered_this_month(compile_df, run_date)
        pre = len(df); df = df[~df["Username"].astype(str).isin(block)]; removed_total += (pre - len(df))
        logger.info("Filter answered(this month): removed %d", pre - len(df))

    if drop_invalid:
        block = _block_invalid_number_ever(compile_df)
        pre = len(df); df = df[~df["Username"].astype(str).isin(block)]; removed_total += (pre - len(df))
        logger.info("Filter invalid number(ever): removed %d", pre - len(df))

    if drop_not_interested:
        block = _block_not_interested_this_month(compile_df, run_date)
        pre = len(df); df = df[~df["Username"].astype(str).isin(block)]; removed_total += (pre - len(df))
        logger.info("Filter not interested(this month): removed %d", pre - len(df))

    if drop_not_owner:
        block = _block_not_owner_hard(compile_df)
        pre = len(df); df = df[~df["Username"].astype(str).isin(block)]; removed_total += (pre - len(df))
        logger.info("Filter not owner(hard): removed %d", pre - len(df))

    if drop_redeemed_today:
        redeemed = _get_redeemed_usernames_today()
        if redeemed:
            pre = len(df); df = df[~df["Username"].astype(str).isin(redeemed)]; removed_total += (pre - len(df))
            logger.info("Filter redeemed today(DB): removed %d", pre - len(df))
        else:
            logger.info("Filter redeemed today(DB): skipped (no results or DB not enabled)")

    df = _normalize_reward_rank(df)
    logger.info("Legacy filters removed %d total (before=%d, after=%d).", removed_total, before, len(df))
    return df

# -------------------------------

def run_assignment_flow(run_date: date) -> pd.DataFrame:
    """
    Full assignment process:
      - Load sources/mix, callers, blacklist from Google Sheets
      - Fetch candidates from each source
      - Apply blacklist (strict triple: source_key+username+phone)
      - Apply legacy filters (compile- & DB-driven)
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

    # 3) NEW: Apply legacy filters (compile + redeemed-today)
    compile_df = _load_compile_df()
    pool_filtered = _apply_legacy_filters(pool_raw, compile_df, run_date)

    if pool_filtered.empty:
        logger.info("No candidates available after legacy filters.")
        return pd.DataFrame()

    # 4) Build windowed pool up to TARGET ROWS using CONFIG["windows"]
    target_rows = len(caller_ids) * PER_CALLER_TARGET
    windows = settings.CONFIG.get("windows", [])

    if not windows:
        logger.warning("No window rules found in CONFIG; skipping window filtering.")
        pool = pool_filtered.copy()
    else:
        # Guard: adapters must supply last_login_date & phone
        missing_cols = [c for c in ("last_login_date", "phone") for _ in [0] if c not in pool_filtered.columns]
        if missing_cols:
            logger.warning(
                "Adapters did not provide required columns %s; skipping window filtering.",
                missing_cols,
            )
            pool = pool_filtered.copy()
        else:
            pool = build_windowed_pool(pool_filtered, run_date, windows, target_rows)
            logger.info(
                "Windowed pool built: %d selected / %d available (target=%d).",
                len(pool), len(pool_filtered), target_rows
            )
            if "window_label" in pool.columns and not pool.empty:
                logger.info("By window:\n%s", pool["window_label"].value_counts())

    if pool.empty:
        logger.info("No candidates after window filtering.")
        return pd.DataFrame()

    # 5) Allocate to callers according to mix and per-caller target
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
