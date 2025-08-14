# config/loader.py
"""
Load operational configuration from Google Sheets:
- sources + normalized source_mix (supports 0–1 or 0–100 weights)
- callers (available only)
- blacklist (global and per-source)
- cabal tiers (optional helper)
"""
from __future__ import annotations

import logging
from typing import Dict, List, Tuple
import pandas as pd

from config.settings import GOOGLE_SHEETS_CONFIG
from utils.gsheets import read_range


# ---------- Helpers ----------

def _to_df(values: List[List[str]], expected_cols: List[str]) -> pd.DataFrame:
    """
    Convert raw Sheets values to a DataFrame with the expected columns.
    Pads/truncates ragged rows, adds any missing expected columns.
    """
    if not values:
        return pd.DataFrame(columns=expected_cols)

    headers = [str(h).strip() for h in values[0]]
    rows = values[1:]
    width = len(headers)
    safe_rows = [(r + [""] * (width - len(r)))[:width] for r in rows]
    df = pd.DataFrame(safe_rows, columns=headers)

    # Ensure all expected cols exist
    for col in expected_cols:
        if col not in df.columns:
            df[col] = None

    return df[expected_cols].copy()


def _require_ranges_key(key: str) -> str:
    ranges = GOOGLE_SHEETS_CONFIG.get("ranges", {})
    if key not in ranges:
        raise KeyError(
            f"GOOGLE_SHEETS_CONFIG['ranges'][{key!r}] is missing. "
            "Add it to settings.py under GOOGLE_SHEETS_CONFIG['ranges']."
        )
    return ranges[key]


# ---------- Public loaders ----------

def load_sources_and_mix() -> Tuple[Dict, Dict]:
    """
    Reads Config!A:E and returns:
      - sources: { source_key: {enabled: bool, adapter: str, country_allowlist: [str]} }
      - source_mix: { source_key: float }   # normalized to sum to 1.0

    mix_weight may be a fraction (0–1) or a percentage (0–100). Per-row: values > 1.0 treated as percent (/100).
    Negative values clipped to 0. Only enabled & valid rows contribute to the normalized mix.
    """
    # Validate required config
    for k in ("service_account_file", "config_sheet_id", "ranges"):
        if k not in GOOGLE_SHEETS_CONFIG:
            logging.error("GOOGLE_SHEETS_CONFIG missing key: %s", k)
            return {}, {}

    a1 = _require_ranges_key("sources")
    vals = read_range(GOOGLE_SHEETS_CONFIG["service_account_file"],
                      GOOGLE_SHEETS_CONFIG["config_sheet_id"], a1)

    df = _to_df(vals, ["source_key", "enabled", "adapter_path", "country_allowlist", "mix_weight"])
    if df.empty:
        logging.warning("Config sheet returned no rows for sources; SOURCES and SOURCE_MIX will be empty.")
        return {}, {}

    # Normalize columns
    for col in ("source_key", "adapter_path", "country_allowlist"):
        df[col] = df[col].astype(str).str.strip()
        df.loc[df[col].str.lower().eq("nan"), col] = ""

    df["enabled"] = df["enabled"].astype(str).str.strip().str.upper().isin(["TRUE", "1", "YES", "Y"])

    # Parse mix_weight robustly
    raw = pd.to_numeric(df["mix_weight"], errors="coerce").fillna(0.0)
    neg_count = int((raw < 0).sum())
    raw = raw.where(raw >= 0, 0.0)

    percent_mask = raw > 1.0
    percent_count = int(percent_mask.sum())
    fraction_count = int((~percent_mask).sum())

    mix_norm = raw.copy()
    mix_norm.loc[percent_mask] = mix_norm.loc[percent_mask] / 100.0
    df["mix_norm"] = mix_norm

    # Warn on duplicates (last wins)
    if not df[df["source_key"] != ""].empty:
        dupes = df.loc[df["source_key"] != "", "source_key"].duplicated(keep="last")
        if dupes.any():
            dup_keys = df.loc[dupes, "source_key"].unique().tolist()
            logging.warning("Duplicate source_key(s) found; last occurrence will be used: %s", ", ".join(dup_keys))

    # Build sources dict from valid rows
    valid_mask = (df["source_key"] != "") & (df["adapter_path"] != "")
    sources: Dict[str, Dict] = {}
    for _, r in df.loc[valid_mask].iterrows():
        allowlist = [c.strip() for c in str(r["country_allowlist"]).split(",")
                     if c.strip() and c.strip().lower() != "nan"]
        sources[r["source_key"]] = {
            "enabled": bool(r["enabled"]),
            "adapter": r["adapter_path"],
            "country_allowlist": allowlist,
        }

    skipped = int((~valid_mask).sum())
    if skipped:
        logging.warning("Skipped %d config row(s) missing source_key or adapter_path.", skipped)

    # Build normalized source_mix from enabled & valid rows
    enabled_valid = df.loc[valid_mask & df["enabled"]]
    total = float(enabled_valid["mix_norm"].sum())
    if total <= 0.0 or enabled_valid.empty:
        logging.warning("No positive mix_weight for enabled sources (after interpretation). SOURCE_MIX will be empty.")
        return sources, {}

    source_mix: Dict[str, float] = {}
    for _, r in enabled_valid.iterrows():
        source_mix[r["source_key"]] = float(r["mix_norm"]) / total

    ordered = sorted(source_mix.items(), key=lambda kv: kv[1], reverse=True)
    mix_text = ", ".join([f"{k}={v*100:.1f}%" for k, v in ordered])
    logging.info(
        "mix_weight interpretation: %d as percent (>1.0), %d as fraction (<=1.0); negatives clipped: %d",
        percent_count, fraction_count, neg_count
    )
    logging.info("Normalized source mix: %s", mix_text)

    return sources, source_mix


def load_callers() -> List[str]:
    """
    Reads Callers!A:B (caller_id | available)
    Returns a list of caller_ids where available is truthy.
    """
    for k in ("service_account_file", "config_sheet_id", "ranges"):
        if k not in GOOGLE_SHEETS_CONFIG:
            logging.error("GOOGLE_SHEETS_CONFIG missing key: %s", k)
            return []

    a1 = _require_ranges_key("callers")
    vals = read_range(GOOGLE_SHEETS_CONFIG["service_account_file"],
                      GOOGLE_SHEETS_CONFIG["config_sheet_id"], a1)

    df = _to_df(vals, ["caller_id", "available"])
    if df.empty:
        logging.warning("Callers sheet is empty.")
        return []

    df["available"] = df["available"].astype(str).str.strip().str.upper().isin(["TRUE", "1", "YES", "Y"])
    out = [c for c, a in df[["caller_id", "available"]].itertuples(index=False, name=None)
           if a and str(c).strip()]
    logging.info("Loaded %d available caller(s).", len(out))
    return out


def load_blacklist() -> Dict[str, object]:
    """
    Reads Blacklist!A:C (username | phone | source_key)
    Returns dict with:
      - triples: set[(src, username_norm, phone_norm)]  # only rows where ALL THREE are present
    """
    for k in ("service_account_file", "config_sheet_id", "ranges"):
        if k not in GOOGLE_SHEETS_CONFIG:
            logging.error("GOOGLE_SHEETS_CONFIG missing key: %s", k)
            return {"triples": set()}

    a1 = _require_ranges_key("blacklist")
    vals = read_range(GOOGLE_SHEETS_CONFIG["service_account_file"],
                      GOOGLE_SHEETS_CONFIG["config_sheet_id"], a1)

    df = _to_df(vals, ["username", "phone", "source_key"])
    if df.empty:
        logging.info("Blacklist sheet is empty.")
        return {"triples": set()}

    def norm_user(s: str) -> str:
        return str(s).strip().lower()

    def norm_phone(s: str) -> str:
        digits = "".join(ch for ch in str(s) if ch.isdigit())
        # Heuristic for TH local: if 9 digits, left-pad to 10 with '0'
        if len(digits) == 9:
            digits = "0" + digits
        return digits

    def nonempty(x: str) -> bool:
        x = str(x).strip()
        return bool(x) and x.lower() != "nan"

    triples = set()
    for _, r in df.iterrows():
        u, p, src = r.get("username", ""), r.get("phone", ""), r.get("source_key", "")
        if nonempty(u) and nonempty(p) and nonempty(src):
            triples.add((str(src).strip(), norm_user(u), norm_phone(p)))

    logging.info("Blacklist (strict) loaded: %d triple row(s) with username+phone+source_key.", len(triples))
    return {"triples": triples}

    return {
        "usernames": usernames,
        "phones": phones,
        "by_source_user": by_source_user,
        "by_source_phone": by_source_phone,
    }


def load_tiers_for_source(source_key: str) -> pd.DataFrame:
    """
    Optional helper for Cabal tiers: returns sorted dataframe with columns [min_topup, label].
    """
    try:
        a1 = _require_ranges_key("cabal_tiers")
    except KeyError:
        return pd.DataFrame(columns=["min_topup", "label"])

    vals = read_range(GOOGLE_SHEETS_CONFIG["service_account_file"],
                      GOOGLE_SHEETS_CONFIG["config_sheet_id"], a1)
    df = _to_df(vals, ["min_topup", "label"])
    if df.empty:
        return df
    df["min_topup"] = pd.to_numeric(df["min_topup"], errors="coerce").fillna(0.0)
    return df.sort_values("min_topup", ascending=False).reset_index(drop=True)
