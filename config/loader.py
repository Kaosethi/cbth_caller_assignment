"""
Thin wrappers that delegate to modular loaders.

Kept for backward compatibility with existing imports like:
  from config.loader import load_sources_and_mix, load_callers, load_blacklist
"""
from __future__ import annotations

from typing import Dict, List, Tuple
import pandas as pd

from config.settings import GOOGLE_SHEETS_CONFIG
from utils.gsheets import read_range

# Delegate to modular loaders
from .sources_loader import load_sources_and_mix as _load_sources_and_mix
from .callers_loader import load_callers as _load_callers
from .blacklist_loader import load_blacklist as _load_blacklist


def load_sources_and_mix() -> Tuple[Dict, Dict]:
    """
    Wrapper for config.sources_loader.load_sources_and_mix(GOOGLE_SHEETS_CONFIG)
    """
    return _load_sources_and_mix(GOOGLE_SHEETS_CONFIG)


def load_callers() -> List[str]:
    """
    Wrapper for config.callers_loader.load_callers(GOOGLE_SHEETS_CONFIG)
    """
    return _load_callers(GOOGLE_SHEETS_CONFIG)


def load_blacklist() -> Dict[str, object]:
    """
    Wrapper for config.blacklist_loader.load_blacklist(GOOGLE_SHEETS_CONFIG)
    """
    return _load_blacklist(GOOGLE_SHEETS_CONFIG)


def load_tiers_for_source(source_key: str) -> pd.DataFrame:
    """
    Optional helper for Cabal tiers: returns sorted DataFrame with columns [min_topup, label].

    NOTE: This remains local since we don't yet have a dedicated tiers_loader.
    Sheet range used: GOOGLE_SHEETS_CONFIG['ranges']['cabal_tiers']
    """
    ranges = GOOGLE_SHEETS_CONFIG.get("ranges", {})
    a1 = ranges.get("cabal_tiers")
    if not a1:
        return pd.DataFrame(columns=["min_topup", "label"])

    vals = read_range(
        GOOGLE_SHEETS_CONFIG["service_account_file"],
        GOOGLE_SHEETS_CONFIG["config_sheet_id"],
        a1,
    )

    # Normalize to a two-column frame with padding for ragged rows
    expected = ["min_topup", "label"]
    if not vals:
        return pd.DataFrame(columns=expected)

    headers = [str(h).strip() for h in vals[0]]
    rows = vals[1:]
    width = len(headers)
    safe_rows = [(r + [""] * (width - len(r)))[:width] for r in rows]
    df = pd.DataFrame(safe_rows, columns=headers)

    for col in expected:
        if col not in df.columns:
            df[col] = None

    df = df[expected].copy()
    df["min_topup"] = pd.to_numeric(df["min_topup"], errors="coerce").fillna(0.0)
    return df.sort_values("min_topup", ascending=False).reset_index(drop=True)
