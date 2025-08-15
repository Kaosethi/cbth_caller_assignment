# config/sources_loader.py
from __future__ import annotations

from typing import Dict, Tuple, List
import logging

import pandas as pd
from utils.gsheets import read_range

log = logging.getLogger(__name__)

EXPECTED: List[str] = ["source_key", "enabled", "adapter_path", "country_allowlist", "mix_weight"]


def _to_df(values, expected=EXPECTED) -> pd.DataFrame:
    """
    Convert raw Sheets values to a DataFrame with normalized lowercase headers,
    padding ragged rows and ensuring all expected columns exist.
    """
    if not values or len(values) < 1:
        return pd.DataFrame(columns=expected)

    headers = [str(h).strip().lower() for h in values[0]]
    rows = values[1:]
    width = len(headers)
    safe_rows = [(r + [""] * (width - len(r)))[:width] for r in rows]
    df = pd.DataFrame(safe_rows, columns=headers)

    for col in expected:
        if col not in df.columns:
            df[col] = ""

    return df[expected].copy()


def load_sources_and_mix(gs_cfg: Dict) -> Tuple[Dict[str, Dict], Dict[str, float]]:
    """
    Reads Config!A:E and returns:
      - sources: { source_key: {enabled: bool, adapter: str, country_allowlist: [str]} }
      - source_mix: { source_key: float }   # normalized to sum to 1.0

    mix_weight may be fraction (0–1) or percent (0–100). Negatives are clipped to 0.
    Duplicate source_key rows: **last one wins**.
    """
    vals = read_range(
        gs_cfg["service_account_file"],
        gs_cfg["config_sheet_id"],
        gs_cfg["ranges"]["sources"],
    )
    df = _to_df(vals)
    if df.empty:
        log.warning("Sources sheet empty; returning no sources/mix.")
        return {}, {}

    # Normalize columns
    df["source_key"] = df["source_key"].astype(str).str.strip()
    df["adapter_path"] = df["adapter_path"].astype(str).str.strip()
    df["country_allowlist"] = df["country_allowlist"].astype(str)
    df["enabled"] = df["enabled"].astype(str).str.strip().str.upper().isin(["TRUE", "1", "YES", "Y"])

    # Parse weights; clip negatives; treat >1 as percent
    raw = pd.to_numeric(df["mix_weight"], errors="coerce").fillna(0.0)
    neg_count = int((raw < 0).sum())
    raw = raw.mask(raw < 0, 0.0)

    percent_mask = raw > 1.0
    percent_count = int(percent_mask.sum())
    fraction_count = int((~percent_mask).sum())

    mix_norm = raw.copy()
    mix_norm.loc[percent_mask] = mix_norm.loc[percent_mask] / 100.0
    df["mix_norm"] = mix_norm

    # Drop invalid rows (need key + adapter)
    valid = (df["source_key"] != "") & (df["adapter_path"] != "")
    if (~valid).any():
        log.warning("Skipped %d config row(s) missing source_key or adapter_path.", int((~valid).sum()))
    df = df.loc[valid].copy()

    # Resolve duplicates: keep **last** occurrence
    if not df.empty:
        dup_mask = df.duplicated(subset=["source_key"], keep="last")
        if dup_mask.any():
            dup_keys = df.loc[dup_mask, "source_key"].unique().tolist()
            log.warning("Duplicate source_key(s) found; last occurrence will be used: %s", ", ".join(dup_keys))
            df = df.loc[~dup_mask | dup_mask]  # dup_mask used only for warning; we keep all and rely on groupby last

    # Keep last per source_key explicitly
    df = df.groupby("source_key", as_index=False).last()

    # Build sources dict (include enabled so registry can honor it)
    sources: Dict[str, Dict] = {}
    for _, r in df.iterrows():
        allowlist = [
            c.strip().upper()
            for c in str(r["country_allowlist"]).split(",")
            if c.strip() and c.strip().lower() != "nan"
        ]
        sources[str(r["source_key"])] = {
            "enabled": bool(r["enabled"]),
            "adapter": str(r["adapter_path"]),
            "country_allowlist": allowlist,
        }

    # Build normalized mix from **enabled** rows with positive weight
    enabled = df[df["enabled"]].copy()
    if enabled.empty:
        log.warning("No enabled sources; SOURCE_MIX will be empty.")
        return sources, {}

    totals = enabled.set_index("source_key")["mix_norm"].clip(lower=0.0)
    total = float(totals.sum())
    if total <= 0.0:
        log.warning("No positive mix_weight for enabled sources; SOURCE_MIX will be empty.")
        return sources, {}

    source_mix = (totals / total).to_dict()

    ordered = sorted(source_mix.items(), key=lambda kv: kv[1], reverse=True)
    mix_text = ", ".join([f"{k}={v*100:.1f}%" for k, v in ordered])
    log.info(
        "mix_weight interpretation: %d as percent (>1.0), %d as fraction (<=1.0); negatives clipped: %d",
        percent_count,
        fraction_count,
        neg_count,
    )
    log.info("Normalized source mix: %s", mix_text)

    return sources, source_mix
