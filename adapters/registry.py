# adapters/registry.py
from __future__ import annotations

import importlib
from typing import Dict, Tuple, List

import pandas as pd


def resolve_callable(path: str):
    mod, func = path.split(":")
    return getattr(importlib.import_module(mod), func)


def load_all_candidates(run_date, SOURCES: Dict[str, Dict]) -> pd.DataFrame:
    """
    Call each enabled source adapter and concatenate results.
    Adds 'source_key' if the adapter didn't include it.
    Ensures a consistent schema for downstream steps.
    """
    frames: List[pd.DataFrame] = []

    for key, cfg in SOURCES.items():
        if not cfg.get("enabled", True):
            continue

        fetch = resolve_callable(cfg["adapter"])
        df = fetch(run_date)

        if df is None:
            continue
        if not isinstance(df, pd.DataFrame):
            raise TypeError(f"Adapter {cfg['adapter']} must return a pandas DataFrame")

        if df.empty:
            continue

        if "source_key" not in df.columns:
            df["source_key"] = key

        frames.append(df)

    if frames:
        return pd.concat(frames, ignore_index=True)

    # unified empty schema used downstream
    return pd.DataFrame(
        columns=[
            "source_key",
            "platform",
            "game",
            "username",
            "phone",
            "calling_code",
            "last_login_date",
        ]
    )
