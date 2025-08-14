import os
import pandas as pd
from config.settings import MOCK_DATA_CONFIG

REQUIRED_COLUMNS = ["platform", "game", "username", "phone", "calling_code"]

def _empty_df():
    return pd.DataFrame(columns=REQUIRED_COLUMNS)

def fetch_candidates(run_date) -> pd.DataFrame:
    """
    Returns a DataFrame with at least columns:
      platform, game, username, phone, calling_code
    'source_key' will be added upstream if missing.
    """
    path = MOCK_DATA_CONFIG.get("pc_data_file", "data/mock/pc_data.csv")
    if not os.path.exists(path):
        return _empty_df()

    df = pd.read_csv(path, dtype=str).fillna("")
    for col in REQUIRED_COLUMNS:
        if col not in df.columns:
            df[col] = ""
    # light cleanup
    df["username"] = df["username"].astype(str).str.strip()
    df["phone"] = df["phone"].astype(str).str.strip()
    df["calling_code"] = df["calling_code"].astype(str).str.strip()
    return df[REQUIRED_COLUMNS].reset_index(drop=True)
