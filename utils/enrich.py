import pandas as pd

def normalize_reward_rank(df: pd.DataFrame) -> pd.DataFrame:
    if "Reward Rank" in df:
        df["Reward Rank"] = df["Reward Rank"].fillna("SILVER").replace("#N/A", "SILVER")
    return df

def enrich_from_compile(df: pd.DataFrame, compile_df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    # Frequency = count of past calls (โทรแล้ว) before today
    freq = compile_df.loc[compile_df["Call Status"] == "โทรแล้ว"].groupby("Username").size()
    out["Frequency"] = out["Username"].map(freq).fillna(0).astype(int)

    # Latest History & Date (ensure date-sorted)
    c = compile_df.copy()
    c["Date"] = pd.to_datetime(c["Date"], errors="coerce")
    c.sort_values("Date", inplace=True)
    latest = c.groupby("Username").tail(1).set_index("Username")[["History","History Date"]]
    out = out.join(latest, on="Username")

    # Recent Admin (last telesale)
    last_admin = c.loc[c["Call Status"] == "โทรแล้ว"].groupby("Username")["Telesale"].last()
    out["Recent Admin"] = out["Username"].map(last_admin)

    # Attempt Number = Frequency + 1
    out["Attempt Number"] = out["Frequency"].astype(int) + 1
    return out
