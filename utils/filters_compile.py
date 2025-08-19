import pandas as pd
from datetime import datetime

UNREACHABLE_STATUSES = ["ไม่รับสาย","ติดต่อไม่ได้","กดตัดสาย","รับสายไม่สะดวกคุย"]

def _month_slice(df: pd.DataFrame, today: datetime) -> pd.DataFrame:
    if df.empty: return df
    start = today.replace(day=1)
    m = (pd.to_datetime(df["Date"], errors="coerce") >= start) & (pd.to_datetime(df["Date"], errors="coerce") <= today)
    return df.loc[m]

def block_unreachable_repeat_this_month(compile_df: pd.DataFrame, today: datetime, min_cnt: int = 2) -> set[str]:
    mdf = _month_slice(compile_df, today)
    if mdf.empty: return set()
    s = (mdf["Answer Status"].isin(UNREACHABLE_STATUSES)).groupby(mdf["Username"]).sum()
    return set(s[s >= min_cnt].index.astype(str))

def block_answered_this_month(compile_df: pd.DataFrame, today: datetime) -> set[str]:
    mdf = _month_slice(compile_df, today)
    if mdf.empty: return set()
    u = mdf.loc[mdf["Answer Status"] == "รับสาย", "Username"].astype(str).unique()
    return set(u)

def block_invalid_number_ever(compile_df: pd.DataFrame) -> set[str]:
    if compile_df.empty: return set()
    u = compile_df.loc[compile_df["Result"] == "เบอร์เสีย", "Username"].astype(str).unique()
    return set(u)

def block_not_interested_this_month(compile_df: pd.DataFrame, today: datetime) -> set[str]:
    mdf = _month_slice(compile_df, today)
    if mdf.empty: return set()
    u = mdf.loc[mdf["Result"] == "ไม่สนใจ", "Username"].astype(str).unique()
    return set(u)

def block_not_owner_hard(compile_df: pd.DataFrame) -> set[str]:
    if compile_df.empty: return set()
    u = compile_df.loc[compile_df["Result"] == "ไม่ใช่เจ้าของไอดี", "Username"].astype(str).unique()
    return set(u)
