import os
from sqlalchemy import create_engine, text

def get_redeemed_usernames_today() -> set[str]:
    url = os.getenv("DATABASE_URL_GRAFANA")
    if not url or os.getenv("USE_REAL_DB","false").lower() not in ("1","true","yes"):
        return set()  # mock mode: no drops
    eng = create_engine(url)
    sql = text("SELECT DISTINCT username FROM redemption_logs WHERE DATE(redeem_time) = CURRENT_DATE")
    with eng.connect() as c:
        rows = c.execute(sql).fetchall()
    return {r[0] for r in rows if r[0]}
