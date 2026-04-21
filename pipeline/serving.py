"""Serving layer — the two read-side functions the tests and consumers call.

    total_revenue(d: str, h: int) -> float   # analyst query
    avg_revenue(h: int) -> float             # backend query

Both are expected to be fast — the stage-4 lesson spells out the budgets
and the split between the analyst store and the backend cache.

Config is read from `pipeline.config` so callers stay arg-less; tests set
env vars via fixtures.
"""
from __future__ import annotations


def total_revenue(d: str, h: int) -> float:
    import mysql.connector
    from pipeline import config
    sr_cfg = config.starrocks_kwargs()
    conn = mysql.connector.connect(**sr_cfg)
    try:
        with conn.cursor() as cur:
            cur.execute("USE nyc_taxi")
            cur.execute("""
                SELECT SUM(total_amount) FROM gold_hour_denorm 
                WHERE date = %s AND hour = %s
            """, (d, h))


            res = cur.fetchone()
            return float(res[0]) if res else 0.0
    finally:
        conn.close()



def avg_revenue(h: int) -> float:
    raise NotImplementedError("Stage 4: implement avg_revenue — see stages/04-backend-serving/")
