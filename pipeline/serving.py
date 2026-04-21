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
                SELECT SUM(total_amount) FROM hour_denorm 
                WHERE date = %s AND hour = %s
            """, (d, h))


            res = cur.fetchone()
            return float(res[0]) if res else 0.0
    finally:
        conn.close()



def avg_revenue(h: int) -> float:
    import redis
    from pipeline import config

    if h < 0 or h > 23:
        raise ValueError(f"hour must be in range [0, 23], got {h}")

    r = redis.Redis(**config.redis_kwargs())
    doc = r.hgetall(f"hourly_revenue:{h}")
    if not doc:
        raise LookupError(f"missing redis document for key 'hourly_revenue:{h}'")

    # Prefer canonical field name; fallback for backward compatibility.
    raw = doc.get(b"revenue")
    if raw is None or raw == 0:
        raise KeyError(f"redis document 'hourly_revenue:{h}' does not have a revenue value")
    return float(raw)
