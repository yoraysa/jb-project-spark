import redis

def migrate_redis(r_cfg: dict) -> None:
    """Redis - Cache Clearing + Serving collection initialization"""
    r = redis.Redis(**r_cfg)
    r.flushdb()
    pipe = r.pipeline()
    for hour in range(24):
        pipe.hset(
            f"hourly_revenue:{hour}",
            mapping={"revenue": 0.0},
        )
    pipe.execute()
