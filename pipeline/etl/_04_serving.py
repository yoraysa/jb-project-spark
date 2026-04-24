import redis
from pipeline import config
from pipeline.etl import utils

def sync_hourly_revenue_to_redis(r_cfg: dict, rows: list[tuple[int, float]]) -> None:
    """Fully refresh serving cache from gold hourly_revenue."""
    if not rows:
        return
    r = redis.Redis(**r_cfg)
    pipe = r.pipeline()
    for hour in range(24):
        pipe.delete(f"hourly_revenue:{hour}")

    revenue_by_hour = {hour: revenue for hour, revenue in rows}
    for hour in range(24):
        revenue = float(revenue_by_hour.get(hour, 0.0))
        pipe.hset(
            f"hourly_revenue:{hour}",
            mapping={"revenue": revenue},
        )
    pipe.execute()

def run_serving_phase(hourly_rows: list[tuple[int, float]] | None = None, gold_files: list[str] | None = None) -> None:
    """Gold -> Serving (Redis fan-out)"""
    connections = config.connections
    if not connections:
        raise RuntimeError("Global connections configuration not found.")
    
    if not gold_files:
        return
    
    pg_cfg = connections['postgres']
    r_cfg = connections['redis']
    
    print(f"[ETL] Serving Stage: Syncing {len(hourly_rows)} hourly records to Redis...")
    sync_hourly_revenue_to_redis(r_cfg, hourly_rows)
    
    # Mark as 'done' only after serving sync is complete
    utils.update_status(pg_cfg, gold_files, 'done', 'status_done_ts')
    print(f"[ETL] Serving Stage complete.")
