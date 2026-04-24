import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from pipeline import config
from pipeline.migrate._01_postgres import migrate_postgres
from pipeline.migrate._02_starrocks import migrate_starrocks
from pipeline.migrate._03_minio import migrate_minio
from pipeline.migrate._04_redis import migrate_redis

def migrate(pg_kwargs: dict) -> None:
    """
    Idempotent initialization of the pipeline environment.
    Orchestrates specialized migration modules.
    """
    sr_cfg = config.starrocks_kwargs()
    m_cfg = config.minio_kwargs()
    r_cfg = config.redis_kwargs()

    # 1. Postgres - State Tracking
    migrate_postgres(pg_kwargs)

    # 2. StarRocks - Gold Tables & External Bucket Tables
    migrate_starrocks(sr_cfg, m_cfg)

    # 3. MinIO - Datalake Buckets
    migrate_minio(m_cfg)

    # 4. Redis - Cache Clearing
    migrate_redis(r_cfg)

    print("✅ Migration complete: Postgres, StarRocks, MinIO, and Redis initialized.")

if __name__ == "__main__":
    migrate(config.postgres_kwargs())
