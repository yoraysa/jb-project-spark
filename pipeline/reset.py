"""Reset utility — clears all data from all stores (Postgres, Redis, StarRocks, MinIO).
Follows a 'truncate only' policy: we clear the data without dropping databases or tables
to maintain schema integrity and connection stability.
"""
from __future__ import annotations

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import psycopg2
import redis
import mysql.connector
import boto3
from botocore.client import Config
from pipeline import config


def reset_all(pg_kwargs: dict, redis_kwargs: dict) -> None:
    """Entry point to clear all database and object stores."""
    _reset_postgres(pg_kwargs)
    _reset_redis(redis_kwargs)
    _reset_starrocks()
    _reset_minio()


def _reset_postgres(pg_kwargs: dict) -> None:
    """Truncates all public tables in the Postgres database."""
    with psycopg2.connect(**pg_kwargs) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
            tables = [row[0] for row in cur.fetchall()]
            for table in tables:
                cur.execute(f'TRUNCATE TABLE "{table}" RESTART IDENTITY CASCADE')


def _reset_redis(redis_kwargs: dict) -> None:
    """Flushes the current Redis database."""
    r = redis.Redis(**redis_kwargs)
    r.flushdb()


def _reset_starrocks() -> None:
    """Truncates all native tables in the StarRocks nyc_taxi database."""
    sr_cfg = config.starrocks_kwargs()
    try:
        conn = mysql.connector.connect(**sr_cfg)
        with conn.cursor() as cur:
            cur.execute("USE nyc_taxi")
            cur.execute("SHOW TABLES")
            tables = [row[0] for row in cur.fetchall()]
            for table in tables:
                try:
                    # Truncate only works on native tables, will fail on EXTERNAL tables
                    cur.execute(f"TRUNCATE TABLE {table}")
                except mysql.connector.Error:
                    # Likely an external table or view; skip
                    pass
        conn.close()
    except mysql.connector.Error:
        pass


def _reset_minio() -> None:
    """Deletes all objects from the bronze, silver, and gold buckets in MinIO."""
    m_cfg = config.minio_kwargs()
    s3 = boto3.resource('s3',
        endpoint_url=m_cfg['endpoint'],
        aws_access_key_id=m_cfg['access_key'],
        aws_secret_access_key=m_cfg['secret_key'],
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )
    
    buckets = ['bronze', 'silver', 'gold']
    for bucket_name in buckets:
        try:
            bucket = s3.Bucket(bucket_name)
            # Delete all objects in bucket
            bucket.objects.all().delete()
        except Exception:
            # Bucket might not exist; skip safely
            pass

if __name__ == "__main__":
    reset_all(config.postgres_kwargs(), config.redis_kwargs())
    print("[reset] all stores wiped")
