from __future__ import annotations

import psycopg2
import redis
import mysql.connector
import boto3
from botocore.client import Config
from pipeline import config

def reset_all(pg_kwargs: dict, redis_kwargs: dict) -> None:
    # 1. Postgres
    with psycopg2.connect(**pg_kwargs) as conn, conn.cursor() as cur:
        cur.execute("DROP SCHEMA IF EXISTS public CASCADE")
        cur.execute("CREATE SCHEMA public")
        cur.execute("GRANT ALL ON SCHEMA public TO public")
    
    # 2. Redis
    redis.Redis(**redis_kwargs).flushdb()

    # 3. StarRocks
    sr_cfg = config.starrocks_kwargs()
    with mysql.connector.connect(**sr_cfg) as conn, conn.cursor() as cur:
        cur.execute("DROP DATABASE IF EXISTS taxi_gold")
    
    # 4. MinIO
    _clear_minio()


def clear_data(pg_kwargs: dict, redis_kwargs: dict) -> None:
    # 1. Postgres
    with psycopg2.connect(**pg_kwargs) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT tablename FROM pg_tables WHERE schemaname = 'public'"
        )
        tables = [row[0] for row in cur.fetchall()]
        for t in tables:
            cur.execute(f'TRUNCATE TABLE "{t}" RESTART IDENTITY CASCADE')
    
    # 2. Redis
    redis.Redis(**redis_kwargs).flushdb()

    # 3. StarRocks
    sr_cfg = config.starrocks_kwargs()
    try:
        with mysql.connector.connect(**sr_cfg) as conn, conn.cursor() as cur:
            cur.execute("USE taxi_gold")
            cur.execute("TRUNCATE TABLE datehour_agg")
            cur.execute("TRUNCATE TABLE zonehour_agg")
    except mysql.connector.Error:
        # Database or tables might not exist yet if migrate wasn't run
        pass

    # 4. MinIO
    _clear_minio()


def _clear_minio() -> None:
    m_cfg = config.minio_kwargs()
    s3 = boto3.resource('s3',
        endpoint_url=m_cfg['endpoint'],
        aws_access_key_id=m_cfg['access_key'],
        aws_secret_access_key=m_cfg['secret_key'],
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )
    buckets = ['bronze', 'silver', 'gold']
    for b in buckets:
        try:
            bucket = s3.Bucket(b)
            bucket.objects.all().delete()
        except:
            pass
