import psycopg2
import mysql.connector
import boto3
import redis
from botocore.client import Config
from pipeline import config

def migrate(pg_kwargs: dict) -> None:
    """
    Idempotent initialization of the pipeline environment:
    - Postgres: etl_file_status table
    - StarRocks: gold aggregated tables
    - MinIO: bucket cleanup/creation
    - Redis: cache clearing
    """
    # 1. Postgres - State Tracking
    conn_pg = psycopg2.connect(**pg_kwargs)
    with conn_pg.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS etl_file_status (
                file_path TEXT PRIMARY KEY,
                status VARCHAR(20) NOT NULL DEFAULT 'new',
                etl_attempts INT DEFAULT 0,
                status_new_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status_bronze_ts TIMESTAMP,
                status_silver_ts TIMESTAMP,
                status_gold_ts TIMESTAMP,
                status_done_ts TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_status ON etl_file_status(status);
        """)
    conn_pg.commit()
    conn_pg.close()

    # 2. StarRocks - Gold Tables
    sr_cfg = config.starrocks_kwargs()
    conn_sr = mysql.connector.connect(**sr_cfg)
    with conn_sr.cursor() as cur:
        # Create database if not exists
        cur.execute("CREATE DATABASE IF NOT EXISTS taxi_gold;")
        cur.execute("USE taxi_gold;")
        
        # Table: datehour_agg
        cur.execute("""
            CREATE TABLE IF NOT EXISTS datehour_agg (
                date DATE NOT NULL,
                hour INT NOT NULL,
                revenue DECIMAL(18, 2) NOT NULL
            ) PRIMARY KEY (date, hour)
            DISTRIBUTED BY HASH(date, hour) BUCKETS 4
            PROPERTIES ("replication_num" = "1");
        """)
        
        # Table: zonehour_agg
        cur.execute("""
            CREATE TABLE IF NOT EXISTS zonehour_agg (
                zone VARCHAR(255) NOT NULL,
                date DATE NOT NULL,
                hour INT NOT NULL,
                total_amount DECIMAL(18, 2) NOT NULL
            ) PRIMARY KEY (zone, date, hour)
            DISTRIBUTED BY HASH(zone, date, hour) BUCKETS 4
            PROPERTIES ("replication_num" = "1");
        """)
    conn_sr.commit()
    conn_sr.close()

    # 3. MinIO - Datalake Buckets
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
        bucket = s3.Bucket(b)
        if bucket.creation_date:
            # For reset purposes, we could clear the bucket here if needed
            # For now, we'll just ensure they exist (minio-init handles this too)
            pass
        else:
            s3.create_bucket(Bucket=b)

    # 4. Redis - Cache Clearing
    r_cfg = config.redis_kwargs()
    r = redis.Redis(**r_cfg)
    r.flushdb()
    print("✅ Migration complete: Postgres, StarRocks, MinIO, and Redis initialized.")

if __name__ == "__main__":
    # Allow running directly for testing
    migrate(config.postgres_kwargs())
