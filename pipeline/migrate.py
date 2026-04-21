import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

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
    - StarRocks: gold aggregated tables + external bucket tables
    - MinIO: bucket cleanup/creation
    - Redis: cache clearing
    """
    # 1. Postgres - State Tracking
    conn_pg = psycopg2.connect(**pg_kwargs)
    with conn_pg.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS etl_file_status (
                file_path TEXT PRIMARY KEY,
                status VARCHAR(20) NOT NULL DEFAULT 'bronze',
                status_bronze_ts TIMESTAMP,
                status_silver_ts TIMESTAMP,
                status_gold_ts TIMESTAMP,
                status_done_ts TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_status ON etl_file_status(status);
            CREATE INDEX IF NOT EXISTS idx_etl_file_status_file_path ON etl_file_status(file_path);
        """)
    conn_pg.commit()
    conn_pg.close()

    # 2. StarRocks - Gold Tables & External Bucket Tables
    sr_cfg = config.starrocks_kwargs()
    m_cfg = config.minio_kwargs()
    
    conn_sr = mysql.connector.connect(**sr_cfg)
    try:
        with conn_sr.cursor() as cur:
            # Create database if not exists
            cur.execute("CREATE DATABASE IF NOT EXISTS nyc_taxi;")
            cur.execute("USE nyc_taxi;")


            # --- EXTERNAL BUCKET TABLES ---
            common_s3_props = f"""
                "aws.s3.endpoint" = "{m_cfg['endpoint']}",
                "aws.s3.access_key" = "{m_cfg['access_key']}",
                "aws.s3.secret_key" = "{m_cfg['secret_key']}",
                "aws.s3.region" = "us-east-1",
                "aws.s3.enable_path_style_access" = "true",
                "enable_recursive_listing" = "true",
                "format" = "parquet"
            """

            cur.execute(f"""
                CREATE EXTERNAL TABLE IF NOT EXISTS _bronze (
                    VendorID BIGINT,
                    tpep_pickup_datetime DATETIME,
                    tpep_dropoff_datetime DATETIME,
                    passenger_count DOUBLE,
                    trip_distance DOUBLE,
                    RatecodeID DOUBLE,
                    store_and_fwd_flag STRING,
                    PULocationID BIGINT,
                    DOLocationID BIGINT,
                    payment_type BIGINT,
                    fare_amount DOUBLE,
                    extra DOUBLE,
                    mta_tax DOUBLE,
                    tip_amount DOUBLE,
                    tolls_amount DOUBLE,
                    improvement_surcharge DOUBLE,
                    total_amount DOUBLE,
                    congestion_surcharge DOUBLE
                ) 
                ENGINE=file
                PROPERTIES (
                    "path" = "s3://bronze/trips/",
                    {common_s3_props}
                );
            """)

            cur.execute(f"""
                CREATE EXTERNAL TABLE IF NOT EXISTS _silver (
                    date DATE,
                    hour INT,
                    PULocationID INT,
                    total_amount DECIMAL(18, 2)
                ) 
                ENGINE=file
                PROPERTIES (
                    "path" = "s3://silver/trips/",
                    {common_s3_props}
                );
            """)


            # --- AGGREGATED GOLD TABLES ---
            
            # Table: hourly_revenue (Summed hourly revenue across all days)
            # Uses PRIMARY KEY to support Spark-driven sum + upsert logic.
            cur.execute("""
                CREATE TABLE IF NOT EXISTS hourly_revenue (
                    hour INT NOT NULL,
                    revenue DECIMAL(18, 2) NOT NULL
                ) PRIMARY KEY (hour)
                DISTRIBUTED BY HASH(hour) BUCKETS 4
                PROPERTIES ("replication_num" = "1");
            """)
            
            # Table: hour_denorm (By Zone and Hour)
            # Uses AGGREGATE KEY to automatically handle multi-file contributions.
            cur.execute("""
                CREATE TABLE IF NOT EXISTS hour_denorm (
                    zone VARCHAR(255) NOT NULL,
                    date DATE NOT NULL,
                    hour INT NOT NULL,
                    total_amount DECIMAL(18, 2) SUM NOT NULL
                ) AGGREGATE KEY (zone, date, hour)
                DISTRIBUTED BY HASH(zone, date, hour) BUCKETS 4
                PROPERTIES ("replication_num" = "1");
            """)


        conn_sr.commit()
    finally:
        conn_sr.close()

    # 3. MinIO - Datalake Buckets
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
            s3.meta.client.head_bucket(Bucket=b)
        except:
            s3.create_bucket(Bucket=b)

    # 4. Redis - Cache Clearing + Serving collection initialization
    r_cfg = config.redis_kwargs()
    r = redis.Redis(**r_cfg)
    r.flushdb()
    pipe = r.pipeline()
    for hour in range(24):
        pipe.hset(
            f"hourly_revenue:{hour}",
            mapping={"revenue": 0.0},
        )
    pipe.execute()
    print("✅ Migration complete: Postgres, StarRocks, MinIO, and Redis initialized.")

if __name__ == "__main__":
    migrate(config.postgres_kwargs())
