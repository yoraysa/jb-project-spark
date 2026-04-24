import mysql.connector

def migrate_starrocks(sr_cfg: dict, m_cfg: dict) -> None:
    """StarRocks - Gold Tables & External Bucket Tables"""
    conn_sr = mysql.connector.connect(**sr_cfg)
    try:
        with conn_sr.cursor() as cur:
            cur.execute("CREATE DATABASE IF NOT EXISTS nyc_taxi;")
            cur.execute("USE nyc_taxi;")

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

            cur.execute("""
                CREATE TABLE IF NOT EXISTS hourly_revenue (
                    hour INT NOT NULL,
                    revenue DECIMAL(18, 2) NOT NULL
                ) PRIMARY KEY (hour)
                DISTRIBUTED BY HASH(hour) BUCKETS 4
                PROPERTIES ("replication_num" = "1");
            """)
            
            cur.execute("""
                CREATE TABLE IF NOT EXISTS hour_denorm (
                    zone VARCHAR(255),
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
