"""ETL pipeline — the write side of the lab.

Contract (called by the scheduler and the stage tests):

    run_etl(spark_session, input_file_dir, connections) -> None

`connections` is a dict with keys `paths`, `postgres`, `redis`, `minio`, `starrocks`.
"""
from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import boto3
import psycopg2
import mysql.connector
import redis
from botocore.client import Config
from pyspark.sql import SparkSession, functions as F


def run_elt(landing_dir: str, connections: dict) -> None:
    """
    Extract from Landing, Load to Bronze (MinIO), and update metadata (Postgres).
    Follows Landing -> MinIO -> Bronze -> Archive lifecycle.
    """
    paths = connections['paths']
    pg_cfg = connections['postgres']
    from pipeline import config
    m_cfg = connections.get('minio', config.minio_kwargs())

    landing_path = Path(landing_dir)
    archive_dir = paths["archive"] if isinstance(paths, dict) else paths.archive
    archive_path = Path(archive_dir)

    # Initialize S3 client
    s3 = boto3.client('s3',
        endpoint_url=m_cfg['endpoint'],
        aws_access_key_id=m_cfg['access_key'],
        aws_secret_access_key=m_cfg['secret_key'],
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )

    # Find all parquet files in landing
    files = list(landing_path.glob("*.parquet"))
    if not files:
        return

    print(f"[ELT] Found {len(files)} files in landing. Processing...")

    # Load known files once so duplicate checks are fast and consistent.
    file_names = [f.name for f in files]
    conn = psycopg2.connect(**pg_cfg)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT file_path FROM etl_file_status WHERE file_path = ANY(%s)",
                (file_names,),
            )
            known_files = {r[0] for r in cur.fetchall()}
    finally:
        conn.close()

    def process_file(file_path: Path):
        try:
            # File already tracked in Postgres -> already loaded to bronze in the past.
            if file_path.name in known_files:
                archive_dest = archive_path / file_path.name
                if archive_dest.exists():
                    file_path.unlink()
                else:
                    file_path.rename(archive_dest)
                return

            # 1. Archive Check
            if (archive_path / file_path.name).exists():
                file_path.unlink()
                return

            # 2. Upload to MinIO Bronze bucket
            file_date = file_path.stem  # e.g., '2019-01-01' from '2019-01-01.parquet'
            s3_key = f"trips/date={file_date}/{file_path.name}"
            s3.upload_file(str(file_path), 'bronze', s3_key)
            
            # 3. Update Postgres status
            conn = psycopg2.connect(**pg_cfg)
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO etl_file_status (file_path, status, status_bronze_ts)
                    VALUES (%s, 'bronze', clock_timestamp())
                    ON CONFLICT (file_path) DO UPDATE SET
                        status = 'bronze',
                        status_bronze_ts = clock_timestamp();
                """, (file_path.name,))
            conn.commit()
            conn.close()
            
            # 4. Move from landing to archive
            archive_dest = archive_path / file_path.name
            file_path.rename(archive_dest)
            
        except Exception as e:
            print(f"[ELT] Error processing {file_path.name}: {e}")

    # We use a ThreadPoolExecutor to parallelize the I/O-bound ingestion phase.
    # Each file in 'landing' is uploaded to MinIO and its metadata is updated in Postgres.
    # Using multiple workers (10) significantly speeds up Phase 1 when handling many small files.
    with ThreadPoolExecutor(max_workers=10) as executor:
        # executor.map returns a lazy iterator; we wrap it in list() to force the 
        # execution of all tasks immediately.
        list(executor.map(process_file, files))

    print(f"[ELT] Phase 1 (Ingestion) finished.")


def _update_status(
    pg_cfg: dict,
    file_paths: list[str],
    status: str,
    ts_col: str,
):
    """Helper to bulk update file status in Postgres."""
    if not file_paths:
        return
    conn = psycopg2.connect(**pg_cfg)
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                UPDATE etl_file_status SET
                    status = %s,
                    {ts_col} = clock_timestamp()
                WHERE file_path = ANY(%s);
            """, (status, file_paths))
        conn.commit()
    finally:
        conn.close()


def _write_gold_hourly(sr_cfg: dict, rows: list[tuple[int, float]]) -> None:
    """Write hourly aggregates to StarRocks in one driver-side transaction."""
    if not rows:
        return
    chunk_size = 1000
    conn = mysql.connector.connect(**sr_cfg, database="nyc_taxi")
    try:
        with conn.cursor() as cur:
            for i in range(0, len(rows), chunk_size):
                cur.executemany(
                    """
                    INSERT INTO hourly_revenue (hour, revenue)
                    VALUES (%s, %s);
                    """,
                    rows[i:i + chunk_size],
                )
        conn.commit()
    finally:
        conn.close()


def _write_gold_denorm(sr_cfg: dict, rows: list[tuple[str, str, int, float]]) -> None:
    """Append zone/date/hour aggregates to StarRocks in one driver-side transaction."""
    if not rows:
        return
    chunk_size = 1000
    conn = mysql.connector.connect(**sr_cfg, database="nyc_taxi")
    try:
        with conn.cursor() as cur:
            for i in range(0, len(rows), chunk_size):
                cur.executemany(
                    """
                    INSERT INTO hour_denorm (zone, date, hour, total_amount)
                    VALUES (%s, %s, %s, %s);
                    """,
                    rows[i:i + chunk_size],
                )
        conn.commit()
    finally:
        conn.close()


def _sync_hourly_revenue_to_redis(r_cfg: dict, rows: list[tuple[int, float]]) -> None:
    """
    Fully refresh serving cache from gold hourly_revenue.
    Stores 24 documents as Redis hashes: hourly_revenue:{hour}.
    """
    r = redis.Redis(**r_cfg)
    pipe = r.pipeline()
    for hour in range(24):
        pipe.delete(f"hourly_revenue:{hour}")

    revenue_by_hour = {hour: revenue for hour, revenue in rows}
    for hour in range(24):
        revenue = float(revenue_by_hour.get(hour, 0.0))
        # Keep both names to be backward/forward compatible with callers.
        pipe.hset(
            f"hourly_revenue:{hour}",
            mapping={"revenue": revenue},
        )
    pipe.execute()



def run_etl(
    spark: SparkSession,
    input_file_dir: str,
    connections: dict,
) -> None:
    """
    End-to-End ETL Pipeline.
    """
    from pipeline import config

    pg_cfg = connections['postgres']
    m_cfg = connections.get('minio', config.minio_kwargs())

    # --- Phase 0: Spark Configuration ---
    spark.conf.set("fs.s3a.endpoint", m_cfg['endpoint'])
    spark.conf.set("fs.s3a.access.key", m_cfg['access_key'])
    spark.conf.set("fs.s3a.secret.key", m_cfg['secret_key'])
    spark.conf.set("fs.s3a.path.style.access", "true")
    spark.conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark.conf.set("fs.s3a.connection.ssl.enabled", "false")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    # --- Phase 1: Landing -> Bronze (ELT) ---
    run_elt(input_file_dir, connections)

    # --- Phase 2: Bronze -> Silver (Cleaning & Partitioning) ---
    conn = psycopg2.connect(**pg_cfg)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT file_path FROM etl_file_status WHERE status = 'bronze'")
            files_to_silver = [r[0] for r in cur.fetchall()]
    finally:
        conn.close()

    if files_to_silver:
        print(f"[ETL] Silver Stage: bulk processing {len(files_to_silver)} files...")
        try:
            bronze_paths = [f"s3a://bronze/trips/date={f.replace('.parquet','')}/{f}" for f in files_to_silver]
            df = spark.read.parquet(*bronze_paths)
            
            df_silver = df.withColumn("pickup_ts", F.col("tpep_pickup_datetime").cast("timestamp")) \
                          .withColumn("date", F.to_date("pickup_ts")) \
                          .withColumn("hour", F.hour("pickup_ts")) \
                          .withColumn("total_amount", F.col("total_amount").cast("decimal(18,2)")) \
                          .withColumn("PULocationID", F.col("PULocationID").cast("int")) \
                          .filter("date IS NOT NULL") \
                          .select("date", "hour", "PULocationID", "total_amount") \
                          .withColumn("p_date", F.col("date"))

            df_silver.write.mode("overwrite").partitionBy("p_date").parquet("s3a://silver/trips")
            
            _update_status(
                pg_cfg,
                files_to_silver,
                'silver',
                'status_silver_ts',
            )
            print(f"[ETL] Silver Stage complete.")
        except Exception as e:
            print(f"[ETL] Silver Stage ERROR: {e}")
            import traceback
            traceback.print_exc()

    # --- Phase 3: Silver -> Gold (Aggregation & Denormalization) ---
    conn = psycopg2.connect(**pg_cfg)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT file_path FROM etl_file_status WHERE status = 'silver'")
            files_to_gold = [r[0] for r in cur.fetchall()]
    finally:
        conn.close()

    if files_to_gold:
        print(f"[ETL] Gold Stage: processing {len(files_to_gold)} files...")
        try:
            # For simplicity, we process the current silver batch by reading all silver/trips
            df = spark.read.parquet("s3a://silver/trips")
            sr_jdbc = connections.get('starrocks_jdbc', config.starrocks_jdbc())
            sr_cfg = connections.get('starrocks', config.starrocks_kwargs())
            
            # --- 3.1: hourly_revenue (Join & Sum in Spark) ---
            
            # Read existing table (Empty if first run)
            try:
                existing_hourly_df = spark.read.format("jdbc") \
                    .option("url", f"{sr_jdbc['url']}nyc_taxi") \
                    .option("dbtable", "nyc_taxi.hourly_revenue") \
                    .option("user", sr_jdbc['user']) \
                    .option("password", sr_jdbc['password']) \
                    .option("driver", sr_jdbc['driver']) \
                    .load()
                existing_hourly_df.cache()
                existing_hourly_df.count() # Force evaluation to avoid read/write deadlocks
            except:
                existing_hourly_df = spark.createDataFrame([], "hour INT, revenue DECIMAL(18,2)")

            # Aggregate current silver data by hour
            new_hourly_agg = df.groupBy("hour").agg(F.sum("total_amount").alias("new_revenue"))
            
            # Join and Sum
            final_hourly = existing_hourly_df.join(new_hourly_agg, "hour", "outer") \
                .select("hour", 
                        (F.coalesce(F.col("revenue"), F.lit(0)) + 
                         F.coalesce(F.col("new_revenue"), F.lit(0))).cast("decimal(18,2)").alias("revenue")) \
                .filter("hour IS NOT NULL")

            # Upsert to StarRocks from driver to avoid Spark task retry transaction storms.
            hourly_rows = [
                (int(r["hour"]), float(r["revenue"]))
                for r in final_hourly.select("hour", "revenue").collect()
            ]
            _write_gold_hourly(sr_cfg, hourly_rows)

            # --- 3.2: hour_denorm (Join with zones.csv) ---
            
            zones_df = spark.read.option("header", "true").csv("/home/jovyan/work/data/zones.csv") \
                        .select(F.col("LocationID").cast("int"), F.col("Zone"))
            
            df_denorm = df.join(zones_df, df.PULocationID == zones_df.LocationID, "left") \
                          .select(F.col("Zone").alias("zone"), "date", "hour", "total_amount") \
                          .fillna({"zone": "Unknown"})

            df_zh = df_denorm.groupBy("zone", "date", "hour") \
                             .agg(F.sum("total_amount").cast("decimal(18,2)").alias("total_amount")) \
                             .withColumn("p_date", F.col("date"))
            
            # Write to Gold Bucket (Partitioned)
            # coalesce(1) ensures one file per partition (date) to combat "tiny files" problem.
            df_zh.coalesce(1).write.mode("overwrite").partitionBy("p_date").parquet("s3a://gold/gold_hour_denorm")
            
            # Write to StarRocks from driver in one transaction.
            zh_rows = [
                (r["zone"], r["date"].isoformat(), int(r["hour"]), float(r["total_amount"]))
                for r in df_zh.drop("p_date").collect()
            ]
            _write_gold_denorm(sr_cfg, zh_rows)

            # Stage transition to gold is complete.
            _update_status(pg_cfg, files_to_gold, 'gold', 'status_gold_ts')

            # --- Phase 4: Gold -> Serving (Redis fan-out operations) ---
            r_cfg = connections.get('redis', config.redis_kwargs())
            _sync_hourly_revenue_to_redis(r_cfg, hourly_rows)

            # Mark done only after ALL gold fan-out operations complete successfully.
            _update_status(pg_cfg, files_to_gold, 'done', 'status_done_ts')
            print(f"[ETL] Gold Stage complete.")
        except Exception as e:
            print(f"[ETL] Gold Stage ERROR: {e}")
            import traceback
            traceback.print_exc()
            raise


if __name__ == "__main__":
    from pipeline import config
    conns = {
        'paths': config.paths(),
        'postgres': config.postgres_kwargs(),
        'minio': config.minio_kwargs(),
        'redis': config.redis_kwargs(),
        'starrocks': config.starrocks_kwargs(),
        'starrocks_jdbc': config.starrocks_jdbc()
    }
    run_elt(conns['paths'].landing, conns)
