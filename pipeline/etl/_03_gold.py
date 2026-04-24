import psycopg2
import mysql.connector
from pyspark.sql import SparkSession
from pipeline.etl import utils
from pipeline import config

def write_gold_hourly(sr_cfg: dict, rows: list[tuple[int, float]]) -> None:
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

def write_gold_denorm(sr_cfg: dict, rows: list[tuple[str, str, int, float]]) -> None:
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

def run_gold_phase(spark: SparkSession) -> tuple[list[tuple[int, float]], list[str]]:
    """Silver -> Gold (Aggregation & Denormalization). Returns (hourly_rows, files_processed)"""
    connections = config.connections
    if not connections:
        raise RuntimeError("Global connections configuration not found.")

    pg_cfg = connections['postgres']
    
    # 1. Fetch files currently in 'silver' status.
    # This ensures we only process data that has been successfully cleaned.
    conn = psycopg2.connect(**pg_cfg)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT file_path FROM etl_file_status WHERE status = 'silver'")
            silver_files = [r[0] for r in cur.fetchall()]
    finally:
        conn.close()

    if not silver_files:
        return [], []

    print(f"[ETL] Gold Stage: processing {len(silver_files)} files...")
    try:
        # Load all silver data. Spark handles the schema from the Parquet metadata.
        df = spark.read.parquet("s3a://silver/trips")
        sr_jdbc = connections.get('starrocks_jdbc', config.starrocks_jdbc())
        sr_cfg = connections.get('starrocks', config.starrocks_kwargs())
        
        # --- 3.1: hourly_revenue (Incremental Aggregation) ---
        # We read existing totals from StarRocks to perform an incremental update.
        try:
            existing_hourly_df = spark.read.format("jdbc") \
                .option("url", f"{sr_jdbc['url']}nyc_taxi") \
                .option("dbtable", "nyc_taxi.hourly_revenue") \
                .option("user", sr_jdbc['user']) \
                .option("password", sr_jdbc['password']) \
                .option("driver", sr_jdbc['driver']) \
                .load()
            existing_hourly_df.cache()
            existing_hourly_df.count()
        except Exception as e:
            raise RuntimeError(f"StarRocks table 'hourly_revenue' not found. Please run migration first. Error: {e}")

        df.createOrReplaceTempView("silver_trips")
        new_hourly_agg = spark.sql("SELECT hour, SUM(total_amount) as new_revenue FROM silver_trips GROUP BY hour")
        
        existing_hourly_df.createOrReplaceTempView("existing_hourly")
        new_hourly_agg.createOrReplaceTempView("new_hourly")
        # Perform a FULL OUTER JOIN between existing data and new batch data.
        # This allows us to update hours that already have data and add new ones.
        final_hourly = spark.sql("""
            SELECT 
                COALESCE(e.hour, n.hour) as hour,
                CAST(COALESCE(e.revenue, 0) + COALESCE(n.new_revenue, 0) AS DECIMAL(18,2)) as revenue
            FROM existing_hourly e
            FULL OUTER JOIN new_hourly n ON e.hour = n.hour
            WHERE COALESCE(e.hour, n.hour) IS NOT NULL
        """)

        hourly_rows = [
            (int(r["hour"]), float(r["revenue"]))
            for r in final_hourly.select("hour", "revenue").collect()
        ]
        write_gold_hourly(sr_cfg, hourly_rows)

        # --- 3.2: hour_denorm (Denormalization & Enrichment) ---
        # Enrich taxi data with human-readable Zone names from a dimension table (CSV).
        zones_df = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/jovyan/work/data/zones.csv")
        zones_df.createOrReplaceTempView("zones")
        
        df_denorm = spark.sql("""
            SELECT 
                z.Zone as zone,
                s.date,
                s.hour,
                s.total_amount
            FROM silver_trips s
            JOIN zones z ON s.PULocationID = z.LocationID
        """)
        df_denorm.createOrReplaceTempView("denorm_trips")

        df_zh = spark.sql("""
            SELECT 
                zone,
                date,
                hour,
                SUM(total_amount) as total_amount,
                date as p_date
            FROM denorm_trips
            GROUP BY zone, date, hour
        """)
        
        # Write the denormalized data to the Gold S3 bucket.
        # We partition by date (p_date) to allow for efficient downstream queries.
        df_zh.coalesce(1).write.mode("overwrite").partitionBy("p_date").parquet("s3a://gold/gold_hour_denorm")
        
        zh_rows = [
            (r["zone"], r["date"].isoformat(), int(r["hour"]), float(r["total_amount"]))
            for r in df_zh.drop("p_date").collect()
        ]
        write_gold_denorm(sr_cfg, zh_rows)

        # Finalize the batch: Update Postgres status to 'gold' for all processed files.
        utils.update_status(pg_cfg, silver_files, 'gold', 'status_gold_ts')
        print(f"[ETL] Gold Stage complete.")
        
        return hourly_rows, silver_files

    except Exception as e:
        print(f"[ETL] Gold Stage ERROR: {e}")
        import traceback
        traceback.print_exc()
        raise
