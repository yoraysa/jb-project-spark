import psycopg2
from pyspark.sql import SparkSession
from pipeline import config
from pipeline.etl import utils

def run_silver_phase(spark: SparkSession) -> None:
    """Bronze -> Silver (Cleaning & Partitioning)"""
    connections = config.connections
    if not connections:
        raise RuntimeError("Global connections configuration not found.")

    pg_cfg = connections['postgres']
    
    conn = psycopg2.connect(**pg_cfg)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT file_path FROM etl_file_status WHERE status = 'bronze'")
            bronze_files = [r[0] for r in cur.fetchall()]
    finally:
        conn.close()

    if bronze_files:
        print(f"[ETL] Silver Stage: bulk processing {len(bronze_files)} files...")
        try:
            # Construct S3 paths for the specific files
            bronze_paths = [f"s3a://bronze/trips/date={f.replace('.parquet','')}/{f}" for f in bronze_files]
            df = spark.read.parquet(*bronze_paths)
            
            df.createOrReplaceTempView("bronze_trips")
            df_silver = spark.sql("""
                SELECT 
                    TO_DATE(CAST(tpep_pickup_datetime AS TIMESTAMP)) as date,
                    HOUR(CAST(tpep_pickup_datetime AS TIMESTAMP)) as hour,
                    CAST(PULocationID AS INT) as PULocationID,
                    CAST(total_amount AS DECIMAL(18,2)) as total_amount,
                    TO_DATE(CAST(tpep_pickup_datetime AS TIMESTAMP)) as p_date
                FROM bronze_trips
                WHERE TO_DATE(CAST(tpep_pickup_datetime AS TIMESTAMP)) IS NOT NULL
            """)

            df_silver.write.mode("overwrite").partitionBy("p_date").parquet("s3a://silver/trips")
            
            utils.update_status(
                pg_cfg,
                bronze_files,
                'silver',
                'status_silver_ts',
            )
            print(f"[ETL] Silver Stage complete.")
        except Exception as e:
            print(f"[ETL] Silver Stage ERROR: {e}")
            import traceback
            traceback.print_exc()
            raise
