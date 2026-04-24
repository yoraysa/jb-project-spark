"""spark-submit entrypoint. Instructor-owned.

Builds the SparkSession, assembles the `connections` dict from environment
config, and calls `pipeline.etl.run_etl`. No business logic lives here.

    spark-submit --master spark://spark-master:7077 \\
        /home/jovyan/work/jobs/run_etl_job.py
"""
from __future__ import annotations

import os
import sys

# Ensure /home/jovyan/work is on sys.path so `pipeline` imports work under spark-submit.
sys.path.insert(0, "/home/jovyan/work")

from pyspark.sql import SparkSession  # noqa: E402

from pipeline import config  # noqa: E402
from pipeline.etl import run_etl  # noqa: E402






def main() -> None:
    spark = config.spark_session("run-etl")
    
    # Configure S3/Hadoop properties
    m_cfg = config.minio_kwargs()
    spark.conf.set("fs.s3a.endpoint", m_cfg['endpoint'])
    spark.conf.set("fs.s3a.access.key", m_cfg['access_key'])
    spark.conf.set("fs.s3a.secret.key", m_cfg['secret_key'])
    spark.conf.set("fs.s3a.path.style.access", "true")
    spark.conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark.conf.set("fs.s3a.connection.ssl.enabled", "false")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    landing = os.environ.get("LANDING_DIR", config.paths().landing)
    run_etl(spark, landing)

    spark.stop()


if __name__ == "__main__":
    main()
