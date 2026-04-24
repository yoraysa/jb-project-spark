"""ETL pipeline — the write side of the lab.

Contract (called by the scheduler and the stage tests):

    run_etl(spark_session, input_file_dir, connections) -> None

`connections` is a dict with keys `paths`, `postgres`, `redis`, `minio`, `starrocks`.
"""
from __future__ import annotations
from pyspark.sql import SparkSession

from pipeline import config
from pipeline.etl._01_bronze import run_bronze_phase
from pipeline.etl._02_silver import run_silver_phase
from pipeline.etl._03_gold import run_gold_phase
from pipeline.etl._04_serving import run_serving_phase


def run_etl(
    spark: SparkSession,
    input_file_dir: str,
    connections: dict | None = None,
) -> None:
    """
    End-to-End ETL Pipeline orchestrator.
    """
    if connections is not None:
        config.connections.update(connections)

    # --- Phase 1: Landing -> Bronze (Ingestion) ---
    run_bronze_phase(input_file_dir)

    # --- Phase 2: Bronze -> Silver (Cleaning & Partitioning) ---
    run_silver_phase(spark)

    # --- Phase 3: Silver -> Gold (Aggregation & Denormalization) ---
    hourly_rows, gold_files = run_gold_phase(spark)

    # --- Phase 4: Gold -> Serving (Redis fan-out) ---
    run_serving_phase(hourly_rows=hourly_rows, gold_files=gold_files)



