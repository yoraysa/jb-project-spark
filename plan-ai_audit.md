# AI Audit: Spark Pipeline Plan

## 🎯 Summary
Your plan is ambitious and adopts modern Data Engineering patterns (Lakehouse/OLAP). However, for a 2-minute SLA and the data scale provided (~2M rows/year), some choices introduce **unnecessary complexity** and **bottlenecks** that might cause your pipeline to fail in a production-like simulation.

---

## ✅ Good Approaches

- **StarRocks for Analyst Store**: Excellent choice for <1s queries on aggregated data. It’s significantly faster than Postgres for OLAP workloads.
- **Redis for Backend Serving**: Using Redis for the `avg_revenue(h)` query is the right pattern for millisecond-budget, high-concurrency requests.
- **Pydantic Validation**: Great for ensuring data quality at the ingestion gate.
- **Watchdog listener**: Allows for immediate reaction to new files rather than waiting for a 1-minute cron tick.

---

## ❌ Risks & Bad Ideas

### 1. The Sequential Bottleneck (Critical)
> *Plan: "one file at a time... each file n runs... and only then file n+1 pipeline executes"*
- **Risk**: A file arrives every **10 seconds**. Spark startup and MinIO I/O overhead often take >10 seconds per job.
- **Result**: Your queue will grow indefinitely. You will fail the 2-minute SLA within the first hour of operation.
- **Advice**: Process files in **batches**. Your `run_etl` should pick up *all* pending files and process them in a single Spark job.

### 2. State Management in Iceberg/StarRocks
> *Plan: Using `etl_file_status` table in StarRocks/Iceberg.*
- **Risk**: Iceberg is a "big data" format. Updating a single row's status frequently (every 10s) creates a massive metadata overhead (too many snapshots/manifests). 
- **Advice**: **Keep Postgres**. Use it specifically for the `etl_file_status` table. Relational DBs are designed for these small, high-frequency "transactional" updates. Use StarRocks only for the actual analytical data.

### 3. StarRocks + Iceberg Overkill
- **Risk**: Using StarRocks *on top* of Iceberg (external tables) is slower than StarRocks' native storage.
- **Advice**: For 2M rows, just use **StarRocks native tables**. It handles the data lakehouse "feel" perfectly without the extra latency of the Iceberg metadata layer on MinIO.

### 4. Watchdog + Polling Conflict
- **Risk**: If both are active, you might trigger two Spark jobs for the same file, leading to race conditions or duplicate data.
- **Advice**: Use the Watchdog to *trigger* a queue, but ensure the `etl_process` is idempotent (Stage 3 requirement).

---

## 🛠 Revised Recommendations

1.  **Batch Processing**: Change the logic to "Process all files in `/landing` that are not yet marked as `bronze` in Postgres."
2.  **State DB**: Use **Postgres** for tracking `etl_file_status`. It is faster and more reliable for this specific task.
3.  **Storage**: Use StarRocks **Internal Tables** for the Gold layer. Skip Iceberg unless you specifically want to practice Iceberg partitioning (which is overkill for this scale).
4.  **SLA**: To hit < 2mins, avoid starting a new Spark context for every single file. Keep a persistent Spark session or process files in large enough batches.
