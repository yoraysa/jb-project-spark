# Data Pipeline Plan: NYC Taxi Trips

## 🏗️ Architecture Stack
- **Storage**: MinIO (Datalake - Bronze/Silver/Gold)
- **Database (OLAP)**: StarRocks (Internal Primary Key tables)
- **Serving (Cache)**: Redis
- **Processing**: Apache Spark (Persistent Session - FAIR Scheduler)
- **Metadata/State**: Postgres (`etl_file_status`)
- **Validation**: Pydantic

## 🖥️ Environment & Data Paths (Container context)
- **Container Context**: This pipeline runs inside **Docker**. The primary processing environment is the `spark-jupyter` container.
- **Operating System**: The container uses a Linux-based environment (Ubuntu) where the default user is named `jovyan`.
- **Path Mapping**:
  - The local code (Python files) is mounted from this host machine to the container.
  - The **Landing Zone (`/landing/`)** resides in the container at `/home/jovyan/work/data/landing/`. 
  - **Storage volumes** (like `spark_data`) are managed by Docker and are isolated from this host's WSL `/home/` directory to ensure consistent performance across different PCs.
- **Browser Access**: You can browse the container's filesystem and data folders directly at [http://localhost:8888/tree/data](http://localhost:8888/tree/data).

---

## 📊 Analytical Requirements
1. **Revenue by Zone/Date/Hour**:
   - Query: `total_revenue(date, hour, zone)`
   - Storage: StarRocks (Aggregated Gold Table)
   - Budget: < 1 second
2. **Average Revenue per Hour (Historical)**:
   - Query: `avg_revenue(hour)`
   - Storage: Redis (Pre-aggregated)
   - Budget: < 50ms

---

## 🛠️ Data Layers
- **Landing (Local)**: Incoming parquet files every 10s.
- **Bronze (MinIO)**: Raw files moved from Landing.
- **Silver (MinIO)**: Cleaned, schema-enforced, and transformed data.
- **Gold (MinIO + StarRocks)**: Aggregated datasets for business questions.

---

## 🔄 State Tracking (`etl_file_status` - Postgres)
| Column | Description |
| :--- | :--- |
| `file_path` | Unique file identifier (Index) |
| `status` | `new` → `bronze` → `silver` → `gold` |
| `attempts` | Restart count |
| `ts_new` / `ts_bronze` / etc | Timestamps for each stage |

---

## ⚙️ Ingestion & ETL Flow

### 1. Landing to Bronze (Decoupled & Parallel)
- **Watchdog**: Scans `/landing/` for `.parquet` files.
- **Parallelism**: Process up to **10 files concurrently** to minimize ingestion lag.
- **Loading State**: Instantly rename local file to `_loading-<name>.parquet`.
- **Action**: Upload to MinIO Bronze.
- **Cleanup**: **Delete local file** after successful Bronze upload.
- **State**: Create record in Postgres with status `new` → `bronze`.
- **Fail-safe (Polling)**: Every 10 minutes, scan `/landing/` for files the watchdog missed; process them in parallel batches.

### 2. Bronze to Gold (Background Processing)
- **Queue**: Process files in `bronze` status.
- **Spark Job (Persistent Session)**:
  - **Scheduler**: `FAIR` mode for multi-file processing efficiency.
  - **Hardware Tuing** (i5-12450H):
    ```python
    spark = SparkSession.builder \
        .config("spark.executor.cores", "2") \
        .config("spark.scheduler.mode", "FAIR") \
        .getOrCreate()
    ```
  - **Silver Stage**: Transformation, date/hour extraction, zone lookup.
  - **Gold Stage**: Aggregate into StarRocks Primary Key tables.
  - **Redis Sync**: Update `revenue_by_hour` in Redis.
- **State**: Update Postgres status to `silver` → `gold`.

---

## 🧹 Initialization (`migrate.py`)
- Drop and recreate StarRocks Primary Key tables.
- Reset Postgres `etl_file_status`.
- Clear Redis and MinIO Buckets.

---

## 🚑 Error Handling
- **Scan**: Every 5 minutes, check for stuck files (status != `gold`).
- **Reset**: If a file is stuck > 1 minute, increment `attempts` and reset status to `new` (re-triggering ingestion).
