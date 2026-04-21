# Data Pipeline Plan: NYC Taxi Trips

## 🏗️ Architecture Stack
- **Storage**: MinIO (Datalake - Bronze/Silver/Gold)
- **Database (OLAP)**: StarRocks (Internal Primary Key tables)
- **Serving (Cache)**: Redis
- **Processing**: Apache Spark
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
| `status` | `bronze` → `silver` → `gold` → `done` |
| `attempts` | Restart count |
| `ts_bronze` / `ts_silver` / `ts_gold` / `ts_done` | Timestamps for each stage |

---

## ⚙️ Ingestion & ETL Flow

### 1. Landing to Bronze (Triggered by Scheduler Tick)
- **Official Trigger**: The `run_etl` function (called by the scheduler) initiates the ingestion.
- **Workflow**:
  1. Scan **`/landing/`** for `.parquet` files.
  2. For each file:
     - **Archive Check**: Skip if file already exists in **`/archive/`**.
  3. Parallel Upload: Up to **10 files concurrently** uploaded to MinIO **`bronze`** bucket.
  4. Finalize Ingestion:
     - Update Postgres status directly to `bronze`.
     - Move file from **`landing/`** to **`archive/`**.

### 2. Bronze to Gold (Spark Processing)
- **Workflow**:
  1. **Identify**: Find files with status `bronze`.
  2. **Silver Stage**: 
     - **Bulk Read**: Identify files with status `bronze` and read them in one Spark batch.
     - **Transform**: Perform cleaning and extraction (date/hour).
     - **Bulk Write**: Write to Silver (MinIO) **partitioned by `date`**.
     - **State**: Update status to `silver` for the processed batch.
  3. **Gold Stage**:
     - **Bulk Read**: Read Silver data.
     - **Aggregate**: Global aggregations per date/hour/zone.
     - **Bulk Write**: Write to Gold (MinIO) **partitioned by `date`** and StarRocks tables.
     - **State**: Update status to `gold`.


### 3. Serving Sync (Redis)
- **Workflow**:
  1. **Identify**: Find files with status `gold`.
  2. **Sync**: Update `revenue_by_hour` pre-aggregations in Redis.
  3. **Finalize**: Update status to `done`.

---

## 🧹 Initialization (`migrate.py`)
- Drop and recreate StarRocks Primary Key tables.
- Reset Postgres `etl_file_status`.
- Clear Redis and MinIO Buckets.

---

## 🚑 Error Handling
- **Scan**: Every 5 minutes, check for stuck files (status != `done`).
- **Reset**: If a file is stuck > 1 minute, increment `attempts` and reset status to `bronze` (re-triggering processing).
