import psycopg2

def migrate_postgres(pg_cfg: dict) -> None:
    """Postgres - State Tracking"""
    conn_pg = psycopg2.connect(**pg_cfg)
    try:
        with conn_pg.cursor() as cur:
            # Indexes:
            # - file_path column - use primary key (Postgres' b-tree index)
            # - status column - use partial indexes on status != 'done'
            cur.execute("""
                CREATE TABLE IF NOT EXISTS etl_file_status (
                    file_path TEXT PRIMARY KEY,
                    status VARCHAR(20) NOT NULL DEFAULT 'bronze',
                    status_bronze_ts TIMESTAMP,
                    status_silver_ts TIMESTAMP,
                    status_gold_ts TIMESTAMP,
                    status_done_ts TIMESTAMP
                );
                CREATE INDEX IF NOT EXISTS idx_etl_file_status_status ON etl_file_status(status) WHERE status != 'done';
            """)
        conn_pg.commit()
    finally:
        conn_pg.close()
