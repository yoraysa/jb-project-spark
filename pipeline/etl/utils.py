import psycopg2

def update_status(
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
