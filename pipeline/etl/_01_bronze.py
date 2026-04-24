from __future__ import annotations
import boto3
import psycopg2
from psycopg2.extras import execute_values
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from botocore.client import Config

from pipeline import config

def run_bronze_phase(landing_dir: str) -> None:
    """
    Extract from Landing, Load to Bronze (MinIO), and update metadata (Postgres).
    Follows Landing -> MinIO -> Bronze -> Archive lifecycle.
    """
    connections = config.connections
    if not connections:
        raise RuntimeError("Global connections configuration not found.")

    paths = connections['paths']
    pg_cfg = connections['postgres']
    m_cfg = connections.get('minio', config.minio_kwargs())

    landing_path = Path(landing_dir)
    archive_dir = paths["archive"] if isinstance(paths, dict) else paths.archive
    archive_path = Path(archive_dir)
    archive_path.mkdir(parents=True, exist_ok=True)

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

    # 1. Pre-filter and Cleanup
    # We separate files that are already processed from those that need uploading.
    to_upload = []
    for f in files:
        is_known = f.name in known_files
        is_archived = (archive_path / f.name).exists()
        
        if is_known or is_archived:
            # File is already in the system, just clean up landing
            archive_dest = archive_path / f.name
            if is_archived:
                f.unlink() # Already there, just delete
            else:
                f.rename(archive_dest) # Move to archive
        else:
            to_upload.append(f)

    if not to_upload:
        print("[ELT] No new files to upload.")
        return

    # 2. Parallel S3 Upload
    # We only parallelize the network-heavy upload part.
    def upload_one(file_path: Path):
        try:
            file_date = file_path.stem  # e.g., '2019-01-01'
            s3_key = f"trips/date={file_date}/{file_path.name}"
            s3.upload_file(str(file_path), 'bronze', s3_key)
            return file_path
        except Exception as e:
            print(f"[ELT] Error uploading {file_path.name}: {e}")
            return None

    print(f"[ELT] Uploading {len(to_upload)} files to MinIO...")
    with ThreadPoolExecutor(max_workers=10) as executor:
        results = list(executor.map(upload_one, to_upload))
    
    loaded_files = [f for f in results if f is not None]

    # 3. Bulk Postgres Update
    # Instead of one connection per file, we use a single bulk operation.
    if loaded_files:
        print(f"[ELT] Bulk updating {len(loaded_files)} records in Postgres...")
        data = [(f.name, 'bronze') for f in loaded_files]
        conn = psycopg2.connect(**pg_cfg)
        try:
            with conn.cursor() as cur:
                # using this "data, 'template'" cause you can't do VALUES (%s, %s, clock_timestamp()) in execute_values
                execute_values(cur, """
                    INSERT INTO etl_file_status (file_path, status, status_bronze_ts)
                    VALUES %s
                    ON CONFLICT (file_path) DO UPDATE SET
                        status = EXCLUDED.status,
                        status_bronze_ts = clock_timestamp();
                """, data, template="(%s, %s, clock_timestamp())")
            conn.commit()
        finally:
            conn.close()

        # 4. Parallel Archive Move
        # We parallelize the moves to handle large volumes of files quickly.
        print(f"[ELT] Moving {len(loaded_files)} files to archive...")
        with ThreadPoolExecutor(max_workers=10) as executor:
            list(executor.map(lambda f: f.rename(archive_path / f.name), loaded_files))

    print(f"[ELT] Bronze Phase finished.")
