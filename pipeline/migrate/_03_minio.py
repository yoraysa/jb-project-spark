import boto3
from botocore.client import Config

def migrate_minio(m_cfg: dict) -> None:
    """MinIO - Datalake Buckets"""
    s3 = boto3.resource('s3',
        endpoint_url=m_cfg['endpoint'],
        aws_access_key_id=m_cfg['access_key'],
        aws_secret_access_key=m_cfg['secret_key'],
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )
    
    buckets = ['bronze', 'silver', 'gold']
    for b in buckets:
        try:
            s3.meta.client.head_bucket(Bucket=b)
        except:
            s3.create_bucket(Bucket=b)
