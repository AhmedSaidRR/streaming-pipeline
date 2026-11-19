"""Shared S3 client for the local data lake (SeaweedFS)."""
import os

import boto3
from botocore.client import Config


def make_client(endpoint: str | None = None):
    return boto3.client(
        "s3",
        endpoint_url=endpoint or os.getenv("S3_ENDPOINT", "http://lake:8333"),
        aws_access_key_id=os.getenv("S3_ACCESS_KEY", "lakeadmin"),
        aws_secret_access_key=os.getenv("S3_SECRET_KEY", "lakeadmin123"),
        region_name=os.getenv("S3_REGION", "us-east-1"),
        config=Config(s3={"addressing_style": "path"}, signature_version="s3v4"),
    )


def bucket_name() -> str:
    return os.getenv("S3_BUCKET", "datalake")
