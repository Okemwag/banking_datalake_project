from __future__ import annotations

from pathlib import Path

import boto3

from lakehouse.runtime import load_settings


def upload_file_to_raw(local_path: str, dataset: str, target_name: str | None = None) -> str:
    settings = load_settings()
    path = Path(local_path)
    key = f"{settings.raw_prefix}/{dataset}/{target_name or path.name}"
    client = boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint_http,
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
    )
    client.upload_file(str(path), settings.bucket, key)
    return key

