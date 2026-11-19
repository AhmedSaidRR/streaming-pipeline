"""Create the lake bucket and verify the S3 endpoint (idempotent, safe to re-run)."""
import sys
import time

from botocore.exceptions import ClientError

from s3_client import bucket_name, make_client


def wait_for_s3(client, attempts=30, delay=2):
    last = None
    for attempt in range(1, attempts + 1):
        try:
            client.list_buckets()
            return
        except Exception as exc:  # noqa: BLE001 - any transport/auth error means "not ready yet"
            last = exc
            print(f"waiting for S3 ({attempt}/{attempts}): {exc}", flush=True)
            time.sleep(delay)
    sys.exit(f"S3 endpoint never became ready: {last}")


def ensure_bucket(client, bucket):
    try:
        client.head_bucket(Bucket=bucket)
        print(f"bucket {bucket} already exists")
    except ClientError:
        client.create_bucket(Bucket=bucket)
        print(f"created bucket {bucket}")


def selftest(client, bucket):
    key = "_healthcheck/ok.txt"
    client.put_object(Bucket=bucket, Key=key, Body=b"ok")
    body = client.get_object(Bucket=bucket, Key=key)["Body"].read()
    if body != b"ok":
        sys.exit(f"selftest failed: read back {body!r}")
    client.delete_object(Bucket=bucket, Key=key)
    print(f"selftest OK: wrote and read s3://{bucket}/{key}")


def main():
    bucket = bucket_name()
    client = make_client()
    wait_for_s3(client)
    ensure_bucket(client, bucket)
    selftest(client, bucket)


if __name__ == "__main__":
    main()
