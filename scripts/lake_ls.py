"""List objects in the lake: python scripts/lake_ls.py [prefix] [limit]."""
import sys

from s3_client import bucket_name, make_client


def main():
    prefix = sys.argv[1] if len(sys.argv) > 1 else ""
    limit = int(sys.argv[2]) if len(sys.argv) > 2 else 30
    client = make_client()
    bucket = bucket_name()
    resp = client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=limit)
    objects = resp.get("Contents", [])
    if not objects:
        print(f"no objects under s3://{bucket}/{prefix}")
        return
    for obj in objects:
        print(f"{obj['Size']:>10}  {obj['LastModified']:%Y-%m-%d %H:%M:%S}  s3://{bucket}/{obj['Key']}")
    print(f"-- {len(objects)} object(s) (truncated={resp.get('IsTruncated', False)})")


if __name__ == "__main__":
    main()
