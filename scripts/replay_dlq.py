"""Replay dead-lettered messages back to Kafka.

Examples:
  python scripts/replay_dlq.py --list
  python scripts/replay_dlq.py --reason missing_price --set price=2500.5 --limit 1
  python scripts/replay_dlq.py --dry-run --reason non_positive_price

Replaying an unfixable message is safe: it is validated again and re-quarantined with a new
rejected_at / kafka offset, so nothing is silently dropped.
"""
import argparse
import io
import json
import os

import pandas as pd
from confluent_kafka import Producer

from s3_client import bucket_name, make_client


def dead_letter_keys(client, bucket, prefix, limit):
    paginator = client.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            # sink_failures holds quarantined window batches, not rejected ticks
            if obj["Key"].endswith(".parquet") and "/sink_failures/" not in obj["Key"]:
                keys.append((obj["LastModified"], obj["Key"]))
    keys.sort(reverse=True)
    return [key for _, key in keys[:limit]]


def load_rows(client, bucket, keys):
    frames = []
    for key in keys:
        body = client.get_object(Bucket=bucket, Key=key)["Body"].read()
        frames.append(pd.read_parquet(io.BytesIO(body)))
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    return df.sort_values("rejected_at").reset_index(drop=True)


def apply_set(raw_value, patches):
    try:
        payload = json.loads(raw_value)
    except (TypeError, ValueError):
        payload = None
    if not patches:
        # nothing to fix: resend byte-for-byte so the pipeline re-quarantines it honestly
        return raw_value if payload is None else json.dumps(payload)
    if not isinstance(payload, dict):
        payload = {}
    for patch in patches:
        key, _, value = patch.partition("=")
        try:
            payload[key] = json.loads(value)
        except ValueError:
            payload[key] = value
    return json.dumps(payload)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prefix", default="dead_letter/", help="lake prefix to read")
    parser.add_argument("--reason", help="only replay rows with this error_reason")
    parser.add_argument("--set", action="append", default=[], metavar="KEY=VALUE",
                        help="patch a JSON field before replaying (repeatable)")
    parser.add_argument("--limit", type=int, default=10, help="max rows to replay")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    bucket = bucket_name()
    client = make_client()
    keys = dead_letter_keys(client, bucket, args.prefix, limit=50)
    rows = load_rows(client, bucket, keys)
    if rows.empty:
        raise SystemExit(f"no dead letters under s3://{bucket}/{args.prefix}")

    if args.reason:
        rows = rows[rows["error_reason"] == args.reason]
    rows = rows.head(args.limit)
    if rows.empty:
        raise SystemExit(f"no dead letters with error_reason={args.reason!r}")

    for _, row in rows.iterrows():
        payload = apply_set(row["raw_value"], args.set)
        print(f"[{row['error_reason']}] offset={row['kafka_offset']} -> {payload}")
        if args.dry_run:
            continue
        producer = Producer({
            "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP", "kafka:9092"),
            "enable.idempotence": True,
            "acks": "all",
        })
        try:
            symbol = json.loads(payload).get("symbol", "")
        except (TypeError, ValueError):
            symbol = ""
        producer.produce(os.getenv("KAFKA_TOPIC", "crypto.prices"), key=symbol,
                         value=payload, callback=lambda err, msg: err and print("delivery failed:", err))
        producer.flush(10)

    if not args.dry_run:
        print(f"replayed {len(rows)} message(s) to {os.getenv('KAFKA_TOPIC', 'crypto.prices')}")


if __name__ == "__main__":
    main()
