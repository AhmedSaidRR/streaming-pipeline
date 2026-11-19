# Verification log

Evidence for every phase. Commands are PowerShell unless stated. Nothing here is inferred:
each block is pasted from a real run on 2026-11-19 (local machine, Docker Desktop 29.8.0,
4 CPUs / 7.8 GB allocated).

## Phase 0 — baseline, cold boot, data flowing

### 0.1 Environment

```
> docker info --format "server={{.ServerVersion}} cpus={{.NCPU}} mem={{.MemTotal}}"
server=29.8.0 cpus=4 mem=8328429568
```

Host Python is 3.14.7 only, and the pinned deps do not build there, so nothing is run on the
host: tests and dbt run inside containers.

```
> py -0p
 -V:3.14 *  C:\Users\DELL\AppData\Local\Programs\Python\Python314\python.exe
```

### 0.2 Upstream blocker: MinIO images no longer exist

The original compose file used `minio/minio:latest` and `minio/mc:latest`. Both failed:

```
> docker pull minio/minio:latest
Error response from daemon: pull access denied for minio/minio, repository does not exist
or may require 'docker login'

> docker pull minio/mc:latest
Error response from daemon: pull access denied for minio/mc, repository does not exist
or may require 'docker login'
```

Checked three sources:

| Source | Result |
|---|---|
| `https://hub.docker.com/v2/repositories/minio/minio/` | `404` |
| `https://hub.docker.com/v2/repositories/minio/mc/` | `404` |
| `https://dl.min.io/server/minio/release/linux-amd64/minio` | `410 Gone` |
| `docker pull quay.io/minio/mc:latest` | `401 UNAUTHORIZED` |

Docker Hub and Maven Central were otherwise reachable (`hello-world`, `apache/kafka:3.8.0`
pulled fine), so this is an upstream removal, not a local network fault.

**Decision:** the lake moved to **SeaweedFS** (`chrislusf/seaweedfs`, pinned by digest). It
speaks the S3 API, so Spark's `s3a://` code path is unchanged.

Proof the replacement store works before wiring the stack:

```
> docker run --rm --network smoke -e S3_ENDPOINT=http://seaweed-smoke:8333 \
    -e S3_BUCKET=datalake -v "$PWD\scripts:/scripts:ro" python:3.11-slim \
    sh -c "pip install -q boto3==1.35.24 && python /scripts/init_lake.py"
created bucket datalake
selftest OK: wrote and read s3://datalake/_healthcheck/ok.txt
```

### 0.3 Git baseline

The folder was not a git repository.

```
> git init -b main
Initialized empty Git repository in D:/projects/streaming-pipeline/streaming-pipeline/.git/
> git commit -m "chore: baseline snapshot before upgrade"
c0043aa chore: baseline snapshot before upgrade
```

A local-only identity (`Pipeline Dev <dev@localhost>`) was set because no global git identity
was configured. `git config --global` was not touched.

### 0.4 Stack up

```
> docker compose config --quiet
compose config: OK

> docker compose up -d --build
...
 Container streaming-pipeline-lake-1 Healthy
 Container streaming-pipeline-lake-init-1 Exited
 Container streaming-pipeline-spark-1 Started
```

`docker compose up` returns exit code 1 in PowerShell because Docker writes progress to
stderr and PowerShell treats that as a native-command error. The container state is the
source of truth:

```
> docker compose ps --format "table {{.Service}}\t{{.Status}}"
SERVICE      STATUS
airflow      Up 7 minutes
airflow-db   Up 7 minutes
dashboard    Up 7 minutes
kafka        Up 7 minutes
lake         Up 7 minutes (healthy)
producer     Up 7 minutes
spark        Up 3 minutes
warehouse    Up 7 minutes
```

Lake bucket init:

```
> docker compose logs lake-init
created bucket datalake
selftest OK: wrote and read s3://datalake/_healthcheck/ok.txt
```

HTTP endpoints:

```
airflow=200      (http://localhost:8080/health)
dashboard=200    (http://localhost:8501)
```

### 0.5 Events are really in Kafka

```
> docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server kafka:9092 --topic crypto.prices --max-messages 3 --timeout-ms 20000
{"symbol": "BTCUSDT", "price": 64915.40644835709, "event_time": "2026-11-19T01:56:56.222321+00:00", "source": "simulated"}
{"symbol": "ETHUSDT", "price": 3195.766124221745, "event_time": "2026-11-19T01:56:56.222410+00:00", "source": "simulated"}
{"symbol": "BNBUSDT", "price": 602.8979218782124, "event_time": "2026-11-19T01:56:56.222428+00:00", "source": "simulated"}
```

### 0.6 Windows landed in PostgreSQL

```
> docker compose exec -T warehouse psql -U warehouse -d warehouse -c \
  "select symbol, window_start, round(avg_price::numeric,2) avg, tick_count, ingested_at
   from raw.price_windows order by window_start limit 10;"
 symbol  |    window_start     |   avg    | tick_count |      ingested_at
---------+---------------------+----------+------------+------------------------
 BNBUSDT | 2026-11-19 02:00:00 |   601.39 |         45 | 2026-11-19 02:03:00.01
 SOLUSDT | 2026-11-19 02:00:00 |   149.59 |         45 | 2026-11-19 02:03:00.01
 XRPUSDT | 2026-11-19 02:00:00 |     0.56 |         45 | 2026-11-19 02:03:00.01
 ETHUSDT | 2026-11-19 02:00:00 |  3205.29 |         45 | 2026-11-19 02:03:00.01
 BTCUSDT | 2026-11-19 02:00:00 | 64646.25 |         45 | 2026-11-19 02:03:00.01
(5 rows)
```

Timing observed: Spark started the queries at 02:00:14, the first 1-minute window (02:00:00)
was committed to Postgres at 02:03:00. That is the 1-minute watermark + 30 s trigger + JDBC
write. This is the reason end-to-end latency will be measured in minutes, not milliseconds.

### 0.7 Raw ticks landed in the lake

```
> docker compose run --rm --no-deps -v "$PWD/scripts:/app/scripts" producer \
    python scripts/lake_ls.py
       244  s3://datalake/raw/ticks/_spark_metadata/2
       ...
      3638  s3://datalake/raw/ticks/event_date=2026-11-19/part-00000-1635981f-...snappy.parquet
      3642  s3://datalake/raw/ticks/event_date=2026-11-19/part-00000-28445738-...snappy.parquet
      3635  s3://datalake/raw/ticks/event_date=2026-11-19/part-00000-5171f889-...snappy.parquet
      2515  s3://datalake/raw/ticks/event_date=2026-11-19/part-00000-61d24934-...snappy.parquet
      3638  s3://datalake/raw/ticks/event_date=2026-11-19/part-00000-80191e82-...snappy.parquet
      3633  s3://datalake/raw/ticks/event_date=2026-11-19/part-00000-94fe3233-...snappy.parquet
-- 20 object(s) (truncated=False)
```

`minio-init` needs `mc`; it was replaced by `scripts/init_lake.py` (boto3), which is also
reused by the verify script.

### 0.8 Known limitations at the end of Phase 0

- Spark downloads Kafka/S3/JDBC jars from Maven on every cold start (took ~4 minutes here).
  Fix scheduled for Phase 1 (`Dockerfile.spark`).
- `docker compose up` still requires a local `.env` (`env_file: .env`). Fix scheduled for
  Phase 1.
- `lake` is the only service with a healthcheck, so boot order is still partly luck.
- No dead-letter proof yet, no metrics, no dbt run (Phase 2-4).
