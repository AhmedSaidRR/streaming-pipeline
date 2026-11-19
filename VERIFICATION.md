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

## Phase 1 — boot reliability

Goals: no Maven at startup, deterministic service order, `up` works without `.env`, everything
pinned, Airflow split into webserver/scheduler.

### 1.1 Spark connectors baked into the image

`Dockerfile.spark` downloads the exact jars Ivy resolved in Phase 0 (spark-sql-kafka,
kafka-clients, commons-pool2, hadoop-aws, aws-java-sdk-bundle, postgresql).

```
> docker compose build spark
+ curl -fsSL -O .../spark-sql-kafka-0-10_2.12-3.5.3.jar
+ curl -fsSL -O .../aws-java-sdk-bundle-1.12.262.jar
-rw-r--r-- 1 root root 280645251 aws-java-sdk-bundle-1.12.262.jar
-rw-r--r-- 1 root root    962685 hadoop-aws-3.3.4.jar
-rw-r--r-- 1 root root   1089312 postgresql-42.7.3.jar
-rw-r--r-- 1 root root    432340 spark-sql-kafka-0-10_2.12-3.5.3.jar
#7 DONE 145.5s
 Image streaming-pipeline-spark:3.5.3 Built
```

After this, a cold start has zero `Ivy` / `downloading` lines and no `--packages` flag.

### 1.2 Boot times (previous ~4 minutes, mostly Ivy)

| Boot | Command | Result | Time |
|---|---|---|---|
| 1 | `docker compose down` + `up -d --build` | 9 services up | 33 s |
| 2 | `.env` moved out of the repo, `down` + `up -d` | 9 services up, `compose config` still OK | 36 s |
| 3 | `down` + `up -d` | 9 services up | 33 s |
| 4 | `down -v` (all volumes wiped) + `up -d` | 9 services up, DAG unpaused | 26 s |

Boot 2 proves the "one command" claim without a `.env`:

```
> Test-Path .env
False
> docker compose config --quiet
config_without_env_exit=0
```

Health after boot 4:

```
SERVICE             STATUS
airflow-db          Up About a minute (healthy)
airflow-scheduler   Up About a minute (healthy)
airflow-webserver   Up About a minute (healthy)
dashboard           Up About a minute (healthy)
kafka               Up About a minute (healthy)
lake                Up About a minute (healthy)
producer            Up About a minute
spark               Up About a minute
warehouse           Up About a minute (healthy)
```

`producer` and `spark` have no HTTP endpoint, so they have no healthcheck; Spark uses
`restart: on-failure`.

### 1.3 DAG is live without manual unpausing

On a fresh metadata database:

```
> docker compose exec -T airflow-webserver airflow dags list
dag_id              | fileloc                                  | owners  | is_paused
====================+==========================================+=========+==========
streaming_analytics | /opt/airflow/dags/analytics_pipeline.py  | airflow | False
```

### 1.4 Bug found and fixed: dbt resolved to an incompatible core

First DAG run failed at `dbt_seed`. The task log had no dbt output because I had set
`AIRFLOW__LOGGING__LOGGING_LEVEL=WARNING` (removed). Running the command by hand showed the
real cause:

```
> docker compose exec -T airflow-scheduler sh -c \
    "cd /opt/airflow/dbt && /home/airflow/dbt_venv/bin/dbt seed --profiles-dir ."
   dbt-oss 2.0.11
[error] [InvalidConfig (dbt1005)]: Configuration Error: The 'postgres' adapter is not yet
supported by dbt. Supported adapters: snowflake, bigquery, databricks, redshift, duckdb, ...

> docker compose exec -T airflow-scheduler sh -c \
    "/home/airflow/dbt_venv/bin/pip list | grep -i dbt"
dbt-core                  2.0.0rc10
dbt-postgres              1.8.2
```

Root cause: `dbt-postgres==1.8.2` has no upper bound on `dbt-core`, so pip installed the 2.x
release candidate, which ships a different CLI (`dbt-oss`) that refuses the postgres adapter.
Fix: pin both packages (`dbt-core==1.11.0`, `dbt-postgres==1.11.0`) and share one image across
the three Airflow services.

### 1.5 dbt pipeline runs end to end

```
> dbt seed        ... Completed successfully   (INSERT 5)
> dbt snapshot    ... OK snapshotted snapshots.symbol_metadata_snapshot (SELECT 5)
> dbt run         ... PASS=6 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=6
> dbt test        ... PASS=15 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=15
```

Marts after the run:

```
 dims | facts | hourly | alerts | history
------+-------+--------+--------+---------
    5 |    60 |      5 |     53 |       5
```

### 1.6 Airflow runs the DAG on its own

```
dag_id              | run_id                              | state   | start_date          | end_date
streaming_analytics | manual__2026-11-19T02:28:17+00:00   | success | 02:28:17            | 02:28:44
streaming_analytics | scheduled__2026-11-19T02:25:00+00:00| success | 02:30:00            | 02:30:28
```

The three earlier failures in the metadata DB are from before the dbt fix and were left in
place as evidence rather than deleted.

### 1.7 Reproducibility notes

- `postgres:16` resolved to 16.16; pinning to `16.4` produced a collation mismatch against the
  existing volume. Pinned to **16.16** instead, which matches the volumes that were created.
- The lake image has no semantic version tags, so it is pinned by digest.
- `env_file` was removed from every service; all values are `${VAR:-default}` and a local
  `.env` (or the shell environment) overrides them.

### 1.8 Still open after Phase 1

- No dead-letter evidence and no sink-failure handling (Phase 2).
- No metrics/latency (Phase 3).
- The dbt `unique` test on `stg_price_windows` cannot fail because the view already dedupes
  (Phase 4).
- `schema.yml` uses the pre-1.10 generic-test shape (`to:` / `field:` instead of `arguments:`);
  it works but warns (Phase 4).

---

# STOP POINT — 2026-11-19, during Phase 2

Status: **Phase 0 and Phase 1 are complete, verified and committed. Phase 2 is coded but only
partly verified.** Nothing after Phase 2 has been started. The stack is left running.

## Verified so far in Phase 2

1. The old dead-letter stream was proven to capture real bad messages with Kafka coordinates.
   Four messages were produced and read back from the lake:

```
[missing_symbol]   offset=6450 -> not json at all
[non_positive_price] offset=6451 -> {"symbol": "BTCUSDT", "price": -5, ...}
[missing_symbol]   offset=6452 -> {"symbol": "", "price": 123, ...}
[missing_price]    offset=6453 -> {"symbol": "ETHUSDT", ...}
```

2. That run exposed a real classification bug: Spark's `from_json` in permissive mode returns a
   **non-null struct with null fields** for garbage input, so the `t IS NULL` check never fired
   and `not json at all` was labelled `missing_symbol`. Fixed in code with an explicit
   "is it a JSON object?" guard (`trim(value)` starts with `{` and ends with `}`) that yields
   `unparseable_json`.

3. Spark restart with a deleted `dead_letter` checkpoint (and intact `ticks` / `windows`
   checkpoints) resumed correctly and kept writing windows:

```
INFO spark.streaming - batch 48 written to Postgres
INFO spark.streaming - batch 51 written to Postgres
```

4. `kafka-console-producer` fed through a PowerShell pipe silently delivered nothing on the
   second attempt. The deterministic method, used afterwards, is:

```
docker compose cp <file> kafka:/tmp/bad_messages.txt
docker compose exec -T kafka sh -c \
  "/opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 \
   --topic crypto.prices < /tmp/bad_messages.txt"
```

With that method the topic end offset moved from 7980 to 8116, i.e. the six test messages were
really produced.

## Coded in Phase 2 but NOT yet verified

- `spark/streaming_job.py`
  - dead-letter rows now carry `error_reason`, `kafka_topic`, `kafka_partition`, `kafka_offset`,
    `kafka_timestamp`, `rejected_at`, `rejected_date`
  - reasons: `empty_value`, `unparseable_json`, `missing_symbol`, `missing_price`,
    `non_positive_price`, `invalid_event_time`
  - `write_windows_to_postgres` logs the batch, copies a failed batch to
    `s3://datalake/dead_letter/sink_failures/batch_id=<n>` and then re-raises so Spark retries
- `scripts/replay_dlq.py` (new): `--list`-style listing via `--dry-run`, `--reason` filter,
  `--set key=value` to repair a message before replaying, `--limit`
- `scripts/lake_ls.py` now prints `LastModified`
- `requirements.txt`: `pyarrow==17.0.0` (needed to read the dead-letter Parquet)
- `docker-compose.yml`: producer / dashboard / lake-init share one image tag
  (`streaming-pipeline-app:1.0.0`) instead of building three

## Not done in Phase 2

- Re-reading the dead letter after the reason fix, to show `unparseable_json` and
  `invalid_event_time` actually appear.
- The replay happy path: `--reason missing_price --set price=9999.99 --limit 1`, then proving
  the repaired message became a valid tick (expect `max(max_price)=9999.99` for ETHUSDT in the
  matching minute).
- The sink-failure proof: stop `warehouse`, confirm batches land in
  `dead_letter/sink_failures/`, restart it and confirm the stream catches up.

## Exact next step

```
# 1. Reasons after the fix (was interrupted, run it again)
docker compose run --rm --no-deps producer python scripts/replay_dlq.py --dry-run --limit 10

# 2. Replay one repaired message
docker compose run --rm --no-deps producer python scripts/replay_dlq.py \
  --reason missing_price --set price=9999.99 --limit 1
# wait ~90 s, then:
docker compose exec -T warehouse psql -U warehouse -d warehouse -c \
  "select symbol, window_start, max_price from raw.price_windows
   where symbol='ETHUSDT' order by window_start desc limit 5;"

# 3. Sink failure
docker compose stop warehouse
# produce valid ticks, wait ~60 s, list s3://datalake/dead_letter/sink_failures/
docker compose start warehouse
```

Then append the outputs here and commit with a `fix(stream):`/`feat(dlq):` message before
starting Phase 3 (metrics + end-to-end latency).

## Session-side notes

- Test file used for bad messages:
  `C:\Users\DELL\AppData\Local\Temp\opencode\bad_messages.txt`
- Local git identity is `Pipeline Dev <dev@localhost>` (repo-local only).
- Docker Hub is intermittently slow on this machine: pulls must be retried (a 5-attempt loop
  was enough every time).


