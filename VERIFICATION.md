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

# STOP POINT — 2026-11-19, Phase 2 complete

Status: **Phase 0, 1 and 2 are complete, verified and committed. Phase 3 (metrics) has NOT
been started.** The stack is left running.

## 2.0 Resume: stack up

```
> docker compose up -d
 Container streaming-pipeline-spark-1 Running
 Container streaming-pipeline-producer-1 Recreate
 Container streaming-pipeline-dashboard-1 Recreate
 ...
 Container streaming-pipeline-lake-init-1 Exited
Exit code: 1
```
Exit code 1 is the known PowerShell stderr quirk (Docker writes progress to stderr); container
state is the source of truth.

```
> docker compose ps
NAME                                     IMAGE                                                                                         COMMAND                  SERVICE             CREATED          STATUS                             PORTS
streaming-pipeline-airflow-db-1          postgres:16.16                                                                                "docker-entrypoint.s…"   airflow-db          11 hours ago     Up 30 seconds (healthy)            5432/tcp
streaming-pipeline-airflow-scheduler-1   streaming-pipeline-airflow:2.10.5-dbt1.11                                                     "/usr/bin/dumb-init …"   airflow-scheduler   11 hours ago     Up 11 seconds (health: starting)   8080/tcp
streaming-pipeline-airflow-webserver-1   streaming-pipeline-airflow:2.10.5-dbt1.11                                                     "/usr/bin/dumb-init …"   airflow-webserver   11 hours ago     Up 11 seconds (health: starting)   0.0.0.0:8080->8080/tcp, [::]:8080->8080/tcp
streaming-pipeline-dashboard-1           streaming-pipeline-app:1.0.0                                                                  "streamlit run dashb…"   dashboard           32 seconds ago   Up 24 seconds (healthy)            0.0.0.0:8501->8501/tcp, [::]:8501->8501/tcp
streaming-pipeline-kafka-1               apache/kafka:3.8.0                                                                            "/__cacert_entrypoin…"   kafka               11 hours ago     Up 30 seconds (healthy)            9092/tcp
streaming-pipeline-lake-1                chrislusf/seaweedfs@sha256:4b42c212543a0acd4c0e675215ecdcaf62a186ef98def76c7e12701871222d15   "/entrypoint.sh serv…"   lake                11 hours ago     Up 31 seconds (healthy)            0.0.0.0:8333->8333/tcp, [::]:8333->8333/tcp, 0.0.0.0:8888->8888/tcp, [::]:8888->8888/tcp, 0.0.0.0:9333->9333/tcp, [::]:9333->9333/tcp
streaming-pipeline-producer-1            streaming-pipeline-app:1.0.0                                                                  "python -m producer.…"   producer            32 seconds ago   Up 22 seconds
streaming-pipeline-spark-1               streaming-pipeline-spark:3.5.3                                                                "/opt/entrypoint.sh …"   spark               11 hours ago     Up 16 minutes
streaming-pipeline-warehouse-1           postgres:16.16                                                                                "docker-entrypoint.s…"   warehouse           11 hours ago     Up 30 seconds (healthy)            0.0.0.0:5433->5432/tcp, [::]:5433->5432/tcp
```

## 2.1 Earlier Phase 2 findings (kept from the previous session)

1. The old dead-letter stream captured real bad messages with Kafka coordinates:

```
[missing_symbol]   offset=6450 -> not json at all
[non_positive_price] offset=6451 -> {"symbol": "BTCUSDT", "price": -5, ...}
[missing_symbol]   offset=6452 -> {"symbol": "", "price": 123, ...}
[missing_price]    offset=6453 -> {"symbol": "ETHUSDT", ...}
```

2. That run exposed a classification bug: Spark's permissive `from_json` returns a non-null
   struct with null fields for garbage input, so `t IS NULL` never fired and `not json at all`
   was labelled `missing_symbol`. Fixed with an explicit "is it a JSON object?" guard
   (`trim(value)` starts with `{` and ends with `}`) yielding `unparseable_json`.

3. Deterministic way to inject messages (a PowerShell pipe into `kafka-console-producer` silently
   delivered nothing on one attempt):

```
docker compose cp <file> kafka:/tmp/bad_messages.txt
docker compose exec -T kafka sh -c \
  "/opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 \
   --topic crypto.prices < /tmp/bad_messages.txt"
```

## 2.2 Bug found while verifying: dead letters silently stopped being written

After the reason fix, newly produced bad messages never appeared in the lake, even though the
`dead_letter` checkpoint advanced (e.g. `offsets/24` committed at 13:38 with end offset 10633)
and no exceptions were logged.

Root cause, proven by listing the lake and checkpoint:

```
> python scripts/lake_ls.py dead_letter
  2  2026-11-19 02:16:52  s3://datalake/dead_letter/_spark_metadata/0
  ...
  2  2026-11-19 02:40:00  s3://datalake/dead_letter/_spark_metadata/47
2603  2026-11-19 02:38:30  s3://datalake/dead_letter/rejected_date=2026-11-19/part-...parquet
```

The built-in Parquet **stream sink** keeps a `_spark_metadata` log keyed by batch id. The schema
change required deleting the `dead_letter` checkpoint, but that left the metadata log behind with
batch ids up to 47. Every newly committed batch had a lower id (0, 1, 2, ...) and Spark silently
skipped it, so no Parquet was ever written. Clearing only the checkpoint did not help; a stray
`_spark_metadata/32` (2 bytes, content `v1`) was enough to suppress all new batches again.

Fix in `spark/streaming_job.py`:
- dead-letter output now uses `foreachBatch(write_dead_letter)` with a plain partitioned Parquet
  write, so there is no file-sink metadata log that can drift from the checkpoint;
- committers switched to `mapreduce.fileoutputcommitter.algorithm.version=2`. Version 1's
  `commitJob` re-merges stale `_temporary` task directories on S3-compatible stores and crashed
  the stream with:

```
26/11/19 13:53:31 ERROR FileFormatWriter: Aborting job 7aa1a97d-...
java.io.FileNotFoundException: No such file or directory:
s3a://datalake/dead_letter/_temporary/0/task_202611191351313453154111589611508_0007_m_000000/rejected_date=2026-11-19
```

The old rows were preserved under `dead_letter_evidence_backup/` before each reset.

## 2.3 PROOF 1 — `unparseable_json` and `invalid_event_time` carry reason + offset

Produced three bad messages (the classifier was also verified in isolation with `spark-submit`,
which returned `unparseable_json` and `invalid_event_time` for these exact values):

```
> produce_all_bad.py
produced: not json at all
produced: {"symbol": "BTCUSDT", "price": 100, "event_time": "not-a-real-date"}
produced: {"symbol": "ETHUSDT", "event_time": "2026-11-19T13:56:06.917598+00:00"}

> docker compose run --rm --no-deps producer python scripts/replay_dlq.py --dry-run --limit 20
[unparseable_json] offset=16070 -> not json at all
[invalid_event_time] offset=16071 -> {"symbol": "BTCUSDT", "price": 100, "event_time": "not-a-real-date"}
[missing_price] offset=16072 -> {"symbol": "ETHUSDT", "event_time": "2026-11-19T13:56:06.917598+00:00"}
```

Both required reasons are present with their real Kafka offsets. `missing_price` is also there,
which is used for Proof 2.

## 2.4 PROOF 2 — replay repairs a `missing_price` row into a valid tick

```
> docker compose run --rm --no-deps producer python scripts/replay_dlq.py \
    --reason missing_price --set price=9999.99 --limit 1
[missing_price] offset=16072 -> {"symbol": "ETHUSDT", "event_time": "2026-11-19T13:56:06.917598+00:00", "price": 9999.99}
replayed 1 message(s) to crypto.prices
```

The message was re-validated, passed, windowed and committed to the warehouse:

```
> docker compose exec -T warehouse psql -U warehouse -d warehouse -c \
   "select symbol, window_start, round(max_price::numeric,2) max_price, tick_count
    from raw.price_windows where symbol='ETHUSDT' and max_price > 9000
    order by window_start desc limit 5;"
 symbol  |    window_start     | max_price | tick_count
---------+---------------------+-----------+------------
 ETHUSDT | 2026-11-19 13:56:00 |   9999.99 |         61
(1 row)
```

Baseline for the same query before replay was `280|2026-11-19 13:56:00` for count/max window;
the 9999.99 value only exists because of the repaired replay.

## 2.5 PROOF 3 — sink failure quarantines the batch and the stream recovers

Baseline before stopping the warehouse:

```
> python scripts/lake_ls.py raw/ticks/ 500 | Select-Object -Last 1
-- 218 object(s) (truncated=False)
> psql -tAc "select count(*), max(window_start) from raw.price_windows;"
280|2026-11-19 13:56:00
```

Stop the warehouse, then inject two more bad messages while it is down:

```
> docker compose stop warehouse
 Container streaming-pipeline-warehouse-1 Stopped
> kafka-console-producer ... < /tmp/bad2.txt
producer_exit=0
```

### Quarantine

```
> python scripts/lake_ls.py dead_letter/sink_failures 100
         0  2026-11-19 14:01:33  s3://datalake/dead_letter/sink_failures/batch_id=111/_SUCCESS
       938  2026-11-19 14:01:33  s3://datalake/dead_letter/sink_failures/batch_id=111/part-00000-0f68ee35-...parquet
       938  2026-11-19 14:00:11  s3://datalake/dead_letter/sink_failures/batch_id=111/part-00000-46c10526-...parquet
       ...
-- 19 object(s) (truncated=False)
```

The repeated files are the same batch being retried every 30 s; each attempt quarantines before
re-raising. Driver log:

```
2026-11-19 14:01:32,086 ERROR spark.streaming - batch 111 failed to reach Postgres; quarantining to the lake
2026-11-19 14:01:48,876 ERROR spark.streaming - batch 111 failed to reach Postgres; quarantining to the lake
2026-11-19 14:02:08,658 ERROR spark.streaming - batch 111 failed to reach Postgres; quarantining to the lake
2026-11-19 14:02:26,170 ERROR spark.streaming - batch 111 failed to reach Postgres; quarantining to the lake
```

### The stream does not wedge

While the warehouse was down, the independent streams kept processing:

```
> python scripts/lake_ls.py raw/ticks/ 500 | Select-Object -Last 1
-- 226 object(s) (truncated=False)        # was 218

> python scripts/replay_dlq.py --dry-run --limit 10
[unparseable_json] offset=16070 -> not json at all
[invalid_event_time] offset=16071 -> {"symbol": "BTCUSDT", ...}
[missing_price] offset=16072 -> {"symbol": "ETHUSDT", ...}
[unparseable_json] offset=17199 -> not json at all
[invalid_event_time] offset=17200 -> {"symbol": "BTCUSDT", ...}
```

Two brand-new dead letters (offsets 17199/17200) landed during the outage. The JDBC failure does
terminate the `windows` query (that is what makes Spark retry the batch) and `restart: on-failure`
brings the whole app back; no data is lost because the batch stays in the checkpoint and in the
quarantine.

A side effect was found and fixed: `replay_dlq.py` was also reading the quarantined window
batches as dead letters and printing `[nan] offset=nan -> nan`. It now excludes keys containing
`/sink_failures/`. After the fix the listing above is clean.

### Recovery

```
> docker compose start warehouse
 Container streaming-pipeline-warehouse-1 Started
> docker compose logs --timestamps spark | Select-String "batch [0-9]+ written to Postgres|quarantining"
2026-11-19 14:03:15,739 ERROR spark.streaming - batch 111 failed to reach Postgres; quarantining to the lake
2026-11-19 14:03:34,600 INFO spark.streaming - batch 111 written to Postgres
2026-11-19 14:03:36,643 INFO spark.streaming - batch 112 written to Postgres
2026-11-19 14:04:01,346 INFO spark.streaming - batch 113 written to Postgres
2026-11-19 14:04:31,180 INFO spark.streaming - batch 114 written to Postgres

> psql -c "select count(*) as windows, max(window_start) as latest from raw.price_windows;"
 windows |       latest
---------+---------------------
     305 | 2026-11-19 14:01:00
(1 row)
```

The stuck batch 111 was written first, then normal batches resumed. Window count grew 280 -> 305
and the latest window advanced from 13:56:00 to 14:01:00. Both services are healthy at the end.

## 2.6 Regression tests

```
> docker compose run --rm --no-deps -v "${PWD}/tests:/app/tests" -v "${PWD}/dbt:/app/dbt" \
    producer sh -c "pip install -q pytest==8.3.2 && python -m pytest -q"
............                                                             [100%]
12 passed in 0.77s
```

No test was weakened or deleted.

The app image was rebuilt so the deployed `replay_dlq.py` contains the `/sink_failures/` fix:

```
> docker compose build producer
 Image streaming-pipeline-app:1.0.0 Built
> docker compose up -d
> docker compose run --rm --no-deps producer python scripts/replay_dlq.py --dry-run --limit 10
[unparseable_json] offset=16070 -> not json at all
[invalid_event_time] offset=16071 -> {"symbol": "BTCUSDT", ...}
[missing_price] offset=16072 -> {"symbol": "ETHUSDT", ...}
[unparseable_json] offset=17199 -> not json at all
[invalid_event_time] offset=17200 -> {"symbol": "BTCUSDT", ...}
```

Final state: all nine services up, Spark still writing windows (`batch 127 written to Postgres`).

## 2.7 Phase 2 status

- [x] `unparseable_json` / `invalid_event_time` dead letters with reason + offset
- [x] `replay_dlq.py --set price=9999.99` repairs a tick and it reaches the stream/warehouse
- [x] sink failure quarantines to `dead_letter/sink_failures/`, other streams keep running,
      and the stuck batch is committed after the warehouse restarts
- [x] 12 unit tests pass

## Session-side notes

- Bad-message files used: `C:\Users\DELL\AppData\Local\Temp\opencode\bad_messages_phase2_v2.txt`
  and the `produce_all_bad.py` helper (current event time).
- Diagnostic Spark classifier script:
  `C:\Users\DELL\AppData\Local\Temp\opencode\diag_classifier.py`.
- Old dead-letter rows were copied to `dead_letter_evidence_backup/` in the lake before each
  checkpoint reset.
- Local git identity is `Pipeline Dev <dev@localhost>` (repo-local only).
- Docker Hub is intermittently slow on this machine; pulls may need a few retries.
