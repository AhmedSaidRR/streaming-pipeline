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
 -V:3.14 *  <user-home>\AppData\Local\Programs\Python\Python314\python.exe
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
Initialized empty Git repository in <project-root>/.git/
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

- Bad-message files used: `<temp>\opencode\bad_messages_phase2_v2.txt`
  and the `produce_all_bad.py` helper (current event time).
- Diagnostic Spark classifier script:
  `<temp>\opencode\diag_classifier.py`.
- Old dead-letter rows were copied to `dead_letter_evidence_backup/` in the lake before each
  checkpoint reset.
- Local git identity is `Pipeline Dev <dev@localhost>` (repo-local only).
- Docker Hub is intermittently slow on this machine; pulls may need a few retries.

---

## Phase 3 — metrics (measurement only)

All commands below were run on 2026-11-19. Host local time is UTC+2; container/database log
timestamps are UTC. Nothing was committed. No code, test or Docker files were changed. No
command printed a credential, so there is nothing to mask with `***` in this section (the S3
client reads its keys from the environment and never echoes them). ANSI colour codes from dbt
output were stripped for readability; the text is otherwise verbatim.

### 3.0 Baseline

```
> git status --short
(no output — clean working tree)

> git log -1 --oneline
93b141a feat(dlq): prove dead-letter reasons, replay repair and sink-failure recovery

> docker compose ps
NAME                                     IMAGE                                                                                         COMMAND                  SERVICE             CREATED          STATUS                       PORTS
streaming-pipeline-airflow-db-1          postgres:16.16                                                                                "docker-entrypoint.s…"   airflow-db          13 hours ago     Up About an hour (healthy)   5432/tcp
streaming-pipeline-airflow-scheduler-1   streaming-pipeline-airflow:2.10.5-dbt1.11                                                     "/usr/bin/dumb-init …"   airflow-scheduler   13 hours ago     Up About an hour (healthy)   8080/tcp
streaming-pipeline-airflow-webserver-1   streaming-pipeline-airflow:2.10.5-dbt1.11                                                     "/usr/bin/dumb-init …"   airflow-webserver   13 hours ago     Up About an hour (healthy)   0.0.0.0:8080->8080/tcp, [::]:8080->8080/tcp
streaming-pipeline-dashboard-1           streaming-pipeline-app:1.0.0                                                                  "streamlit run dashb…"   dashboard           50 minutes ago   Up 50 minutes (healthy)      0.0.0.0:8501->8501/tcp, [::]:8501->8501/tcp
streaming-pipeline-kafka-1               apache/kafka:3.8.0                                                                            "/__cacert_entrypoin…"   kafka               13 hours ago     Up About an hour (healthy)   9092/tcp
streaming-pipeline-lake-1                chrislusf/seaweedfs@sha256:4b42c212543a0acd4c0e675215ecdcaf62a186ef98def76c7e12701871222d15   "/entrypoint.sh serv…"   lake                13 hours ago     Up About an hour (healthy)   0.0.0.0:8333->8333/tcp, [::]:8333->8333/tcp, 0.0.0.0:8888->8888/tcp, [::]:8888->8888/tcp, 0.0.0.0:9333->9333/tcp, [::]:9333->9333/tcp
streaming-pipeline-producer-1            streaming-pipeline-app:1.0.0                                                                  "python -m producer.…"   producer            50 minutes ago   Up 50 minutes
streaming-pipeline-spark-1               streaming-pipeline-spark:3.5.3                                                                "/opt/entrypoint.sh …"   spark               13 hours ago     Up 57 minutes
streaming-pipeline-warehouse-1           postgres:16.16                                                                                "docker-entrypoint.s…"   warehouse           13 hours ago     Up 57 minutes (healthy)      0.0.0.0:5433->5432/tcp, [::]:5433->5432/tcp

> docker compose exec -T airflow-webserver airflow dags list
dag_id              | fileloc                                 | owners  | is_paused
====================+=========================================+=========+==========
streaming_analytics | /opt/airflow/dags/analytics_pipeline.py | airflow | False
```

### 3.1 Pause the DAG

```
> docker compose exec -T airflow-webserver airflow dags pause streaming_analytics
dag_id              | is_paused
====================+==========
streaming_analytics | True

> docker compose exec -T airflow-webserver airflow dags list
dag_id              | fileloc                                 | owners  | is_paused
====================+=========================================+=========+==========
streaming_analytics | /opt/airflow/dags/analytics_pipeline.py | airflow | True
```

### 3.2 Throughput — simulated, N=505

`$env:SIM_EXTRA_SYMBOLS='500'` is set in the same PowerShell invocation as the recreate, because
each tool call is a fresh shell. `SYMBOLS` already has 5 base symbols, so 5 + 500 = 505.

```
> $env:SIM_EXTRA_SYMBOLS='500'; docker compose up -d --force-recreate --no-deps producer
 Container streaming-pipeline-producer-1 Recreate
 Container streaming-pipeline-producer-1 Recreated
 Container streaming-pipeline-producer-1 Starting
 Container streaming-pipeline-producer-1 Started
SIM_EXTRA_SYMBOLS=500
```

`kafka.tools.GetOffsetShell` is absent from `apache/kafka:3.8.0`, so the end offset was read with
the bundled `kafka-get-offsets.sh` (`-1` = latest/end offset by default):

```
> docker compose exec -T kafka /opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list kafka:9092 --topic crypto.prices -time -1
Error: Could not find or load main class kafka.tools.GetOffsetShell
Caused by: java.lang.ClassNotFoundException: kafka.tools.GetOffsetShell

> docker compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:9092 --topic crypto.prices
crypto.prices:0:61511
```

The producer's own counter confirms 505 events per 1-second poll (increment 505 each line):

```
> docker compose logs --tail 8 producer
producer-1  | 2026-11-19 15:01:50,506 INFO producer - sent=2525 rejected=0
producer-1  | 2026-11-19 15:01:51,514 INFO producer - sent=3030 rejected=0
producer-1  | 2026-11-19 15:01:52,502 INFO producer - sent=3535 rejected=0
producer-1  | 2026-11-19 15:01:53,502 INFO producer - sent=4040 rejected=0
producer-1  | 2026-11-19 15:01:54,512 INFO producer - sent=4545 rejected=0
producer-1  | 2026-11-19 15:01:55,513 INFO producer - sent=5050 rejected=0
producer-1  | 2026-11-19 15:01:56,501 INFO producer - sent=5555 rejected=0
producer-1  | 2026-11-19 15:01:57,502 INFO producer - sent=6060 rejected=0
```

Measurement 1 — Kafka end-offset delta over a 60 s sleep (the extra ~1.7 s is the JVM startup of
the two `docker compose exec kafka ...` calls, so elapsed is reported as well as nominal):

```
$t0 = Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff"
$o0raw = docker compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:9092 --topic crypto.prices
$o0 = [int]($o0raw.Trim().Split(':')[-1])
Start-Sleep -Seconds 60
$t1 = Get-Date -Format "yyyy-MM-ddTHH:mm:ss.fff"
$o1raw = docker compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:9092 --topic crypto.prices
$o1 = [int]($o1raw.Trim().Split(':')[-1])
$delta = $o1 - $o0
$elapsed = (...).TotalSeconds

t0=2026-11-19T17:03:00.728
offset_t0_raw=crypto.prices:0:74136
offset_t0=74136
t1=2026-11-19T17:04:02.455
offset_t1_raw=crypto.prices:0:105446
offset_t1=105446
delta=31310
elapsed_sec=61.727
events_per_sec_nominal=521.833333333333
events_per_sec_actual=507.23
```

Measurement 2 — same 60 s sleep, cross-checked against the producer counter read in the same
window (this is the number used in the Results table):

```
function Get-KafkaEnd { $r = docker compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:9092 --topic crypto.prices; return [int]($r.Trim().Split(':')[-1]) }
function Get-ProducerSent { $l = docker compose logs --tail 6 producer | Select-String "sent=" | Select-Object -Last 1; if ($l -match 'sent=(\d+)') { return [int]$Matches[1] } else { return -1 } }
$ko0 = Get-KafkaEnd; $ps0 = Get-ProducerSent; $tw0 = Get-Date
Start-Sleep -Seconds 60
$ko1 = Get-KafkaEnd; $ps1 = Get-ProducerSent; $tw1 = Get-Date
$elapsed = ($tw1-$tw0).TotalSeconds

t0=17:04:57.567 kafka_end=132211 producer_sent=96960
t1=17:05:59.288 kafka_end=163521 producer_sent=127765
kafka_delta=31310 producer_delta=30805 elapsed=61.7208793
kafka_events_per_sec=507.28 producer_events_per_sec=499.1
```

Result: **~507 events/s** from the Kafka end-offset delta (31,310 events / 61.72 s), consistent
with the producer's configured 505 ticks/s. The nominal `delta / 60` = 521.8 is inflated by the
~1.7 s JVM exec overhead inside the 60 s window, so the elapsed-time rate is the reported one.

### 3.3 End-to-end latency — probe tick

One tick with a known `event_time` (current UTC minus 5 s) was written to a host temp file, copied
into the Kafka container with `docker compose cp` and produced from the file (a PowerShell pipe
had previously been unreliable — see 2.1). The probe symbol is unique, so it gets its own
warehouse row.

```
> $et = (Get-Date).ToUniversalTime().AddSeconds(-5)
> $etStr = $et.ToString("yyyy-MM-ddTHH:mm:ss.ffffff+00:00")
> $json = '{"symbol": "LATENCYPROBEUSDT", "price": 12345.67, "event_time": "' + $etStr + '", "source": "probe"}'
> Set-Content -Path $probePath -Value $json -Encoding ascii
probe_file=<temp>\latency_probe_phase3.txt
event_time=2026-11-19T15:06:44.368926+00:00
payload={"symbol": "LATENCYPROBEUSDT", "price": 12345.67, "event_time": "2026-11-19T15:06:44.368926+00:00", "source": "probe"}

> docker compose cp $probePath kafka:/tmp/latency_probe.txt
 streaming-pipeline-kafka-1 Copied <temp>\latency_probe_phase3.txt to streaming-pipeline-kafka-1:/tmp/latency_probe.txt
> docker compose exec -T kafka sh -c "/opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 --topic crypto.prices < /tmp/latency_probe.txt"
producer_exit=0
```

Polled the warehouse every 15 s until the row appeared (host time; database timestamps UTC):

```
poll=1 at=17:07:00 not-yet
poll=2 at=17:07:15 not-yet
poll=3 at=17:07:31 not-yet
poll=4 at=17:07:46 not-yet
poll=5 at=17:08:01 not-yet
poll=6 at=17:08:16 not-yet
poll=7 at=17:08:32 not-yet
poll=8 at=17:08:47 not-yet
poll=9 found_at=17:09:02
row=LATENCYPROBEUSDT|2026-11-19 15:06:00|12345.67|1|2026-11-19 15:09:00.01
found=True

> docker compose exec -T warehouse psql -U warehouse -d warehouse -c "select symbol, window_start, window_end, avg_price, tick_count, ingested_at, extract(epoch from (ingested_at - timestamp '2026-11-19 15:06:44.368926')) as latency_sec from raw.price_windows where symbol='LATENCYPROBEUSDT' order by ingested_at desc limit 1;"
      symbol      |    window_start     |     window_end      | avg_price | tick_count |      ingested_at       | latency_sec
------------------+---------------------+---------------------+-----------+------------+------------------------+-------------
 LATENCYPROBEUSDT | 2026-11-19 15:06:00 | 2026-11-19 15:07:00 |  12345.67 |          1 | 2026-11-19 15:09:00.01 |  135.641074
(1 row)

latency_sec_powershell=135.641074
```

Result: **135.641074 s** (2 m 15.6 s) from `event_time` to `ingested_at`. This matches the design
in 0.6: 1-minute watermark + 30 s trigger + JDBC write, plus the 5 s deliberately added to
`event_time`.

### 3.4 Restore

```
> Remove-Item Env:SIM_EXTRA_SYMBOLS -ErrorAction SilentlyContinue
> Write-Output "SIM_EXTRA_SYMBOLS_after_remove=$env:SIM_EXTRA_SYMBOLS"
SIM_EXTRA_SYMBOLS_after_remove=
> docker compose up -d --force-recreate --no-deps producer
 Container streaming-pipeline-producer-1 Recreate
 Container streaming-pipeline-producer-1 Recreated
 Container streaming-pipeline-producer-1 Starting
 Container streaming-pipeline-producer-1 Started
> docker compose exec -T producer printenv SIM_EXTRA_SYMBOLS
0
> docker compose exec -T airflow-webserver airflow dags unpause streaming_analytics
dag_id              | is_paused
====================+==========
streaming_analytics | False

> docker compose ps --format "table {{.Service}}\t{{.Status}}"
SERVICE             STATUS
airflow-db          Up 2 hours (healthy)
airflow-scheduler   Up 2 hours (healthy)
airflow-webserver   Up 2 hours (healthy)
dashboard           Up 59 minutes (healthy)
kafka               Up 2 hours (healthy)
lake                Up 2 hours (healthy)
producer            Up 11 seconds
spark               Up About an hour
warehouse           Up About an hour (healthy)

> docker compose exec -T airflow-webserver airflow dags list
dag_id              | fileloc                                 | owners  | is_paused
====================+=========================================+=========+==========
streaming_analytics | /opt/airflow/dags/analytics_pipeline.py | airflow | False
```

Nine services up, DAG unpaused, producer back to `SIM_EXTRA_SYMBOLS=0` (N=5).

### 3.5 Window count and lake footprint

```
> docker compose exec -T warehouse psql -U warehouse -d warehouse -c "select count(*) as windows, min(window_start) as first_window, max(window_start) as last_window from raw.price_windows;"
 windows |    first_window     |     last_window
---------+---------------------+---------------------
    4136 | 2026-11-19 02:16:00 | 2026-11-19 15:07:00
(1 row)
```

Paginated `list_objects_v2` over `raw/ticks/` (the repo's `lake_ls.py` is single-page, so an
inline read against the same `scripts/s3_client.py` was used to get all 366 objects):

```
> docker compose run --rm --no-deps -v "${PWD}/scripts:/app/scripts" producer python -c "import sys; sys.path.insert(0,'/app/scripts'); from s3_client import make_client,bucket_name; c=make_client(); b=bucket_name(); pg=c.get_paginator('list_objects_v2'); objs=[]; [objs.extend(p.get('Contents',[])) for p in pg.paginate(Bucket=b,Prefix='raw/ticks/')]; data=[o for o in objs if '_spark_metadata/' not in o['Key']]; meta=[o for o in objs if '_spark_metadata/' in o['Key']]; print('objects_total',len(objs)); print('objects_data_parquet',len(data)); print('objects_metadata',len(meta)); print('bytes_total',sum(o['Size'] for o in objs)); print('bytes_data_parquet',sum(o['Size'] for o in data)); print('oldest',min(o['LastModified'] for o in objs)); print('newest',max(o['LastModified'] for o in objs))"
objects_total 366
objects_data_parquet 262
objects_metadata 104
bytes_total 5093976
bytes_data_parquet 4514829
oldest 2026-11-19 02:17:04+00:00
newest 2026-11-19 15:10:00+00:00

> (computation from those two timestamps and bytes_total)
observed_span_hours=12.882222
observed_bytes=5093976
observed_mib=4.858
rate_bytes_per_hour=395426.8
projected_24h_bytes=9490243
projected_24h_mib=9.051
```

**This is a projection, not a measured 24-hour figure.** 5,093,976 B (4.858 MiB) observed over
12.88 h, linearly projected to ~9.05 MiB / 24 h (parquet data alone: 4,514,829 B). The observed
window includes the ~4-minute N=505 throughput test, so the per-hour rate is slightly inflated;
the real steady-state 24 h figure should be lower.

### 3.6 dbt test

```
> docker compose exec -T -w /opt/airflow/dbt airflow-webserver /home/airflow/dbt_venv/bin/dbt test --profiles-dir /opt/airflow/dbt
15:10:42  Running with dbt=1.11.0
15:10:42  Registered adapter: postgres=1.11.0
15:10:43  Found 6 models, 1 snapshot, 15 data tests, 1 seed, 1 source, 479 macros
15:10:43  Concurrency: 2 threads (target='dev')
15:10:43  1 of 15 START test assert_min_le_avg_le_max .................................... [RUN]
15:10:43  2 of 15 START test assert_prices_positive ...................................... [RUN]
15:10:43  1 of 15 PASS assert_min_le_avg_le_max .......................................... [PASS in 0.09s]
15:10:43  2 of 15 PASS assert_prices_positive ............................................ [PASS in 0.09s]
15:10:43  3 of 15 START test assert_stg_unique_symbol_window ............................. [RUN]
15:10:43  4 of 15 START test not_null_dim_symbol_asset_class ............................. [RUN]
15:10:43  4 of 15 PASS not_null_dim_symbol_asset_class ................................... [PASS in 0.06s]
15:10:43  5 of 15 START test not_null_dim_symbol_symbol .................................. [RUN]
15:10:43  3 of 15 PASS assert_stg_unique_symbol_window ................................... [PASS in 0.07s]
15:10:43  6 of 15 START test not_null_dim_symbol_symbol_key .............................. [RUN]
15:10:43  5 of 15 PASS not_null_dim_symbol_symbol ........................................ [PASS in 0.06s]
15:10:43  6 of 15 PASS not_null_dim_symbol_symbol_key .................................... [PASS in 0.04s]
15:10:43  7 of 15 START test not_null_fct_price_1m_symbol_key ............................ [RUN]
15:10:43  8 of 15 START test not_null_mart_hourly_prices_symbol .......................... [RUN]
15:10:43  7 of 15 PASS not_null_fct_price_1m_symbol_key .................................. [PASS in 0.05s]
15:10:43  8 of 15 PASS not_null_mart_hourly_prices_symbol ................................. [PASS in 0.05s]
15:10:43  9 of 15 START test not_null_mart_symbol_history_valid_from ..................... [RUN]
15:10:43  10 of 15 START test not_null_stg_price_windows_avg_price ....................... [RUN]
15:10:43  9 of 15 PASS not_null_mart_symbol_history_valid_from ........................... [PASS in 0.06s]
15:10:43  11 of 15 START test not_null_stg_price_windows_symbol .......................... [RUN]
15:10:43  10 of 15 PASS not_null_stg_price_windows_avg_price ............................. [PASS in 0.07s]
15:10:43  12 of 15 START test not_null_stg_price_windows_window_start .................... [RUN]
15:10:43  11 of 15 PASS not_null_stg_price_windows_symbol ................................ [PASS in 0.06s]
15:10:43  13 of 15 START test relationships_fct_price_1m_symbol_key__symbol_key__ref_dim_symbol_  [RUN]
15:10:43  12 of 15 PASS not_null_stg_price_windows_window_start .......................... [PASS in 0.05s]
15:10:43  14 of 15 START test unique_dim_symbol_symbol ................................... [RUN]
15:10:43  14 of 15 PASS unique_dim_symbol_symbol ......................................... [PASS in 0.05s]
15:10:43  13 of 15 PASS relationships_fct_price_1m_symbol_key__symbol_key__ref_dim_symbol_  [PASS in 0.06s]
15:10:43  15 of 15 START test unique_dim_symbol_symbol_key ............................... [RUN]
15:10:43  15 of 15 PASS unique_dim_symbol_symbol_key ..................................... [PASS in 0.05s]
15:10:43  Finished running 15 data tests in 0 hours 0 minutes and 0.73 seconds (0.73s).
15:10:43  Completed successfully
15:10:43  Done. PASS=15 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=15
```

### 3.7 Unit tests

```
> docker compose run --rm --no-deps -v "${PWD}/tests:/app/tests" -v "${PWD}/dbt:/app/dbt" producer sh -c "pip install -q pytest==8.3.2 && python -m pytest -q"
 Container streaming-pipeline-producer-run-9b459a7bf123 Creating
 Container streaming-pipeline-producer-run-9b459a7bf123 Created
WARNING: Running pip as the 'root' user can result in broken permissions and conflicting behaviour with the system package manager. It is recommended to use a virtual environment instead: https://pip.pypa.io/warnings/venv

[notice] A new release of pip is available: 24.0 -> 26.3
[notice] To update, run: pip install --upgrade pip
............                                                             [100%]
12 passed in 0.99s
```

### 3.8 Phase 3 status

- [x] DAG paused, N=505 throughput measured from Kafka end offsets, DAG unpaused and restored
- [x] Probe tick with known `event_time` reached `raw.price_windows`; latency computed
- [x] 9 services up after restore
- [x] Window count and lake footprint measured; 24 h figure explicitly labelled a projection
- [x] dbt test: 15 passed
- [x] Unit tests: 12 passed
- [x] No commit made; only this file and the README Results table edited

---

## Phase 4 — dbt quality

All commands below were run on 2026-11-19 on this machine (Docker Desktop 29.8.0). PowerShell
unless a `sh -c` is shown. ANSI colour codes and trailing spaces were stripped from dbt output
for readability; the text is otherwise verbatim. No command printed a credential, so there is nothing to mask with
`***`. Only `dbt/`, `README.md` and this file were edited; no Spark, producer, Docker or pytest
file was changed. Existing evidence above was left untouched.

### 4.0 Baseline

```
> git status --short; git log -3 --oneline
8c50bad docs: phase 3 measured results (throughput, latency, windows, lake, tests)
93b141a feat(dlq): prove dead-letter reasons, replay repair and sink-failure recovery
93a555e wip: phase 2 dead-letter and sink-failure handling (coded, partly verified)
```
(`git status --short` produced no output — clean working tree.)

```
> docker compose ps
NAME                                     IMAGE                                                                                         COMMAND                  SERVICE             CREATED             STATUS                       PORTS
streaming-pipeline-airflow-db-1          postgres:16.16                                                                                "docker-entrypoint.s…"   airflow-db          13 hours ago        Up 2 hours (healthy)         5432/tcp
streaming-pipeline-airflow-scheduler-1   streaming-pipeline-airflow:2.10.5-dbt1.11                                                     "/usr/bin/dumb-init …"   airflow-scheduler   13 hours ago        Up 2 hours (healthy)         8080/tcp
streaming-pipeline-airflow-webserver-1   streaming-pipeline-airflow:2.10.5-dbt1.11                                                     "/usr/bin/dumb-init …"   airflow-webserver   13 hours ago        Up 2 hours (healthy)         0.0.0.0:8080->8080/tcp, [::]:8080->8080/tcp
streaming-pipeline-dashboard-1           streaming-pipeline-app:1.0.0                                                                  "streamlit run dashb…"   dashboard            About an hour ago   Up About an hour (healthy)   0.0.0.0:8501->8501/tcp, [::]:8501->8501/tcp
streaming-pipeline-kafka-1               apache/kafka:3.8.0                                                                            "/__cacert_entrypoin…"   kafka               13 hours ago        Up 2 hours (healthy)         9092/tcp
streaming-pipeline-lake-1                chrislusf/seaweedfs@sha256:4b42c212543a0acd4c0e675215ecdcaf62a186ef98def76c7e12701871222d15   "/entrypoint.sh serv…"   lake                13 hours ago        Up 2 hours (healthy)         0.0.0.0:8333->8333/tcp, [::]:8333->8333/tcp, 0.0.0.0:8888->8888/tcp, [::]:8888->8888/tcp, 0.0.0.0:9333->9333/tcp, [::]:9333->9333/tcp
streaming-pipeline-producer-1            streaming-pipeline-app:1.0.0                                                                  "python -m producer.…"   producer            17 minutes ago      Up 17 minutes
streaming-pipeline-spark-1               streaming-pipeline-spark:3.5.3                                                                "/opt/entrypoint.sh …"   spark               13 hours ago        Up About an hour
streaming-pipeline-warehouse-1           postgres:16.16                                                                                "docker-entrypoint.s…"   warehouse           13 hours ago        Up About an hour (healthy)   0.0.0.0:5433->5432/tcp, [::]:5433->5432/tcp
```

### 4.1 BEFORE — the deprecation warning

A normal `dbt test` printed no warning because dbt's partial-parse cache had already parsed the
files. Forcing a full parse surfaces it:

```
> docker compose exec -T -w /opt/airflow/dbt airflow-scheduler /home/airflow/dbt_venv/bin/dbt test --profiles-dir /opt/airflow/dbt 2>&1 | ForEach-Object { $_ -replace "\x1b\[[0-9;]*m","" }
15:28:43  Running with dbt=1.11.0
15:28:43  Registered adapter: postgres=1.11.0
15:28:44  Found 6 models, 1 snapshot, 15 data tests, 1 seed, 1 source, 479 macros
...
15:28:45  Done. PASS=15 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=15
```

```
> docker compose exec -T -w /opt/airflow/dbt airflow-scheduler sh -c "/home/airflow/dbt_venv/bin/dbt --no-partial-parse test --profiles-dir /opt/airflow/dbt > /tmp/dbt_before.txt 2>&1; grep -inE 'deprecat|warn' /tmp/dbt_before.txt | head -40; echo '--- FAILED_SUMMARY ---'; tail -12 /tmp/dbt_before.txt"
3:15:29:29  [WARNING][MissingArgumentsPropertyInGenericTestDeprecation]: Deprecated
47:15:29:31  Done. PASS=15 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=15
48:15:29:31  [WARNING][DeprecationsSummary]: Deprecated functionality
49:Summary of encountered deprecations:
50:- MissingArgumentsPropertyInGenericTestDeprecation: 1 occurrence
51:To see all deprecation instances instead of just the first occurrence of each,
52:run command again with the `--show-all-deprecations` flag. You may also need to
53:run with `--no-partial-parse` as some deprecations are only encountered during
```

```
> docker compose exec -T -w /opt/airflow/dbt airflow-scheduler sh -c "/home/airflow/dbt_venv/bin/dbt --no-partial-parse --show-all-deprecations test --profiles-dir /opt/airflow/dbt 2>&1 | sed -E 's/\x1B\[[0-9;]*[mK]//g' | head -22"
15:29:41  Running with dbt=1.11.0
15:29:41  Registered adapter: postgres=1.11.0
15:29:43  [WARNING][MissingArgumentsPropertyInGenericTestDeprecation]: Deprecated
functionality
Found top-level arguments to test `relationships` defined on 'fct_price_1m' in
package 'streaming_pipeline' (models/schema.yml). Arguments to generic tests
should be nested under the `arguments` property.
15:29:44  Found 6 models, 1 snapshot, 15 data tests, 1 seed, 1 source, 479 macros
15:29:44
15:29:44  Concurrency: 2 threads (target='dev')
15:29:44
15:29:44  1 of 15 START test assert_min_le_avg_le_max .................................... [RUN]
15:29:44  2 of 15 START test assert_prices_positive ...................................... [RUN]
15:29:44  2 of 15 PASS assert_prices_positive ............................................ [PASS in 0.09s]
15:29:44  1 of 15 PASS assert_min_le_avg_le_max .......................................... [PASS in 0.09s]
15:29:44  3 of 15 START test assert_stg_unique_symbol_window ............................. [RUN]
15:29:44  4 of 15 START test not_null_dim_symbol_asset_class ............................. [RUN]
15:29:44  4 of 15 PASS not_null_dim_symbol_asset_class ................................... [PASS in 0.12s]
15:29:44  5 of 15 START test not_null_dim_symbol_symbol .................................. [RUN]
15:29:44  3 of 15 PASS assert_stg_unique_symbol_window ................................... [PASS in 0.15s]
15:29:44  6 of 15 START test not_null_dim_symbol_symbol_key .............................. [RUN]
15:29:44  5 of 15 PASS not_null_dim_symbol_symbol ........................................ [PASS in 0.05s]
...
```

### 4.2 Change made and AFTER — no deprecation warning

`dbt/models/schema.yml` now nests the `relationships` arguments:

```yaml
        - not_null
        - relationships:
            arguments:
              to: ref('dim_symbol')
              field: symbol_key
```

`sources.yml`, `dbt_project.yml` and `profiles.yml` contain no generic-test arguments, so no
other yml needed a syntax change.

```
> docker compose exec -T -w /opt/airflow/dbt airflow-scheduler sh -c "/home/airflow/dbt_venv/bin/dbt --no-partial-parse --show-all-deprecations test --profiles-dir /opt/airflow/dbt 2>&1 | sed -E 's/\x1B\[[0-9;]*[mK]//g'"
15:30:54  Running with dbt=1.11.0
15:30:55  Registered adapter: postgres=1.11.0
15:30:57  Found 6 models, 1 snapshot, 15 data tests, 1 seed, 1 source, 479 macros
15:30:57
15:30:57  Concurrency: 2 threads (target='dev')
15:30:57
15:30:57  1 of 15 START test assert_min_le_avg_le_max .................................... [RUN]
15:30:57  2 of 15 START test assert_prices_positive ...................................... [RUN]
15:30:58  2 of 15 PASS assert_prices_positive ............................................ [PASS in 0.09s]
15:30:58  1 of 15 PASS assert_min_le_avg_le_max .......................................... [PASS in 0.12s]
15:30:58  3 of 15 START test assert_source_price_windows_unique .......................... [RUN]
15:30:58  4 of 15 START test not_null_dim_symbol_asset_class ............................. [RUN]
15:30:58  4 of 15 PASS not_null_dim_symbol_asset_class ................................... [PASS in 0.12s]
15:30:58  5 of 15 START test not_null_dim_symbol_symbol .................................. [RUN]
15:30:58  3 of 15 PASS assert_source_price_windows_unique ................................ [PASS in 0.13s]
15:30:58  6 of 15 START test not_null_dim_symbol_symbol_key .............................. [RUN]
15:30:58  6 of 15 PASS not_null_dim_symbol_symbol_key .................................... [PASS in 0.05s]
15:30:58  5 of 15 PASS not_null_dim_symbol_symbol ........................................ [PASS in 0.05s]
15:30:58  7 of 15 START test not_null_fct_price_1m_symbol_key ............................ [RUN]
15:30:58  8 of 15 START test not_null_mart_hourly_prices_symbol .......................... [RUN]
15:30:58  7 of 15 PASS not_null_fct_price_1m_symbol_key .................................. [PASS in 0.05s]
15:30:58  8 of 15 PASS not_null_mart_hourly_prices_symbol ................................. [PASS in 0.05s]
15:30:58  9 of 15 START test not_null_mart_symbol_history_valid_from ..................... [RUN]
15:30:58  10 of 15 START test not_null_stg_price_windows_avg_price ....................... [RUN]
15:30:58  9 of 15 PASS not_null_mart_symbol_history_valid_from ........................... [PASS in 0.05s]
15:30:58  11 of 15 START test not_null_stg_price_windows_symbol .......................... [RUN]
15:30:58  10 of 15 PASS not_null_stg_price_windows_avg_price ............................. [PASS in 0.06s]
15:30:58  12 of 15 START test not_null_stg_price_windows_window_start .................... [RUN]
15:30:58  11 of 15 PASS not_null_stg_price_windows_symbol ................................ [PASS in 0.05s]
15:30:58  13 of 15 START test relationships_fct_price_1m_symbol_key__symbol_key__ref_dim_symbol_  [RUN]
15:30:58  12 of 15 PASS not_null_stg_price_windows_window_start .......................... [PASS in 0.06s]
15:30:58  14 of 15 START test unique_dim_symbol_symbol ................................... [RUN]
15:30:58  13 of 15 PASS relationships_fct_price_1m_symbol_key__symbol_key__ref_dim_symbol_  [PASS in 0.06s]
15:30:58  15 of 15 START test unique_dim_symbol_symbol_key ............................... [RUN]
15:30:58  14 of 15 PASS unique_dim_symbol_symbol ......................................... [PASS in 0.06s]
15:30:58  15 of 15 PASS unique_dim_symbol_symbol_key ..................................... [PASS in 0.05s]
15:30:58
15:30:58  Finished running 15 data tests in 0 hours 0 minutes and 0.74 seconds (0.74s).
15:30:58
15:30:58  Completed successfully
15:30:58
15:30:58  Done. PASS=15 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=15
```

No `WARNING` / `DeprecationsSummary` line appears. (The `--no-partial-parse` flag was kept in
this verification run so the comparison is apples-to-apples with 4.1.)

### 4.3 Meaningful uniqueness test — source table, severity warn, scratch-proofed

`dbt/tests/assert_stg_unique_symbol_window.sql` was removed (the staging view dedupes, so it
could never fail) and replaced by `dbt/tests/assert_source_price_windows_unique.sql`:

```sql
-- Spark writes raw.price_windows at-least-once: after a restart the same
-- (symbol, window_start) can legitimately appear twice. stg_price_windows
-- deduplicates, so testing uniqueness there can never fail -- test the
-- append-only source instead and report duplicates as a warning.
--
-- The relation is a var so the exact same test can be pointed at a scratch
-- copy for the proof, without inserting into or deleting from real rows.
{{ config(severity='warn') }}

select
    upper(symbol) as symbol,
    window_start,
    count(*) as duplicate_count
from {{ var('price_windows_uniqueness_relation', source('raw', 'price_windows')) }}
group by 1, 2
having count(*) > 1
```

The default relation is the real source. Baseline — the real source currently has no duplicates:

```
> docker compose exec -T warehouse psql -U warehouse -d warehouse -c "select count(*) as total_rows, count(distinct (upper(symbol), window_start)) as distinct_pairs from raw.price_windows;"
 total_rows | distinct_pairs
------------+----------------
       5236 |           5236
(1 row)
```

Scratch table: an empty copy of the real table plus two identical rows. No real row was
inserted, updated or deleted.

```
> docker compose exec -T warehouse psql -U warehouse -d warehouse -c "create table raw.price_windows_dup_probe as select * from raw.price_windows where false;"
SELECT 0
> docker compose exec -T warehouse psql -U warehouse -d warehouse -c "insert into raw.price_windows_dup_probe select * from raw.price_windows order by ingested_at desc limit 1; insert into raw.price_windows_dup_probe select * from raw.price_windows order by ingested_at desc limit 1; select symbol, window_start, count(*) as n from raw.price_windows_dup_probe group by 1,2;"
INSERT 0 1
INSERT 0 1
 symbol  |    window_start     | n
---------+---------------------+---
 SOLUSDT | 2026-11-19 15:28:00 | 2
(1 row)
```

The same dbt test, pointed at the scratch table with `--vars` (no file edit between runs):

```
> docker compose exec -T -w /opt/airflow/dbt airflow-scheduler /home/airflow/dbt_venv/bin/dbt test --profiles-dir /opt/airflow/dbt --select assert_source_price_windows_unique --vars '{price_windows_uniqueness_relation: raw.price_windows_dup_probe}' 2>&1 | ForEach-Object { $_ -replace "\x1b\[[0-9;]*m","" }; "dbt_exit=$LASTEXITCODE"
15:31:38  Running with dbt=1.11.0
15:31:38  Registered adapter: postgres=1.11.0
15:31:38  Unable to do partial parsing because config vars, config profile, or config target have changed
15:31:41  Found 6 models, 1 snapshot, 15 data tests, 1 seed, 1 source, 479 macros
15:31:41
15:31:41  Concurrency: 2 threads (target='dev')
15:31:41
15:31:41  1 of 1 START test assert_source_price_windows_unique ........................... [RUN]
15:31:41  1 of 1 WARN 1 assert_source_price_windows_unique ............................... [WARN 1 in 0.07s]
15:31:41
15:31:41  Finished running 1 test in 0 hours 0 minutes and 0.34 seconds (0.34s).
15:31:41
15:31:41  Completed with 1 warning:
15:31:41
15:31:41  Warning in test assert_source_price_windows_unique (tests/assert_source_price_windows_unique.sql)
15:31:41  Got 1 result, configured to warn if != 0
15:31:41
15:31:41    compiled code at target/compiled/streaming_pipeline/tests/assert_source_price_windows_unique.sql
15:31:41
15:31:41  Done. PASS=0 WARN=1 ERROR=0 SKIP=0 NO-OP=0 TOTAL=1
dbt_exit=0
```

The test detects the duplicate (`WARN 1`, one result, `severity: warn`). dbt exits **0** on a
warning: duplicates are expected with at-least-once writes, so the test is informative rather
than a pipeline breaker. On the real source (no duplicates) the same test passes — see 4.2 and
4.4.

```
> docker compose exec -T warehouse psql -U warehouse -d warehouse -c "drop table raw.price_windows_dup_probe;"
DROP TABLE
> docker compose exec -T warehouse psql -U warehouse -d warehouse -c "select to_regclass('raw.price_windows_dup_probe') as scratch_table, count(*) as real_rows, count(distinct (upper(symbol), window_start)) as real_distinct_pairs from raw.price_windows;"
 scratch_table | real_rows | real_distinct_pairs
---------------+-----------+---------------------
               |      5246 |                5246
(1 row)
```

`scratch_table` is NULL (dropped). `real_rows` grew 5236 -> 5246 because the live Spark stream
keeps appending windows; `real_distinct_pairs` equals `real_rows`, so no duplicate was left
behind.

### 4.4 Full dbt pipeline inside airflow-scheduler

```
> docker compose exec -T -w /opt/airflow/dbt airflow-scheduler /home/airflow/dbt_venv/bin/dbt seed --profiles-dir /opt/airflow/dbt 2>&1 | ForEach-Object { $_ -replace "\x1b\[[0-9;]*m","" }
15:32:30  Running with dbt=1.11.0
15:32:31  Registered adapter: postgres=1.11.0
15:32:31  Unable to do partial parsing because config vars, config profile, or config target have changed
15:32:34  Found 6 models, 1 snapshot, 15 data tests, 1 seed, 1 source, 479 macros
15:32:34
15:32:34  Concurrency: 2 threads (target='dev')
15:32:34
15:32:34  1 of 1 START seed file reference.symbol_metadata ............................... [RUN]
15:32:34  1 of 1 OK loaded seed file reference.symbol_metadata ........................... [INSERT 5 in 0.18s]
15:32:34
15:32:34  Finished running 1 seed in 0 hours 0 minutes and 0.37 seconds (0.37s).
15:32:34
15:32:34  Completed successfully
15:32:34
15:32:34  Done. PASS=1 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=1
```

```
> docker compose exec -T -w /opt/airflow/dbt airflow-scheduler /home/airflow/dbt_venv/bin/dbt snapshot --profiles-dir /opt/airflow/dbt 2>&1 | ForEach-Object { $_ -replace "\x1b\[[0-9;]*m","" }
15:32:45  Running with dbt=1.11.0
15:32:45  Registered adapter: postgres=1.11.0
15:32:46  Found 6 models, 1 snapshot, 15 data tests, 1 seed, 1 source, 479 macros
15:32:46
15:32:46  Concurrency: 2 threads (target='dev')
15:32:46
15:32:46  1 of 1 START snapshot snapshots.symbol_metadata_snapshot ....................... [RUN]
15:32:46  1 of 1 OK snapshotted snapshots.symbol_metadata_snapshot ....................... [INSERT 0 0 in 0.27s]
15:32:46
15:32:46  Finished running 1 snapshot in 0 hours 0 minutes and 0.44 seconds (0.44s).
15:32:46
15:32:46  Completed successfully
15:32:46
15:32:46  Done. PASS=1 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=1
```

```
> docker compose exec -T -w /opt/airflow/dbt airflow-scheduler /home/airflow/dbt_venv/bin/dbt run --profiles-dir /opt/airflow/dbt 2>&1 | ForEach-Object { $_ -replace "\x1b\[[0-9;]*m","" }
15:32:57  Running with dbt=1.11.0
15:32:57  Registered adapter: postgres=1.11.0
15:32:58  Found 6 models, 1 snapshot, 15 data tests, 1 seed, 1 source, 479 macros
15:32:58
15:32:58  Concurrency: 2 threads (target='dev')
15:32:58
15:32:58  1 of 6 START sql table model marts.mart_symbol_history ......................... [RUN]
15:32:58  2 of 6 START sql view model staging.stg_price_windows .......................... [RUN]
15:32:58  2 of 6 OK created sql view model staging.stg_price_windows ..................... [CREATE VIEW in 0.19s]
15:32:58  1 of 6 OK created sql table model marts.mart_symbol_history .................... [SELECT 5 in 0.20s]
15:32:58  3 of 6 START sql table model marts.dim_symbol .................................. [RUN]
15:32:58  3 of 6 OK created sql table model marts.dim_symbol ............................. [SELECT 506 in 0.08s]
15:32:58  4 of 6 START sql table model marts.fct_price_1m ................................ [RUN]
15:32:59  4 of 6 OK created sql table model marts.fct_price_1m ........................... [SELECT 5246 in 0.12s]
15:32:59  5 of 6 START sql table model marts.mart_hourly_prices .......................... [RUN]
15:32:59  6 of 6 START sql table model marts.mart_volatility_alerts ...................... [RUN]
15:32:59  6 of 6 OK created sql table model marts.mart_volatility_alerts ................. [SELECT 4454 in 0.16s]
15:32:59  5 of 6 OK created sql table model marts.mart_hourly_prices ..................... [SELECT 521 in 0.16s]
15:32:59
15:32:59  Finished running 5 table models, 1 view model in 0 hours 0 minutes and 0.79 seconds (0.79s).
15:32:59
15:32:59  Completed successfully
15:32:59
15:32:59  Done. PASS=6 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=6
```

```
> docker compose exec -T -w /opt/airflow/dbt airflow-scheduler /home/airflow/dbt_venv/bin/dbt test --profiles-dir /opt/airflow/dbt 2>&1 | ForEach-Object { $_ -replace "\x1b\[[0-9;]*m","" }; "dbt_exit=$LASTEXITCODE"
15:33:17  Running with dbt=1.11.0
15:33:18  Registered adapter: postgres=1.11.0
15:33:19  Found 6 models, 1 snapshot, 15 data tests, 1 seed, 1 source, 479 macros
15:33:19
15:33:19  Concurrency: 2 threads (target='dev')
15:33:19
15:33:19  1 of 15 START test assert_min_le_avg_le_max .................................... [RUN]
15:33:19  2 of 15 START test assert_prices_positive ...................................... [RUN]
15:33:19  2 of 15 PASS assert_prices_positive ............................................ [PASS in 0.11s]
15:33:19  1 of 15 PASS assert_min_le_avg_le_max .......................................... [PASS in 0.12s]
15:33:19  3 of 15 START test assert_source_price_windows_unique .......................... [RUN]
15:33:19  4 of 15 START test not_null_dim_symbol_asset_class ............................. [RUN]
15:33:19  4 of 15 PASS not_null_dim_symbol_asset_class ................................... [PASS in 0.06s]
15:33:19  5 of 15 START test not_null_dim_symbol_symbol .................................. [RUN]
15:33:19  3 of 15 PASS assert_source_price_windows_unique ................................ [PASS in 0.08s]
15:33:19  6 of 15 START test not_null_dim_symbol_symbol_key .............................. [RUN]
15:33:19  6 of 15 PASS not_null_dim_symbol_symbol_key .................................... [PASS in 0.05s]
15:33:19  5 of 15 PASS not_null_dim_symbol_symbol ........................................ [PASS in 0.06s]
15:33:19  7 of 15 START test not_null_fct_price_1m_symbol_key ............................ [RUN]
15:33:19  8 of 15 START test not_null_mart_hourly_prices_symbol .......................... [RUN]
15:33:19  8 of 15 PASS not_null_mart_hourly_prices_symbol ................................. [PASS in 0.08s]
15:33:19  7 of 15 PASS not_null_fct_price_1m_symbol_key .................................. [PASS in 0.09s]
15:33:19  9 of 15 START test not_null_mart_symbol_history_valid_from ..................... [RUN]
15:33:19  10 of 15 START test not_null_stg_price_windows_avg_price ....................... [RUN]
15:33:19  9 of 15 PASS not_null_mart_symbol_history_valid_from ........................... [PASS in 0.08s]
15:33:19  11 of 15 START test not_null_stg_price_windows_symbol .......................... [RUN]
15:33:19  10 of 15 PASS not_null_stg_price_windows_avg_price ............................. [PASS in 0.10s]
15:33:19  12 of 15 START test not_null_stg_price_windows_window_start .................... [RUN]
15:33:19  11 of 15 PASS not_null_stg_price_windows_symbol ................................ [PASS in 0.06s]
15:33:19  13 of 15 START test relationships_fct_price_1m_symbol_key__symbol_key__ref_dim_symbol_  [RUN]
15:33:19  12 of 15 PASS not_null_stg_price_windows_window_start .......................... [PASS in 0.06s]
15:33:19  14 of 15 START test unique_dim_symbol_symbol ................................... [RUN]
15:33:19  13 of 15 PASS relationships_fct_price_1m_symbol_key__symbol_key__ref_dim_symbol_  [PASS in 0.08s]
15:33:19  14 of 15 PASS unique_dim_symbol_symbol ......................................... [PASS in 0.06s]
15:33:19  15 of 15 START test unique_dim_symbol_symbol_key ............................... [RUN]
15:33:20  15 of 15 PASS unique_dim_symbol_symbol_key ..................................... [PASS in 0.11s]
15:33:20
15:33:20  Finished running 15 data tests in 0 hours 0 minutes and 0.95 seconds (0.95s).
15:33:20
15:33:20  Completed successfully
15:33:20
15:33:20  Done. PASS=15 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=15
dbt_exit=0
```

All four commands succeeded: seed 1/1, snapshot 1/1, run 6/6, test 15/15 PASS (the new source
test is PASS because the live source has no duplicates right now). WARN=0 here; the WARN path was
proved separately in 4.3 with the scratch table.

### 4.5 Unit tests (Phase 2 command, unchanged)

```
> docker compose run --rm --no-deps -v "${PWD}/tests:/app/tests" -v "${PWD}/dbt:/app/dbt" producer sh -c "pip install -q pytest==8.3.2 && python -m pytest -q" 2>&1 | ForEach-Object { $_ -replace "\x1b\[[0-9;]*m","" }; "pytest_exit=$LASTEXITCODE"
 Container streaming-pipeline-producer-run-9c888934e3f7 Creating
 Container streaming-pipeline-producer-run-9c888934e3f7 Created
WARNING: Running pip as the 'root' user can result in broken permissions and conflicting behaviour with the system package manager. It is recommended to use a virtual environment instead: https://pip.pypa.io/warnings/venv
System.Management.Automation.RemoteException
[notice] A new release of pip is available: 24.0 -> 26.3
[notice] To update, run: pip install --upgrade pip
............                                                             [100%]
12 passed in 0.77s
pytest_exit=0
```

`System.Management.Automation.RemoteException` is PowerShell's wrapper for pip's stderr notice,
not a test error; `12 passed` and exit 0 are the result. No test was weakened or deleted.

### 4.6 Evidence behind the README fixes

Spark jars are in the image, and the current Spark log contains no resolution/download lines:

```
> docker compose exec -T spark sh -c "ls -1 /opt/spark/jars | grep -E 'spark-sql-kafka|kafka-clients|hadoop-aws|aws-java-sdk-bundle|postgresql'"
aws-java-sdk-bundle-1.12.262.jar
hadoop-aws-3.3.4.jar
kafka-clients-3.4.1.jar
postgresql-42.7.3.jar
spark-sql-kafka-0-10_2.12-3.5.3.jar
> $hits = docker compose logs spark 2>&1 | Select-String -Pattern "Ivy|downloading|Resolving dependencies|--packages"; "ivy_or_download_log_lines=$($hits.Count)"; docker compose logs spark 2>&1 | Select-Object -First 4
ivy_or_download_log_lines=0
spark-1  | 26/11/19 02:16:43 INFO SparkContext: Running Spark version 3.5.3
spark-1  | 26/11/19 02:16:43 INFO SparkContext: OS info Linux, 6.18.33.2-microsoft-standard-WSL2, amd64
spark-1  | 26/11/19 02:16:43 INFO SparkContext: Java version 17.0.12
spark-1  | 26/11/19 02:16:43 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
```
(That command exits 1 in PowerShell because `docker compose logs` writes to stderr; the printed
output is complete.)

The "~30 s cold start" figure comes from the measured full-stack boots in **1.2** earlier in
this session, re-quoted here (not re-run in Phase 4, to avoid restarting the live stream):

| Boot | Command | Result | Time |
|---|---|---|---|
| 1 | `docker compose down` + `up -d --build` | 9 services up | 33 s |
| 2 | `.env` moved out of the repo, `down` + `up -d` | 9 services up, `compose config` still OK | 36 s |
| 3 | `down` + `up -d` | 9 services up | 33 s |
| 4 | `down -v` (all volumes wiped) + `up -d` | 9 services up, DAG unpaused | 26 s |

N=505 = 5 base symbols + 500 extra, and the throughput measurement is from **3.2** earlier in
this session (re-quoted, not re-run, to avoid reintroducing the 505-symbol load):

```
> Get-Content docker-compose.yml | Select-Object -Skip 107 -First 6; docker compose exec -T producer printenv SYMBOLS SIM_EXTRA_SYMBOLS
      <<: *app-env
      SOURCE: ${SOURCE:-simulated}
      SYMBOLS: ${SYMBOLS:-BTCUSDT,ETHUSDT,BNBUSDT,SOLUSDT,XRPUSDT}
      POLL_INTERVAL_SEC: ${POLL_INTERVAL_SEC:-1}
      SIM_EXTRA_SYMBOLS: ${SIM_EXTRA_SYMBOLS:-0}
      PYTHONUNBUFFERED: "1"
BTCUSDT,ETHUSDT,BNBUSDT,SOLUSDT,XRPUSDT
0
```

```
> $env:SIM_EXTRA_SYMBOLS='500'; docker compose up -d --force-recreate --no-deps producer
  Container streaming-pipeline-producer-1 Recreate
  Container streaming-pipeline-producer-1 Recreated
  Container streaming-pipeline-producer-1 Starting
  Container streaming-pipeline-producer-1 Started
SIM_EXTRA_SYMBOLS=500
> docker compose logs --tail 8 producer
producer-1  | 2026-11-19 15:01:50,506 INFO producer - sent=2525 rejected=0
producer-1  | 2026-11-19 15:01:51,514 INFO producer - sent=3030 rejected=0
producer-1  | 2026-11-19 15:01:52,502 INFO producer - sent=3535 rejected=0
producer-1  | 2026-11-19 15:01:53,502 INFO producer - sent=4040 rejected=0
producer-1  | 2026-11-19 15:01:54,512 INFO producer - sent=4545 rejected=0
producer-1  | 2026-11-19 15:01:55,513 INFO producer - sent=5050 rejected=0
producer-1  | 2026-11-19 15:01:56,501 INFO producer - sent=5555 rejected=0
producer-1  | 2026-11-19 15:01:57,502 INFO producer - sent=6060 rejected=0
> (60 s Kafka end-offset measurement from 3.2)
t0=17:04:57.567 kafka_end=132211 producer_sent=96960
t1=17:05:59.288 kafka_end=163521 producer_sent=127765
kafka_delta=31310 producer_delta=30805 elapsed=61.7208793
kafka_events_per_sec=507.28 producer_events_per_sec=499.1
```

README lines now read: ~30 s cold start / jars baked in; `SIM_EXTRA_SYMBOLS=500` -> ~505
events/sec (5 + 500); latency "by design: 1-min watermark + 30 s trigger"; lake "~9.05 MiB
**projection** from mostly the 5-symbol load, not 505". The design-decision bullet about the
staging uniqueness test was updated to describe the new source-level warn test.

### 4.7 Decisions, deviations and honest notes

- The deprecation warning is only emitted on a full parse. Normal runs after the fix were clean
  both with and without `--no-partial-parse`; 4.2 uses the stricter form.
- "Existing tests" was interpreted as the 12 pytest unit tests: those were left alone and still
  pass. The dbt singular test that could never fail was explicitly in scope for replacement;
  it was removed and a new one added, keeping the total at 15 data tests.
- A `var()` relation was used so the very same test SQL can be proven against a scratch table
  without changing the default (the real source) or touching real rows. This is a test-only
  parameter, not a weakening: no filter, threshold or tolerance was added.
- `severity: warn` is deliberate: at-least-once writes can legitimately duplicate, so a
  duplicate produces `WARN` (dbt exit 0) rather than `ERROR`. The live source has 0 duplicates
  now (4.0 proof: 5236 rows / 5236 pairs; later 5246 / 5246), hence the normal run is all-PASS.
- The cold-start and throughput figures in 4.6 are re-quoted from Phase 1 and Phase 3, where
  their commands and real outputs were already pasted in this file; they were not re-measured in
  Phase 4 so the live 505-symbol load was not reintroduced and Spark was not restarted.
- The lake-size row remains a projection: it is derived from a mostly 5-symbol window, and the
  README now says so explicitly. No real warehouse or lake row was deleted.
- Final post-commit `git log --oneline -3`, `git status --short` and `docker compose ps` were run
  after the commit and are reported in the chat response; the commit hash cannot be pasted into
  a file that is itself part of that commit. Pre-commit state is in 4.0.

### 4.8 Phase 4 status (before commit)

- [x] `relationships` arguments nested under `arguments:`; `MissingArgumentsPropertyInGenericTestDeprecation`
      gone on a forced full parse (1 occurrence before, 0 after)
- [x] Tautological staging uniqueness test replaced with a source-level `(symbol, window_start)`
      duplicate test at `severity: warn`; proven to emit `WARN 1` on a scratch duplicate, scratch
      object dropped afterwards
- [x] `dbt seed`, `dbt snapshot`, `dbt run`, `dbt test` all succeeded in `airflow-scheduler`
      (1/1, 1/1, 6/6, 15/15 PASS)
- [x] 12 unit tests pass with the Phase 2 Docker command
- [x] Two stale README lines and the latency/lake Results rows corrected
- [x] No step failed in Phase 4

---

## Phase 5 — CI and one-command verification

All commands below were run on 2026-11-20 in this single session (PowerShell on Windows 11,
Docker Desktop 29.8.0). ANSI colour codes were stripped from dbt output; the text is otherwise
verbatim. No command printed a credential, so there is nothing to mask with `***`. Existing
evidence above was left untouched. Only `.github/workflows/ci.yml`, `spark/streaming_job.py`
(one-line lint fix, see 5.5), `scripts/verify.ps1`, `README.md` and this file were edited.

### 5.0 Baseline, stack up, container state

```
> git status --short
(no output - clean working tree)

> git log -3 --oneline
c9595f3 feat(dbt): phase 4 test cleanup and README fixes
8c50bad docs: phase 3 measured results (throughput, latency, windows, lake, tests)
93b141a feat(dlq): prove dead-letter reasons, replay repair and sink-failure recovery

> docker compose ps
NAME                         IMAGE                            COMMAND                  SERVICE   CREATED        STATUS         PORTS
streaming-pipeline-spark-1   streaming-pipeline-spark:3.5.3   "/opt/entrypoint.sh …"   spark     36 hours ago   Up 9 minutes
```

`docker compose up -d` (exit code 1 in PowerShell is the known stderr quirk; container state is
the source of truth — this time it actually returned 0):

```
> docker compose up -d
 Container streaming-pipeline-spark-1 Running
 Container streaming-pipeline-airflow-db-1 Starting
 Container streaming-pipeline-warehouse-1 Starting
 Container streaming-pipeline-lake-1 Starting
 Container streaming-pipeline-kafka-1 Starting
 Container streaming-pipeline-airflow-db-1 Started
 Container streaming-pipeline-warehouse-1 Started
 Container streaming-pipeline-warehouse-1 Waiting
 Container streaming-pipeline-airflow-db-1 Waiting
 Container streaming-pipeline-warehouse-1 Waiting
 Container streaming-pipeline-lake-1 Started
 Container streaming-pipeline-lake-1 Waiting
 Container streaming-pipeline-kafka-1 Started
 Container streaming-pipeline-kafka-1 Waiting
 Container streaming-pipeline-airflow-db-1 Healthy
 Container streaming-pipeline-warehouse-1 Healthy
 Container streaming-pipeline-warehouse-1 Healthy
 Container streaming-pipeline-dashboard-1 Starting
 Container streaming-pipeline-airflow-init-1 Starting
 Container streaming-pipeline-dashboard-1 Started
 Container streaming-pipeline-airflow-init-1 Started
 Container streaming-pipeline-airflow-init-1 Waiting
 Container streaming-pipeline-airflow-init-1 Waiting
 Container streaming-pipeline-kafka-1 Healthy
 Container streaming-pipeline-producer-1 Starting
 Container streaming-pipeline-producer-1 Started
 Container streaming-pipeline-airflow-init-1 Exited
 Container streaming-pipeline-airflow-scheduler-1 Starting
 Container streaming-pipeline-airflow-init-1 Exited
 Container streaming-pipeline-airflow-webserver-1 Starting
 Container streaming-pipeline-airflow-scheduler-1 Started
 Container streaming-pipeline-airflow-webserver-1 Started
 Container streaming-pipeline-lake-1 Healthy
 Container streaming-pipeline-lake-init-1 Starting
 Container streaming-pipeline-lake-init-1 Started
 Container streaming-pipeline-kafka-1 Waiting
 Container streaming-pipeline-warehouse-1 Waiting
 Container streaming-pipeline-lake-init-1 Waiting
 Container streaming-pipeline-kafka-1 Healthy
 Container streaming-pipeline-warehouse-1 Healthy
 Container streaming-pipeline-lake-init-1 Exited
up_exit=0
```

Poll until all nine long-running services are Up (producer/spark have no healthcheck) — the
first poll already passed, well inside the 3-minute budget:

```
> (poll loop: docker compose ps --format "{{.Service}}|{{.Status}}", 10 s interval, 180 s deadline)
poll at 16:36:35: bad=0
ALL_UP_HEALTHY=True

> docker compose ps --format "table {{.Service}}\t{{.Status}}"
SERVICE             STATUS
airflow-db          Up About a minute (healthy)
airflow-scheduler   Up 46 seconds (healthy)
airflow-webserver   Up 46 seconds (healthy)
dashboard           Up About a minute (healthy)
kafka               Up About a minute (healthy)
lake                Up About a minute (healthy)
producer            Up 57 seconds
spark               Up 11 minutes
warehouse           Up About a minute (healthy)
```

### 5.1 Workflow rewrite

The old `python -m py_compile ...` step is gone. Final `.github/workflows/ci.yml` (full file):

```yaml
name: CI
on: [push, pull_request]

# Every job pins its tool versions (requirements files / explicit pip pins) and uses
# Python 3.11: the pinned dependencies do not build on the host's Python 3.14.
# The old `python -m py_compile ...` step is replaced by the five real checks below.

jobs:
  ruff:
    name: ruff check
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
      - run: pip install -r requirements-dev.txt
      - run: ruff check .

  pytest:
    name: pytest (12 unit tests)
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
      - run: pip install -r requirements-dev.txt -r requirements.txt
      - run: python -m pytest -q

  dbt-parse:
    name: dbt parse
    runs-on: ubuntu-latest
    # dbt/profiles.yml reads all connection values from env vars. These are dummy
    # values: `dbt parse` only compiles the project, it does not open a database
    # connection (proved locally with an unreachable WH_HOST), so no postgres
    # service container is required.
    env:
      WH_HOST: localhost
      WH_PORT: "5432"
      WH_USER: postgres
      WH_PASSWORD: ***
      WH_DB: postgres
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
      - run: pip install dbt-core==1.11.0 dbt-postgres==1.11.0
      - run: dbt parse --project-dir dbt --profiles-dir dbt

  compose-config:
    name: docker compose config
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: cp .env.example .env
      - run: docker compose config -q

  compose-build:
    name: docker compose build
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: docker compose build
```

Proof that `dbt parse` needs no live database (dummy/unreachable host, exit 0); this is why the
job uses env-var dummy values and no postgres service container:

```
> docker compose exec -T -w /opt/airflow/dbt -e WH_HOST=nonexistent-host airflow-scheduler \
    /home/airflow/dbt_venv/bin/dbt parse --profiles-dir /opt/airflow/dbt
14:37:50  Running with dbt=1.11.0
14:37:50  Registered adapter: postgres=1.11.0
14:37:51  Unable to do partial parsing because profile has changed
14:37:53  Performance info: /opt/airflow/dbt/target/perf_info.json
parse_exit=0
```

YAML validity and job/step inventory (a first quoting attempt failed in the shell only and is
not evidence about the file; this is the successful retry):

```
> docker run --rm -v "${PWD}:/work" -v "<temp>\phase5_yamlcheck.py:/check.py:ro" -w /work \
    python:3.11-slim sh -c "pip install -q pyyaml==6.0.2 && python /check.py"
jobs=compose-build,compose-config,dbt-parse,pytest,ruff
steps=actions/checkout@v4,actions/setup-python@v5,cp .env.example .env,dbt parse --project-dir dbt --profiles-dir dbt,docker compose build,docker compose config -q,pip install -r requirements-dev.txt,pip install -r requirements-dev.txt -r requirements.txt,pip install dbt-core==1.11.0 dbt-postgres==1.11.0,python -m pytest -q,ruff check .
yaml_exit=0
```

### 5.2 The same steps run locally

Host Python is 3.14 and the pins do not build there, so Python steps run in Python 3.11
containers. Every command below is the local equivalent of a CI job step.

#### 5.2.1 ruff — real failure found, fixed, re-run

First run (the new CI step exposed three genuine `F841` unused-variable errors in the Spark
job; no lint rule was changed):

```
> docker compose run --rm --no-deps -v "${PWD}:/work" -w /work producer \
    sh -c "pip install -q ruff==0.6.4 && ruff check ."
spark/streaming_job.py:132:5: F841 Local variable `q_raw` is assigned to but never used
    |
130 |             .withColumn("rejected_date", F.to_date("rejected_at")))
131 |
132 |     q_raw = (ticks.withColumn("event_date", F.to_date("event_time"))
    |             ^^^^^ F841
...
spark/streaming_job.py:139:5: F841 Local variable `q_dead` is assigned to but never used
spark/streaming_job.py:152:5: F841 Local variable `q_win` is assigned to but never used
Found 3 errors.
No fixes available (3 hidden fixes can be enabled with the `--unsafe-fixes` option).
ruff_exit=1
```

Fix (one line, see 5.5): the three query handles already existed, so the log line now uses them
instead of `spark.streams.active`. Re-run:

```
> docker compose run --rm --no-deps -v "${PWD}:/work" -w /work producer \
    sh -c "pip install -q ruff==0.6.4 && ruff check ."
All checks passed!
ruff_exit=0
```

#### 5.2.2 pytest — 12 unit tests

```
> docker compose run --rm --no-deps -v "${PWD}:/work" -w /work producer \
    sh -c "pip install -q -r requirements-dev.txt -r requirements.txt && python -m pytest -q"
............                                                             [100%]
12 passed in 1.44s
pytest_exit=0
```

(`System.Management.Automation.RemoteException` / pip root warnings on stderr are PowerShell
wrappers, not test errors; `12 passed` and exit 0 are the result.)

#### 5.2.3 dbt parse — CI command bug found, fixed, re-run

First attempt used `--profiles-dir dbt` only. It failed because the project root was the shell's
cwd, not `dbt/`:

```
> docker run --rm -v "${PWD}:/work" -w /work -e WH_HOST=localhost -e WH_PORT=5432 \
    -e WH_USER=postgres -e WH_PASSWORD=*** -e WH_DB=postgres python:3.11-slim \
    sh -c "pip install -q dbt-core==1.11.0 dbt-postgres==1.11.0 && dbt parse --profiles-dir dbt"
14:42:27  Running with dbt=1.11.0
14:42:27  Encountered an error:
Runtime Error
  No dbt_project.yml found at expected path /work/dbt_project.yml
  Verify that each entry within packages.yml (and their transitive dependencies) contains a file named dbt_project.yml
dbt_parse_exit=2
```

The workflow was changed to `dbt parse --project-dir dbt --profiles-dir dbt`. Re-run with the
exact same fresh-install command:

```
> docker run --rm -v "${PWD}:/work" -w /work -e WH_HOST=localhost -e WH_PORT=5432 \
    -e WH_USER=postgres -e WH_PASSWORD=*** -e WH_DB=postgres python:3.11-slim \
    sh -c "pip install -q dbt-core==1.11.0 dbt-postgres==1.11.0 && dbt parse --project-dir dbt --profiles-dir dbt"
WARNING: The candidate selected for download or install is a yanked version: 'dbt-core' candidate (version 1.11.0 ...)
Reason for being yanked: Some install issues with required dependency bounds
14:43:51  Running with dbt=1.11.0
14:43:51  Registered adapter: postgres=1.11.0
14:43:52  Unable to do partial parsing because profile has changed
14:43:54  Performance info: dbt/target/perf_info.json
dbt_parse_exit=0
```

The yanked-version notice is a pip warning, not a failure; the 1.11.0 pin is kept because it is
the pin already used by `Dockerfile.airflow` (see 1.4).

#### 5.2.4 docker compose config -q

The real `.env` was backed up, `.env.example` copied over it, and the original restored in a
`finally` block (hash comparison proves restoration):

```
> Copy-Item .env .env.phase5.bak -Force; Copy-Item .env.example .env -Force
> docker compose config -q
config_exit=0
> Copy-Item .env.phase5.bak .env -Force; Remove-Item .env.phase5.bak -Force
.env_restored=True
git ls-files .env -> ''
```

#### 5.2.5 docker compose build

```
> docker compose build
 Image streaming-pipeline-airflow:2.10.5-dbt1.11 Building
 Image streaming-pipeline-app:1.0.0 Building
 Image streaming-pipeline-spark:3.5.3 Building
...
#13 [airflow-init 2/2] RUN python -m venv /home/airflow/dbt_venv ... dbt-core==1.11.0 dbt-postgres==1.11.0
#13 CACHED
#14 [spark 2/2] RUN set -eux; cd /opt/spark/jars; ... curl -fsSL -O "${MAVEN}/${artifact}"; ...
#14 CACHED
[... intermediate BuildKit layer/export progress lines omitted here ...]
#34 [dashboard] resolving provenance for metadata file DONE 0.0s
 Image streaming-pipeline-spark:3.5.3 Built
 Image streaming-pipeline-app:1.0.0 Built
 Image streaming-pipeline-airflow:2.10.5-dbt1.11 Built
 Image streaming-pipeline-app:1.0.0 Built
 Image streaming-pipeline-airflow:2.10.5-dbt1.11 Built
 Image streaming-pipeline-airflow:2.10.5-dbt1.11 Built
 Image streaming-pipeline-app:1.0.0 Built
build_exit=0
```

(The omitted block is ordinary BuildKit progress: context load, layer `CACHED` lines and manifest
export. No step failed and no image tag uses `:latest` — see 5.4.)

### 5.3 scripts/verify.ps1

One command, seven checks, PASS/FAIL table, exit 1 if any check fails. Successful run:

```
> powershell -ExecutionPolicy Bypass -File scripts/verify.ps1

Check                   Result Detail
-----                   ------ ------
services up/healthy     PASS   9/9 up (7 healthy + producer/spark)
kafka has messages      PASS   end offset=285062 (exit=0)
lake raw/ticks objects  PASS   5 event_date partition object(s) listed (exit=0)
raw.price_windows fresh PASS   rows=5436 max_ingested_at=2026-11-20 14:45:00 age_sec=60 (exit=0)
dbt test passes         PASS   14:46:05  Done. PASS=15 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=15
DAG exists & unpaused   PASS   streaming_analytics | /opt/airflow/dags/analytics_pipeline.py | airflow | False
dashboard HTTP 200      PASS   HTTP 200 on port 8501

SUMMARY: 7/7 checks passed
verify_exit=0
```

FAIL path proved by stopping only the dashboard container (no data touched), running the script,
then starting it again:

```
> docker compose stop dashboard
 Container streaming-pipeline-dashboard-1 Stopping
 Container streaming-pipeline-dashboard-1 Stopped
> powershell -ExecutionPolicy Bypass -File scripts/verify.ps1
services up/healthy     FAIL   dashboard missing
...
dashboard HTTP 200      FAIL   HTTP 0 on port 8501
SUMMARY: 5/7 checks passed
verify_failpath_exit=1
> docker compose start dashboard
 Container streaming-pipeline-warehouse-1 Waiting
 Container streaming-pipeline-warehouse-1 Healthy
 Container streaming-pipeline-dashboard-1 Starting
 Container streaming-pipeline-dashboard-1 Started
```

After the dashboard reported healthy again, the full run passed once more (final state of the
script verification):

```
> powershell -ExecutionPolicy Bypass -File scripts/verify.ps1
Check                   Result Detail
-----                   ------ ------
services up/healthy     PASS   9/9 up (7 healthy + producer/spark)
kafka has messages      PASS   end offset=285377 (exit=0)
lake raw/ticks objects  PASS   5 event_date partition object(s) listed (exit=0)
raw.price_windows fresh PASS   rows=5446 max_ingested_at=2026-11-20 14:47:00 age_sec=3 (exit=0)
dbt test passes         PASS   14:47:07  Done. PASS=15 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=15
DAG exists & unpaused   PASS   streaming_analytics | /opt/airflow/dags/analytics_pipeline.py | airflow | False
dashboard HTTP 200      PASS   HTTP 200 on port 8501

SUMMARY: 7/7 checks passed
verify_exit=0
```

### 5.4 Hygiene

```
> Select-String -Path docker-compose.yml,Dockerfile.app,Dockerfile.airflow,Dockerfile.spark -Pattern ':latest' -SimpleMatch
NO ':latest' FOUND
```

`Select-String` is a PowerShell cmdlet, so `$LASTEXITCODE` is not meaningful here (it printed
empty); the real evidence is the absence of matches.

```
> git ls-files .env
['']
> git ls-files .env.example
['.env.example']
> Select-String -Path .env.example -Pattern '^SOURCE='
SOURCE=simulated
```

Before the README fix, `SOURCE=binance` was documented only implicitly (the file implied
Binance was the default and simulated the fallback), which contradicted `.env.example`:

```
> Select-String -Path README.md -Pattern 'SOURCE=' | ForEach-Object { ... }
README:61: If Binance is blocked in your region, set `SOURCE=simulated` in `.env` and run `docker compose up -d --force-recreate producer`.
README:64: Set `SOURCE=simulated` and `SIM_EXTRA_SYMBOLS=500` to generate ~505 events/sec (5 base symbols + 500 extra), then record the numbers in the Results table.
```

After the fix (new "Verify everything" section and explicit optional-Binance wording):

```
> Select-String -Path README.md -Pattern 'SOURCE=|Verify everything|ci.yml|status badge' | ForEach-Object { ... }
README:61: `SOURCE=simulated` is the default (offline and reproducible, see `.env.example`). Using live
README:62: prices is **optional**: set `SOURCE=binance` in `.env` (the public API is geo-blocked in some
README:64: or restore `SOURCE=simulated`.
README:67: Set `SOURCE=simulated` and `SIM_EXTRA_SYMBOLS=500` to generate ~505 events/sec (5 base symbols + 500 extra), then record the numbers in the Results table.
README:69: ## Verify everything
README:81: CI runs on every push and pull request (`.github/workflows/ci.yml`): ruff, the 12 pytest unit
README:83: those same steps run locally are in `VERIFICATION.md`. No status badge is included yet because
```

No badge URL was invented: the repo has not been pushed to GitHub.

### 5.5 Decisions, deviations and honest notes

- **Spark one-line change.** 5.2.1 is a real failure of the new ruff job, not a cosmetic edit:
  `q_raw`, `q_dead` and `q_win` were assigned but never read (F841). The fix makes the existing
  log line use those handles (`[q.name for q in (q_raw, q_dead, q_win)]`) instead of
  `spark.streams.active`. No streaming logic, checkpoint, schema, producer, dbt model or test
  was changed. The live Spark container was not restarted; the line only affects logging and
  `docker compose build` (5.2.5) completed successfully.
- **dbt parse CI command.** 5.2.3 was a real failure of my first workflow draft; adding
  `--project-dir dbt` fixes it. No postgres service container is needed (5.1 proof).
- **README wording.** This was a real documentation bug: `.env.example` defaults to
  `SOURCE=simulated`, but the README implied Binance was the default. It now says simulated is
  the default and `SOURCE=binance` is optional.
- **Test/lint integrity.** No pytest test was weakened, deleted or skipped, and no ruff rule or
  `select`/`ignore` setting was changed. The only new Python edit is the log line above.
- **Safety.** No `rm -rf`, `docker compose down -v`, prune, or data deletion was used. The only
  container interruption was the deliberate dashboard stop/start in 5.3, followed by a restored
  7/7 verification. `.env` was temporarily replaced and restored byte-for-byte (5.2.4).
- **Post-commit outputs.** As in Phase 4 (4.0/4.8), the final `git log --oneline -3`,
  `git status --short` and `docker compose ps` are run after the commit and reported in the chat
  response; they cannot be pasted into a file that is itself part of that commit.

### 5.6 Phase 5 status (before commit)

- [x] CI rewritten with five jobs: ruff, pytest (12 tests), dbt parse, compose config, compose build
- [x] Old `py_compile` step removed
- [x] All five CI steps run locally with real output pasted (two real failures found and fixed)
- [x] `scripts/verify.ps1` prints a PASS/FAIL table, 7/7 pass, and exits 1 on failure (proved)
- [x] Hygiene: no `:latest`, `.env` untracked, `.env.example` tracked and `SOURCE=simulated`
- [x] README documents optional `SOURCE=binance`, verification command and CI; no invented badge
- [x] No test weakened or deleted; no data deleted; stack left running and verified

---

## Phase 6 — observability

All commands below were run on 2026-11-21 in this single session (PowerShell on Windows 11,
Docker Desktop 29.8.0, local time UTC+2 while container/database timestamps are UTC). ANSI
colour codes were stripped from dbt output; the text is otherwise verbatim. No command printed
a credential, so there is nothing to mask with `***` (Airflow's own task log masks some of its
context values as `***`; that is the tool's redaction, pasted unchanged). Existing evidence
above was left untouched. Only `dags/`, `dbt/`, `dashboard/`, `scripts/verify.ps1`, `README.md`
and this file were edited. No Spark or producer logic was changed, no data was deleted, and
neither `rm -rf`, `docker compose down -v` nor prune was used.

### 6.0 Baseline (Step 0)

```
> git status --short; git log -3 --oneline
4f3ab0f ci: phase 5 workflow and verify script
c9595f3 feat(dbt): phase 4 test cleanup and README fixes
8c50bad docs: phase 3 measured results (throughput, latency, windows, lake, tests)
```
(`git status --short` produced no output — clean working tree; HEAD is the approved Phase 5
commit `4f3ab0f`.)

```
> docker compose ps
NAME                                     IMAGE                                                                                         COMMAND                  SERVICE             CREATED        STATUS                        PORTS
streaming-pipeline-airflow-db-1          postgres:16.16                                                                                "docker-entrypoint.s…"   airflow-db          2 days ago     Up About a minute (healthy)   5432/tcp
streaming-pipeline-airflow-scheduler-1   sha256:7e426dc278cefc7693576d6663e380cea06113610baf44ef0d4d23177ce68167                       "/usr/bin/dumb-init …"   airflow-scheduler   2 days ago     Up About a minute (healthy)   8080/tcp
streaming-pipeline-airflow-webserver-1   sha256:7e426dc278cefc7693576d6663e380cea06113610baf44ef0d4d23177ce68167                       "/usr/bin/dumb-init …"   airflow-webserver   2 days ago     Up About a minute (healthy)   0.0.0.0:8080->8080/tcp, [::]:8080->8080/tcp
streaming-pipeline-dashboard-1           sha256:4b2b693656e96ddbbf73037fc0444c6f9ca3ca09fecb82650cd732540ad42b12                       "streamlit run dashb…"   dashboard           2 days ago     Up About a minute (healthy)   0.0.0.0:8501->8501/tcp, [::]:8501->8501/tcp
streaming-pipeline-kafka-1               apache/kafka:3.8.0                                                                            "/__cacert_entrypoin…"   kafka               2 days ago     Up About a minute (healthy)   9092/tcp
streaming-pipeline-lake-1                chrislusf/seaweedfs@sha256:4b42c212543a0acd4c0e675215ecdcaf62a186ef98def76c7e12701871222d15   "/entrypoint.sh serv…"   lake                2 days ago     Up About a minute (healthy)   0.0.0.0:8333->8333/tcp, [::]:8333->8333/tcp, 0.0.0.0:8888->8888/tcp, [::]:8888->8888/tcp, 0.0.0.0:9333->9333/tcp, [::]:9333->9333/tcp
streaming-pipeline-producer-1            sha256:4b2b693656e96ddbbf73037fc0444c6f9ca3ca09fecb82650cd732540ad42b12                       "python -m producer.…"   producer            47 hours ago   Up About a minute
streaming-pipeline-spark-1               sha256:4e05edf5805ccffab278d586b95b786d2feff2f51a812004cd13bae29b89dc7e                       "/opt/entrypoint.sh …"   spark               2 days ago     Up About a minute
streaming-pipeline-warehouse-1           postgres:16.16                                                                                "docker-entrypoint.s…"   warehouse           2 days ago     Up About a minute (healthy)   0.0.0.0:5433->5432/tcp, [::]:5433->5432/tcp
```

All nine long-running services were already Up, so no `docker compose up -d` was needed.

```
> powershell -ExecutionPolicy Bypass -File scripts/verify.ps1

Check                   Result Detail
-----                   ------ ------
services up/healthy     PASS   9/9 up (7 healthy + producer/spark)
kafka has messages      PASS   end offset=289467 (exit=0)
lake raw/ticks objects  PASS   5 event_date partition object(s) listed (exit=0)
raw.price_windows fresh PASS   rows=5521 max_ingested_at=2026-11-21 14:01:00 age_sec=35 (exit=0)
dbt test passes         PASS   14:01:40  Done. PASS=15 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=15
DAG exists & unpaused   PASS   streaming_analytics | /opt/airflow/dags/analytics_pipeline.py | airflow | False
dashboard HTTP 200      PASS   HTTP 200 on port 8501

SUMMARY: 7/7 checks passed
```

### 6.1 Freshness check: `window_end` -> `ingested_at`, threshold from env var

The old task in `dags/analytics_pipeline.py` measured the wrong thing (hard-coded 600 s and the
age of the *window end*, which includes the deliberate watermark + trigger delay):

```python
MAX_LAG_SECONDS = 600
...
cur.execute("SELECT max(window_end) FROM raw.price_windows")
...
lag = (now - latest).total_seconds()
```

New logic. `dags/freshness.py` holds the pure, dependency-free helpers; the DAG queries
`max(ingested_at)` and reads its limit from `FRESHNESS_MAX_LAG_SECONDS` (default 300 s):

```python
DEFAULT_MAX_LAG_SECONDS = 300
MAX_LAG_ENV_VAR = "FRESHNESS_MAX_LAG_SECONDS"

def assert_fresh(latest, now, max_lag_seconds: int) -> float:
    if latest is None:
        raise ValueError("raw.price_windows is empty: is the streaming job running?")
    lag = ingestion_lag_seconds(latest, now)
    if lag > max_lag_seconds:
        raise ValueError(f"Stream is stale: newest ingested_at was {lag:.0f}s ago "
                         f"(limit {max_lag_seconds}s)")
    return lag
```

```python
cur.execute("SELECT max(ingested_at) FROM raw.price_windows")
latest = cur.fetchone()[0]
now = datetime.now(timezone.utc).replace(tzinfo=None)
lag = assert_fresh(latest, now, threshold)
```

The env var is unset in the deployment, so the default applies:

```
> docker compose exec -T airflow-scheduler printenv FRESHNESS_MAX_LAG_SECONDS; "env_var_exit=$LASTEXITCODE"
env_var_exit=1
```

PASS with the default threshold (direct call to the deployed function; the DAG folder is
volume-mounted, so no Airflow image rebuild was needed):

```
> docker compose exec -T airflow-scheduler sh -c "cd /opt/airflow/dags && python -c 'from analytics_pipeline import check_stream_freshness; print(check_stream_freshness())'"; "freshness_pass_exit=$LASTEXITCODE"
Freshness OK: latest ingested_at=2026-11-21 14:07:00.016000, lag=14s (limit=300s)
13.812879
freshness_pass_exit=0
```

The real Airflow task also succeeds:

```
> docker compose exec -T airflow-webserver airflow tasks test streaming_analytics check_stream_freshness 2026-11-21 2>&1 | Select-Object -Last 22; "task_test_exit=$LASTEXITCODE"
...
[2026-11-21T14:10:44.056+0000] {taskinstance.py:732} INFO - ::endgroup::
Freshness OK: latest ingested_at=2026-11-21 14:10:00.022000, lag=44s (limit=300s)
[2026-11-21T14:10:44.102+0000] {python.py:240} INFO - Done. Returned value was: 44.080186
[2026-11-21T14:10:44.111+0000] {taskinstance.py:341} INFO - ::group::Post task execution logs
[2026-11-21T14:10:44.112+0000] {taskinstance.py:353} INFO - Marking task as SUCCESS. dag_id=streaming_analytics, task_id=check_stream_freshness, run_id=__***_temporary_run_2026-11-21T14:10:43.660055+00:00__, execution_date=20261121T000000, start_date=, end_date=20261121T141044
task_test_exit=0
```

SAFE FAIL proof — only the threshold was changed (to 1 s) via the env var for one `exec`;
Spark was not stopped, no table or row was touched:

```
> docker compose exec -T -e FRESHNESS_MAX_LAG_SECONDS=1 airflow-scheduler sh -c "cd /opt/airflow/dags && python -c 'from analytics_pipeline import check_stream_freshness; check_stream_freshness()'" 2>&1 | ForEach-Object { "$_" }; "freshness_fail_exit=$LASTEXITCODE"
Traceback (most recent call last):
  File "<string>", line 1, in <module>
  File "/opt/airflow/dags/analytics_pipeline.py", line 27, in check_stream_freshness
    lag = assert_fresh(latest, now, threshold)
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/airflow/dags/freshness.py", line 31, in assert_fresh
    raise ValueError(
ValueError: Stream is stale: newest ingested_at was 48s ago (limit 1s)
freshness_fail_exit=1
```

A first attempt at the PASS proof failed on my shell quoting (`%: 1: Syntax error: Unterminated
quoted string`, exit 2); the retry above uses a quote-free one-liner and is the real result.

One real problem found and fixed during this phase: Airflow's DagBag initially parsed the new
unit-test file as a DAG and reported `ModuleNotFoundError: No module named 'pytest'`. Adding
`dags/.airflowignore` with `tests/` removed it — no test was deleted or weakened:

```
> docker compose exec -T airflow-webserver airflow dags list-import-errors; "import_errors_exit=$LASTEXITCODE"
No data found
import_errors_exit=0
```

### 6.2 `pipeline_metrics` and `pipeline_windows_per_hour` (dbt)

Two new views under `dbt/models/marts/`, both `materialized='view'` so "now" is evaluated at
query time. `windows_per_hour_last_24h` is defined as `windows_last_24h / 24`; commit lag is
`ingested_at - window_end` over all deduplicated rows (staging view), not just the last 24 h.
Dead letters are **not** counted: they are Parquet objects under `dead_letter/` in the S3 lake,
not a SQL relation, and counting them from dbt would require an S3 scan.

```
> docker compose exec -T -w /opt/airflow/dbt airflow-scheduler /home/airflow/dbt_venv/bin/dbt run --profiles-dir /opt/airflow/dbt 2>&1 | ForEach-Object { $_ -replace "\x1b\[[0-9;]*m","" }; "dbt_run_exit=$LASTEXITCODE"
14:07:29  Running with dbt=1.11.0
14:07:29  Registered adapter: postgres=1.11.0
14:07:31  Found 8 models, 1 snapshot, 24 data tests, 1 seed, 1 source, 479 macros
14:07:31
14:07:31  Concurrency: 2 threads (target='dev')
14:07:31
14:07:31  1 of 8 START sql table model marts.mart_symbol_history ......................... [RUN]
14:07:31  2 of 8 START sql view model staging.stg_price_windows .......................... [RUN]
14:07:32  2 of 8 OK created sql view model staging.stg_price_windows ..................... [CREATE VIEW in 0.29s]
14:07:32  1 of 8 OK created sql table model marts.mart_symbol_history .................... [SELECT 5 in 0.29s]
14:07:32  3 of 8 START sql table model marts.dim_symbol .................................. [RUN]
14:07:32  4 of 8 START sql view model marts.pipeline_metrics ............................. [RUN]
14:07:32  4 of 8 OK created sql view model marts.pipeline_metrics ........................ [CREATE VIEW in 0.10s]
14:07:32  5 of 8 START sql view model marts.pipeline_windows_per_hour .................... [RUN]
14:07:32  3 of 8 OK created sql table model marts.dim_symbol ............................. [SELECT 506 in 0.15s]
14:07:32  6 of 8 START sql table model marts.fct_price_1m ................................ [RUN]
14:07:32  5 of 8 OK created sql view model marts.pipeline_windows_per_hour ............... [CREATE VIEW in 0.11s]
14:07:32  6 of 8 OK created sql table model marts.fct_price_1m ........................... [SELECT 5546 in 0.20s]
14:07:32  7 of 8 START sql table model marts.mart_hourly_prices .......................... [RUN]
14:07:32  8 of 8 START sql table model marts.mart_volatility_alerts ...................... [RUN]
14:07:32  8 of 8 OK created sql table model marts.mart_volatility_alerts ................. [SELECT 138 in 0.12s]
14:07:32  7 of 8 OK created sql table model marts.mart_hourly_prices ..................... [SELECT 531 in 0.14s]
14:07:32
14:07:32  Finished running 5 table models, 3 view models in 0 hours 0 minutes and 1.08 seconds (1.08s).
14:07:32
14:07:32  Completed successfully
14:07:32
14:07:32  Done. PASS=8 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=8
dbt_run_exit=0
```

Tests added: `not_null` on all six `pipeline_metrics` columns, `unique` + `not_null` on
`pipeline_windows_per_hour.hour_start`, `not_null` on `.windows`, and a singular test
`assert_pipeline_metrics_sane` (negative lag, or max < avg, or zero windows => fail).
15 -> 24 data tests.

```
> docker compose exec -T -w /opt/airflow/dbt airflow-scheduler /home/airflow/dbt_venv/bin/dbt test --profiles-dir /opt/airflow/dbt 2>&1 | ForEach-Object { $_ -replace "\x1b\[[0-9;]*m","" }; "dbt_test_exit=$LASTEXITCODE"
14:07:46  Running with dbt=1.11.0
14:07:46  Registered adapter: postgres=1.11.0
14:07:46  Found 8 models, 1 snapshot, 24 data tests, 1 seed, 1 source, 479 macros
...
14:07:46  2 of 24 PASS assert_pipeline_metrics_sane ...................................... [PASS in 0.12s]
...
14:07:47  11 of 24 PASS not_null_pipeline_metrics_avg_commit_lag_seconds ................. [PASS in 0.09s]
14:07:46  12 of 24 PASS not_null_pipeline_metrics_ingestion_lag_seconds .................. [PASS in 0.08s]
14:07:47  13 of 24 PASS not_null_pipeline_metrics_latest_ingested_at ..................... [PASS in 0.15s]
14:07:47  14 of 24 PASS not_null_pipeline_metrics_max_commit_lag_seconds ................. [PASS in 0.15s]
14:07:47  15 of 24 PASS not_null_pipeline_metrics_windows_last_24h ....................... [PASS in 0.10s]
14:07:47  16 of 24 PASS not_null_pipeline_windows_per_hour_hour_start .................... [PASS in 0.11s]
14:07:47  17 of 24 PASS not_null_pipeline_windows_per_hour_windows ....................... [PASS in 0.09s]
14:07:47  24 of 24 PASS unique_pipeline_windows_per_hour_hour_start ...................... [PASS in 0.08s]
14:07:47
14:07:47  Finished running 24 data tests in 0 hours 0 minutes and 1.40 seconds (1.40s).
14:07:47
14:07:47  Completed successfully
14:07:47
14:07:47  Done. PASS=24 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=24
dbt_test_exit=0
```

(The `...` lines are unchanged pre-existing tests from Phase 4; the complete run passed 24/24.)

### 6.3 Streamlit "Pipeline health"

The dashboard reads SQL constants from `dashboard/queries.py`; the new section shows ingestion
lag, windows/hour (24 h average and per-hour bar chart), average and max commit lag, and latest
ingested timestamp. Dashboard code is baked into `streaming-pipeline-app:1.0.0`, so only that
service was rebuilt and recreated (the DAG is volume-mounted, so Airflow was not rebuilt):

```
> docker compose build dashboard; "build_exit=$LASTEXITCODE"
#11 [6/7] COPY dashboard ./dashboard
#11 DONE 0.0s
 Image streaming-pipeline-app:1.0.0 Built
build_exit=0

> docker compose up -d --no-deps dashboard; "up_exit=$LASTEXITCODE"
 Container streaming-pipeline-dashboard-1 Recreate
 Container streaming-pipeline-dashboard-1 Recreated
 Container streaming-pipeline-dashboard-1 Starting
 Container streaming-pipeline-dashboard-1 Started
up_exit=0
```

HTTP check after the rebuild:

```
> (poll docker compose ps for dashboard healthy, 3 s interval)
dashboard_status=dashboard|Up 15 seconds (healthy)
> (Invoke-WebRequest http://localhost:8501)
dashboard_http=200
```

The exact SQL constants used by the dashboard were executed inside the dashboard container
(without starting Streamlit), via a temp script copied to `/tmp`; no credential is printed:

```
> docker compose cp <temp>\phase6_dashboard_metrics.py dashboard:/tmp/phase6_dashboard_metrics.py
> docker compose exec -T -e PYTHONPATH=/app dashboard python /tmp/phase6_dashboard_metrics.py; "dashboard_metrics_exit=$LASTEXITCODE"
     latest_ingested_at  ingestion_lag_seconds  windows_last_24h  windows_per_hour_last_24h  avg_commit_lag_seconds  max_commit_lag_seconds
2026-11-21 14:09:00.011                   13.2               175                       7.29                   668.0                 82980.0
hourly_rows=2 first_hour=2026-11-20 14:00:00 last_hour=2026-11-21 14:00:00
dashboard_metrics_exit=0
```

Direct psql query of the same views (run immediately afterwards):

```
> docker compose exec -T warehouse psql -U warehouse -d warehouse -c "select * from marts.pipeline_metrics;"
   latest_ingested_at    | ingestion_lag_seconds | windows_last_24h | windows_per_hour_last_24h | avg_commit_lag_seconds | max_commit_lag_seconds
-------------------------+-----------------------+------------------+---------------------------+------------------------+------------------------
 2026-11-21 14:09:00.011 |                  33.9 |              175 |                      7.29 |                   668.0 |                82980.0
(1 row)

> docker compose exec -T warehouse psql -U warehouse -d warehouse -c "select hour_start, windows from marts.pipeline_windows_per_hour order by hour_start;"
     hour_start      | windows
---------------------+---------
 2026-11-20 14:00:00 |     125
 2026-11-21 14:00:00 |      50
(2 rows)
```

`latest_ingested_at`, `windows_last_24h`, `windows_per_hour_last_24h`, `avg_commit_lag_seconds`
and `max_commit_lag_seconds` match exactly. `ingestion_lag_seconds` differs (13.2 vs 33.9) only
because the view computes `now()` at query time and the two queries were ~20 s apart; it is the
same expression. Streamlit logged no exception after the rebuild (`docker compose logs --tail 25
dashboard`: only "You can now view your Streamlit app in your browser", URL http://0.0.0.0:8501).

The large `max_commit_lag_seconds` (82,980 s) is real: a window from the previous day was
committed late after a Spark restart. Source check:

```
> docker compose exec -T warehouse psql -U warehouse -d warehouse -c "select count(*) as all_rows, round(avg(extract(epoch from (ingested_at-window_end)))::numeric,1) as avg_lag_all, round(max(extract(epoch from (ingested_at-window_end)))::numeric,1) as max_lag_all, count(*) filter (where ingested_at > now()-interval '24 hours') as rows_24h, round(avg(extract(epoch from (ingested_at-window_end))) filter (where ingested_at > now()-interval '24 hours')::numeric,1) as avg_lag_24h, round(max(extract(epoch from (ingested_at-window_end))) filter (where ingested_at > now()-interval '24 hours')::numeric,1) as max_lag_24h from raw.price_windows;"
 all_rows | avg_lag_all | max_lag_all | rows_24h | avg_lag_24h | max_lag_24h
----------+-------------+-------------+----------+-------------+-------------
     5556 |       668.0 |     82980.0 |      175 |     14185.7 |     82980.0
(1 row)
```

### 6.4 `scripts/verify.ps1` — eighth check

```
> powershell -ExecutionPolicy Bypass -File scripts/verify.ps1; "verify_exit=$LASTEXITCODE"

Check                        Result Detail
-----                        ------ ------
services up/healthy          PASS   9/9 up (7 healthy + producer/spark)
kafka has messages           PASS   end offset=291967 (exit=0)
lake raw/ticks objects       PASS   5 event_date partition object(s) listed (exit=0)
raw.price_windows fresh      PASS   rows=5556 max_ingested_at=2026-11-21 14:09:00 age_sec=55 (exit=0)
dbt test passes              PASS   14:09:59  Done. PASS=24 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=24
DAG exists & unpaused        PASS   streaming_analytics | /opt/airflow/dags/analytics_pipeline.py | airflow | False
dashboard HTTP 200           PASS   HTTP 200 on port 8501
pipeline_metrics lag <= 300s PASS   latest_ingested_at=2026-11-21 14:10:00 lag_sec=5 limit=300 (exit=0)

SUMMARY: 8/8 checks passed
verify_exit=0
```

### 6.5 ruff and unit tests

Host Python is 3.14, so ruff and pytest were run in pinned Python 3.11 containers, as in
Phases 4 and 5. No lint rule, select/ignore setting, or existing test was changed or removed.

```
> docker compose run --rm --no-deps -v "${PWD}:/work" -w /work producer sh -c "pip install -q ruff==0.6.4 && ruff check ."; "ruff_exit=$LASTEXITCODE"
WARNING: Running pip as the 'root' user can result in broken permissions and conflicting behaviour with the system package manager. It is recommended to use a virtual environment instead: https://pip.pypa.io/warnings/venv
[notice] A new release of pip is available: 24.0 -> 26.3
[notice] To update, run: pip install --upgrade pip
All checks passed!
ruff_exit=0
```

Exact Phase 2 command — original 12 tests stay green (the new DAG tests live in `dags/tests`
and are not collected without that mount):

```
> docker compose run --rm --no-deps -v "${PWD}/tests:/app/tests" -v "${PWD}/dbt:/app/dbt" producer sh -c "pip install -q pytest==8.3.2 && python -m pytest -q"; "pytest_exit=$LASTEXITCODE"
............                                                             [100%]
12 passed in 1.84s
pytest_exit=0
```

New logic covered: the same command with `dags/` mounted collects the six freshness tests too
(default/env threshold, lag arithmetic, fresh/stale/empty cases):

```
> docker compose run --rm --no-deps -v "${PWD}/tests:/app/tests" -v "${PWD}/dbt:/app/dbt" -v "${PWD}/dags:/app/dags" producer sh -c "pip install -q pytest==8.3.2 && python -m pytest -q"; "pytest_with_dags_exit=$LASTEXITCODE"
..................                                                       [100%]
18 passed in 1.88s
pytest_with_dags_exit=0
```

(Pip root warnings / PowerShell `RemoteException` wrappers on stderr are not test errors;
`passed` and exit 0 are the results.)

### 6.6 README

A short "Observability" section documents the freshness formula and env var, both views and
their columns, the dashboard section, how to read the numbers, and why dead-letter count is
omitted. The "Verify everything" paragraph now mentions the eighth check. No number in the
README was invented: all figures quoted in this phase came from the commands above.

### 6.7 Decisions, deviations and honest notes

- **Interpretation of "windows per hour".** The scalar `windows_per_hour_last_24h` is
  `windows_last_24h / 24` (an average rate); the actual per-hour series is the companion
  `pipeline_windows_per_hour` view used by the chart. Both are provided so neither reading of
  the request is lost.
- **Commit lag scope.** `avg`/`max_commit_lag_seconds` are computed over all deduplicated rows
  (the request attached "last 24 h" only to windows/hour). This makes them lifetime stats;
  a Spark restart replay can push the max up, as seen in 6.3. README explains how to read it.
- **No dead-letter count.** Dead letters are Parquet in the S3 lake, with no cheap SQL path from
  dbt. Added no fake/zero column; omitted it and documented why, as the task permits.
- **Rebuild scope.** The DAG is bind-mounted (`./dags:/opt/airflow/dags`), so Airflow was not
  rebuilt. Only `dashboard` was rebuilt/recreated. The app image tag is shared with producer
  and lake-init, but those containers were not recreated and their code was not changed.
- **View vs table.** Both new models are views so lag reflects the moment of the query. The
  consequence (lag differs between two queries seconds apart) is shown explicitly in 6.3.
- **Ambiguous failure proof.** "Tiny threshold" was implemented with `-e
  FRESHNESS_MAX_LAG_SECONDS=1` on a single `docker compose exec`; this exercises the real env
  var path and the real function, without touching Spark, checkpoints or data.
- **Quoting and import-error retries.** Two first attempts failed (6.1): a shell-quoting error
  in the PASS proof and Airflow parsing the new test file. Both were fixed and the successful
  retries are pasted. The earlier outputs are quoted honestly rather than hidden.
- **Post-commit outputs.** As in Phases 4 and 5, the final `git log --oneline -3`,
  `git status --short`, `docker compose ps` and `scripts/verify.ps1` run *after* the commit are
  reported in the chat response; they cannot be pasted into a file that is part of that commit.

### 6.8 Phase 6 status (before commit)

- [x] Freshness measures `max(ingested_at)` with `FRESHNESS_MAX_LAG_SECONDS` (default 300 s);
      PASS at 300 s, FAIL at 1 s, real Airflow task SUCCESS — all with live data untouched
- [x] `pipeline_metrics` and `pipeline_windows_per_hour` views added with 9 new dbt tests;
      `dbt run` 8/8 and `dbt test` 24/24 in `airflow-scheduler`
- [x] Dashboard "Pipeline health" section rebuilt and serving HTTP 200; its SQL matches a
      direct psql query (lag differs only by query time)
- [x] `scripts/verify.ps1` extended -> 8/8 checks passed
- [x] ruff clean; 12/12 original unit tests and 18/18 with new freshness tests
- [x] README Observability section added; no Spark/producer logic, lint rule or test weakened

---

## Phase 7 — final polish

All commands below were run on 2026-11-21 in this single session (PowerShell on Windows 11,
Docker Desktop 29.8.0). Existing evidence above was left in place; the only changes to it are the
publication sanitization described in 7.3 (personal paths -> placeholders, dummy credential-like
literals -> `***`). Only documentation/hygiene files were changed: `README.md`, `VERIFICATION.md`,
`.gitignore`, `LICENSE` (new) and `docs/INTERVIEW_NOTES.md` (new). No Spark, producer, dbt model,
DAG, compose, SQL, pipeline logic or test was changed. No `rm -rf`, `docker compose down -v`, prune
or data deletion was used.

### 7.0 Step 0 — baseline

```
> git status --short
(no output - clean working tree)

> git log -3 --oneline
d0e98d2 feat(obs): phase 6 pipeline metrics, freshness fix, dashboard health
4f3ab0f ci: phase 5 workflow and verify script
c9595f3 feat(dbt): phase 4 test cleanup and README fixes
```

```
> docker compose ps
NAME                                     IMAGE                                                                                         COMMAND                  SERVICE             CREATED          STATUS                    PORTS
streaming-pipeline-airflow-db-1          postgres:16.16                                                                                "docker-entrypoint.s…"   airflow-db          2 days ago       Up 44 minutes (healthy)   5432/tcp
streaming-pipeline-airflow-scheduler-1   sha256:7e426dc278cefc7693576d6663e380cea06113610baf44ef0d4d23177ce68167                       "/usr/bin/dumb-init …"   airflow-scheduler   2 days ago       Up 44 minutes (healthy)   8080/tcp
streaming-pipeline-airflow-webserver-1   sha256:7e426dc278cefc7693576d6663e380cea06113610baf44ef0d4d23177ce68167                       "/usr/bin/dumb-init …"   airflow-webserver   2 days ago       Up 44 minutes (healthy)   0.0.0.0:8080->8080/tcp, [::]:8080->8080/tcp
streaming-pipeline-dashboard-1           streaming-pipeline-app:1.0.0                                                                  "streamlit run dashb…"   dashboard           36 minutes ago   Up 36 minutes (healthy)   0.0.0.0:8501->8501/tcp, [::]:8501->8501/tcp
streaming-pipeline-kafka-1               apache/kafka:3.8.0                                                                            "/__cacert_entrypoin…"   kafka               2 days ago       Up 44 minutes (healthy)   9092/tcp
streaming-pipeline-lake-1                chrislusf/seaweedfs@sha256:4b42c212543a0acd4c0e675215ecdcaf62a186ef98def76c7e12701871222d15   "/entrypoint.sh serv…"   lake                2 days ago       Up 44 minutes (healthy)   0.0.0.0:8333->8333/tcp, [::]:8333->8333/tcp, 0.0.0.0:8888->8888/tcp, [::]:8888->8888/tcp, 0.0.0.0:9333->9333/tcp, [::]:9333->9333/tcp
streaming-pipeline-producer-1            sha256:4b2b693656e96ddbbf73037fc0444c6f9ca3ca09fecb82650cd732540ad42b12                       "python -m producer.…"   producer            2 days ago       Up 44 minutes
streaming-pipeline-spark-1               sha256:4e05edf5805ccffab278d586b95b786d2feff2f51a812004cd13bae29b89dc7e                       "/opt/entrypoint.sh …"   spark               2 days ago       Up 44 minutes
streaming-pipeline-warehouse-1           postgres:16.16                                                                                "docker-entrypoint.s…"   warehouse           2 days ago       Up 44 minutes (healthy)   0.0.0.0:5433->5432/tcp, [::]:5433->5432/tcp
```

All nine long-running services were already Up (seven healthy; producer/spark have no HTTP
healthcheck), so no `docker compose up -d` was needed.

```
> powershell -ExecutionPolicy Bypass -File scripts/verify.ps1; "verify_exit=$LASTEXITCODE"

Check                        Result Detail
-----                        ------ ------
services up/healthy          PASS   9/9 up (7 healthy + producer/spark)
kafka has messages           PASS   end offset=302392 (exit=0)
lake raw/ticks objects       PASS   5 event_date partition object(s) listed (exit=0)
raw.price_windows fresh      PASS   rows=5731 max_ingested_at=2026-11-21 14:44:00 age_sec=41 (exit=0)
dbt test passes              PASS   14:44:45  Done. PASS=24 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=24
DAG exists & unpaused        PASS   streaming_analytics | /opt/airflow/dags/analytics_pipeline.py | airflow | False
dashboard HTTP 200           PASS   HTTP 200 on port 8501
pipeline_metrics lag <= 300s PASS   latest_ingested_at=2026-11-21 14:44:00 lag_sec=54 limit=300 (exit=0)

SUMMARY: 8/8 checks passed
verify_exit=0
```

### 7.1 Final README

The README was rewritten in the requested order: title + one-paragraph summary, architecture
(both mermaid blocks kept), measured Results, Quick start, Verify everything, Observability, Design
decisions (including lessons learned), Limitations, Roadmap, Tests and Data note. Stale text removed:
the "fill in after your own run" Results heading, the "Add screenshots" TODO, the checked-off SCD2
roadmap item, and host-level `pip install` / `pytest` instructions that contradicted AGENTS.md
(tests now show the container commands). The unsourced "every 5 min" wording in the diagram and
quick start was changed to "scheduled" because that number is not in this file.

```
> Select-String -Path README.md -Pattern '^# ' | ForEach-Object { "$($_.LineNumber): $($_.Line)" }
1: # Real-Time Crypto Price Streaming Pipeline
> Select-String -Path README.md -Pattern '^## ' | ForEach-Object { "$($_.LineNumber): $($_.Line)" }
12: ## Architecture
47: ## Results (measured)
63: ## Quick start
95: ## Verify everything
112: ## Observability
141: ## Design decisions
179: ## Limitations
188: ## Roadmap
195: ## Tests
208: ## Data note
```

Every metric in the Results table is re-quoted from an existing section of this file (no new
numbers): 8/8 (§6.4), ~507 events/s / N=505 / 31,310 / 61.72 s (§3.2), 135.641074 s (§3.3),
5,093,976 B / 4.858 MiB / 12.88 h / ~9.05 MiB projection (§3.5), 4,136 windows (§3.5),
24 dbt tests (§6.2), 12 unit tests / 18 with DAG tests (§6.5), 26–36 s cold boot (§1.2). The
latency row explicitly says "by design" and the lake row explicitly says "projection".

### 7.2 Interview notes

```
> Get-ChildItem docs | Select-Object Name,Length

Name               Length
----               ------
INTERVIEW_NOTES.md   5690

> (Select-String -Path docs/INTERVIEW_NOTES.md -Pattern '^### ').Count
12
```

Twelve likely Data Engineering interview questions with short, honest answers, each citing the
VERIFICATION.md section that supports it (Kafka, at-least-once, watermark/latency, DLQ and replay,
sink failure, SCD2, idempotency, hardest bug, health checks, scale, do-differently, evidence).

### 7.3 Publish hygiene

Initial scan over `git ls-files` found five host user-profile paths and one host project path in
this file, plus three dummy CI password literals. Replacements (no evidence line was deleted):

| What was found | Where | Replacement |
|---|---|---|
| Host user-profile path in the `py -0p` output | §0.1 | `<user-home>\AppData\...` |
| Host project path in the `git init` output | §0.3 | `<project-root>/.git/` |
| Host temp path for the phase-2 bad-message file | Session-side notes | `<temp>\opencode\...` |
| Host temp path for the classifier script | Session-side notes | `<temp>\opencode\...` |
| Host temp path for the latency probe (command + `docker compose cp` output) | §3.3 | `<temp>\...` |
| Dummy `postgres` CI password literals (no live DB connection; §5.1 proves `dbt parse` works with an unreachable host) | §5.1 workflow quote and two local-equivalent commands | `***` |

Post-sanitization scans (paths shown repo-relative; no personal data remains):

```
> git ls-files | ForEach-Object { Select-String -Path $_ -Pattern 'C:\\Users|C:/Users|D:\\projects|D:/projects|DELL\\' ... }
(no output)

> Select-String -Path VERIFICATION.md -Pattern 'WH_PASSWORD=postgres|WH_PASSWORD: postgres'
(no output)
```

Email and credential-like literals still present, all judged safe and left in place:

```
> (scan for emails over git ls-files)
README.md:92: `a@b.com`, and the Flask secret `local-dev-secret-change-me`. They must be overridden in the local
docker-compose.yml:165: --role Admin --email a@b.com || true)"

> (scan for credential-like literals over git ls-files, abridged to the value-bearing lines)
.env.example:14: S3_ACCESS_KEY=lakeadmin
.env.example:15: S3_SECRET_KEY=lakeadmin123
.env.example:20: WH_PASSWORD=warehouse
.github/workflows/ci.yml:42: WH_PASSWORD: postgres
docker-compose.yml:9: S3_ACCESS_KEY: ${S3_ACCESS_KEY:-lakeadmin}
docker-compose.yml:10: S3_SECRET_KEY: ${S3_SECRET_KEY:-lakeadmin123}
docker-compose.yml:16: WH_PASSWORD: ${WH_PASSWORD:-warehouse}
docker-compose.yml:86: POSTGRES_PASSWORD: ${WH_PASSWORD:-warehouse}
docker-compose.yml:145: POSTGRES_PASSWORD: airflow
docker-compose.yml:164: (airflow users create --username admin --password admin --firstname a --lastname b
docker-compose.yml:172: AIRFLOW__WEBSERVER__SECRET_KEY: ${AIRFLOW_SECRET_KEY:-local-dev-secret-change-me}
infra/seaweedfs/s3.json:7: "accessKey": "lakeadmin",
infra/seaweedfs/s3.json:8: "secretKey": "lakeadmin123"
scripts/s3_client.py:12: aws_access_key_id=os.getenv("S3_ACCESS_KEY", "lakeadmin"),
scripts/s3_client.py:13: aws_secret_access_key=os.getenv("S3_SECRET_KEY", "lakeadmin123"),
spark/streaming_job.py:27: "password": os.getenv("WH_PASSWORD", "warehouse"),
spark/streaming_job.py:43: .config("spark.hadoop.fs.s3a.access.key", os.getenv("S3_ACCESS_KEY", "lakeadmin"))
spark/streaming_job.py:44: .config("spark.hadoop.fs.s3a.secret.key", os.getenv("S3_SECRET_KEY", "lakeadmin123"))
```

These are the local demo defaults permitted by the scope. The README "Demo defaults (local
development only)" section now names every one of them explicitly (S3, warehouse, Airflow
admin/admin with the dummy `a@b.com` email, and the Flask secret) and says they must be overridden
in `.env` for any non-local use. No phone numbers were found by the phone-pattern scan. Git author
identity is the local-only `Pipeline Dev <dev@localhost>`, not a personal address.

```
> git ls-files .env
(no output)
> git check-ignore -v .env
.gitignore:1:.env	.env
```

`.env` is untracked and ignored; only `.env.example` is committed.

### 7.4 .gitignore and status afterwards

```
> Get-Content .gitignore
.env
__pycache__/
*.pyc
.pytest_cache/
.ruff_cache/
.venv/
.DS_Store
dbt/target/
dbt/logs/
dbt/dbt_packages/
logs/
dbt/.user.yml

# Local scratch / temporary files (never publish these)
*.tmp
*.bak
*.swp
*~
.env.local
```

It already covered `.env`, `.venv`, `__pycache__`, `.pytest_cache`, dbt `target/` and `logs/`;
this phase added the local scratch/temp patterns (keeps artefacts like the phase-5 `.env.bak` out).

```
> git status --short
 M .gitignore
 M README.md
 M VERIFICATION.md
?? LICENSE
?? docs/

> git status --short --ignored | Select-Object -First 15
 M .gitignore
 M README.md
 M VERIFICATION.md
?? LICENSE
?? docs/
!! .env
!! .pytest_cache/
!! .ruff_cache/
!! dags/__pycache__/
!! dags/tests/__pycache__/
!! dbt/.user.yml
!! dbt/logs/
!! dbt/target/
!! logs/
!! producer/__pycache__/
!! scripts/__pycache__/
!! tests/__pycache__/
```

### 7.5 LICENSE and architecture doc decision

No LICENSE existed, so an MIT LICENSE was added with the placeholder copyright holder "Ahmed":

```
> Get-Content LICENSE | Select-Object -First 5
MIT License

Copyright (c) 2026 Ahmed

Permission is hereby granted, free of charge, to any person obtaining a copy
```

`docs/` was empty before this phase. A separate `docs/ARCHITECTURE.md` was **not** added: the README
already carries the mermaid architecture and ER diagrams, the data-model grain, the dead-letter and
sink-failure paths, and all design decisions, so a second document would duplicate rather than add.
`docs/INTERVIEW_NOTES.md` is the only file in `docs/`.

### 7.6 Final checks

Host Python is 3.14 and the pinned deps do not build there, so ruff and pytest ran in pinned
Python 3.11 containers, as in phases 4–6.

```
> docker compose run --rm --no-deps -v "${PWD}:/work" -w /work producer sh -c "pip install -q ruff==0.6.4 && ruff check ."; "ruff_exit=$LASTEXITCODE"
 Container streaming-pipeline-producer-run-ba8dc03701c3 Creating
 Container streaming-pipeline-producer-run-ba8dc03701c3 Created
WARNING: Running pip as the 'root' user can result in broken permissions and conflicting behaviour with the system package manager. It is recommended to use a virtual environment instead: https://pip.pypa.io/warnings/venv

[notice] A new release of pip is available: 24.0 -> 26.3
[notice] To update, run: pip install --upgrade pip
All checks passed!
ruff_exit=0
```

```
> docker compose run --rm --no-deps -v "${PWD}/tests:/app/tests" -v "${PWD}/dbt:/app/dbt" producer sh -c "pip install -q pytest==8.3.2 && python -m pytest -q"; "pytest_exit=$LASTEXITCODE"
 Container streaming-pipeline-producer-run-f912bd287223 Creating
 Container streaming-pipeline-producer-run-f912bd287223 Created
WARNING: Running pip as the 'root' user can result in broken permissions and conflicting behaviour with the system package manager. It is recommended to use a virtual environment instead: https://pip.pypa.io/warnings/venv

[notice] A new release of pip is available: 24.0 -> 26.3
[notice] To update, run: pip install --upgrade pip
............                                                             [100%]
12 passed in 1.03s
pytest_exit=0
```

```
> powershell -ExecutionPolicy Bypass -File scripts/verify.ps1; "verify_exit=$LASTEXITCODE"

Check                        Result Detail
-----                        ------ ------
services up/healthy          PASS   9/9 up (7 healthy + producer/spark)
kafka has messages           PASS   end offset=304047 (exit=0)
lake raw/ticks objects       PASS   5 event_date partition object(s) listed (exit=0)
raw.price_windows fresh      PASS   rows=5761 max_ingested_at=2026-11-21 14:50:00 age_sec=11 (exit=0)
dbt test passes              PASS   14:50:16  Done. PASS=24 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=24
DAG exists & unpaused        PASS   streaming_analytics | /opt/airflow/dags/analytics_pipeline.py | airflow | False
dashboard HTTP 200           PASS   HTTP 200 on port 8501
pipeline_metrics lag <= 300s PASS   latest_ingested_at=2026-11-21 14:50:00 lag_sec=21 limit=300 (exit=0)

SUMMARY: 8/8 checks passed
verify_exit=0
```

```
> (git ls-files).Count
55
```

That count is the tracked-file inventory **before** `git add -A`; the two new files (`LICENSE`,
`docs/INTERVIEW_NOTES.md`) are added by the commit steps below, so the post-add count is reported
in the chat response (it cannot be pasted into a file that is part of its own commit).

### 7.7 Decisions, deviations and honest notes

- **No questions asked, as instructed.** Ambiguities resolved conservatively: a separate architecture
  doc was skipped to avoid duplicating the README; all credential-like literals were treated as
  permitted local demo defaults and named explicitly instead of being removed; dummy CI password
  literals in older evidence were masked rather than left as readable values.
- **No invented numbers.** New observations in this section are only the outputs of the commands
  shown (verify offsets/row counts/times, file counts, question count, test durations). All Results
  figures in the README are re-quoted from sections 0.2, 0.6, 1.2, 3.2, 3.3, 3.5, 6.2, 6.4 and 6.5.
- **Sanitization trade-off.** Replacing literals in pasted commands means those lines no longer
  reproduce the exact keystrokes (e.g. a dummy password is now `***`). No claim, output or test
  result was altered; the values were non-production dummy values and no real credential existed.
- **No code or data changes.** Spark, producer, dbt models, DAG logic, compose files, SQL and tests
  were not touched. `docker compose ps` shows the same nine services up before and after.
- **verify.ps1 ran twice** (7.0 and 7.6), both 8/8; the second run is the final state.
- **Post-commit outputs.** As in phases 4–6, `git log --oneline -3`, `git status --short`,
  `docker compose ps`, the verify output and the post-add `git ls-files` count are run after the
  commit and reported in the chat response.

### 7.8 Phase 7 status (before commit)

- [x] Step 0 recorded: clean tree, HEAD d0e98d2, nine services up, verify 8/8
- [x] Final README with all requested sections, mermaid diagrams kept, measured Results, demo
      defaults named, stale/contradictory text removed
- [x] `docs/INTERVIEW_NOTES.md` with 12 evidence-cited questions
- [x] Publish hygiene: personal paths replaced, dummy credential-like literals masked, emails and
      local demo defaults reviewed and documented, `.env` confirmed untracked
- [x] `.gitignore` covers env/venv/caches/dbt artefacts/temp files; status checked afterwards
- [x] MIT `LICENSE` added with placeholder "Ahmed"; no duplicate architecture doc added
- [x] Final checks: ruff clean, 12/12 unit tests, verify 8/8, `git ls-files` count recorded
- [x] No step failed in Phase 7

---

## Publishing — GitHub publication

All commands below were run on 2026-11-21 in this single session (PowerShell on Windows 11).
This section documents the publication of the Phase 7 commit `edb1a47` to GitHub. No token or
password was requested, printed or stored; Git Credential Manager handled authentication and no
browser prompt appeared. No secret-pattern match occurred, so nothing needed masking (the only
credential-like values are the documented local demo defaults from §7.3). The post-commit outputs
(`git log`/`git status` after the Publishing commit) are reported in the chat response, since they
cannot be pasted into a file that is part of their own commit.

### P.0 Step 0 — baseline

```
> git status
On branch main
nothing to commit, working tree clean
> git log -3 --oneline
edb1a47 docs: phase 7 final README, interview notes, publish hygiene
d0e98d2 feat(obs): phase 6 pipeline metrics, freshness fix, dashboard health
4f3ab0f ci: phase 5 workflow and verify script
> git remote -v
(no output)
> git config user.name
Pipeline Dev
> git config user.email
dev@localhost
> git branch -a
* main
```

All ten commits had `Pipeline Dev <dev@localhost>` as both author and committer (full log
checked before rewriting).

### P.1 Step 1 — backup, identity rewrite, proof

```
> git branch backup-before-rewrite
> git config user.name "Ahmed Said"
> git config user.email "12345+AhmedSaidRR@users.noreply.github.com"
> git config user.name
Ahmed Said
> git config user.email
12345+AhmedSaidRR@users.noreply.github.com
```

Rewrite with `filter-branch`, changing author and committer only when both match the old local
identity. The env-filter was sourced from a temporary helper file (removed afterwards) because
PowerShell mangled the inline quoting and because `filter-branch` changes its working directory:

```
> $env:FILTER_BRANCH_SQUELCH_WARNING=1
> git filter-branch --env-filter '. /d/projects/streaming-pipeline/streaming-pipeline/_filter_env.sh' -- main
Rewrite c0043aa36317cdebc5418cc073473e03e9988ec8 (1/10) ...
...
Rewrite edb1a4790c156693d26244e362f982a267acd1cf (7/10) (2 seconds passed, remaining 0 predicted)
Ref 'refs/heads/main' was rewritten
```

Helper script (both guards use the same new identity):

```sh
if [ "$GIT_AUTHOR_NAME" = "Pipeline Dev" ] && [ "$GIT_AUTHOR_EMAIL" = "dev@localhost" ]; then
  export GIT_AUTHOR_NAME="Ahmed Said"
  export GIT_AUTHOR_EMAIL="12345+AhmedSaidRR@users.noreply.github.com"
fi
if [ "$GIT_COMMITTER_NAME" = "Pipeline Dev" ] && [ "$GIT_COMMITTER_EMAIL" = "dev@localhost" ]; then
  export GIT_COMMITTER_NAME="Ahmed Said"
  export GIT_COMMITTER_EMAIL="12345+AhmedSaidRR@users.noreply.github.com"
fi
```

Three first attempts failed and are reported honestly rather than hidden: an inline `--env-filter`
was split by PowerShell argument parsing (`fatal: bad revision 'Dev ] && [ $GIT_AUTHOR_EMAIL =
dev@localhost ]; then export GIT_AUTHOR_NAME=Ahmed'`, exit 1); running the helper with `sh
./_filter_env.sh` failed because `filter-branch` runs from a temporary directory (`sh:
./_filter_env.sh: No such file or directory`); and running it with an absolute path via `sh`
produced `WARNING: Ref 'refs/heads/main' is unchanged` because the `export`s happened in a child
shell and were lost. Sourcing the file (`.`) fixed the third case. No commit was lost: the backup
branch still points at `edb1a47`.

Proof — new identity on every commit:

```
> git log --format="%h %an <%ae> | %cn <%ce> %s"
8ee4307 Ahmed Said <12345+AhmedSaidRR@users.noreply.github.com> | Ahmed Said <12345+AhmedSaidRR@users.noreply.github.com> docs: phase 7 final README, interview notes, publish hygiene
8c3c8f7 Ahmed Said <12345+AhmedSaidRR@users.noreply.github.com> | Ahmed Said <12345+AhmedSaidRR@users.noreply.github.com> feat(obs): phase 6 pipeline metrics, freshness fix, dashboard health
f7d1a7d Ahmed Said <12345+AhmedSaidRR@users.noreply.github.com> | Ahmed Said <12345+AhmedSaidRR@users.noreply.github.com> ci: phase 5 workflow and verify script
1586762 Ahmed Said <12345+AhmedSaidRR@users.noreply.github.com> | Ahmed Said <12345+AhmedSaidRR@users.noreply.github.com> feat(dbt): phase 4 test cleanup and README fixes
ac63488 Ahmed Said <12345+AhmedSaidRR@users.noreply.github.com> | Ahmed Said <12345+AhmedSaidRR@users.noreply.github.com> docs: phase 3 measured results (throughput, latency, windows, lake, tests)
a96ca50 Ahmed Said <12345+AhmedSaidRR@users.noreply.github.com> | Ahmed Said <12345+AhmedSaidRR@users.noreply.github.com> feat(dlq): prove dead-letter reasons, replay repair and sink-failure recovery
02d1609 Ahmed Said <12345+AhmedSaidRR@users.noreply.github.com> | Ahmed Said <12345+AhmedSaidRR@users.noreply.github.com> wip: phase 2 dead-letter and sink-failure handling (coded, partly verified)
c7828d8 Ahmed Said <12345+AhmedSaidRR@users.noreply.github.com> | Ahmed Said <12345+AhmedSaidRR@users.noreply.github.com> fix(boot): deterministic cold start in ~30s and a working dbt/Airflow path
28a5e61 Ahmed Said <12345+AhmedSaidRR@users.noreply.github.com> | Ahmed Said <12345+AhmedSaidRR@users.noreply.github.com> feat(lake): run the stack on SeaweedFS after MinIO images were removed upstream
ecd068e Ahmed Said <12345+AhmedSaidRR@users.noreply.github.com> | Ahmed Said <12345+AhmedSaidRR@users.noreply.github.com> chore: baseline snapshot before upgrade
```

Proof — tree unchanged (note the quotes needed for PowerShell):

```
> $bt = git rev-parse 'backup-before-rewrite^{tree}'; $ht = git rev-parse 'HEAD^{tree}'
> "backup-before-rewrite tree = $bt"
backup-before-rewrite tree = d4595ebc18f2c0c0f28167200d2261022960e582
> "HEAD tree                 = $ht"
HEAD tree                 = d4595ebc18f2c0c0f28167200d2261022960e582
> "tree_equal = $($bt -eq $ht)"
tree_equal = True
```

Cleanup of the `filter-branch` backup ref and the temporary helper (no `rm -rf`):

```
> git update-ref -d refs/original/refs/heads/main
> Remove-Item -Path _filter_env.sh -Force
> git for-each-ref refs/original
(no output)
> git status --short
(no output)
> git branch -v
  backup-before-rewrite edb1a47 docs: phase 7 final README, interview notes, publish hygiene
* main                  8ee4307 docs: phase 7 final README, interview notes, publish hygiene
```

`backup-before-rewrite` stays local-only and was never pushed.

### P.2 Step 2 — final secret check on tracked files

```
> git ls-files | findstr /i "\.env"
.env.example
> git grep -n -I -E 'BEGIN .* PRIVATE KEY|ghp_|AKIA|sk-'
(no output; git_grep_exit=1, i.e. no matches)
```

A broader review (`PASSWORD|SECRET|accessKey|secretKey|TOKEN|api[_-]?key`) returned only the known
local demo defaults from §7.3 — `lakeadmin`/`lakeadmin123`, `warehouse`, `airflow`, `postgres`
(dummy CI value), `local-dev-secret-change-me` — plus their documentation in this file. Nothing
that looks like a real credential was found, so the push proceeded.

### P.3 Step 3 — remote and push

```
> git remote add origin https://github.com/AhmedSaidRR/streaming-pipeline.git
> git remote -v
origin  https://github.com/AhmedSaidRR/streaming-pipeline.git (fetch)
origin  https://github.com/AhmedSaidRR/streaming-pipeline.git (push)
> git push -u origin main
To https://github.com/AhmedSaidRR/streaming-pipeline.git
 * [new branch]      main -> main
branch 'main' set up to track 'origin/main'.
```

No `--force` was used, no browser authentication prompt appeared, and the backup branch was not
pushed.

### P.4 Step 4 — verification

```
> git ls-remote origin
8ee4307d35ed7e64d05790a69cdd5de79424ee03        HEAD
8ee4307d35ed7e64d05790a69cdd5de79424ee03        refs/heads/main
> git status -sb
## main...origin/main
> git log --oneline -3
8ee4307 docs: phase 7 final README, interview notes, publish hygiene
8c3c8f7 feat(obs): phase 6 pipeline metrics, freshness fix, dashboard health
f7d1a7d ci: phase 5 workflow and verify script
```

Remote `HEAD` and `main` match the local `main`; only `main` exists on the remote.

### P.5 Step 5 — CI

`gh` is not installed and was not installed. Open
https://github.com/AhmedSaidRR/streaming-pipeline/actions and paste the failing job log if any job
is red. No CI fix was attempted without a log.

### P.6 Decisions, deviations and honest notes

- The rewrite covered only `main`, not `--all`, so the backup branch preserves the original
  history and the tree comparison is meaningful. Author **and** committer were rewritten using
  `GIT_AUTHOR_*` and `GIT_COMMITTER_*` in the env-filter.
- The GitHub noreply email is a public identity, not a secret. No token, password or credential
  was requested, printed or stored at any point.
- The Publishing commit itself uses the new repo-local identity (`Ahmed Said`), verified after
  the commit in the chat response.
- No `rm -rf`, `docker compose down -v`, prune, or change to Spark/producer/dbt/DAG logic or tests
  was used in this section.

### P.7 Commit-identity fix — placeholder 12345 replaced with real GitHub noreply ID

The author/committer email on every commit used a placeholder `12345+` local part. This section
records the rewrite to the real `208723650+` noreply identity (author **and** committer) on `main`.

Step 0 — baseline:

```
> git status
On branch main
Your branch is up to date with 'origin/main'.
nothing to commit, working tree clean
> git log -1 --format="%an <%ae>"
Ahmed Said <12345+AhmedSaidRR@users.noreply.github.com>
> git remote -v
origin  https://github.com/AhmedSaidRR/streaming-pipeline.git (fetch)
origin  https://github.com/AhmedSaidRR/streaming-pipeline.git (push)
```

Step 1 — local-only backup branch (never pushed):

```
> git branch backup-before-email-fix
> git branch -v
  backup-before-email-fix b31b0b4 docs: record GitHub publication and identity rewrite
  backup-before-rewrite   edb1a47 docs: phase 7 final README, interview notes, publish hygiene
* main                    b31b0b4 docs: record GitHub publication and identity rewrite
```

Steps 2-3 — helper script sourced by absolute path (filter-branch runs from a temp dir), rewrite
of all 11 commits, then repo-local config and cleanup of the backup ref and helper:

```
> $env:FILTER_BRANCH_SQUELCH_WARNING=1
> git filter-branch --env-filter '. /d/projects/streaming-pipeline/streaming-pipeline/_filter_env.sh' -- main
Rewrite ecd068ef... (1/11) ... Rewrite b31b0b48... (9/11) ...
Ref 'refs/heads/main' was rewritten
> git config --local user.name "Ahmed Said"
> git config --local user.email "208723650+AhmedSaidRR@users.noreply.github.com"
> git update-ref -d refs/original/refs/heads/main
> Remove-Item -Path _filter_env.sh -Force
> git for-each-ref refs/original
(no output)
```

Helper script (both guards match the old placeholder identity and export the new one):

```sh
if [ "$GIT_AUTHOR_NAME" = "Ahmed Said" ] && [ "$GIT_AUTHOR_EMAIL" = "12345+AhmedSaidRR@users.noreply.github.com" ]; then
  export GIT_AUTHOR_NAME="Ahmed Said"
  export GIT_AUTHOR_EMAIL="208723650+AhmedSaidRR@users.noreply.github.com"
fi
if [ "$GIT_COMMITTER_NAME" = "Ahmed Said" ] && [ "$GIT_COMMITTER_EMAIL" = "12345+AhmedSaidRR@users.noreply.github.com" ]; then
  export GIT_COMMITTER_NAME="Ahmed Said"
  export GIT_COMMITTER_EMAIL="208723650+AhmedSaidRR@users.noreply.github.com"
fi
```

Proof — new author/committer identity on all 11 commits, and unchanged tree (quotes for PowerShell):

```
> git log --format="%h %an <%ae> | %cn <%ce>"
4121824 Ahmed Said <208723650+AhmedSaidRR@users.noreply.github.com> | Ahmed Said <208723650+AhmedSaidRR@users.noreply.github.com>
783a8cb Ahmed Said <208723650+AhmedSaidRR@users.noreply.github.com> | Ahmed Said <208723650+AhmedSaidRR@users.noreply.github.com>
2ffc68a Ahmed Said <208723650+AhmedSaidRR@users.noreply.github.com> | Ahmed Said <208723650+AhmedSaidRR@users.noreply.github.com>
75efe9a Ahmed Said <208723650+AhmedSaidRR@users.noreply.github.com> | Ahmed Said <208723650+AhmedSaidRR@users.noreply.github.com>
4a75419 Ahmed Said <208723650+AhmedSaidRR@users.noreply.github.com> | Ahmed Said <208723650+AhmedSaidRR@users.noreply.github.com>
b12ace5 Ahmed Said <208723650+AhmedSaidRR@users.noreply.github.com> | Ahmed Said <208723650+AhmedSaidRR@users.noreply.github.com>
6fd536a Ahmed Said <208723650+AhmedSaidRR@users.noreply.github.com> | Ahmed Said <208723650+AhmedSaidRR@users.noreply.github.com>
22a26bb Ahmed Said <208723650+AhmedSaidRR@users.noreply.github.com> | Ahmed Said <208723650+AhmedSaidRR@users.noreply.github.com>
dfc5d72 Ahmed Said <208723650+AhmedSaidRR@users.noreply.github.com> | Ahmed Said <208723650+AhmedSaidRR@users.noreply.github.com>
5e28286 Ahmed Said <208723650+AhmedSaidRR@users.noreply.github.com> | Ahmed Said <208723650+AhmedSaidRR@users.noreply.github.com>
122d919 Ahmed Said <208723650+AhmedSaidRR@users.noreply.github.com> | Ahmed Said <208723650+AhmedSaidRR@users.noreply.github.com>
> $bt = git rev-parse 'backup-before-email-fix^{tree}'; $ht = git rev-parse 'HEAD^{tree}'
> "tree_equal = $($bt -eq $ht)"
tree_equal = True
```

Step 4 — the single allowed force push (the repo is new and mine); no browser auth prompt appeared:

```
> git push --force-with-lease origin main
To https://github.com/AhmedSaidRR/streaming-pipeline.git
 + b31b0b4...4121824 main -> main (forced update)
```

Step 5 — remote verification; only `main` exists on the remote, backup branch was not pushed:

```
> git ls-remote origin
41218242fa2ab55bb8f7b759bb5495bea848b3c7	HEAD
41218242fa2ab55bb8f7b759bb5495bea848b3c7	refs/heads/main
> git status -sb
## main...origin/main
> git log -1 --format="%an <%ae>"
Ahmed Said <208723650+AhmedSaidRR@users.noreply.github.com>
```

Step 6 — this note is committed with the new identity and pushed normally (no force). No `rm -rf`,
`docker compose down -v`, prune, or change to code/tests was used; `backup-before-email-fix` stays
local-only.

---

## README-only accuracy polish — command re-runs (2026-11-26)

README.md was the only documentation file changed for the prose fixes (plus this note). No code,
test, Docker or CI file was touched. The two `docker compose` commands from the README "Tests"
section were run once, in PowerShell on Windows 11 with the stack already up. The README now
splits them with bash backslash continuations; the commands below are the same commands in their
single-line form, and the arguments are identical.

```
> docker compose run --rm --no-deps -v "${PWD}/tests:/app/tests" -v "${PWD}/dbt:/app/dbt" producer sh -c "pip install -q pytest==8.3.2 && python -m pytest -q"; "pytest_exit=$LASTEXITCODE"
 Container streaming-pipeline-producer-run-c3804b7d0f9d Creating
 Container streaming-pipeline-producer-run-c3804b7d0f9d Created
WARNING: Running pip as the 'root' user can result in broken permissions and conflicting behaviour with the system package manager. It is recommended to use a virtual environment instead: https://pip.pypa.io/warnings/venv

[notice] A new release of pip is available: 24.0 -> 26.3
[notice] To update, run: pip install --upgrade pip
............                                                             [100%]
12 passed in 1.13s
pytest_exit=0
```

```
> docker compose run --rm --no-deps -v "${PWD}:/work" -w /work producer sh -c "pip install -q ruff==0.6.4 && ruff check ."; "ruff_exit=$LASTEXITCODE"
 Container streaming-pipeline-producer-run-da4f9f770be6 Creating
 Container streaming-pipeline-producer-run-da4f9f770be6 Created
WARNING: Running pip as the 'root' user can result in broken permissions and conflicting behaviour with the system package manager. It is recommended to use a virtual environment instead: https://pip.pypa.io/warnings/venv

[notice] A new release of pip is available: 24.0 -> 26.3
[notice] To update, run: pip install --upgrade pip
All checks passed!
ruff_exit=0
```

CI's `python -m pytest -q` step runs from the repository root (`.github/workflows/ci.yml`), where
`dags/tests/test_freshness.py` is present, so it collects those six tests too. The CI-equivalent
command, run in the pinned producer container with the whole repository mounted, gives the actual
count — 18, despite the job name "pytest (12 unit tests)":

```
> docker compose run --rm --no-deps -v "${PWD}:/work" -w /work producer sh -c "pip install -q pytest==8.3.2 && python -m pytest -q"; "ci_equivalent_exit=$LASTEXITCODE"
 Container streaming-pipeline-producer-run-2476dafa7745 Creating
 Container streaming-pipeline-producer-run-2476dafa7745 Created
WARNING: Running pip as the 'root' user can result in broken permissions and conflicting behaviour with the system package manager. It is recommended to use a virtual environment instead: https://pip.pypa.io/warnings/venv

[notice] A new release of pip is available: 24.0 -> 26.3
[notice] To update, run: pip install --upgrade pip
..................                                                       [100%]
18 passed in 2.18s
ci_equivalent_exit=0
```

No `rm -rf`, `docker compose down -v`, prune, or change to code/tests/Docker/CI was used.
