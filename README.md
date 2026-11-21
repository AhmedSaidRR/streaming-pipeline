# Real-Time Crypto Price Streaming Pipeline

End-to-end streaming data engineering project: **Kafka -> Spark Structured Streaming -> S3-compatible lake (SeaweedFS) + PostgreSQL -> dbt -> Airflow -> Streamlit**.

It reads live prices from the public Binance API (or a built-in simulator), streams them through Kafka, aggregates them into 1-minute windows with Spark, lands raw data in an S3-compatible lake, models it with dbt, and monitors freshness and quality with Airflow.

## Architecture

```mermaid
flowchart LR
    A[Binance API / Simulator] --> B[Python Producer]
    B -->|JSON ticks| C[(Kafka: crypto.prices)]
    C --> D[Spark Structured Streaming]
    D -->|valid ticks, Parquet| E[(S3 lake: datalake/raw)]
    D -->|invalid messages| F[(S3 lake: dead_letter)]
    D -->|1-min windows| G[(PostgreSQL raw.price_windows)]
    G --> H[dbt: staging -> marts]
    I[Airflow every 5 min] -->|freshness check, dbt run, dbt test| H
    H --> J[Streamlit dashboard]
```

## Data model (dbt marts)

```mermaid
erDiagram
    fct_price_1m }o--|| dim_symbol : symbol_key
    fct_price_1m {
        int symbol_key
        timestamp window_start
        numeric avg_price
        numeric min_price
        numeric max_price
        bigint tick_count
        numeric price_range_pct
        boolean is_volatile
        numeric change_vs_prev_pct
    }
```
Grain of `fct_price_1m`: **one row per symbol per 1-minute window**. Also: `mart_hourly_prices` (OHLC-style) and `mart_volatility_alerts`.

## Stack
Kafka (KRaft) · Spark 3.5 Structured Streaming · SeaweedFS (S3 API) · PostgreSQL 16 · dbt-postgres · Apache Airflow 2.10 · Streamlit · Docker Compose · pytest · ruff · GitHub Actions

## Quick start

```bash
cp .env.example .env          # Windows: copy .env.example .env
docker compose up -d --build
```

| What | Where |
|---|---|
| Airflow (admin / admin) | http://localhost:8080 |
| Lake UI (SeaweedFS filer) | http://localhost:8888 || Dashboard | http://localhost:8501 |
| Warehouse (psql) | `docker compose exec warehouse psql -U warehouse -d warehouse` (host port 5433) |

1. Wait ~30 s for a cold start (Spark's Kafka/S3/JDBC jars are baked into the image at build time; no Maven/Ivy download on first start). Check: `docker compose logs -f spark`.
2. In Airflow, unpause `streaming_analytics`. It runs every 5 minutes: freshness check -> `dbt seed` -> `dbt snapshot` -> `dbt run` -> `dbt test`.
3. Open the dashboard.

`SOURCE=simulated` is the default (offline and reproducible, see `.env.example`). Using live
prices is **optional**: set `SOURCE=binance` in `.env` (the public API is geo-blocked in some
regions) and run `docker compose up -d --force-recreate producer`. If Binance is blocked, keep
or restore `SOURCE=simulated`.

### Throughput test
Set `SOURCE=simulated` and `SIM_EXTRA_SYMBOLS=500` to generate ~505 events/sec (5 base symbols + 500 extra), then record the numbers in the Results table.

## Verify everything

After `docker compose up -d` and a few minutes of data flow, one command checks the whole stack
— all services up/healthy, Kafka has messages, lake has `raw/ticks` objects, `raw.price_windows`
has fresh rows, `dbt test` passes, the `streaming_analytics` DAG exists and is unpaused, the
dashboard answers HTTP 200 on port 8501, and `marts.pipeline_metrics` reports ingestion lag
within 5 minutes. It prints a PASS/FAIL table and exits 1 if anything fails; it does not
modify data.

```powershell
powershell -ExecutionPolicy Bypass -File scripts/verify.ps1
```

CI runs on every push and pull request (`.github/workflows/ci.yml`): ruff, the 12 pytest unit
tests, `dbt parse`, `docker compose config -q` and `docker compose build`. The real outputs of
those same steps run locally are in `VERIFICATION.md`. No status badge is included yet because
the repository has not been pushed to GitHub.

## Observability

The `check_stream_freshness` task in the DAG measures **ingestion lag** —
`now() - max(raw.price_windows.ingested_at)` — rather than the age of `window_end`. It fails
when the lag exceeds `FRESHNESS_MAX_LAG_SECONDS` (default `300`, i.e. 5 minutes), so a stalled
stream stops the dbt run instead of quietly publishing stale marts.

`dbt run` builds two views in `marts` (computed at query time, never stale):

| View | Columns / purpose |
|---|---|
| `marts.pipeline_metrics` | `latest_ingested_at`, `ingestion_lag_seconds`, `windows_last_24h`, `windows_per_hour_last_24h` (count / 24), `avg_commit_lag_seconds` and `max_commit_lag_seconds` (`ingested_at - window_end`, over all rows) |
| `marts.pipeline_windows_per_hour` | one row per hour with at least one ingested window in the last 24 h — powers the dashboard chart |

The Streamlit **Pipeline health** section shows ingestion lag, windows/hour, average and max
commit lag, and the per-hour chart. It uses the SQL constants in `dashboard/queries.py`;
`scripts/verify.ps1` checks the same view (its eighth check: lag <= 300 s).

How to read it:

- Ingestion lag of a few seconds to ~60 s is healthy; approaching 300 s means Spark or the
  JDBC sink has stopped committing.
- Commit lag in the tens of seconds is expected (1-minute watermark + 30 s trigger + JDBC).
  A large **max** usually means a window was replayed from the Spark checkpoint after a
  restart, not a live delay.
- A drop in windows/hour means fewer windows are being committed for the configured symbols.

Dead-letter count is intentionally **not** a metric: dead letters are Parquet objects under
`dead_letter/` in the S3 lake, not a SQL relation, so counting them from dbt would require an
S3 scan. Inspect them in the lake UI or with `scripts/replay_dlq.py --dry-run`.

## SCD Type 2 demo (slowly changing dimension)

Symbol attributes (display name, asset class, active flag) live in `dbt/seeds/symbol_metadata.csv`. A dbt **snapshot** (`check` strategy) keeps the full history: when an attribute changes, the old row is closed (`dbt_valid_to`) and a new row is opened (`dbt_valid_from`). `dim_symbol` shows only the current version; `mart_symbol_history` shows every version.

Try it:
```bash
# 1. change a row, e.g. set SOLUSDT asset_class to "Layer 1"
# 2. let the DAG run (or trigger it); it runs: dbt seed -> dbt snapshot -> dbt run -> dbt test
docker compose exec warehouse psql -U warehouse -d warehouse -c \
 "SELECT symbol, asset_class, valid_from, valid_to, is_current
  FROM marts.mart_symbol_history WHERE symbol='SOLUSDT';"
```
You should see two rows: the old version with a `valid_to`, and the new current one.

## Design decisions
- **Kafka as the buffer:** the producer and the processing job are decoupled; Spark can restart and resume from its checkpoint.
- **Idempotent producer** (`enable.idempotence`, `acks=all`) avoids duplicates on retry.
- **Event-time windows + 1-minute watermark:** late data is handled by event time, not arrival time.
- **At-least-once into Postgres, dedupe in dbt:** after a Spark restart a window can be written twice. `stg_price_windows` keeps the latest row per (symbol, window_start); a source-level dbt test warns if the raw table contains duplicate (symbol, window_start) rows.
- **Dead-letter storage:** malformed or invalid messages are written to `dead_letter/` in the lake instead of being dropped.
- **Raw ticks kept in Parquet (partitioned by date):** the lake is the source of truth, so the warehouse can be rebuilt.
- **dbt in its own venv inside the Airflow image:** avoids dependency conflicts with Airflow.
- **SCD Type 2 via snapshot:** history of dimension attributes is kept without custom merge code, and facts always join to the current version.
- **Freshness check before dbt:** if streaming silently stops, the DAG fails instead of modelling stale data.

## Break it on purpose (dead-letter demo)
```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka:9092 --topic crypto.prices
# type:  {"symbol":"BTCUSDT","price":-5,"event_time":"2026-11-19T10:00:00+00:00"}
# type:  not json at all
```
After ~30 s both messages appear under `datalake/dead_letter/` in the lake, and the valid stream is unaffected.

## Tests
```bash
pip install -r requirements-dev.txt
pytest -q
ruff check .
```
dbt tests run inside Airflow (`dbt test`): not_null, unique, relationships, plus singular tests (prices positive, min <= avg <= max).

## Results (fill in after your own run)
| Metric | Value |
|---|---|
| Events/sec (simulated, N symbols) | ~507 events/s (N=505; Kafka end-offset delta 31,310 over 61.72 s; producer logs +505/s) |
| End-to-end latency (event -> warehouse) | 135.641074 s (2 m 15.6 s) — by design: 1-min watermark + 30 s trigger (probe `LATENCYPROBEUSDT`, `event_time` 15:06:44.368926 UTC -> `ingested_at` 15:09:00.01 UTC) |
| Raw data in lake after 24h | ~9.05 MiB **projection** from mostly the 5-symbol load, not 505 (5,093,976 B / 4.858 MiB observed over 12.88 h under `raw/ticks/`; not a measured 24 h figure; the ~4 min N=505 burst is a small part of that window) |
| 1-minute windows in warehouse | 4,136 (first 02:16:00, last 15:07:00 UTC) |
| dbt tests passed | 15 (PASS=15, WARN=0, ERROR=0, TOTAL=15) |
| Unit tests | 12 passed in 0.99s |

Add screenshots: Airflow DAG green, dashboard, lake bucket.

## Roadmap
- [x] SCD Type 2 on symbol attributes (dbt snapshot), see below
- [ ] Incremental dbt models instead of full-table rebuilds
- [ ] Great Expectations / Soda checks on the lake
- [ ] Prometheus + Grafana for Kafka lag and Spark metrics
- [ ] Deploy on a cloud VM with real AWS S3

## Data note
Prices come from Binance's public market-data endpoint (no key, no personal data). This project is for learning and is not financial advice.
