# Data Engineering interview notes

Short, honest answers grounded in this repository. Section references are to
[VERIFICATION.md](../VERIFICATION.md), which contains the commands and real outputs behind every
claim. Numbers here are the ones measured there; nothing is rounded up for effect.

### 1. Why Kafka in the middle instead of writing straight to the lake or warehouse?

It decouples the producer from processing: Spark can restart and resume from its checkpoint without
the producer knowing, and the topic is a replayable buffer during outages. The producer is
idempotent (`enable.idempotence`, `acks=all`). We demonstrated real messages in `crypto.prices`
before Spark was even wired up (§0.5) and measured throughput from the Kafka end offset (§3.2).

### 2. Why at-least-once rather than exactly-once?

Exactly-once spanning Kafka, S3 and JDBC would need coordinated transactions and would be much
harder to reason about locally. We chose the simpler contract: append to Postgres, quarantine a
failed batch to the lake, and deduplicate in dbt. `stg_price_windows` keeps the latest row per
`(symbol, window_start)`; the source-level uniqueness test is `severity: warn` because duplicates
are legitimate, not a data error (§2.5, §4.3).

### 3. What is the watermark/latency tradeoff?

The job uses a 1-minute watermark, a 30 s trigger and event-time 1-minute windows, so a result is
only committed after the watermark has passed. The measured probe took 135.641074 s from
`event_time` to `ingested_at` (including 5 s deliberately added to the probe). That is slow by
design in exchange for bounded late-data handling, not an accident (§0.6, §3.3).

### 4. How do dead letters and replay work?

Every rejected message keeps a reason (`unparseable_json`, `missing_symbol`, `missing_price`,
`non_positive_price`, `invalid_event_time`) plus its Kafka topic/partition/offset and is written as
date-partitioned Parquet under `dead_letter/`. `scripts/replay_dlq.py --dry-run` lists them; with
`--set price=9999.99` we repaired one `missing_price` row, replayed it, and saw the value reach a
warehouse window (§2.1, §2.3, §2.4).

### 5. What happens when the warehouse sink goes down?

`write_windows_to_postgres` first writes the failed microbatch to
`dead_letter/sink_failures/batch_id=...`, then re-raises so Spark retries. While Postgres was
stopped, the raw-tick and dead-letter streams kept running, new bad messages still landed, and the
stuck batch was committed first after restart — window count grew 280 → 305 and no data was lost
(§2.5).

### 6. How does SCD2 work, and where does the history live?

A dbt snapshot with the `check` strategy watches display name, asset class and active flag. When a
value changes, the old row gets a `dbt_valid_to` and a new current row opens. `dim_symbol` exposes
only current versions; `mart_symbol_history` keeps every version (§1.5, §4.4; snapshot definition
in `dbt/snapshots/`).

### 7. Where do you rely on idempotency and deduplication?

Producer retries are idempotent, the JDBC sink is deliberately at-least-once, and dbt dedupes by
`row_number()` over `ingested_at desc` per symbol/window. We proved the duplicate test can actually
fail by pointing it at a scratch table with two identical rows (it emitted `WARN 1`), then dropped
the scratch table without touching real rows (§4.3).

### 8. What was the hardest bug, and how did you find it?

The Parquet dead-letter sink silently stopped writing. The checkpoint had been reset after a schema
change, but a leftover `_spark_metadata` log still held higher batch ids, so Spark skipped every
new batch. Listing the lake and checkpoint exposed the drift; the fix was `foreachBatch` with a
plain Parquet write and output-committer v2, which also fixed a `FileNotFoundException` from stale
`_temporary` directories (§2.2).

### 9. How do you know the pipeline is healthy right now?

`scripts/verify.ps1` checks nine services, Kafka messages, lake objects, warehouse freshness, dbt
tests, the DAG state, dashboard HTTP 200 and `marts.pipeline_metrics` lag — 8/8 passed in the final
run (§6.4). Separately, the DAG's freshness task measures `now() - max(ingested_at)` with a 300 s
default; a deliberately tiny 1 s threshold was proven to fail it, while the real task succeeded
(§6.1).

### 10. What would you change at scale?

Replace the single-node stack with managed Kafka, a distributed lake and a real warehouse; make
dbt models incremental; add partitioning/compaction for the Parquet lake; and add Prometheus/Grafana
alerts on consumer lag and commit lag. Those are exactly the open items in the README Roadmap, plus
the 82,980 s replayed-window max showing why commit-lag history needs better handling (§6.3).

### 11. What would you do differently next time?

Pin every dependency pair up front — the unpinned dbt-core/adapter combination once installed an
incompatible 2.x release candidate (§1.4). Check upstream image availability before designing
around a vendor: the MinIO images were gone (404/410/401), which forced the SeaweedFS switch
(§0.2). And avoid file-sink metadata logs on S3-compatible storage; use `foreachBatch` from the
start (§2.2).

### 12. How do you back up the numbers in the README?

Each figure has a command and output in VERIFICATION.md: throughput from a 60 s Kafka offset delta
(§3.2), latency from a probe row's `ingested_at` (§3.3), lake size from a paginated S3 listing —
explicitly labelled a **projection**, not a measured 24-hour result (§3.5). Cold boots were timed
four times (§1.2), and phases 4–6 re-quote earlier measurements rather than re-running them so the
live stream was not disturbed (§4.7).
