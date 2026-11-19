# AGENTS.md — working rules for this repo

Real-time crypto pipeline: Binance/simulator -> Kafka (KRaft) -> Spark Structured Streaming
-> S3-compatible lake (SeaweedFS) + PostgreSQL -> dbt -> Airflow -> Streamlit.

## Environment

- Windows 11 + Docker Desktop (WSL2 backend). Shell is PowerShell.
- Everything runs in containers. Do not rely on host Python (only 3.14 is installed, and the
  pins do not build there). Run tests with:
  `docker compose run --rm --entrypoint python producer -m pytest -q`
- Run dbt with:
  `docker compose exec airflow /home/airflow/dbt_venv/bin/dbt <cmd> --profiles-dir /opt/airflow/dbt`
  (working dir `/opt/airflow/dbt`).

## Hard rules

1. **Evidence or it didn't happen.** Paste the command and its real output. Never claim a
   result you did not observe.
2. **No invented numbers.** Every metric in docs must come from a command run in this session,
   with duration and inputs stated.
3. **Never weaken a test to make it pass.** If a test cannot fail, delete it or make it real.
4. **Pin everything.** No `:latest` image tags; use a version tag or a digest.
5. **No secrets in git.** Only `.env.example` is committed. `.env` stays local.
6. One phase at a time; conventional commits (`feat:`, `fix:`, `test:`, `docs:`, `chore:`).

## Architecture notes

- The lake is **SeaweedFS**, not MinIO: MinIO's free images were removed upstream
  (Docker Hub `minio/minio` -> 404, `dl.min.io` -> 410, `quay.io/minio` -> 401).
  SeaweedFS speaks S3, so Spark keeps using the s3a filesystem unchanged.
- S3 credentials live in `infra/seaweedfs/s3.json` and `.env` (`S3_ACCESS_KEY`, `S3_SECRET_KEY`).
  They must match.
- Lake layout: `s3://datalake/raw/ticks/event_date=YYYY-MM-DD/...` and
  `s3://datalake/dead_letter/rejected_date=YYYY-MM-DD/...`.
- Postgres `raw.price_windows` is append-only (at-least-once); dbt dedupes it.

## Ports

| Service | Port |
|---|---|
| Kafka (in-network only) | kafka:9092 |
| Lake S3 API | 8333 |
| Lake filer UI | 8888 |
| Lake master UI | 9333 |
| Warehouse Postgres | 5433 |
| Airflow | 8080 |
| Dashboard | 8501 |
