"""Every 5 minutes: freshness check -> dbt seed -> dbt snapshot (SCD2) -> dbt run -> dbt test."""
import os
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from freshness import assert_fresh, configured_max_lag_seconds

DBT = "/home/airflow/dbt_venv/bin/dbt"
DBT_CMD = f"cd /opt/airflow/dbt && {DBT} {{}} --profiles-dir ."


def check_stream_freshness(max_lag_seconds=None):
    """Fail loudly if no window has been *ingested* recently (not just window_end)."""
    import psycopg2

    threshold = configured_max_lag_seconds() if max_lag_seconds is None else max_lag_seconds
    conn = psycopg2.connect(
        host=os.environ["WH_HOST"], port=os.environ.get("WH_PORT", "5432"),
        user=os.environ["WH_USER"], password=os.environ["WH_PASSWORD"], dbname=os.environ["WH_DB"])
    with conn, conn.cursor() as cur:
        cur.execute("SELECT max(ingested_at) FROM raw.price_windows")
        latest = cur.fetchone()[0]
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    lag = assert_fresh(latest, now, threshold)
    print(f"Freshness OK: latest ingested_at={latest}, lag={lag:.0f}s (limit={threshold}s)")
    return lag


with DAG(
    dag_id="streaming_analytics",
    start_date=datetime(2026, 1, 1),
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["portfolio", "streaming", "dbt"],
) as dag:
    freshness = PythonOperator(
        task_id="check_stream_freshness",
        python_callable=check_stream_freshness,
        # First boot after `docker compose up` has no windows yet; give the stream time to catch up.
        retries=3,
        retry_delay=timedelta(seconds=60),
    )
    dbt_seed = BashOperator(task_id="dbt_seed", bash_command=DBT_CMD.format("seed"))
    dbt_snapshot = BashOperator(task_id="dbt_snapshot", bash_command=DBT_CMD.format("snapshot"))
    dbt_run = BashOperator(task_id="dbt_run", bash_command=DBT_CMD.format("run"))
    dbt_test = BashOperator(task_id="dbt_test", bash_command=DBT_CMD.format("test"))
    freshness >> dbt_seed >> dbt_snapshot >> dbt_run >> dbt_test
