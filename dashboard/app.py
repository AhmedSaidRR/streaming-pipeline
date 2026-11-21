import os
import time

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text

from dashboard.queries import PIPELINE_METRICS_SQL, PIPELINE_WINDOWS_PER_HOUR_SQL

st.set_page_config(page_title="Crypto Streaming Dashboard", layout="wide")


@st.cache_resource
def engine():
    url = "postgresql+psycopg2://{u}:{p}@{h}:{port}/{d}".format(
        u=os.getenv("WH_USER", "warehouse"), p=os.getenv("WH_PASSWORD", "warehouse"),
        h=os.getenv("WH_HOST", "warehouse"), port=os.getenv("WH_PORT", "5432"),
        d=os.getenv("WH_DB", "warehouse"))
    return create_engine(url)


def query(sql: str, **params) -> pd.DataFrame:
    with engine().connect() as conn:
        return pd.read_sql(text(sql), conn, params=params)


st.title("Real-Time Crypto Price Pipeline")
st.caption("Kafka -> Spark Structured Streaming -> PostgreSQL -> dbt -> Streamlit")

st.subheader("Pipeline health")
try:
    health = query(PIPELINE_METRICS_SQL)
    if health.empty:
        st.info("Pipeline metrics are not available yet.")
    else:
        h = health.iloc[0]
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Ingestion lag (s)", f"{h.ingestion_lag_seconds:.0f}")
        c2.metric("Windows / hour (24h)", f"{h.windows_per_hour_last_24h:.1f}")
        c3.metric("Avg commit lag (s)", f"{h.avg_commit_lag_seconds:.0f}")
        c4.metric("Max commit lag (s)", f"{h.max_commit_lag_seconds:.0f}")
        c5.metric("Latest ingested (UTC)", str(h.latest_ingested_at))
        by_hour = query(PIPELINE_WINDOWS_PER_HOUR_SQL)
        if not by_hour.empty:
            st.bar_chart(by_hour.set_index("hour_start")["windows"])
except Exception:
    st.info("No pipeline metrics yet. Run the Airflow DAG (or `dbt run`) to create them.")

try:
    symbols = query("select symbol from marts.dim_symbol order by symbol")["symbol"].tolist()
except Exception:
    st.info("No marts yet. Wait for the Airflow DAG `streaming_analytics` to run dbt (every 5 min), "
            "or run `dbt run` manually.")
    st.stop()

symbol = st.sidebar.selectbox("Symbol", symbols)
minutes = st.sidebar.slider("Last N minutes", 10, 240, 60)
auto = st.sidebar.checkbox("Auto refresh (15s)")

df = query("""
    select f.window_start, f.avg_price, f.min_price, f.max_price, f.tick_count,
           f.price_range_pct, f.is_volatile, f.change_vs_prev_pct
    from marts.fct_price_1m f join marts.dim_symbol d using (symbol_key)
    where d.symbol = :s and f.window_start > (now() at time zone 'utc') - make_interval(mins => :m)
    order by f.window_start
""", s=symbol, m=minutes)

if df.empty:
    st.warning("No data in this time range.")
else:
    last = df.iloc[-1]
    c1, c2, c3 = st.columns(3)
    c1.metric("Latest avg price", f"{last.avg_price:,.4f}", f"{(last.change_vs_prev_pct or 0):.3f}%")
    c2.metric("Ticks in last window", int(last.tick_count))
    c3.metric("Volatile windows", int(df.is_volatile.sum()))
    st.line_chart(df.set_index("window_start")[["avg_price", "min_price", "max_price"]])

st.subheader("Volatility alerts (last 24h)")
st.dataframe(query("select * from marts.mart_volatility_alerts limit 50"), use_container_width=True)

if auto:
    time.sleep(15)
    st.rerun()
