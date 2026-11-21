{{ config(materialized='view') }}

-- One row per hour with at least one ingested window in the last 24 h.
-- Powers the "windows per hour" chart in the Streamlit dashboard.
select
    date_trunc('hour', ingested_at) as hour_start,
    count(*) as windows
from {{ ref('stg_price_windows') }}
where ingested_at > (now() at time zone 'utc') - interval '24 hours'
group by 1
