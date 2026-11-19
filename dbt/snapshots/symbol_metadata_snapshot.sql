{# SCD Type 2: every time display_name / asset_class / is_active changes in the seed,
   dbt closes the old row (dbt_valid_to) and inserts a new one (dbt_valid_from). #}
{% snapshot symbol_metadata_snapshot %}
{{
    config(
        target_schema='snapshots',
        unique_key='symbol',
        strategy='check',
        check_cols=['display_name', 'asset_class', 'is_active']
    )
}}
select * from {{ ref('symbol_metadata') }}
{% endsnapshot %}
