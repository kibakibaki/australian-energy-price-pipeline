{{ config(materialized='table') }}

select
    country,
    commodity,
    market,
    region_code,
    price_type,
    currency,
    unit,
    interval_minutes,

    count(*) as row_count,
    min(interval_start_utc) as first_interval_utc,
    max(interval_start_utc) as last_interval_utc,
    avg(price_value) as avg_price,
    min(price_value) as min_price,
    max(price_value) as max_price,
    avg(total_demand_mw) as avg_total_demand_mw

from {{ ref('stg_energy_price') }}

where price_value is not null

group by
    country,
    commodity,
    market,
    region_code,
    price_type,
    currency,
    unit,
    interval_minutes
