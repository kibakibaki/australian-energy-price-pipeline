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
    min(market_datetime_local) as first_market_datetime,
    max(market_datetime_local) as last_market_datetime,
    avg(price_value) as avg_price,
    median(price_value) as median_price,
    min(price_value) as min_price,
    max(price_value) as max_price

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
