{{ config(materialized='view') }}

select
    interval_start_utc,
    market_datetime_local,
    timezone,
    country,
    commodity,
    market,
    region_code,
    price_type,
    price as price_value,
    total_demand_mw,
    available_generation_mw,
    available_load_mw,
    currency,
    unit,
    interval_minutes,
    source,
    source_file,
    ingested_at_utc
from fact_energy_price
