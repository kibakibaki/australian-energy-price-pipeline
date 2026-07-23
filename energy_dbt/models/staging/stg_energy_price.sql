{{ config(materialized='view') }}

select
    interval_start_utc,
    market_datetime_local,
    timezone,
    trim(country) as country,
    upper(trim(commodity)) as commodity,
    upper(trim(market)) as market,
    upper(trim(region_code)) as region_code,
    trim(price_type) as price_type,
    cast(price as double) as price_value,
    cast(total_demand_mw as double) as total_demand_mw,
    cast(available_generation_mw as double)
        as available_generation_mw,
    cast(available_load_mw as double) as available_load_mw,
    upper(trim(currency)) as currency,
    trim(unit) as unit,
    cast(interval_minutes as integer) as interval_minutes,
    trim(source) as source,
    trim(source_file) as source_file,
    ingested_at_utc
from {{ source('energy_raw', 'fact_energy_price') }}
