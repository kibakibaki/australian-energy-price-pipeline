{{ config(materialized='view') }}

select
    interval_start_utc,
    market_datetime_local,
    timezone,
    case region_code
        when 'NSW1' then 'NSW'
        when 'QLD1' then 'QLD'
        when 'SA1' then 'SA'
        when 'TAS1' then 'TAS'
        when 'VIC1' then 'VIC'
    end as state_code,
    region_code as aemo_region_code,
    market,
    price_type,
    price_value as power_price_aud_mwh,
    total_demand_mw,
    available_generation_mw,
    available_load_mw,
    interval_minutes,
    source,
    source_file,
    ingested_at_utc
from {{ ref('stg_energy_price') }}
where commodity = 'POWER'
  and market = 'AEMO_NEM'
