{{ config(materialized='view') }}

select
    interval_start_utc,
    market_datetime_local,
    timezone,
    region_code as state_code,
    market,
    price_type,
    price_value as gas_price_aud_gj,
    interval_minutes,
    source,
    source_file,
    ingested_at_utc
from {{ ref('stg_energy_price') }}
where commodity = 'GAS'
  and market in ('AEMO_STTM', 'AEMO_DWGM')
  and region_code in ('NSW', 'QLD', 'SA', 'VIC')
