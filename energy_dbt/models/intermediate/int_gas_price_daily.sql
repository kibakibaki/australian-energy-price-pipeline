{{ config(materialized='table') }}

select
    cast(market_datetime_local as date) as gas_date,
    state_code,
    market as gas_market,
    price_type as gas_price_type,
    gas_price_aud_gj
from {{ ref('stg_gas_price') }}
where market = 'AEMO_STTM'
   or (
       market = 'AEMO_DWGM'
       and extract(hour from market_datetime_local) = 6
   )
