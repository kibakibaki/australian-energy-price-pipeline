select
    interval_start_utc,
    state_code,
    gas_price_aud_gj
from {{ ref('stg_gas_price') }}
where state_code in ('TAS', 'TAS1')
