select
    market_date,
    state_code,
    interval_count
from {{ ref('int_power_price_daily') }}
where interval_count <> 288
