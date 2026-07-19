select
    interval_start_utc,
    state_code,
    count(*) as duplicate_count
from {{ ref('stg_power_price') }}
group by
    interval_start_utc,
    state_code
having count(*) > 1
