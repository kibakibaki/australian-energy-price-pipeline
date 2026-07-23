select
    gas_date,
    state_code,
    count(*) as duplicate_count
from {{ ref('int_gas_price_daily') }}
group by
    gas_date,
    state_code
having count(*) > 1
