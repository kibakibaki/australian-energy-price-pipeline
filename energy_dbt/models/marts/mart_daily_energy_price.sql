{{ config(materialized='table') }}

select
    power.market_date,
    power.state_code,
    power.interval_count,
    power.avg_power_price_aud_mwh,
    power.median_power_price_aud_mwh,
    power.min_power_price_aud_mwh,
    power.max_power_price_aud_mwh,
    power.stddev_power_price_aud_mwh,
    power.negative_price_interval_count,
    power.high_price_interval_count,
    gas.gas_price_aud_gj,
    gas.gas_market,
    gas.gas_price_type,
    gas.gas_price_aud_gj is not null as has_gas_spot_price,
    case
        when gas.gas_price_aud_gj is not null then 'matched'
        when power.state_code = 'TAS' then 'not_available_for_state'
        else 'missing_source_date'
    end as gas_alignment_status
from {{ ref('int_power_price_daily') }} as power
left join {{ ref('int_gas_price_daily') }} as gas
    on power.market_date = gas.gas_date
    and power.state_code = gas.state_code
