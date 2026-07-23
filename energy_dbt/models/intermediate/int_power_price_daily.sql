{{ config(materialized='table') }}

select
    cast(market_datetime_local - interval 5 minute as date) as market_date,
    state_code,
    count(*) as interval_count,
    avg(power_price_aud_mwh) as avg_power_price_aud_mwh,
    median(power_price_aud_mwh) as median_power_price_aud_mwh,
    min(power_price_aud_mwh) as min_power_price_aud_mwh,
    max(power_price_aud_mwh) as max_power_price_aud_mwh,
    stddev_samp(power_price_aud_mwh) as stddev_power_price_aud_mwh,
    sum(case when power_price_aud_mwh < 0 then 1 else 0 end)
        as negative_price_interval_count,
    sum(case when power_price_aud_mwh >= 1000 then 1 else 0 end)
        as high_price_interval_count
from {{ ref('stg_power_price') }}
group by
    market_date,
    state_code
