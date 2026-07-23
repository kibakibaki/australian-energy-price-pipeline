SELECT
    commodity,
    market,
    unit,
    MIN(market_datetime_local) AS first_time,
    MAX(market_datetime_local) AS last_time,
    COUNT(*) AS row_count
FROM fact_energy_price
GROUP BY commodity, market, unit;

--
SELECT
    YEAR(market_datetime_local) AS year,
    region_code,
    COUNT(*) AS rows,
    MIN(market_datetime_local) AS first_time,
    MAX(market_datetime_local) AS last_time
FROM v_power_price
GROUP BY year, region_code
ORDER BY year, region_code;


--check how many records are in v_power_price view
SELECT COUNT(*)
FROM v_power_price;


--check if there are any duplicate records in fact_energy_price table for POWER commodity
SELECT
    interval_start_utc,
    market,
    region_code,
    price_type,
    COUNT(*) AS duplicate_count
FROM fact_energy_price
WHERE commodity = 'POWER'
GROUP BY
    interval_start_utc,
    market,
    region_code,
    price_type
HAVING COUNT(*) > 1;

--check every 5 minute interval has 288 records for each region_code in v_power_price view
-- expect 0
SELECT
    CAST(
        market_datetime_local - INTERVAL 5 MINUTE
        AS DATE
    ) AS market_date,
    region_code,
    COUNT(*) AS interval_count
FROM v_power_price
GROUP BY market_date, region_code
HAVING COUNT(*) <> 288
ORDER BY market_date, region_code;

-- Check dbt summary output
select *
from mart_energy_price_summary
limit 20;

-- Check fact table row count
select count(*) as row_count
from fact_energy_price;

-- Check regional price summary
select
    region_code,
    count(*) as row_count,
    avg(price) as avg_price,
    min(price) as min_price,
    max(price) as max_price
from fact_energy_price
group by region_code
order by region_code;
SELECT distinct region_code
FROM v_power_price
