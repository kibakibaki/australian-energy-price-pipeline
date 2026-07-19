# Australian Energy Price Pipeline

A reproducible Python and DuckDB pipeline for ingesting historical Australian
electricity and gas spot prices from the Australian Energy Market Operator
(AEMO).

## Data coverage

| Commodity | Market | Resolution | Unit | Source |
| --- | --- | --- | --- | --- |
| Electricity | AEMO National Electricity Market (NEM) | 5 minutes | AUD/MWh | NEMWeb Dispatch Price |
| Gas | Short Term Trading Market (NSW, QLD, SA) | Daily gas day | AUD/GJ | AEMO STTM Price and Withdrawals |
| Gas | Victorian Declared Wholesale Gas Market (DWGM) | Schedule interval | AUD/GJ | AEMO DWGM Prices and Demand |

The ingestion commands below reproduce the documented electricity period and
the 2022-01-01 through 2026-01-01 gas period locally.
Generated DuckDB files and downloaded source archives are intentionally excluded
from Git because they are large and reproducible.

## Project structure

```text
aemo_ingestion.py       NEM electricity price ingestion
aemo_gas_ingestion.py   Victorian DWGM gas price ingestion
db_schema.py            Shared DuckDB schema and compatibility views
init_duckdb.py           Database initialisation
query.sql                Example validation queries
requirements.txt         Python dependencies
```

## Setup

Python 3.9 or later is required.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python init_duckdb.py
```

## Ingest electricity prices

The monthly archive mode supports historical MMSDM file naming used across
2024 and 2025.

```bash
python aemo_ingestion.py \
  --start-date 2024-01-01 \
  --end-date 2025-12-31 \
  --archive-mode monthly
```

## Ingest gas prices

```bash
python aemo_gas_ingestion.py \
  --start-date 2022-01-01 \
  --end-date 2026-01-01
```

The default ingests both STTM and DWGM. Use `--market sttm` or
`--market dwgm` to select one market, and `--refresh` to replace cached
AEMO workbooks.

## Query the data

The unified table is `fact_energy_price`. Convenience views expose explicit
price column names:

```sql
SELECT * FROM v_power_price LIMIT 100;
SELECT * FROM v_gas_price LIMIT 100;
```

Check coverage by commodity:

```sql
SELECT
    commodity,
    market,
    unit,
    MIN(market_datetime_local) AS first_timestamp,
    MAX(market_datetime_local) AS last_timestamp,
    COUNT(*) AS row_count
FROM fact_energy_price
GROUP BY commodity, market, unit
ORDER BY commodity, market;
```

## Notes

- NEM timestamps are interval-ending timestamps in Australian Eastern Standard
  Time as used by the market data.
- Negative electricity prices and prices at the market floor or cap are valid.
- Historical monthly electricity price files do not include dispatch demand
  metrics, so the optional demand fields are null for those records.
- AEMO STTM prices cover the Sydney (NSW), Brisbane (QLD), and Adelaide (SA)
  hubs. DWGM covers Victoria.
- Tasmania does not have an AEMO-operated wholesale gas spot market, so there
  is no equivalent official TAS gas spot-price series to ingest.

## Data sources

- [AEMO NEM data](https://www.aemo.com.au/energy-systems/electricity/national-electricity-market-nem/data-nem)
- [AEMO VIC wholesale gas prices](https://www.aemo.com.au/energy-systems/gas/declared-wholesale-gas-market-dwgm/data-dwgm/vic-wholesale-price-withdrawals)
- [AEMO STTM gas prices](https://www.aemo.com.au/energy-systems/gas/short-term-trading-market-sttm/data-sttm/daily-sttm-reports)
