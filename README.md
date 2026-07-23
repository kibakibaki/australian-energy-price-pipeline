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

## Data Lake structure

```text
data/
├── raw/
│   └── australia/
│       ├── electricity/aemo/ Original AEMO NEM archives (Bronze)
│       └── gas/aemo/         Original STTM and DWGM workbooks (Bronze)
├── raw_parquet/
│   └── australia/
│       ├── electricity/
│       │   └── aemo_nem_dispatch_price.parquet
│       └── gas/
│           ├── aemo_sttm_price.parquet
│           └── aemo_dwgm_price.parquet
└── metadata/
    └── australia/
        ├── source_catalog.csv
        ├── ingestion_manifest.csv
        └── data_dictionary.csv

database/
└── australian_energy_market.duckdb    Local dbt/DuckDB database
```

Weather is intentionally omitted until a weather source is selected.
The raw-equivalent Parquet files preserve the unified ingestion schema while
remaining separated by source dataset. dbt staging, intermediate, and mart
relations remain inside DuckDB instead of being exported as duplicate files.

## Rebuild DuckDB from the shared Data Lake

Download the following Google Drive folder into the repository root, preserving
the directory names:

```text
data/raw/australia/
├── electricity/aemo/monthly/{year}/{month}/*.zip
└── gas/aemo/
    ├── sttm/sttm-price-and-withdrawals.xlsx
    └── dwgm/dwgm-prices-and-demand.xlsx
```

The ingestion scripts reuse these local source files when they exist. Network
access to AEMO is only required for a missing source file or when gas ingestion
is run with `--refresh`.

### 1. Set up Python

Python 3.9 or later is required.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python init_duckdb.py
```

### 2. Load electricity into DuckDB

The requested period is represented as `[2022-01-01, 2026-01-01)`: four
complete calendar years, including the 2024 leap day. The monthly archive mode
supports both historical MMSDM filename conventions present in this period.

```bash
python aemo_ingestion.py \
  --start-date 2022-01-01 \
  --end-date 2025-12-31 \
  --archive-mode monthly
```

Expected result: 2,103,840 five-minute rows, or 420,768 rows for each of NSW,
QLD, SA, TAS, and VIC.

### 3. Load gas into DuckDB

```bash
python aemo_gas_ingestion.py \
  --start-date 2022-01-01 \
  --end-date 2026-01-01
```

The default ingests both STTM and DWGM. Use `--market sttm` or
`--market dwgm` to select one market, and `--refresh` to replace cached
AEMO workbooks.

Expected result: 11,696 rows: 1,462 STTM gas days for each of NSW, QLD, and SA,
plus 7,310 Victorian DWGM schedule rows. Tasmania has no equivalent
AEMO-operated wholesale gas spot market.

### 4. Build and test dbt models

Run dbt from its project directory with the repository-local profile:

```bash
cd energy_dbt
../.venv/bin/dbt build --profiles-dir .
cd ..
```

The dbt test result is written to `energy_dbt/target/run_results.json` and the
detailed execution log to `energy_dbt/logs/dbt.log`.

Expected result:

```text
PASS=63 WARN=0 ERROR=0 SKIP=0
```

### 5. Export Data Lake files and validate

```bash
.venv/bin/python scripts/convert_csv_to_parquet.py
.venv/bin/python scripts/generate_data_lake_metadata.py
.venv/bin/python scripts/validate_energy_price_data.py
```

Upload the regenerated `data/raw_parquet/australia` and
`data/metadata/australia` folders to the matching shared Google Drive
directories. These files are snapshots and do not update automatically after a
local pipeline run.

### 6. Verify the physical DuckDB tables

The local database is `database/australian_energy_market.duckdb`. The raw
physical table is `fact_energy_price`; dbt staging models are views, while
intermediate and mart models are physical tables.

```bash
.venv/bin/python -c "import duckdb; c=duckdb.connect('database/australian_energy_market.duckdb'); print(c.execute('show tables').fetchdf().to_string(index=False))"
```

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
- The daily mart uses the power calendar as its base and LEFT JOINs same-date,
  same-state gas prices. Missing gas values are not forward-filled. The
  `gas_alignment_status` field distinguishes `matched`,
  `missing_source_date`, and `not_available_for_state`.

## Data sources

- [AEMO NEM data](https://www.aemo.com.au/energy-systems/electricity/national-electricity-market-nem/data-nem)
- [AEMO VIC wholesale gas prices](https://www.aemo.com.au/energy-systems/gas/declared-wholesale-gas-market-dwgm/data-dwgm/vic-wholesale-price-withdrawals)
- [AEMO STTM gas prices](https://www.aemo.com.au/energy-systems/gas/short-term-trading-market-sttm/data-sttm/daily-sttm-reports)
