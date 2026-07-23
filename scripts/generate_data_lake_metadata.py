from __future__ import annotations

import csv
from datetime import datetime, timezone
from pathlib import Path

import duckdb


DB_PATH = Path("database/australian_energy_market.duckdb")
METADATA_DIR = Path("data/metadata/australia")

SOURCE_CATALOG = [
    {
        "source": "AEMO_NEM",
        "dataset_name": "aemo_nem_dispatch_price",
        "source_url": (
            "https://nemweb.com.au/Data_Archive/"
            "Wholesale_Electricity/MMSDM"
        ),
        "native_frequency": "5-minute",
        "native_grain": "market interval + NEM region",
        "unit": "AUD/MWh",
        "timezone_context": "Australian NEM market time",
        "notes": "NSW, QLD, SA, TAS and VIC regional reference prices",
    },
    {
        "source": "AEMO_STTM",
        "dataset_name": "aemo_sttm_price",
        "source_url": (
            "https://www.aemo.com.au/energy-systems/gas/"
            "short-term-trading-market-sttm/data-sttm/daily-sttm-reports"
        ),
        "native_frequency": "daily gas day",
        "native_grain": "gas day + hub",
        "unit": "AUD/GJ",
        "timezone_context": "Local STTM hub time",
        "notes": "Sydney (NSW), Brisbane (QLD) and Adelaide (SA)",
    },
    {
        "source": "AEMO_DWGM",
        "dataset_name": "aemo_dwgm_price",
        "source_url": (
            "https://www.aemo.com.au/energy-systems/gas/"
            "declared-wholesale-gas-market-dwgm/data-dwgm/"
            "vic-wholesale-price-withdrawals"
        ),
        "native_frequency": "schedule interval",
        "native_grain": "schedule timestamp + market",
        "unit": "AUD/GJ",
        "timezone_context": "Australia/Melbourne",
        "notes": "Victorian Declared Wholesale Gas Market",
    },
]

MANIFEST_CONFIG = {
    "AEMO_NEM": {
        "source": "AEMO_NEM",
        "dataset_name": "aemo_nem_dispatch_price",
        "source_file": "48 monthly AEMO MMSDM ZIP files",
        "local_raw_path": "data/raw/australia/electricity/aemo/monthly",
        "source_url": SOURCE_CATALOG[0]["source_url"],
        "requested_date": "2022-01-01 to 2026-01-01 (end boundary)",
    },
    "AEMO_STTM": {
        "source": "AEMO_STTM",
        "dataset_name": "aemo_sttm_price",
        "source_file": "sttm-price-and-withdrawals.xlsx",
        "local_raw_path": "data/raw/australia/gas/aemo/sttm",
        "source_url": SOURCE_CATALOG[1]["source_url"],
        "requested_date": "2022-01-01 to 2026-01-01",
    },
    "AEMO_DWGM": {
        "source": "AEMO_DWGM",
        "dataset_name": "aemo_dwgm_price",
        "source_file": "dwgm-prices-and-demand.xlsx",
        "local_raw_path": "data/raw/australia/gas/aemo/dwgm",
        "source_url": SOURCE_CATALOG[2]["source_url"],
        "requested_date": "2022-01-01 to 2026-01-01",
    },
}


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(
            output,
            fieldnames=list(rows[0]),
            lineterminator="\n",
        )
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    if not DB_PATH.exists():
        raise FileNotFoundError(f"DuckDB database not found: {DB_PATH}")

    generated_at = datetime.now(timezone.utc).isoformat()
    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        coverage_rows = con.execute(
            """
            SELECT market,
                   COUNT(*) AS record_count,
                   MIN(interval_start_utc) AS actual_start_timestamp,
                   MAX(interval_start_utc) AS actual_end_timestamp
            FROM fact_energy_price
            GROUP BY market
            ORDER BY market
            """
        ).fetchall()
        manifest = []
        for (
            market,
            record_count,
            actual_start_timestamp,
            actual_end_timestamp,
        ) in coverage_rows:
            config = MANIFEST_CONFIG[market]
            manifest.append(
                {
                    "source": config["source"],
                    "dataset_name": config["dataset_name"],
                    "source_file": config["source_file"],
                    "local_raw_path": config["local_raw_path"],
                    "ingestion_timestamp": generated_at,
                    "actual_start_timestamp": actual_start_timestamp,
                    "actual_end_timestamp": actual_end_timestamp,
                    "record_count": record_count,
                    "status": "migrated_verified",
                    "source_url": config["source_url"],
                    "requested_date": config["requested_date"],
                }
            )

        columns = con.execute(
            """
            SELECT table_name AS dataset, column_name, data_type,
                   is_nullable, ordinal_position
            FROM information_schema.columns
            WHERE table_schema = 'main'
            ORDER BY table_name, ordinal_position
            """
        ).fetchdf().to_dict("records")
    finally:
        con.close()

    write_csv(METADATA_DIR / "source_catalog.csv", SOURCE_CATALOG)
    write_csv(METADATA_DIR / "ingestion_manifest.csv", manifest)
    write_csv(METADATA_DIR / "data_dictionary.csv", columns)
    print(f"Metadata written to {METADATA_DIR}")


if __name__ == "__main__":
    main()
