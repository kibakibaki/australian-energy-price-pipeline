from __future__ import annotations

import csv
from datetime import datetime, timezone
from pathlib import Path

import duckdb


DB_PATH = Path("data/database/australian_energy_market.duckdb")
METADATA_DIR = Path("data/metadata")

SOURCE_CATALOG = [
    {
        "dataset": "aemo_nem_dispatch_price",
        "commodity": "POWER",
        "regions": "NSW1|QLD1|SA1|TAS1|VIC1",
        "frequency": "5 minutes",
        "source": "AEMO NEMWeb MMSDM",
        "raw_path": "data/raw/electricity/aemo/monthly",
    },
    {
        "dataset": "aemo_sttm_price",
        "commodity": "GAS",
        "regions": "NSW|QLD|SA",
        "frequency": "daily gas day",
        "source": "AEMO STTM",
        "raw_path": "data/raw/gas/aemo/sttm",
    },
    {
        "dataset": "aemo_dwgm_price",
        "commodity": "GAS",
        "regions": "VIC",
        "frequency": "schedule interval",
        "source": "AEMO DWGM",
        "raw_path": "data/raw/gas/aemo/dwgm",
    },
]


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
        coverage = con.execute(
            """
            SELECT commodity, market, region_code,
                   COUNT(*) AS row_count,
                   MIN(interval_start_utc) AS first_timestamp_utc,
                   MAX(interval_start_utc) AS last_timestamp_utc
            FROM fact_energy_price
            GROUP BY ALL
            ORDER BY commodity, market, region_code
            """
        ).fetchdf()
        manifest = coverage.assign(
            dataset="fact_energy_price",
            status="loaded",
            generated_at_utc=generated_at,
        )[
            [
                "dataset", "commodity", "market", "region_code", "status",
                "row_count", "first_timestamp_utc", "last_timestamp_utc",
                "generated_at_utc",
            ]
        ].to_dict("records")

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
