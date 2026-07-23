from pathlib import Path
import duckdb


DB_PATH = Path("database/australian_energy_market.duckdb")
PARQUET_DIR = Path("data/raw_parquet/australia")

DATASETS_TO_EXPORT = {
    "aemo_nem_dispatch_price": {
        "query": """
            SELECT *
            FROM fact_energy_price
            WHERE commodity = 'POWER'
              AND market = 'AEMO_NEM'
        """,
        "output_path": (
            PARQUET_DIR
            / "electricity"
            / "aemo_nem_dispatch_price.parquet"
        ),
    },
    "aemo_sttm_price": {
        "query": """
            SELECT *
            FROM fact_energy_price
            WHERE commodity = 'GAS'
              AND market = 'AEMO_STTM'
        """,
        "output_path": PARQUET_DIR / "gas" / "aemo_sttm_price.parquet",
    },
    "aemo_dwgm_price": {
        "query": """
            SELECT *
            FROM fact_energy_price
            WHERE commodity = 'GAS'
              AND market = 'AEMO_DWGM'
        """,
        "output_path": PARQUET_DIR / "gas" / "aemo_dwgm_price.parquet",
    },
}

LEGACY_COMBINED_PARQUET = PARQUET_DIR / "fact_energy_price.parquet"


def quote_string(value: str) -> str:
    """Quote a DuckDB string literal."""
    return "'" + value.replace("'", "''") + "'"


def export_table_to_parquet(
    con: duckdb.DuckDBPyConnection,
    dataset_name: str,
    query: str,
    output_path: Path,
) -> int:
    """Export one raw-equivalent dataset atomically and return its row count."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = output_path.with_suffix(".parquet.tmp")
    temporary_path.unlink(missing_ok=True)

    print(f"Exporting {dataset_name} -> {output_path}")

    destination = quote_string(str(temporary_path))
    con.execute(
        f"""
        COPY ({query})
        TO {destination}
        (FORMAT PARQUET, COMPRESSION ZSTD);
        """
    )
    temporary_path.replace(output_path)

    row_count = con.execute(
        f"SELECT COUNT(*) FROM ({query}) AS exported_data"
    ).fetchone()[0]
    print(f"Done. Rows exported: {row_count}")
    return row_count


def main() -> None:
    if not DB_PATH.exists():
        raise FileNotFoundError(f"DuckDB database not found: {DB_PATH}")

    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        fact_table_exists = con.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = 'main'
              AND table_name = 'fact_energy_price'
            """
        ).fetchone()[0]
        if not fact_table_exists:
            raise RuntimeError(
                "Load the raw data before exporting. "
                "Missing relation: fact_energy_price"
            )

        total_rows = 0
        for dataset_name, export_config in DATASETS_TO_EXPORT.items():
            total_rows += export_table_to_parquet(
                con,
                dataset_name,
                export_config["query"],
                export_config["output_path"],
            )
    finally:
        con.close()

    LEGACY_COMBINED_PARQUET.unlink(missing_ok=True)
    print(
        "\nRaw-equivalent Australia datasets exported successfully. "
        f"Total rows across exports: {total_rows}"
    )


if __name__ == "__main__":
    main()
