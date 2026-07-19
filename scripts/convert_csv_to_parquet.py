from pathlib import Path
import duckdb


DB_PATH = Path("energy.duckdb")
PARQUET_DIR = Path("data/parquet")

PARQUET_DIR.mkdir(parents=True, exist_ok=True)


def export_table_to_parquet(con, table_name: str, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"Exporting {table_name} -> {output_path}")

    con.execute(f"""
        COPY (
            SELECT *
            FROM {table_name}
        )
        TO '{output_path}'
        (FORMAT PARQUET);
    """)

    row_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    print(f"Done. Rows exported: {row_count}")


def main() -> None:
    if not DB_PATH.exists():
        raise FileNotFoundError(f"DuckDB database not found: {DB_PATH}")

    con = duckdb.connect(str(DB_PATH))

    tables_to_export = [
        "fact_energy_price",
        "v_power_price",
        "v_gas_price",
        "mart_energy_price_summary",
    ]

    for table_name in tables_to_export:
        output_path = PARQUET_DIR / f"{table_name}.parquet"
        export_table_to_parquet(con, table_name, output_path)

    con.close()

    print("\nAll DuckDB tables/views exported to Parquet successfully.")


if __name__ == "__main__":
    main()