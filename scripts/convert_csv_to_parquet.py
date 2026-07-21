from pathlib import Path
import duckdb


DB_PATH = Path("data/database/australian_energy_market.duckdb")
PARQUET_DIR = Path("data/parquet")

TABLES_TO_EXPORT = {
    # One complete, analysis-ready dataset containing POWER and GAS.
    "fact_energy_price": PARQUET_DIR / "fact_energy_price.parquet",
}


def quote_identifier(identifier: str) -> str:
    """Quote a trusted DuckDB identifier."""
    return '"' + identifier.replace('"', '""') + '"'


def quote_string(value: str) -> str:
    """Quote a DuckDB string literal."""
    return "'" + value.replace("'", "''") + "'"


def export_table_to_parquet(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    output_path: Path,
) -> int:
    """Export one relation atomically and return its row count."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = output_path.with_suffix(".parquet.tmp")
    temporary_path.unlink(missing_ok=True)

    print(f"Exporting {table_name} -> {output_path}")

    relation = quote_identifier(table_name)
    destination = quote_string(str(temporary_path))
    con.execute(
        f"""
        COPY (SELECT * FROM {relation})
        TO {destination}
        (FORMAT PARQUET, COMPRESSION ZSTD);
        """
    )
    temporary_path.replace(output_path)

    row_count = con.execute(
        f"SELECT COUNT(*) FROM {relation}"
    ).fetchone()[0]
    print(f"Done. Rows exported: {row_count}")
    return row_count


def main() -> None:
    if not DB_PATH.exists():
        raise FileNotFoundError(f"DuckDB database not found: {DB_PATH}")

    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        existing_relations = {
            row[0]
            for row in con.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'main'
                """
            ).fetchall()
        }
        missing_relations = set(TABLES_TO_EXPORT) - existing_relations
        if missing_relations:
            raise RuntimeError(
                "Run dbt build before exporting. Missing relations: "
                + ", ".join(sorted(missing_relations))
            )

        total_rows = 0
        for table_name, output_path in TABLES_TO_EXPORT.items():
            total_rows += export_table_to_parquet(
                con,
                table_name,
                output_path,
            )
    finally:
        con.close()

    print(
        "\nComplete energy dataset exported successfully. "
        f"Total rows across exports: {total_rows}"
    )


if __name__ == "__main__":
    main()
