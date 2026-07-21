import duckdb
from pathlib import Path

from db_schema import initialise_database


def main() -> None:
    database_path = Path("database/australian_energy_market.duckdb")
    database_path.parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(database_path))
    try:
        initialise_database(connection)
        print("Tables created or migrated:")
        print(connection.execute("SHOW TABLES").fetchall())
    finally:
        connection.close()


if __name__ == "__main__":
    main()
