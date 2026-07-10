import duckdb

from db_schema import initialise_database


def main() -> None:
    connection = duckdb.connect("energy.duckdb")
    try:
        initialise_database(connection)
        print("Tables created or migrated:")
        print(connection.execute("SHOW TABLES").fetchall())
    finally:
        connection.close()


if __name__ == "__main__":
    main()
