from __future__ import annotations

from datetime import date, datetime

import duckdb


ENERGY_PRICE_COLUMNS = [
    "interval_start_utc",
    "market_datetime_local",
    "timezone",
    "country",
    "commodity",
    "market",
    "region_code",
    "price_type",
    "price",
    "total_demand_mw",
    "available_generation_mw",
    "available_load_mw",
    "currency",
    "unit",
    "interval_minutes",
    "source",
    "source_file",
    "ingested_at_utc",
]


def initialise_database(connection: duckdb.DuckDBPyConnection) -> None:
    """Create the shared power/gas schema and migrate legacy power rows."""
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS fact_energy_price (
            interval_start_utc TIMESTAMPTZ NOT NULL,
            market_datetime_local TIMESTAMP NOT NULL,
            timezone VARCHAR NOT NULL,
            country VARCHAR NOT NULL,
            commodity VARCHAR NOT NULL,
            market VARCHAR NOT NULL,
            region_code VARCHAR NOT NULL,
            price_type VARCHAR NOT NULL,
            price DOUBLE NOT NULL,
            total_demand_mw DOUBLE,
            available_generation_mw DOUBLE,
            available_load_mw DOUBLE,
            currency VARCHAR NOT NULL,
            unit VARCHAR NOT NULL,
            interval_minutes INTEGER NOT NULL,
            source VARCHAR NOT NULL,
            source_file VARCHAR NOT NULL,
            ingested_at_utc TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (
                interval_start_utc,
                commodity,
                market,
                region_code,
                price_type
            )
        );
        """
    )

    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS etl_ingestion_log (
            dataset_name VARCHAR,
            source_file VARCHAR,
            report_date DATE,
            status VARCHAR,
            row_count BIGINT,
            started_at_utc TIMESTAMPTZ,
            completed_at_utc TIMESTAMPTZ,
            error_message VARCHAR
        );
        """
    )

    tables = {
        row[0]
        for row in connection.execute("SHOW TABLES").fetchall()
    }
    if "fact_electricity_price" in tables:
        columns = {
            row[0]
            for row in connection.execute(
                "DESCRIBE fact_electricity_price"
            ).fetchall()
        }
        if "market_datetime_local" in columns:
            connection.execute(
                """
                INSERT INTO fact_energy_price (
                    interval_start_utc,
                    market_datetime_local,
                    timezone,
                    country,
                    commodity,
                    market,
                    region_code,
                    price_type,
                    price,
                    total_demand_mw,
                    available_generation_mw,
                    available_load_mw,
                    currency,
                    unit,
                    interval_minutes,
                    source,
                    source_file,
                    ingested_at_utc
                )
                SELECT
                    market_datetime_local AT TIME ZONE 'Australia/Brisbane',
                    market_datetime_local,
                    'Australia/Brisbane',
                    country,
                    'POWER',
                    market,
                    region_code,
                    price_type,
                    spot_price,
                    total_demand_mw,
                    available_generation_mw,
                    available_load_mw,
                    currency,
                    unit,
                    interval_minutes,
                    source,
                    source_file,
                    ingested_at_utc
                FROM fact_electricity_price AS legacy
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM fact_energy_price AS current
                    WHERE current.interval_start_utc =
                            legacy.market_datetime_local
                            AT TIME ZONE 'Australia/Brisbane'
                      AND current.commodity = 'POWER'
                      AND current.market = legacy.market
                      AND current.region_code = legacy.region_code
                      AND current.price_type = legacy.price_type
                );
                """
            )

    connection.execute(
        """
        CREATE OR REPLACE VIEW v_power_price AS
        SELECT
            interval_start_utc,
            market_datetime_local,
            timezone,
            country,
            market,
            region_code,
            price_type,
            price AS power_price,
            currency,
            unit,
            interval_minutes,
            source,
            source_file,
            ingested_at_utc
        FROM fact_energy_price
        WHERE commodity = 'POWER';
        """
    )
    connection.execute(
        """
        CREATE OR REPLACE VIEW v_gas_price AS
        SELECT
            interval_start_utc,
            market_datetime_local,
            timezone,
            country,
            market,
            region_code,
            price_type,
            price AS gas_price,
            currency,
            unit,
            interval_minutes,
            source,
            source_file,
            ingested_at_utc
        FROM fact_energy_price
        WHERE commodity = 'GAS';
        """
    )


def write_ingestion_log(
    connection: duckdb.DuckDBPyConnection,
    dataset_name: str,
    source_file: str,
    report_date: date,
    status: str,
    row_count: int,
    started_at: datetime,
    error_message: str | None = None,
) -> None:
    connection.execute(
        """
        INSERT INTO etl_ingestion_log (
            dataset_name,
            source_file,
            report_date,
            status,
            row_count,
            started_at_utc,
            completed_at_utc,
            error_message
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?);
        """,
        [
            dataset_name,
            source_file,
            report_date,
            status,
            row_count,
            started_at,
            datetime.now().astimezone(),
            error_message,
        ],
    )
