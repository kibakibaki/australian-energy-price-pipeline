from __future__ import annotations

import argparse
import logging
from datetime import date, datetime
from pathlib import Path
from urllib.parse import urljoin

import duckdb
import pandas as pd
import requests
from bs4 import BeautifulSoup

from aemo_ingestion import create_http_session
from db_schema import initialise_database, write_ingestion_log


DWGM_LANDING_PAGE = (
    "https://www.aemo.com.au/energy-systems/gas/"
    "declared-wholesale-gas-market-dwgm/data-dwgm/"
    "vic-wholesale-price-withdrawals"
)
STTM_LANDING_PAGE = (
    "https://www.aemo.com.au/energy-systems/gas/"
    "short-term-trading-market-sttm/data-sttm/daily-sttm-reports"
)
RAW_DIRECTORY = Path("data/raw/gas/aemo")
DATABASE_PATH = Path("data/database/australian_energy_market.duckdb")
LOGGER = logging.getLogger(__name__)

STTM_SHEETS = {
    "SYD price and withdrawals": {
        "price_column": "SYD exante_price",
        "region_code": "NSW",
        "timezone": "Australia/Sydney",
    },
    "BRI price and withdrawals": {
        "price_column": "BRI exante_price",
        "region_code": "QLD",
        "timezone": "Australia/Brisbane",
    },
    "ADL price and withdrawals": {
        "price_column": "ADL exante_price",
        "region_code": "SA",
        "timezone": "Australia/Adelaide",
    },
}


def discover_workbook_url(
    session: requests.Session,
    landing_page: str,
    workbook_name: str,
) -> str:
    """Find an AEMO all-history gas workbook."""
    response = session.get(landing_page, timeout=60)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    for link in soup.find_all("a", href=True):
        href = link["href"]
        if workbook_name.lower() in href.lower():
            return urljoin(landing_page, href)

    raise ValueError(f"AEMO workbook link was not found: {workbook_name}")


def download_workbook(
    session: requests.Session,
    workbook_url: str,
    market_directory: str,
    workbook_name: str,
    refresh: bool = False,
) -> Path:
    destination_directory = RAW_DIRECTORY / market_directory
    destination_directory.mkdir(parents=True, exist_ok=True)
    destination = destination_directory / workbook_name

    if destination.exists() and destination.stat().st_size > 0 and not refresh:
        LOGGER.info("Using cached file: %s", destination)
        return destination

    LOGGER.info("Downloading %s", workbook_url)
    with session.get(workbook_url, timeout=120, stream=True) as response:
        response.raise_for_status()
        temporary = destination.with_suffix(".xlsx.part")
        with temporary.open("wb") as output:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    output.write(chunk)
        temporary.replace(destination)

    return destination


def parse_dwgm_prices(
    workbook_path: Path,
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    """Transform the official Prices sheet to the shared energy schema."""
    if start_date > end_date:
        raise ValueError("start_date must not be after end_date")

    raw = pd.read_excel(workbook_path, sheet_name="Prices")
    raw.columns = [str(column).strip() for column in raw.columns]
    required = {"Gas_Date", "Hour", "Price"}
    missing = required.difference(raw.columns)
    if missing:
        raise ValueError(
            f"Missing DWGM columns {sorted(missing)}; "
            f"available columns are {raw.columns.tolist()}"
        )

    gas_date = pd.to_datetime(raw["Gas_Date"], errors="coerce")
    hour = pd.to_numeric(raw["Hour"], errors="coerce")
    price = pd.to_numeric(raw["Price"], errors="coerce")
    market_time = gas_date + pd.to_timedelta(hour, unit="h")

    cleaned = pd.DataFrame(
        {
            "market_datetime_local": market_time,
            "price": price,
            "interval_minutes": hour.map(
                {6: 240, 10: 240, 14: 240, 18: 240, 22: 480}
            ),
        }
    )
    cleaned = cleaned[
        gas_date.dt.date.between(start_date, end_date)
        & cleaned["market_datetime_local"].notna()
        & cleaned["price"].notna()
        & cleaned["interval_minutes"].notna()
    ].copy()

    cleaned["timezone"] = "Australia/Melbourne"
    cleaned["interval_start_utc"] = (
        cleaned["market_datetime_local"]
        .dt.tz_localize("Australia/Melbourne")
        .dt.tz_convert("UTC")
    )
    cleaned["country"] = "Australia"
    cleaned["commodity"] = "GAS"
    cleaned["market"] = "AEMO_DWGM"
    cleaned["region_code"] = "VIC"
    cleaned["price_type"] = "DWGM Schedule Price"
    cleaned["total_demand_mw"] = pd.NA
    cleaned["available_generation_mw"] = pd.NA
    cleaned["available_load_mw"] = pd.NA
    cleaned["currency"] = "AUD"
    cleaned["unit"] = "AUD/GJ"
    cleaned["interval_minutes"] = cleaned["interval_minutes"].astype(int)
    cleaned["source"] = "AEMO DWGM"
    cleaned["source_file"] = workbook_path.name
    cleaned["ingested_at_utc"] = pd.Timestamp.now(tz="UTC")
    cleaned.drop_duplicates(
        subset=["interval_start_utc", "market", "price_type"],
        keep="last",
        inplace=True,
    )
    return cleaned


def parse_sttm_prices(
    workbook_path: Path,
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    """Transform official STTM daily ex-ante prices to the shared schema."""
    if start_date > end_date:
        raise ValueError("start_date must not be after end_date")

    frames: list[pd.DataFrame] = []
    for sheet_name, settings in STTM_SHEETS.items():
        raw = pd.read_excel(workbook_path, sheet_name=sheet_name)
        raw.columns = [str(column).strip() for column in raw.columns]
        price_column = settings["price_column"]
        required = {"DateTime", price_column}
        missing = required.difference(raw.columns)
        if missing:
            raise ValueError(
                f"Missing STTM columns {sorted(missing)} in {sheet_name}; "
                f"available columns are {raw.columns.tolist()}"
            )

        gas_date = pd.to_datetime(raw["DateTime"], errors="coerce")
        price = pd.to_numeric(raw[price_column], errors="coerce")
        # An STTM gas day begins at 6:00 AM local time.
        market_time = gas_date + pd.Timedelta(hours=6)
        frame = pd.DataFrame(
            {
                "market_datetime_local": market_time,
                "price": price,
            }
        )
        frame = frame[
            gas_date.dt.date.between(start_date, end_date)
            & frame["market_datetime_local"].notna()
            & frame["price"].notna()
        ].copy()

        timezone = settings["timezone"]
        frame["timezone"] = timezone
        frame["interval_start_utc"] = (
            frame["market_datetime_local"]
            .dt.tz_localize(timezone)
            .dt.tz_convert("UTC")
        )
        frame["country"] = "Australia"
        frame["commodity"] = "GAS"
        frame["market"] = "AEMO_STTM"
        frame["region_code"] = settings["region_code"]
        frame["price_type"] = "STTM Ex Ante Market Price"
        frame["total_demand_mw"] = pd.NA
        frame["available_generation_mw"] = pd.NA
        frame["available_load_mw"] = pd.NA
        frame["currency"] = "AUD"
        frame["unit"] = "AUD/GJ"
        frame["interval_minutes"] = 1440
        frame["source"] = "AEMO STTM"
        frame["source_file"] = workbook_path.name
        frame["ingested_at_utc"] = pd.Timestamp.now(tz="UTC")
        frames.append(frame)

    cleaned = pd.concat(frames, ignore_index=True)
    cleaned.drop_duplicates(
        subset=[
            "interval_start_utc",
            "market",
            "region_code",
            "price_type",
        ],
        keep="last",
        inplace=True,
    )
    return cleaned


def load_gas_prices(
    connection: duckdb.DuckDBPyConnection,
    dataframe: pd.DataFrame,
) -> int:
    if dataframe.empty:
        return 0

    before_count = connection.execute(
        "SELECT COUNT(*) FROM fact_energy_price"
    ).fetchone()[0]
    connection.register("incoming_gas_data", dataframe)
    try:
        connection.execute(
            """
            INSERT INTO fact_energy_price (
                interval_start_utc, market_datetime_local, timezone,
                country, commodity, market, region_code, price_type,
                price, total_demand_mw, available_generation_mw,
                available_load_mw, currency, unit, interval_minutes,
                source, source_file, ingested_at_utc
            )
            SELECT
                incoming.interval_start_utc,
                incoming.market_datetime_local,
                incoming.timezone,
                incoming.country,
                incoming.commodity,
                incoming.market,
                incoming.region_code,
                incoming.price_type,
                incoming.price,
                incoming.total_demand_mw,
                incoming.available_generation_mw,
                incoming.available_load_mw,
                incoming.currency,
                incoming.unit,
                incoming.interval_minutes,
                incoming.source,
                incoming.source_file,
                incoming.ingested_at_utc
            FROM incoming_gas_data AS incoming
            WHERE NOT EXISTS (
                SELECT 1
                FROM fact_energy_price AS existing
                WHERE existing.interval_start_utc = incoming.interval_start_utc
                  AND existing.commodity = incoming.commodity
                  AND existing.market = incoming.market
                  AND existing.region_code = incoming.region_code
                  AND existing.price_type = incoming.price_type
            );
            """
        )
    finally:
        connection.unregister("incoming_gas_data")

    after_count = connection.execute(
        "SELECT COUNT(*) FROM fact_energy_price"
    ).fetchone()[0]
    return after_count - before_count


def replace_gas_period(
    connection: duckdb.DuckDBPyConnection,
    market: str,
    start_date: date,
    end_date: date,
) -> None:
    """Remove a requested gas period so corrected source rows can be reloaded."""
    connection.execute(
        """
        DELETE FROM fact_energy_price
        WHERE commodity = 'GAS'
          AND market = ?
          AND CAST(market_datetime_local AS DATE) BETWEEN ? AND ?
        """,
        [market, start_date, end_date],
    )


def ingest_gas_period(
    start_date: date,
    end_date: date,
    refresh: bool = False,
    markets: tuple[str, ...] = ("sttm", "dwgm"),
) -> None:
    DATABASE_PATH.parent.mkdir(parents=True, exist_ok=True)
    session = create_http_session()
    connection = duckdb.connect(str(DATABASE_PATH))
    initialise_database(connection)

    try:
        jobs = {
            "sttm": {
                "landing_page": STTM_LANDING_PAGE,
                "directory": "sttm",
                "source_file": "sttm-price-and-withdrawals.xlsx",
                "dataset_name": "AEMO_STTM_PRICE",
                "market": "AEMO_STTM",
                "parser": parse_sttm_prices,
            },
            "dwgm": {
                "landing_page": DWGM_LANDING_PAGE,
                "directory": "dwgm",
                "source_file": "dwgm-prices-and-demand.xlsx",
                "dataset_name": "AEMO_DWGM_PRICE",
                "market": "AEMO_DWGM",
                "parser": parse_dwgm_prices,
            },
        }

        for market_name in markets:
            job = jobs[market_name]
            started_at = datetime.now().astimezone()
            source_file = job["source_file"]
            try:
                cached_workbook = (
                    RAW_DIRECTORY / job["directory"] / source_file
                )
                if cached_workbook.exists() and not refresh:
                    workbook_path = cached_workbook
                    LOGGER.info("Using cached file: %s", workbook_path)
                else:
                    workbook_url = discover_workbook_url(
                        session,
                        job["landing_page"],
                        source_file,
                    )
                    workbook_path = download_workbook(
                        session,
                        workbook_url,
                        job["directory"],
                        source_file,
                        refresh,
                    )

                dataframe = job["parser"](
                    workbook_path,
                    start_date,
                    end_date,
                )
                connection.execute("BEGIN")
                replace_gas_period(
                    connection,
                    job["market"],
                    start_date,
                    end_date,
                )
                inserted_count = load_gas_prices(connection, dataframe)
                connection.execute("COMMIT")
                write_ingestion_log(
                    connection=connection,
                    dataset_name=job["dataset_name"],
                    source_file=source_file,
                    report_date=start_date,
                    status="SUCCESS",
                    row_count=inserted_count,
                    started_at=started_at,
                )
                LOGGER.info(
                    "%s %s to %s: parsed %d, inserted %d",
                    market_name.upper(),
                    start_date,
                    end_date,
                    len(dataframe),
                    inserted_count,
                )
            except Exception as error:
                try:
                    connection.execute("ROLLBACK")
                except duckdb.TransactionException:
                    pass
                write_ingestion_log(
                    connection=connection,
                    dataset_name=job["dataset_name"],
                    source_file=source_file,
                    report_date=start_date,
                    status="FAILED",
                    row_count=0,
                    started_at=started_at,
                    error_message=str(error),
                )
                raise
    finally:
        session.close()
        connection.close()


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingest historical AEMO STTM and DWGM gas prices."
    )
    parser.add_argument("--start-date", type=date.fromisoformat, required=True)
    parser.add_argument("--end-date", type=date.fromisoformat, required=True)
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Download current AEMO workbooks instead of using the cache.",
    )
    parser.add_argument(
        "--market",
        choices=["all", "sttm", "dwgm"],
        default="all",
        help="Gas market to ingest (default: all).",
    )
    return parser.parse_args()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    arguments = parse_arguments()
    selected_markets = (
        ("sttm", "dwgm")
        if arguments.market == "all"
        else (arguments.market,)
    )
    ingest_gas_period(
        arguments.start_date,
        arguments.end_date,
        arguments.refresh,
        selected_markets,
    )
