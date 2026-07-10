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


LANDING_PAGE = (
    "https://www.aemo.com.au/energy-systems/gas/"
    "declared-wholesale-gas-market-dwgm/data-dwgm/"
    "vic-wholesale-price-withdrawals"
)
RAW_DIRECTORY = Path("data/raw/aemo_gas/dwgm")
DATABASE_PATH = Path("energy.duckdb")
LOGGER = logging.getLogger(__name__)


def discover_workbook_url(session: requests.Session) -> str:
    """Find the current AEMO DWGM all-history workbook."""
    response = session.get(LANDING_PAGE, timeout=60)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    for link in soup.find_all("a", href=True):
        href = link["href"]
        if "dwgm-prices-and-demand.xlsx" in href.lower():
            return urljoin(LANDING_PAGE, href)

    raise ValueError("AEMO DWGM prices workbook link was not found")


def download_workbook(
    session: requests.Session,
    workbook_url: str,
    refresh: bool = False,
) -> Path:
    RAW_DIRECTORY.mkdir(parents=True, exist_ok=True)
    destination = RAW_DIRECTORY / "dwgm-prices-and-demand.xlsx"

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

    cleaned["timezone"] = "Australia/Brisbane"
    cleaned["interval_start_utc"] = (
        cleaned["market_datetime_local"]
        .dt.tz_localize("Australia/Brisbane")
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


def load_dwgm_prices(
    connection: duckdb.DuckDBPyConnection,
    dataframe: pd.DataFrame,
) -> int:
    if dataframe.empty:
        return 0

    before_count = connection.execute(
        "SELECT COUNT(*) FROM fact_energy_price"
    ).fetchone()[0]
    connection.register("incoming_dwgm_data", dataframe)
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
            FROM incoming_dwgm_data AS incoming
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
        connection.unregister("incoming_dwgm_data")

    after_count = connection.execute(
        "SELECT COUNT(*) FROM fact_energy_price"
    ).fetchone()[0]
    return after_count - before_count


def ingest_dwgm_period(
    start_date: date,
    end_date: date,
    refresh: bool = False,
) -> None:
    started_at = datetime.now().astimezone()
    session = create_http_session()
    connection = duckdb.connect(str(DATABASE_PATH))
    initialise_database(connection)
    source_file = "dwgm-prices-and-demand.xlsx"

    try:
        cached_workbook = RAW_DIRECTORY / source_file
        if cached_workbook.exists() and not refresh:
            workbook_path = cached_workbook
            LOGGER.info("Using cached file: %s", workbook_path)
        else:
            workbook_url = discover_workbook_url(session)
            workbook_path = download_workbook(session, workbook_url, refresh)
        dataframe = parse_dwgm_prices(workbook_path, start_date, end_date)
        inserted_count = load_dwgm_prices(connection, dataframe)
        write_ingestion_log(
            connection=connection,
            dataset_name="AEMO_DWGM_PRICE",
            source_file=source_file,
            report_date=start_date,
            status="SUCCESS",
            row_count=inserted_count,
            started_at=started_at,
        )
        LOGGER.info(
            "DWGM %s to %s: parsed %d, inserted %d",
            start_date,
            end_date,
            len(dataframe),
            inserted_count,
        )
    except Exception as error:
        write_ingestion_log(
            connection=connection,
            dataset_name="AEMO_DWGM_PRICE",
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
        description="Ingest historical AEMO Victorian DWGM gas prices."
    )
    parser.add_argument("--start-date", type=date.fromisoformat, required=True)
    parser.add_argument("--end-date", type=date.fromisoformat, required=True)
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Download the current AEMO workbook instead of using the cache.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    arguments = parse_arguments()
    ingest_dwgm_period(
        arguments.start_date,
        arguments.end_date,
        arguments.refresh,
    )
