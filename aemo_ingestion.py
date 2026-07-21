from __future__ import annotations

import argparse
import csv
import io
import logging
import re
import time
import zipfile
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterator
from urllib.parse import unquote, urljoin

import duckdb
import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from db_schema import initialise_database, write_ingestion_log


ARCHIVE_URL = (
    "https://nemweb.com.au/Reports/ARCHIVE/DispatchIS_Reports/"
)
MMSDM_ARCHIVE_URL = (
    "https://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM"
)

RAW_DIRECTORY = Path("data/raw/electricity/aemo")
DATABASE_PATH = Path("data/database/australian_energy_market.duckdb")

VALID_REGIONS = {
    "NSW1",
    "QLD1",
    "VIC1",
    "SA1",
    "TAS1",
}

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class AemoFile:
    report_date: date
    filename: str
    url: str


@dataclass(frozen=True)
class AemoMonthlyFile:
    report_month: date
    filename: str
    url: str


def create_http_session() -> requests.Session:
    """
    Create a requests session with retry behaviour.

    This helps when NEMWeb temporarily returns a server error
    or closes a connection.
    """
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )

    adapter = HTTPAdapter(max_retries=retry)

    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    session.headers.update(
        {
            "User-Agent": (
                "Battery-RUL-Energy-Data-POC/1.0 "
                "(educational data ingestion project)"
            )
        }
    )

    return session


def date_range(start_date: date, end_date: date) -> Iterator[date]:
    """
    Yield every date from start_date to end_date, inclusive.
    """
    if start_date > end_date:
        raise ValueError("start_date must not be after end_date")

    current_date = start_date

    while current_date <= end_date:
        yield current_date
        current_date += timedelta(days=1)


def month_range(start_date: date, end_date: date) -> Iterator[date]:
    """Yield the first day of every month touched by a date range."""
    if start_date > end_date:
        raise ValueError("start_date must not be after end_date")

    current_month = start_date.replace(day=1)
    final_month = end_date.replace(day=1)
    while current_month <= final_month:
        yield current_month
        if current_month.month == 12:
            current_month = date(current_month.year + 1, 1, 1)
        else:
            current_month = date(
                current_month.year,
                current_month.month + 1,
                1,
            )


def build_monthly_file(report_month: date) -> AemoMonthlyFile:
    """Build the legacy AEMO MMSDM DISPATCHPRICE monthly URL."""
    stamp = report_month.strftime("%Y%m010000")
    filename = f"PUBLIC_DVD_DISPATCHPRICE_{stamp}.zip"
    directory = (
        f"{MMSDM_ARCHIVE_URL}/{report_month.year}/"
        f"MMSDM_{report_month.strftime('%Y_%m')}/"
        "MMSDM_Historical_Data_SQLLoader/DATA/"
    )
    return AemoMonthlyFile(
        report_month=report_month,
        filename=filename,
        url=urljoin(directory, filename),
    )


def discover_monthly_file(
    session: requests.Session,
    report_month: date,
) -> AemoMonthlyFile:
    """Discover a monthly file across AEMO's old and new naming schemes."""
    directory = (
        f"{MMSDM_ARCHIVE_URL}/{report_month.year}/"
        f"MMSDM_{report_month.strftime('%Y_%m')}/"
        "MMSDM_Historical_Data_SQLLoader/DATA/"
    )
    response = session.get(directory, timeout=60)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    for link in soup.find_all("a", href=True):
        filename = Path(unquote(link["href"])).name
        upper_name = filename.upper()
        if (
            "DISPATCHPRICE" in upper_name
            and "PREDISPATCH" not in upper_name
            and filename.lower().endswith(".zip")
        ):
            return AemoMonthlyFile(
                report_month=report_month,
                filename=filename,
                url=urljoin(directory, link["href"]),
            )

    raise FileNotFoundError(
        f"No DISPATCHPRICE file listed for {report_month:%Y-%m}"
    )


def discover_archive_files(
    session: requests.Session,
) -> dict[date, AemoFile]:
    """
    Read the NEMWeb archive directory and discover available daily ZIP files.

    Expected filename pattern:
        PUBLIC_DISPATCHIS_YYYYMMDD.zip
    """
    LOGGER.info("Reading archive index: %s", ARCHIVE_URL)

    response = session.get(ARCHIVE_URL, timeout=60)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")

    pattern = re.compile(
        r"^PUBLIC_DISPATCHIS_(\d{8})\.zip$",
        flags=re.IGNORECASE,
    )

    discovered_files: dict[date, AemoFile] = {}

    for link in soup.find_all("a", href=True):
        filename = Path(link["href"]).name
        match = pattern.match(filename)

        if not match:
            continue

        report_date = datetime.strptime(
            match.group(1),
            "%Y%m%d",
        ).date()

        discovered_files[report_date] = AemoFile(
            report_date=report_date,
            filename=filename,
            url=urljoin(ARCHIVE_URL, link["href"]),
        )

    LOGGER.info(
        "Discovered %d DispatchIS files",
        len(discovered_files),
    )

    return discovered_files


def download_zip(
    session: requests.Session,
    aemo_file: AemoFile,
) -> Path:
    """
    Download a ZIP file into the raw data directory.

    Existing non-empty files are reused.
    """
    destination_directory = (
        RAW_DIRECTORY
        / str(aemo_file.report_date.year)
        / f"{aemo_file.report_date.month:02d}"
    )

    destination_directory.mkdir(
        parents=True,
        exist_ok=True,
    )

    destination_path = destination_directory / aemo_file.filename

    if destination_path.exists() and destination_path.stat().st_size > 0:
        LOGGER.info("Using cached file: %s", destination_path)
        return destination_path

    LOGGER.info("Downloading %s", aemo_file.url)

    with session.get(
        aemo_file.url,
        timeout=120,
        stream=True,
    ) as response:
        response.raise_for_status()

        temporary_path = destination_path.with_suffix(".zip.part")

        with temporary_path.open("wb") as output_file:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    output_file.write(chunk)

        temporary_path.replace(destination_path)

    LOGGER.info(
        "Downloaded %s (%d bytes)",
        destination_path,
        destination_path.stat().st_size,
    )

    return destination_path


def download_monthly_zip(
    session: requests.Session,
    aemo_file: AemoMonthlyFile,
) -> Path:
    """Download one DISPATCHPRICE monthly archive, reusing its cache."""
    destination_directory = (
        RAW_DIRECTORY
        / "monthly"
        / str(aemo_file.report_month.year)
        / f"{aemo_file.report_month.month:02d}"
    )
    destination_directory.mkdir(parents=True, exist_ok=True)
    destination_path = destination_directory / aemo_file.filename

    if destination_path.exists() and destination_path.stat().st_size > 0:
        LOGGER.info("Using cached monthly file: %s", destination_path)
        return destination_path

    LOGGER.info("Downloading monthly archive %s", aemo_file.url)
    with session.get(aemo_file.url, timeout=180, stream=True) as response:
        response.raise_for_status()
        temporary_path = destination_path.with_suffix(".zip.part")
        with temporary_path.open("wb") as output_file:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    output_file.write(chunk)
        temporary_path.replace(destination_path)

    return destination_path

def iter_csv_payloads(
    zip_source,
    prefix: str = "",
    depth: int = 0,
):
    """
    Recursively read CSV files from an AEMO ZIP.

    AEMO daily archive files may contain nested ZIP files:
        daily ZIP
            -> interval ZIP
                -> CSV
    """
    if depth > 3:
        raise ValueError("ZIP nesting is deeper than expected")

    with zipfile.ZipFile(zip_source) as archive:
        for member in archive.infolist():
            if member.is_dir():
                continue

            member_name = member.filename
            full_name = f"{prefix}{member_name}"
            file_bytes = archive.read(member)

            if member_name.lower().endswith(".csv"):
                yield full_name, file_bytes

            elif member_name.lower().endswith(".zip"):
                nested_zip = io.BytesIO(file_bytes)

                yield from iter_csv_payloads(
                    nested_zip,
                    prefix=f"{full_name}!/",
                    depth=depth + 1,
                )


def parse_aemo_csv_content(
    csv_bytes: bytes,
    source_file: str,
) -> dict[str, pd.DataFrame]:
    """
    Extract DISPATCH/PRICE and DISPATCH/REGIONSUM records.

    AEMO record format:
        I -> table header
        D -> data
        C -> comment or report metadata
    """
    text = csv_bytes.decode(
        "utf-8-sig",
        errors="replace",
    )

    reader = csv.reader(io.StringIO(text))

    headers: dict[tuple[str, str, str], list[str]] = {}

    records: dict[str, list[dict[str, str]]] = {
        "PRICE": [],
        "REGIONSUM": [],
    }

    for row in reader:
        if len(row) < 4:
            continue

        record_type = row[0].strip().upper()

        if record_type not in {"I", "D"}:
            continue

        dataset_name = row[1].strip().upper()
        table_name = row[2].strip().upper()
        version = row[3].strip()

        if dataset_name != "DISPATCH":
            continue

        if table_name not in records:
            continue

        table_key = (
            dataset_name,
            table_name,
            version,
        )

        if record_type == "I":
            # The first four values identify the report.
            # The remaining values are column names.
            headers[table_key] = [
                column.strip().upper()
                for column in row[4:]
            ]
            continue

        if table_key not in headers:
            continue

        column_names = headers[table_key]
        values = row[4:]

        if len(values) < len(column_names):
            values += [""] * (
                len(column_names) - len(values)
            )

        elif len(values) > len(column_names):
            values = values[: len(column_names)]

        record = dict(zip(column_names, values))
        record["SOURCE_FILE"] = source_file

        records[table_name].append(record)

    return {
        table_name: pd.DataFrame(table_records)
        for table_name, table_records in records.items()
    }


def parse_dispatch_zip(
    zip_path: Path,
) -> pd.DataFrame:
    """
    Parse all nested CSV files inside one daily AEMO archive.

    PRICE contains regional spot prices.
    REGIONSUM contains demand and generation information.
    """
    price_frames: list[pd.DataFrame] = []
    region_frames: list[pd.DataFrame] = []

    csv_count = 0

    for csv_name, csv_bytes in iter_csv_payloads(zip_path):
        csv_count += 1

        parsed_tables = parse_aemo_csv_content(
            csv_bytes=csv_bytes,
            source_file=csv_name,
        )

        price_frame = parsed_tables["PRICE"]
        region_frame = parsed_tables["REGIONSUM"]

        if not price_frame.empty:
            price_frames.append(price_frame)

        if not region_frame.empty:
            region_frames.append(region_frame)

    if csv_count == 0:
        raise ValueError(
            f"No CSV files found inside {zip_path}, "
            "including nested ZIP files"
        )

    if not price_frames:
        raise ValueError(
            f"No DISPATCH/PRICE records found in {zip_path}"
        )

    prices = pd.concat(
        price_frames,
        ignore_index=True,
    )

    # Exclude intervention pricing runs when the field exists.
    if "INTERVENTION" in prices.columns:
        intervention = pd.to_numeric(
            prices["INTERVENTION"],
            errors="coerce",
        ).fillna(0)

        prices = prices[intervention.eq(0)].copy()

    merge_keys = [
        "SETTLEMENTDATE",
        "REGIONID",
    ]

    missing_price_keys = [
        key
        for key in merge_keys
        if key not in prices.columns
    ]

    if missing_price_keys:
        raise ValueError(
            "Missing required PRICE columns: "
            f"{missing_price_keys}. "
            f"Available columns: {prices.columns.tolist()}"
        )

    prices = prices.drop_duplicates(
        subset=merge_keys,
        keep="last",
    )

    if not region_frames:
        LOGGER.warning(
            "No DISPATCH/REGIONSUM records found; "
            "demand columns will be empty"
        )

        return prices

    regions = pd.concat(
        region_frames,
        ignore_index=True,
    )

    if "INTERVENTION" in regions.columns:
        intervention = pd.to_numeric(
            regions["INTERVENTION"],
            errors="coerce",
        ).fillna(0)

        regions = regions[intervention.eq(0)].copy()

    missing_region_keys = [
        key
        for key in merge_keys
        if key not in regions.columns
    ]

    if missing_region_keys:
        LOGGER.warning(
            "REGIONSUM is missing merge columns %s",
            missing_region_keys,
        )

        return prices

    region_value_columns = [
        column_name
        for column_name in [
            "TOTALDEMAND",
            "AVAILABLEGENERATION",
            "AVAILABLELOAD",
        ]
        if column_name in regions.columns
    ]

    regions = regions[
        merge_keys + region_value_columns
    ].drop_duplicates(
        subset=merge_keys,
        keep="last",
    )

    combined = prices.merge(
        regions,
        on=merge_keys,
        how="left",
    )

    LOGGER.info(
        "Parsed %d nested CSV files, %d price records",
        csv_count,
        len(combined),
    )

    return combined

def first_existing_column(
    dataframe: pd.DataFrame,
    possible_names: list[str],
) -> pd.Series:
    """
    Return the first available column from a list of candidate names.

    AEMO formats can change slightly, so this gives the POC
    some tolerance across report versions.
    """
    for column_name in possible_names:
        if column_name in dataframe.columns:
            return dataframe[column_name]

    return pd.Series(
        [pd.NA] * len(dataframe),
        index=dataframe.index,
    )


def transform_dispatch_price(
    raw_dataframe: pd.DataFrame,
) -> pd.DataFrame:
    """
    Convert raw AEMO DISPATCH/PRICE records into the project schema.
    """
    settlement_date = first_existing_column(
        raw_dataframe,
        ["SETTLEMENTDATE", "DATETIME"],
    )

    region_id = first_existing_column(
        raw_dataframe,
        ["REGIONID"],
    )

    rrp = first_existing_column(
        raw_dataframe,
        ["RRP"],
    )

    total_demand = first_existing_column(
        raw_dataframe,
        ["TOTALDEMAND"],
    )

    available_generation = first_existing_column(
        raw_dataframe,
        ["AVAILABLEGENERATION"],
    )

    available_load = first_existing_column(
        raw_dataframe,
        ["AVAILABLELOAD"],
    )

    cleaned = pd.DataFrame(
        {
            "market_datetime_local": pd.to_datetime(
                settlement_date,
                errors="coerce",
            ),
            "region_code": region_id.astype("string"),
            "price": pd.to_numeric(
                rrp,
                errors="coerce",
            ),
            "total_demand_mw": pd.to_numeric(
                total_demand,
                errors="coerce",
            ),
            "available_generation_mw": pd.to_numeric(
                available_generation,
                errors="coerce",
            ),
            "available_load_mw": pd.to_numeric(
                available_load,
                errors="coerce",
            ),
            "source_file": raw_dataframe["SOURCE_FILE"].astype(
                "string"
            ),
        }
    )

    cleaned["market"] = "AEMO_NEM"
    cleaned["country"] = "Australia"
    cleaned["commodity"] = "POWER"
    cleaned["timezone"] = "Australia/Brisbane"
    cleaned["price_type"] = "Regional Reference Price"
    cleaned["currency"] = "AUD"
    cleaned["unit"] = "AUD/MWh"
    cleaned["interval_minutes"] = 5
    cleaned["source"] = "AEMO NEMWeb"
    cleaned["ingested_at_utc"] = pd.Timestamp.now(tz="UTC")
    cleaned["interval_start_utc"] = (
        cleaned["market_datetime_local"]
        .dt.tz_localize("Australia/Brisbane")
        .dt.tz_convert("UTC")
    )

    cleaned = cleaned[
        cleaned["market_datetime_local"].notna()
        & cleaned["region_code"].isin(VALID_REGIONS)
        & cleaned["price"].notna()
    ].copy()

    cleaned.drop_duplicates(
        subset=[
            "market_datetime_local",
            "region_code",
            "price_type",
        ],
        keep="last",
        inplace=True,
    )

    return cleaned


def load_into_duckdb(
    connection: duckdb.DuckDBPyConnection,
    dataframe: pd.DataFrame,
) -> int:
    """
    Insert transformed records while avoiding duplicate primary keys.
    """
    if dataframe.empty:
        return 0

    connection.register(
        "incoming_aemo_data",
        dataframe,
    )

    before_count = connection.execute(
        "SELECT COUNT(*) FROM fact_energy_price"
    ).fetchone()[0]

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
        FROM incoming_aemo_data AS incoming
        WHERE NOT EXISTS (
            SELECT 1
            FROM fact_energy_price AS existing
            WHERE
                existing.interval_start_utc = incoming.interval_start_utc
                AND existing.commodity = incoming.commodity
                AND existing.market = incoming.market
                AND existing.region_code = incoming.region_code
                AND existing.price_type = incoming.price_type
        );
        """
    )

    after_count = connection.execute(
        "SELECT COUNT(*) FROM fact_energy_price"
    ).fetchone()[0]

    connection.unregister("incoming_aemo_data")

    return after_count - before_count


def ingest_aemo_period(
    start_date: date,
    end_date: date,
) -> None:
    """
    Run the complete extract-transform-load process.
    """
    RAW_DIRECTORY.mkdir(parents=True, exist_ok=True)
    DATABASE_PATH.parent.mkdir(parents=True, exist_ok=True)

    session = create_http_session()
    available_files = discover_archive_files(session)

    connection = duckdb.connect(str(DATABASE_PATH))
    initialise_database(connection)

    try:
        for requested_date in date_range(start_date, end_date):
            started_at = datetime.now().astimezone()

            expected_filename = (
                f"PUBLIC_DISPATCHIS_"
                f"{requested_date.strftime('%Y%m%d')}.zip"
            )

            aemo_file = available_files.get(requested_date)

            if aemo_file is None:
                LOGGER.warning(
                    "No archive file found for %s",
                    requested_date,
                )

                write_ingestion_log(
                    connection=connection,
                    dataset_name="AEMO_DISPATCH_PRICE",
                    source_file=expected_filename,
                    report_date=requested_date,
                    status="NOT_FOUND",
                    row_count=0,
                    started_at=started_at,
                    error_message="File not listed in archive index",
                )

                continue

            try:
                zip_path = download_zip(
                    session=session,
                    aemo_file=aemo_file,
                )

                raw_dataframe = parse_dispatch_zip(zip_path)
                cleaned_dataframe = transform_dispatch_price(
                    raw_dataframe
                )

                inserted_count = load_into_duckdb(
                    connection=connection,
                    dataframe=cleaned_dataframe,
                )

                write_ingestion_log(
                    connection=connection,
                    dataset_name="AEMO_DISPATCH_PRICE",
                    source_file=aemo_file.filename,
                    report_date=requested_date,
                    status="SUCCESS",
                    row_count=inserted_count,
                    started_at=started_at,
                )

                LOGGER.info(
                    "%s: parsed %d cleaned records",
                    requested_date,
                    len(cleaned_dataframe),
                )

                # Be polite to the public server.
                time.sleep(0.5)

            except Exception as error:
                LOGGER.exception(
                    "Failed to ingest %s",
                    requested_date,
                )

                write_ingestion_log(
                    connection=connection,
                    dataset_name="AEMO_DISPATCH_PRICE",
                    source_file=aemo_file.filename,
                    report_date=requested_date,
                    status="FAILED",
                    row_count=0,
                    started_at=started_at,
                    error_message=str(error),
                )

    finally:
        session.close()
        connection.close()


def ingest_aemo_monthly_period(
    start_date: date,
    end_date: date,
) -> None:
    """Backfill historical NEM prices from AEMO MMSDM monthly files."""
    if start_date > end_date:
        raise ValueError("start_date must not be after end_date")

    RAW_DIRECTORY.mkdir(parents=True, exist_ok=True)
    DATABASE_PATH.parent.mkdir(parents=True, exist_ok=True)
    session = create_http_session()
    connection = duckdb.connect(str(DATABASE_PATH))
    initialise_database(connection)

    try:
        for report_month in month_range(start_date, end_date):
            started_at = datetime.now().astimezone()
            aemo_file = build_monthly_file(report_month)
            try:
                aemo_file = discover_monthly_file(session, report_month)
                zip_path = download_monthly_zip(session, aemo_file)
                raw_dataframe = parse_dispatch_zip(zip_path)
                cleaned_dataframe = transform_dispatch_price(raw_dataframe)

                local_timestamps = cleaned_dataframe[
                    "market_datetime_local"
                ]
                first_interval_end = pd.Timestamp(start_date)
                final_interval_end = pd.Timestamp(
                    end_date + timedelta(days=1)
                )
                cleaned_dataframe = cleaned_dataframe[
                    local_timestamps.gt(first_interval_end)
                    & local_timestamps.le(final_interval_end)
                ].copy()

                inserted_count = load_into_duckdb(
                    connection,
                    cleaned_dataframe,
                )
                write_ingestion_log(
                    connection=connection,
                    dataset_name="AEMO_MONTHLY_DISPATCH_PRICE",
                    source_file=aemo_file.filename,
                    report_date=report_month,
                    status="SUCCESS",
                    row_count=inserted_count,
                    started_at=started_at,
                )
                LOGGER.info(
                    "%s: parsed %d historical power prices, inserted %d",
                    report_month.strftime("%Y-%m"),
                    len(cleaned_dataframe),
                    inserted_count,
                )
            except requests.HTTPError as error:
                status_code = error.response.status_code
                status = "NOT_FOUND" if status_code == 404 else "FAILED"
                LOGGER.exception(
                    "Failed to ingest monthly power data for %s",
                    report_month.strftime("%Y-%m"),
                )
                write_ingestion_log(
                    connection=connection,
                    dataset_name="AEMO_MONTHLY_DISPATCH_PRICE",
                    source_file=aemo_file.filename,
                    report_date=report_month,
                    status=status,
                    row_count=0,
                    started_at=started_at,
                    error_message=str(error),
                )
            except Exception as error:
                LOGGER.exception(
                    "Failed to ingest monthly power data for %s",
                    report_month.strftime("%Y-%m"),
                )
                write_ingestion_log(
                    connection=connection,
                    dataset_name="AEMO_MONTHLY_DISPATCH_PRICE",
                    source_file=aemo_file.filename,
                    report_date=report_month,
                    status="FAILED",
                    row_count=0,
                    started_at=started_at,
                    error_message=str(error),
                )
    finally:
        session.close()
        connection.close()


def print_validation_results() -> None:
    """
    Display simple POC validation results.
    """
    connection = duckdb.connect(
        str(DATABASE_PATH),
        read_only=True,
    )

    try:
        result = connection.execute(
            """
            SELECT
                region_code,
                MIN(market_datetime_local) AS first_timestamp,
                MAX(market_datetime_local) AS last_timestamp,
                COUNT(*) AS row_count,
                ROUND(AVG(price), 2) AS average_price,
                ROUND(MIN(price), 2) AS minimum_price,
                ROUND(MAX(price), 2) AS maximum_price
            FROM fact_energy_price
            WHERE commodity = 'POWER'
              AND market = 'AEMO_NEM'
            GROUP BY region_code
            ORDER BY region_code;
            """
        ).fetchdf()

        print("\nValidation summary:")
        print(result.to_string(index=False))

    finally:
        connection.close()


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingest historical AEMO NEM power spot prices."
    )
    parser.add_argument(
        "--start-date",
        type=date.fromisoformat,
        default=date(2025, 7, 1),
        help="First report date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--end-date",
        type=date.fromisoformat,
        default=date(2025, 7, 7),
        help="Last report date, inclusive (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--archive-mode",
        choices=["auto", "daily", "monthly"],
        default="auto",
        help=(
            "Source archive. 'auto' uses monthly MMSDM files for completed "
            "historical months and daily DispatchIS for the current month."
        ),
    )
    return parser.parse_args()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s | %(levelname)s | %(message)s"
        ),
    )

    arguments = parse_arguments()
    current_month = date.today().replace(day=1)
    use_monthly = arguments.archive_mode == "monthly" or (
        arguments.archive_mode == "auto"
        and arguments.end_date < current_month
    )
    if use_monthly:
        ingest_aemo_monthly_period(
            arguments.start_date,
            arguments.end_date,
        )
    else:
        ingest_aemo_period(arguments.start_date, arguments.end_date)

    print_validation_results()
