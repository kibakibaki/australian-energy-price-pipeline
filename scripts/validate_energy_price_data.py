from pathlib import Path
import duckdb
import pandas as pd


DB_PATH = Path("database/australian_energy_market.duckdb")


REQUIRED_COLUMNS = [
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


EXPECTED_REGIONS = {
    "NSW1", "QLD1", "SA1", "TAS1", "VIC1",
    "NSW", "QLD", "SA", "VIC",
}
EXPECTED_COMMODITIES = {"POWER", "GAS"}
EXPECTED_CURRENCY = {"AUD"}
EXPECTED_MARKETS = {"AEMO_NEM", "AEMO_STTM", "AEMO_DWGM"}


def load_energy_data() -> pd.DataFrame:
    """Load fact_energy_price data from DuckDB."""
    if not DB_PATH.exists():
        raise FileNotFoundError(f"DuckDB database not found: {DB_PATH}")

    con = duckdb.connect(str(DB_PATH))
    df = con.execute("select * from fact_energy_price").fetchdf()
    con.close()

    print("\n1. Loaded energy data")
    print(f"Rows: {len(df)}")
    print(f"Columns: {list(df.columns)}")

    return df


def validate_schema_consistency(df: pd.DataFrame) -> None:
    """Check whether required columns exist."""
    print("\n2. Schema consistency check")

    actual_columns = set(df.columns)
    missing_columns = [col for col in REQUIRED_COLUMNS if col not in actual_columns]
    extra_columns = [col for col in df.columns if col not in REQUIRED_COLUMNS]

    if missing_columns:
        print("Missing columns:")
        print(missing_columns)
    else:
        print("All required columns exist.")

    if extra_columns:
        print("Extra columns:")
        print(extra_columns)
    else:
        print("No unexpected extra columns.")


def validate_date_coverage(df: pd.DataFrame) -> None:
    """Check timestamp coverage by commodity, market, and region."""
    print("\n3. Date coverage check")

    if df.empty:
        print("DataFrame is empty. Cannot check date coverage.")
        return

    coverage = (
        df.groupby(["commodity", "market", "region_code"])
        .agg(
            first_timestamp=("interval_start_utc", "min"),
            last_timestamp=("interval_start_utc", "max"),
            row_count=("interval_start_utc", "count"),
        )
        .reset_index()
        .sort_values("region_code")
    )

    print(coverage)


def validate_date_range(df: pd.DataFrame) -> None:
    """Check overall date range."""
    print("\n4. Date range check")

    if df.empty:
        print("DataFrame is empty. Cannot check date range.")
        return

    print("First timestamp:", df["interval_start_utc"].min())
    print("Last timestamp:", df["interval_start_utc"].max())


def validate_missing_values(df: pd.DataFrame) -> None:
    """Check missing values for all columns."""
    print("\n5. Missing values check")

    if df.empty:
        print("DataFrame is empty. Cannot check missing values.")
        return

    missing = df.isna().sum().reset_index()
    missing.columns = ["column_name", "missing_count"]
    missing["missing_percentage"] = missing["missing_count"] / len(df) * 100

    print(missing.sort_values("missing_count", ascending=False))

    core_columns = [
        "interval_start_utc",
        "country",
        "commodity",
        "market",
        "region_code",
        "price_type",
        "price",
        "currency",
        "unit",
        "interval_minutes",
    ]

    core_missing = missing[
        (missing["column_name"].isin(core_columns)) & (missing["missing_count"] > 0)
    ]

    if core_missing.empty:
        print("Core price-related fields have no missing values.")
    else:
        print("Warning: Core fields have missing values:")
        print(core_missing)


def validate_duplicates(df: pd.DataFrame) -> None:
    """Check duplicate records based on business key."""
    print("\n6. Duplicate records check")

    if df.empty:
        print("DataFrame is empty. Cannot check duplicates.")
        return

    key_columns = [
        "interval_start_utc",
        "country",
        "commodity",
        "market",
        "region_code",
        "price_type",
    ]

    duplicate_mask = df.duplicated(subset=key_columns, keep=False)
    duplicates = df.loc[duplicate_mask].sort_values(key_columns)

    if duplicates.empty:
        print("No duplicate records found based on business key.")
    else:
        print(f"Duplicate records found: {len(duplicates)}")
        print(duplicates[key_columns + ["price"]].head(20))


def validate_data_types(df: pd.DataFrame) -> None:
    """Check data types."""
    print("\n7. Data type check")

    print(df.dtypes)

    expected_numeric_columns = [
        "price",
        "total_demand_mw",
        "available_generation_mw",
        "available_load_mw",
        "interval_minutes",
    ]

    for col in expected_numeric_columns:
        if col in df.columns:
            non_numeric = pd.to_numeric(df[col], errors="coerce").isna() & df[col].notna()
            if non_numeric.any():
                print(f"Warning: Non-numeric values found in {col}")
            else:
                print(f"{col}: numeric check passed")


def validate_daily_periods(df: pd.DataFrame) -> None:
    """Check expected daily row counts for each market."""
    print("\n8. Daily / interval count check")

    if df.empty:
        print("DataFrame is empty. Cannot check daily periods.")
        return

    check = (
        df.groupby(["commodity", "market", "region_code"])
        .size()
        .reset_index(name="actual_rows")
        .sort_values(["commodity", "market", "region_code"])
    )

    print(check)
    print(
        "Note: NEM has 5-minute rows, STTM has one row per gas day, "
        "and DWGM has five schedule rows per gas day."
    )


def validate_numeric_ranges(df: pd.DataFrame) -> None:
    """Check abnormal numeric values."""
    print("\n9. Numeric range / abnormal value check")

    if df.empty:
        print("DataFrame is empty. Cannot check numeric ranges.")
        return

    price_summary = (
        df.groupby("region_code")
        .agg(
            row_count=("price", "count"),
            avg_price=("price", "mean"),
            min_price=("price", "min"),
            max_price=("price", "max"),
        )
        .reset_index()
        .sort_values("region_code")
    )

    print("\nPrice summary by region:")
    print(price_summary)

    negative_prices = (
        df[df["price"] < 0]
        .groupby("region_code")
        .agg(
            negative_price_count=("price", "count"),
            lowest_price=("price", "min"),
        )
        .reset_index()
        .sort_values("negative_price_count", ascending=False)
    )

    print("\nNegative price check:")
    if negative_prices.empty:
        print("No negative prices found.")
    else:
        print(negative_prices)
        print("Note: Negative electricity prices can occur in AEMO and should be flagged, not automatically removed.")

    extreme_prices = df[(df["price"] < -1000) | (df["price"] > 17500)]

    print("\nExtreme price check:")
    if extreme_prices.empty:
        print("No prices outside the review threshold found.")
    else:
        print(extreme_prices[["interval_start_utc", "region_code", "price"]].head(20))
        print(f"Extreme records found: {len(extreme_prices)}")


def validate_categorical_values(df: pd.DataFrame) -> None:
    """Check categorical values."""
    print("\n10. Categorical value check")

    if df.empty:
        print("DataFrame is empty. Cannot check categorical values.")
        return

    categorical_columns = [
        "country",
        "commodity",
        "market",
        "region_code",
        "price_type",
        "currency",
        "unit",
        "source",
    ]

    for col in categorical_columns:
        if col in df.columns:
            print(f"\n{col}:")
            print(df[col].value_counts(dropna=False))

    unexpected_regions = set(df["region_code"].dropna().unique()) - EXPECTED_REGIONS
    unexpected_commodities = set(df["commodity"].dropna().unique()) - EXPECTED_COMMODITIES
    unexpected_currency = set(df["currency"].dropna().unique()) - EXPECTED_CURRENCY
    unexpected_markets = set(df["market"].dropna().unique()) - EXPECTED_MARKETS

    if unexpected_regions:
        print("Unexpected regions:", unexpected_regions)
    else:
        print("Region values look valid.")

    if unexpected_commodities:
        print("Unexpected commodities:", unexpected_commodities)
    else:
        print("Commodity values look valid.")

    if unexpected_currency:
        print("Unexpected currencies:", unexpected_currency)
    else:
        print("Currency values look valid.")

    if unexpected_markets:
        print("Unexpected markets:", unexpected_markets)
    else:
        print("Market values look valid.")


def main() -> None:
    energy_df = load_energy_data()

    validate_schema_consistency(energy_df)

    validate_date_coverage(energy_df)

    validate_date_range(energy_df)

    validate_missing_values(energy_df)

    validate_duplicates(energy_df)

    validate_data_types(energy_df)

    validate_daily_periods(energy_df)

    validate_numeric_ranges(energy_df)

    validate_categorical_values(energy_df)


if __name__ == "__main__":
    main()
