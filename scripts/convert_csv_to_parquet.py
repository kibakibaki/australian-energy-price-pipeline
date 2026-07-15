from pathlib import Path
import csv
import pandas as pd

RAW_DIR = Path("data/raw")
PARQUET_DIR = Path("data/parquet")

PARQUET_DIR.mkdir(parents=True, exist_ok=True)

csv_files = list(RAW_DIR.rglob("*.CSV")) + list(RAW_DIR.rglob("*.csv"))

if not csv_files:
    print("No CSV files found under data/raw/")
    raise SystemExit

for csv_path in csv_files:
    print(f"Reading AEMO CSV: {csv_path}")

    header = None
    data_rows = []

    with open(csv_path, "r", encoding="utf-8", errors="ignore", newline="") as f:
        reader = csv.reader(f)

        for row in reader:
            if not row:
                continue

            # AEMO metadata/header row
            if row[0] == "I":
                # Usually first 4 columns are metadata, actual data columns start after that
                header = row[4:]

            # AEMO data row
            elif row[0] == "D":
                if header is not None:
                    data_rows.append(row[4:])

    if header is None:
        print(f"No header row found in {csv_path}, skipped.")
        continue

    if not data_rows:
        print(f"No data rows found in {csv_path}, skipped.")
        continue

    # Make row lengths consistent
    clean_rows = []
    for row in data_rows:
        if len(row) < len(header):
            row = row + [None] * (len(header) - len(row))
        elif len(row) > len(header):
            row = row[:len(header)]
        clean_rows.append(row)

    df = pd.DataFrame(clean_rows, columns=header)

    relative_path = csv_path.relative_to(RAW_DIR)
    parquet_path = PARQUET_DIR / relative_path.with_suffix(".parquet")
    parquet_path.parent.mkdir(parents=True, exist_ok=True)

    df.to_parquet(parquet_path, index=False)

    print(f"Converted to: {parquet_path}")
    print(f"Rows: {len(df)}")
    print(f"Columns: {list(df.columns)}")

print("AEMO CSV to Parquet conversion completed.")