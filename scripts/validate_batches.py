import os
import pandas as pd
from tqdm import tqdm

# Define base directory for parquet files
BASE_DIR = "../output/raw_batches"

# Expected columns and their types
EXPECTED_COLUMNS = {
    "userId": "int64",
    "movieId": "int64",
    "rating": "float64",
    "timestamp": "int64",
    "watch_date": "datetime64[ns]",
    "year": "int32",
    "month": "int32"
}

# Initialize counters
total_rows = 0
valid_files = 0
invalid_files = 0

# Walk through the output directory recursively
for root, _, files in os.walk(BASE_DIR):
    for file in files:
        if file.endswith(".parquet"):
            file_path = os.path.join(root, file)
            try:
                df = pd.read_parquet(file_path)

                # Check columns match
                if set(df.columns) != set(EXPECTED_COLUMNS.keys()):
                    raise ValueError(f"Unexpected columns in {file_path}")

                # Check dtypes
                for col, expected_dtype in EXPECTED_COLUMNS.items():
                    actual_dtype = str(df[col].dtype)
                    if actual_dtype != expected_dtype:
                        raise TypeError(f"{file_path}: Column {col} is {actual_dtype}, expected {expected_dtype}")

                # Check nulls in critical columns
                if df[["userId", "movieId", "rating", "watch_date"]].isnull().any().any():
                    raise ValueError(f"Nulls found in critical columns in {file_path}")

                # Check date range
                if df["watch_date"].min().year < 1900 or df["watch_date"].max() > pd.Timestamp.now():
                    raise ValueError(f"Date range error in {file_path}")

                # Count rows
                total_rows += len(df)
                valid_files += 1

            except Exception as e:
                print(f"[ERROR] {file_path} → {e}")
                invalid_files += 1

print(f"\n✅ Validation Complete:")
print(f"Total Valid Files: {valid_files}")
print(f"Total Invalid Files: {invalid_files}")
print(f"Total Rows Read: {total_rows}")
