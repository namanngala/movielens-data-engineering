import pandas as pd
import os
from tqdm import tqdm

# Paths
RAW_BATCHES_DIR = "../output/raw_batches/"
OUTPUT_DIR = "../output/enriched/"
MOVIES_FILE = "../data/movies.csv"

# Load the movie metadata once
movies_df = pd.read_csv(MOVIES_FILE)

# Create output directory
os.makedirs(OUTPUT_DIR, exist_ok=True)

# List all Parquet files
parquet_files = [
    f for f in os.listdir(RAW_BATCHES_DIR) if f.endswith(".parquet")
]
print(f"Found {len(parquet_files)} parquet files.")

# Process each file one by one
for filename in tqdm(parquet_files, desc="Enriching files"):
    input_path = os.path.join(RAW_BATCHES_DIR, filename)
    try:
        # Read Parquet file
        df = pd.read_parquet(input_path)

        # Enrich with time features
        df['hour'] = pd.to_datetime(df['timestamp'], unit='s').dt.hour
        df['day_of_week'] = pd.to_datetime(df['timestamp'], unit='s').dt.dayofweek

        # Join with movie titles
        df = df.merge(movies_df, how='left', on='movieId')

        # Save enriched version
        output_path = os.path.join(OUTPUT_DIR, filename)
        df.to_parquet(output_path, index=False)

    except Exception as e:
        print(f"[ERROR] {filename} → {e}")

print("✅ Enrichment complete.")
