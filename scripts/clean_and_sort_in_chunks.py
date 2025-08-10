import os
import pandas as pd
import pyarrow.parquet as pq
from tqdm import tqdm

INPUT_FILE = '../output/final/ratings_merged.parquet'
OUTPUT_DIR = '../output/cleaned_chunks/'
os.makedirs(OUTPUT_DIR, exist_ok=True)

parquet_file = pq.ParquetFile(INPUT_FILE)
num_row_groups = parquet_file.num_row_groups
print(f"Found {num_row_groups} row groups. Writing cleaned chunks...")

for i in tqdm(range(num_row_groups), desc="Cleaning row groups"):
    try:
        table = parquet_file.read_row_group(i)
        df = table.to_pandas()

        df.drop_duplicates(subset=['userId', 'movieId', 'timestamp'], inplace=True)
        df.sort_values(by='timestamp', inplace=True)

        chunk_path = os.path.join(OUTPUT_DIR, f'ratings_cleaned_chunk_{i}.parquet')
        df.to_parquet(chunk_path, index=False)

    except Exception as e:
        print(f"[ERROR] Row group {i} → {e}")

print(f"\n✅ Cleaned chunks saved to: {OUTPUT_DIR}")
