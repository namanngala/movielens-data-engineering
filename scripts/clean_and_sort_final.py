import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

INPUT_DIR = '../output/cleaned_chunks/'
OUTPUT_FILE = '../output/analytics/ratings_cleaned_final.parquet'
os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

# Collect all chunk files
chunk_files = sorted([
    os.path.join(INPUT_DIR, f) 
    for f in os.listdir(INPUT_DIR) 
    if f.endswith('.parquet')
])

print(f"Found {len(chunk_files)} cleaned chunk files. Merging...")

writer = None
for file in tqdm(chunk_files, desc="Writing final file"):
    try:
        df = pd.read_parquet(file)
        table = pa.Table.from_pandas(df, preserve_index=False)

        if writer is None:
            writer = pq.ParquetWriter(OUTPUT_FILE, table.schema)
        writer.write_table(table)

    except Exception as e:
        print(f"[ERROR] {file} → {e}")

if writer:
    writer.close()
    print(f"\n✅ Final cleaned and sorted file written: {OUTPUT_FILE}")
