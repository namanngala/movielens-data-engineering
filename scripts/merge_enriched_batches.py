import os
from glob import glob
from tqdm import tqdm
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

ENRICHED_DIR = "../output/enriched"
FINAL_OUTPUT = "../output/final/ratings_merged.parquet"
os.makedirs(os.path.dirname(FINAL_OUTPUT), exist_ok=True)

enriched_files = sorted(glob(os.path.join(ENRICHED_DIR, "*.parquet")))
print(f"Found {len(enriched_files)} enriched files. Writing incrementally...")

writer = None

for i, file in enumerate(tqdm(enriched_files, desc="Writing to final file")):
    try:
        df = pd.read_parquet(file)
        table = pa.Table.from_pandas(df, preserve_index=False)

        if writer is None:
            writer = pq.ParquetWriter(FINAL_OUTPUT, table.schema)

        writer.write_table(table)

    except Exception as e:
        print(f"[ERROR] {file} → {e}")

if writer:
    writer.close()
    print(f"✅ Finished writing to {FINAL_OUTPUT}")
