import pandas as pd
import os
from tqdm import tqdm
import pyarrow as pa
import pyarrow.parquet as pq

INPUT_FILE = '../data/ratings.csv'
OUTPUT_DIR = '../output/raw_batches/'
CHUNK_SIZE = 1_000_000

os.makedirs(OUTPUT_DIR, exist_ok=True)
reader = pd.read_csv(INPUT_FILE, chunksize=CHUNK_SIZE)

for i, chunk in enumerate(tqdm(reader, desc="Processing chunks")):
    chunk['watch_date'] = pd.to_datetime(chunk['timestamp'], unit='s')
    chunk['year'] = chunk['watch_date'].dt.year.astype("int32")
    chunk['month'] = chunk['watch_date'].dt.month.astype("int32")

    # Define schema
    schema = pa.schema([
        ("userId", pa.int64()),
        ("movieId", pa.int64()),
        ("rating", pa.float64()),
        ("timestamp", pa.int64()),
        ("watch_date", pa.timestamp("ns")),
        ("year", pa.int32()),
        ("month", pa.int32())
    ])

    table = pa.Table.from_pandas(chunk, schema=schema, preserve_index=False)
    file_path = os.path.join(OUTPUT_DIR, f"ratings_batch_{i}.parquet")
    pq.write_table(table, file_path, use_dictionary=False)
