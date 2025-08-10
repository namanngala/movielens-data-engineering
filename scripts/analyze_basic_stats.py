import pyarrow.parquet as pq
import pandas as pd

INPUT_FILE = '../output/analytics/ratings_cleaned_final.parquet'

print("Loading metadata only...")

pf = pq.ParquetFile(INPUT_FILE)
total_rows = pf.metadata.num_rows

earliest = None
latest = None

for i in range(pf.num_row_groups):
    batch = pf.read_row_group(i, columns=["timestamp"])
    df = batch.to_pandas()
    df['watch_date'] = pd.to_datetime(df['timestamp'], unit='s')
    min_date = df['watch_date'].min()
    max_date = df['watch_date'].max()
    earliest = min(earliest, min_date) if earliest else min_date
    latest = max(latest, max_date) if latest else max_date

print("\n📊 Quick Summary")
print(f"Total Rows: {total_rows}")
print(f"Watch Date Range: {earliest} → {latest}")
