# MovieLens 25M Data Engineering Pipeline

## Overview

This repository implements a **true end-to-end batch data pipeline** on the **MovieLens “32M/25M-scale” ratings data** (~25,000,095 rating rows in our run). The goal is to mirror how a data engineering team would take large, raw CSVs and turn them into **analytics-ready datasets** and **BI dashboards**—while staying **memory-safe on a single machine** (no Spark, no cluster).

### What the pipeline does

- **Ingest (chunked):** Stream the massive `ratings.csv` in **1M-row chunks** to avoid OOM, add a `watch_date` (from Unix `timestamp`), and write **raw Parquet batches** (`/output/raw_batches/`).
- **Validate:** Per-file sanity checks to catch schema/type drift, failed reads, and obviously bad values (e.g., ratings outside **0.5–5.0**).
- **Enrich:** Merge ratings with `movies.csv` to attach **`title`** and **`genres`** (pipe-separated). Write **enriched Parquet** files (`/output/enriched/`) without blowing RAM.
- **Merge:** Append enriched batches into a single **columnar** file using **PyArrow’s `ParquetWriter`** (`/output/final/ratings_merged.parquet`).
- **Clean & sort:** Read the merged file **row-group by row-group** to deduplicate by `(userId, movieId, timestamp)`, sort by `timestamp`, and write `ratings_cleaned_final.parquet`.
- **Analytics export:** Compute realistic product analytics without ever loading all rows at once, and export small CSVs for BI:
  - Monthly rating trends
  - Genre popularity
  - User engagement segmentation
  - Cohort retention
  - New vs. returning user funnel
  - Top movies by rating count
  - Rating outliers (optional large CSV excluded from GitHub)
- **BI wiring:** Upload CSVs to **Google Sheets** and connect to **Looker Studio** to build interactive dashboards.

---

## Project Structure

movielens-data-engineering/
│
├── data/
│ ├── movies.csv
│ ├── ratings.csv # Large dataset (25M+ rows)
│
├── output/
│ ├── raw_batches/ # Chunked Parquet files from ingestion
│ ├── enriched/ # Batches with titles and genres
│ ├── final/ # Merged full dataset
│ ├── analytics/ # Cleaned final parquet + CSV analytics
│ ├── csvs/
│ ├── monthly_rating_trends.csv
│ ├── top_genres.csv
│ ├── user_engagement_segments.csv
│ ├── cohort_retention_matrix.csv
│ ├── monthly_user_funnel.csv
│ ├── top_movies.csv
│ └── rating_outliers_sample.csv
│
├── scripts/
│ ├── 1_ingest_to_parquet.py
│ ├── 2_validate_batches.py
│ ├── 3_enrich_with_movies.py
│ ├── 4_merge_batches.py
│ ├── 5_clean_and_sort.py
│ ├── 6_generate_analytics_csvs.py
│
└── README.md


---

## Script Details

### 1_ingest_to_parquet.py
- Reads `ratings.csv` in **1,000,000-row chunks** with `pandas.read_csv`.
- Adds `watch_date` column (converted from Unix `timestamp`).
- Writes each chunk as **Snappy-compressed Parquet** to `/output/raw_batches/`.

### 2_validate_batches.py
- Iterates through `/output/raw_batches/` and verifies:
  - Schema matches expectations.
  - Ratings are between 0.5 and 5.0.
  - No missing critical columns.
- Reports any anomalies for manual inspection.

### 3_enrich_with_movies.py
- Loads `movies.csv` into memory (small file).
- Joins each ratings batch with movie metadata (title, genres).
- Writes enriched Parquet files to `/output/enriched/`.

### 4_merge_batches.py
- Uses `pyarrow.parquet.ParquetWriter` to **append** enriched batches into a single file.
- Output: `/output/final/ratings_merged.parquet`.

### 5_clean_and_sort.py
- Reads merged Parquet **row-group by row-group** to avoid full-memory load.
- Deduplicates by `(userId, movieId, timestamp)`.
- Sorts globally by `timestamp`.
- Writes `/output/analytics/ratings_cleaned_final.parquet`.

### 6_generate_analytics_csvs.py
- Computes analytics from the cleaned Parquet:
  - **monthly_rating_trends.csv**
  - **top_genres.csv**
  - **user_engagement_segments.csv**
  - **cohort_retention_matrix.csv**
  - **monthly_user_funnel.csv**
  - **top_movies.csv**
  - **rating_outliers_sample.csv**
- Uses incremental aggregation to remain memory-safe.

---

## Technologies Used

- **Python 3.10+**
- **Pandas** — chunked CSV reading, joins, transformations.
- **PyArrow** — Parquet I/O, schema handling, row-group streaming.
- **NumPy** — numeric operations.
- **Google Sheets + Looker Studio** — BI dashboards.
- **Git** — version control with `.gitignore` for large files.

---

## Challenges & Solutions

### Large file size (25M+ rows)
- **Challenge:** Processing 25M+ rows locally without OOM.
- **Solution:** Chunked reading & row-group streaming; avoided `pd.concat`.

### Maintaining reproducibility
- **Challenge:** Consistent output across runs.
- **Solution:** Deterministic deduplication key + global sort order.

---

## How to Run

```bash
1. Clone the repository

git clone https://github.com/namanngala/movielens-data-engineering.git
cd movielens-data-engineering

2. Install dependencies

pip install -r requirements.txt

3. Prepare data
Place the MovieLens dataset (movies.csv, ratings.csv) in the data/ folder.

4. Run the pipeline

python scripts/1_ingest_to_parquet.py
python scripts/2_validate_batches.py
python scripts/3_enrich_with_movies.py
python scripts/4_merge_batches.py
python scripts/5_clean_and_sort.py
python scripts/6_generate_analytics_csvs.py

5. View outputs

Final cleaned data: output/analytics/ratings_cleaned_final.parquet
Analytics CSVs: output/analytics/csvs/
Connect CSVs to Google Sheets → Looker Studio for BI dashboards.




