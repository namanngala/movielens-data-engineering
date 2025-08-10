import os
import pandas as pd
import pyarrow.parquet as pq
from collections import defaultdict, Counter
from tqdm import tqdm

# === CONFIGURATION ===
INPUT_FILE = '../output/analytics/ratings_cleaned_final.parquet'
OUTPUT_DIR = '../output/analytics/csvs/'
os.makedirs(OUTPUT_DIR, exist_ok=True)

print("🔍 Loading metadata only...")
pf = pq.ParquetFile(INPUT_FILE)
total_rows = pf.metadata.num_rows
print(f"📦 Total Rows: {total_rows}")
print("📊 Generating analytics...\n")

# === STORAGE STRUCTURES ===
monthly_rating_counts = Counter()
genre_counter = Counter()
user_rating_counts = defaultdict(int)
user_first_watch = {}
user_year_months = defaultdict(set)
movie_rating_stats = defaultdict(list)
top_movie_counts = Counter()

# === PROCESSING LOOP ===
for i in tqdm(range(pf.num_row_groups), desc="Processing row groups"):
    batch = pf.read_row_group(i)
    df = batch.to_pandas()

    # Convert timestamp to datetime and extract calendar features
    df['watch_date'] = pd.to_datetime(df['timestamp'], unit='s')
    df['year_month'] = df['watch_date'].dt.to_period('M')
    df['year'] = df['watch_date'].dt.year
    df['month'] = df['watch_date'].dt.month

    # 1. Monthly Rating Trends
    monthly_rating_counts.update(df['year_month'].astype(str))

    # 2. Top Genres
    genres_exploded = df['genres'].dropna().str.split('|').explode()
    genre_counter.update(genres_exploded)

    # 3. User Engagement Segments
    for uid in df['userId']:
        user_rating_counts[uid] += 1

    # 4. Cohort Retention
    for uid, ym in zip(df['userId'], df['year_month']):
        if uid not in user_first_watch:
            user_first_watch[uid] = ym
        user_year_months[uid].add(ym)

    # 5. Outlier Ratings
    for mid, rating in zip(df['movieId'], df['rating']):
        movie_rating_stats[mid].append(rating)

    # 6. Monthly User Funnel
    df_sorted = df.sort_values(by='watch_date')
    for uid, ym in zip(df_sorted['userId'], df_sorted['year_month']):
        if uid not in user_first_watch:
            user_first_watch[uid] = ym

    # 7. Top Movies
    for mid, title in zip(df['movieId'], df['title']):
        top_movie_counts[(mid, title)] += 1

# === 1. Monthly Rating Trends ===
monthly_df = pd.DataFrame(list(monthly_rating_counts.items()), columns=['year_month', 'num_ratings'])
monthly_df.sort_values('year_month').to_csv(os.path.join(OUTPUT_DIR, 'monthly_rating_trends.csv'), index=False)

# === 2. Top Genres ===
genre_df = pd.DataFrame(genre_counter.items(), columns=['genre', 'count']).sort_values(by='count', ascending=False)
genre_df.to_csv(os.path.join(OUTPUT_DIR, 'top_genres.csv'), index=False)

# === 3. User Engagement Segmentation ===
engagement_df = pd.DataFrame(user_rating_counts.items(), columns=['userId', 'num_ratings'])
engagement_df['segment'] = pd.cut(
    engagement_df['num_ratings'],
    bins=[0, 50, 200, float('inf')],
    labels=['Passive', 'Active', 'Engaged']
)
engagement_df.to_csv(os.path.join(OUTPUT_DIR, 'user_engagement_segments.csv'), index=False)

# === 4. Cohort Retention Matrix ===
cohort_matrix = defaultdict(lambda: defaultdict(int))
for uid, join_month in user_first_watch.items():
    for ym in user_year_months[uid]:
        cohort_matrix[str(join_month)][str(ym)] += 1
cohort_df = pd.DataFrame(cohort_matrix).fillna(0).astype(int).sort_index().sort_index(axis=1)
cohort_df.to_csv(os.path.join(OUTPUT_DIR, 'cohort_retention_matrix.csv'))

# === 5. Outlier Ratings ===
movie_stats_df = pd.DataFrame([
    (mid, sum(ratings) / len(ratings), pd.Series(ratings).std())
    for mid, ratings in movie_rating_stats.items()
], columns=['movieId', 'mean', 'std'])

# Reload full movie-rating rows in streaming to compute z-scores only for outliers
outlier_rows = []
for i in tqdm(range(pf.num_row_groups), desc="Finding outlier ratings"):
    batch = pf.read_row_group(i, columns=['userId', 'movieId', 'rating', 'timestamp', 'title'])
    df = batch.to_pandas()
    df = df.merge(movie_stats_df, on='movieId', how='left')
    df['z_score'] = (df['rating'] - df['mean']) / df['std']
    outlier_rows.append(df[df['z_score'].abs() > 2])

outliers_df = pd.concat(outlier_rows, ignore_index=True)
outliers_df.to_csv(os.path.join(OUTPUT_DIR, 'rating_outliers.csv'), index=False)

# === 6. Monthly User Funnel ===
monthly_counts = defaultdict(lambda: {'total_users': 0, 'new_users': 0})
for uid, first_month in user_first_watch.items():
    monthly_counts[str(first_month)]['new_users'] += 1
for uid, months in user_year_months.items():
    for ym in months:
        monthly_counts[str(ym)]['total_users'] += 1

funnel_df = pd.DataFrame([
    {'year_month': ym, **counts,
     'returning_users': counts['total_users'] - counts['new_users']}
    for ym, counts in monthly_counts.items()
])
funnel_df.sort_values('year_month').to_csv(os.path.join(OUTPUT_DIR, 'monthly_user_funnel.csv'), index=False)

# === 7. Top Movies by Rating Count ===
top_movies_df = pd.DataFrame([
    {'movieId': mid, 'title': title, 'rating_count': count}
    for (mid, title), count in top_movie_counts.items()
])
top_movies_df = top_movies_df.sort_values('rating_count', ascending=False).head(50)
top_movies_df.to_csv(os.path.join(OUTPUT_DIR, 'top_movies.csv'), index=False)

print("\n✅ All analytics CSVs successfully generated in:", OUTPUT_DIR)
