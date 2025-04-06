import pandas as pd

# Load both CSVs
films_df = pd.read_csv("tables/normalized_films.csv")
tmdb_df = pd.read_csv("tables/TMDB_movie_dataset_v11.csv")

# Clean and prepare: ensure consistent formatting
films_df["imdb_id"] = films_df["imdb_id"].str.strip()
tmdb_df["imdb_id"] = tmdb_df["imdb_id"].str.strip()

# Merge on imdb_id to get the 'overview'
merged = pd.merge(films_df, tmdb_df[["imdb_id", "overview"]], on="imdb_id", how="left")

# Save it back with the new column
merged.to_csv("tables/updated_normalized_films.csv", index=False)
print("✅ Added 'overview' to updated_normalized_films.csv!")
