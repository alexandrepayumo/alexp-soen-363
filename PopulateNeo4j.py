import pandas as pd
from neo4j import GraphDatabase

# Neo4j config
uri = "bolt://localhost:7687"
username = "neo4j"
password = "TestPass123!"  # <-- Change this!

driver = GraphDatabase.driver(uri, auth=(username, password))

def load_csv(file_path):
    return pd.read_csv(file_path)

def merge_nodes(tx, label, data, match_field="id"):
    for record in data.to_dict(orient="records"):
        # Fix: Ensure lists/arrays are not misinterpreted
        props = {
            k: v for k, v in record.items()
            if not (isinstance(v, float) and pd.isna(v))  # allow arrays and strings, skip NaN
        }

        match_clause = f"{match_field}: ${match_field}"
        update_clause = ", ".join([f"{k}: ${k}" for k in props])
        query = f"""
        MERGE (n:{label} {{{match_clause}}})
        SET n += {{{update_clause}}}
        """
        tx.run(query, **props)


def create_keyword_array_per_film(film_df, keywords_df):
    grouped = keywords_df.groupby("film_id")["keyword"].apply(list).reset_index()
    return film_df.merge(grouped, on="film_id", how="left").rename(columns={"keyword": "keywords"})

def apply_ratings_to_films(films_df, ratings_df, providers_df):
    provider_map = dict(zip(providers_df["provider_id"], providers_df["provider_name"]))
    ratings_df["provider_name"] = ratings_df["provider_id"].map(provider_map)
    ratings_df["key"] = ratings_df["provider_name"].str.lower().str.replace(" ", "_") + "_rating"
    ratings_df["rating"] = pd.to_numeric(ratings_df["rating"], errors="coerce")
    pivot = ratings_df.pivot(index="film_id", columns="key", values="rating").reset_index()
    return films_df.merge(pivot, on="film_id", how="left")

def import_all():
    with driver.session() as session:
        # Load normalized tables
        films = load_csv("tables/updated_normalized_films.csv")
        planets = load_csv("tables/normalized_planets.csv")
        people = load_csv("tables/normalized_people.csv")
        species = load_csv("tables/normalized_species.csv")
        vehicles = load_csv("tables/normalized_vehicles.csv")
        starships = load_csv("tables/starships.csv")  # non-normalized
        keywords = load_csv("tables/normalized_keywords.csv")
        ratings = load_csv("tables/normalized_ratings.csv")
        providers = load_csv("tables/normalized_rating_providers.csv")

        # Preprocessing
        films["opening_crawl"] = films["opening_crawl"].str.replace('\n', ' ', regex=False)
        films = create_keyword_array_per_film(films, keywords)
        films = apply_ratings_to_films(films, ratings, providers)

        print("Merging nodes with deduplication...")
        session.execute_write(merge_nodes, "Film", films, match_field="film_id")
        session.execute_write(merge_nodes, "Person", people, match_field="person_id")
        session.execute_write(merge_nodes, "Planet", planets, match_field="planet_id")
        session.execute_write(merge_nodes, "Species", species, match_field="species_id")
        session.execute_write(merge_nodes, "Vehicle", vehicles, match_field="vehicle_id")
        session.execute_write(merge_nodes, "Starship", starships, match_field="id")

    driver.close()
    print("✅ Import complete using normalized tables!")

if __name__ == "__main__":
    import_all()
