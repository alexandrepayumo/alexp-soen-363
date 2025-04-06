import pandas as pd
from neo4j import GraphDatabase

# Neo4j connection details
uri = "bolt://localhost:7687"
username = "neo4j"
password = "TestPass123!"  # Replace this!

driver = GraphDatabase.driver(uri, auth=(username, password))

def load_csv(file):
    return pd.read_csv(file)

def merge_nodes(tx, label, data, match_field="id"):
    for record in data.to_dict(orient="records"):
        props = {k: v for k, v in record.items() if pd.notnull(v)}
        match_clause = f"{match_field}: ${match_field}"
        update_clause = ", ".join([f"{k}: ${k}" for k in props])
        query = f"""
        MERGE (n:{label} {{{match_clause}}})
        SET n += {{{update_clause}}}
        """
        tx.run(query, **props)

def create_keyword_relationships(tx, keywords_df):
    for _, row in keywords_df.iterrows():
        if pd.notnull(row["tmdb_id"]) and pd.notnull(row["keyword"]):
            query = """
            MATCH (f:Film {id: $tmdb_id})
            MERGE (k:Keyword {name: $keyword})
            MERGE (f)-[:HAS_KEYWORD]->(k)
            """
            tx.run(query, tmdb_id=int(row["tmdb_id"]), keyword=row["keyword"])

def apply_ratings_to_films(films_df, ratings_df, providers_df):
    provider_map = dict(zip(providers_df["provider_id"], providers_df["provider_name"]))
    ratings_df["provider_name"] = ratings_df["provider_id"].map(provider_map)
    
    # Clean provider names to use as property keys (e.g., imdb_rating)
    ratings_df["key"] = ratings_df["provider_name"].str.lower().str.replace(" ", "_") + "_rating"
    ratings_df["rating"] = pd.to_numeric(ratings_df["rating"], errors='coerce')

    # Pivot to one row per film_id with columns for each rating
    ratings_pivot = ratings_df.pivot(index="film_id", columns="key", values="rating").reset_index()
    
    # Merge ratings into films DataFrame by matching film_id (film.id)
    films_df = films_df.merge(ratings_pivot, left_on="id", right_on="film_id", how="left")
    return films_df.drop(columns=["film_id"])

with driver.session() as session:
    # Load all CSVs
    films = load_csv("tables/films.csv")
    people = load_csv("tables/people.csv")
    planets = load_csv("tables/planets.csv")
    species = load_csv("tables/species.csv")
    vehicles = load_csv("tables/vehicles.csv")
    starships = load_csv("tables/starships.csv")
    keywords = load_csv("tables/tmdb_keywords.csv")
    ratings = load_csv("tables/normalized_ratings.csv")
    providers = load_csv("tables/normalized_rating_providers.csv")

    # Clean up opening crawl
    films["opening_crawl"] = films["opening_crawl"].str.replace('\n', ' ', regex=False)

    # 🔥 Add ratings to film DataFrame
    films = apply_ratings_to_films(films, ratings, providers)

    print("Merging nodes...")
    session.write_transaction(merge_nodes, "Film", films)
    session.write_transaction(merge_nodes, "Person", people)
    session.write_transaction(merge_nodes, "Planet", planets)
    session.write_transaction(merge_nodes, "Species", species)
    session.write_transaction(merge_nodes, "Vehicle", vehicles)
    session.write_transaction(merge_nodes, "Starship", starships)

    print("Creating keyword relationships...")
    session.write_transaction(create_keyword_relationships, keywords)

driver.close()
print("✅ Done! Films now include individual rating fields.")
