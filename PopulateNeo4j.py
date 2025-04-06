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
        props = {
            k: v for k, v in record.items()
            if not (isinstance(v, float) and pd.isna(v))
        }

        if match_field not in record:
            continue

        props[match_field] = record[match_field]

        # Cypher doesn't like lists passed into SET n += {...}
        # So we will unpack explicitly
        set_lines = []
        for key in props:
            set_lines.append(f"n.{key} = ${key}")
        set_clause = ", ".join(set_lines)

        query = f"""
        MERGE (n:{label} {{{match_field}: ${match_field}}})
        SET {set_clause}
        """
        tx.run(query, **props)

def delete_all_nodes(tx):
    tx.run("MATCH (n) DETACH DELETE n")

def create_relationships(tx, query, parameters_list):
    for params in parameters_list:
        tx.run(query, **params)

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

def create_film_planet_relationships(tx, film_planets_df):
    for row in film_planets_df.to_dict(orient="records"):
        tx.run("""
            MATCH (f:Film {film_id: $film_id})
            MATCH (p:Planet {planet_id: $planet_id})
            MERGE (f)-[:FEATURES_PLANET]->(p)
        """, film_id=row["film_id"], planet_id=row["planet_id"])
        print(f"Linking film {row['film_id']} with planet {row['planet_id']}")

def import_all():
    with driver.session() as session:
        print("🧹 Deleting all existing nodes and relationships...")
        session.execute_write(delete_all_nodes)

        # Load normalized tables
        films = load_csv("tables/updated_normalized_films.csv")
        planets = load_csv("tables/normalized_planets.csv")
        people = load_csv("tables/normalized_people.csv")
        species = load_csv("tables/normalized_species.csv")
        vehicles = load_csv("tables/normalized_vehicles.csv")
        starships = load_csv("tables/starships.csv")
        keywords = load_csv("tables/normalized_keywords.csv")
        ratings = load_csv("tables/normalized_ratings.csv")
        providers = load_csv("tables/normalized_rating_providers.csv")
        film_planets = load_csv("tables/film_planets.csv")

        # Preprocessing
        films["opening_crawl"] = films["opening_crawl"].str.replace('\n', ' ', regex=False)
        films = create_keyword_array_per_film(films, keywords)
        films = apply_ratings_to_films(films, ratings, providers)

        print("Merging nodes with deduplication...")
        print(films[["title", "keywords"]])
        session.execute_write(merge_nodes, "Film", films, match_field="film_id")
        session.execute_write(merge_nodes, "Person", people, match_field="person_id")
        session.execute_write(merge_nodes, "Planet", planets, match_field="planet_id")
        session.execute_write(merge_nodes, "Species", species, match_field="species_id")
        session.execute_write(merge_nodes, "Vehicle", vehicles, match_field="vehicle_id")
        session.execute_write(merge_nodes, "Starship", starships, match_field="id")

        print("🔗 Creating relationships...")

        # 1. Person FROM Planet
        people_from = people.dropna(subset=["homeworld_id"])
        from_params = people_from[["person_id", "homeworld_id"]].to_dict(orient="records")
        session.execute_write(
            create_relationships,
            """
            MATCH (p:Person {person_id: $person_id})
            MATCH (pl:Planet {planet_id: $homeworld_id})
            MERGE (p)-[:FROM]->(pl)
            """,
            from_params
        )

        # 2. Film FEATURES_VEHICLE (example only – replace with actual data if you have it)
        # TEMP: Let's assume film 1 features vehicle 1 and 2
        features_vehicle = [
            {"film_id": 1, "vehicle_id": 1},
            {"film_id": 1, "vehicle_id": 2},
            {"film_id": 2, "vehicle_id": 2}
        ]
        session.execute_write(
            create_relationships,
            """
            MATCH (f:Film {film_id: $film_id})
            MATCH (v:Vehicle {vehicle_id: $vehicle_id})
            MERGE (f)-[:FEATURES_VEHICLE]->(v)
            """,
            features_vehicle
        )

        # 3. Film FEATURES_PLANET (inferred through people)
        features_planet = people.dropna(subset=["homeworld_id"])
        features_planet = features_planet[["homeworld_id", "person_id"]]

        inferred_links = []
        for _, row in features_planet.iterrows():
            person_id = row["person_id"]
            homeworld_id = row["homeworld_id"]
            # Assume every film linked to this person features the planet
            inferred_links.append({"person_id": person_id, "planet_id": homeworld_id})

        session.execute_write(
            create_relationships,
            """
            MATCH (p:Person {person_id: $person_id})<-[:APPEARED_IN]-(f:Film)
            MATCH (pl:Planet {planet_id: $planet_id})
            MERGE (f)-[:FEATURES_PLANET]->(pl)
            """,
            inferred_links
        )

        session.execute_write(create_film_planet_relationships, film_planets)


    driver.close()
    print("✅ Import complete using normalized tables!")

if __name__ == "__main__":
    import_all()
