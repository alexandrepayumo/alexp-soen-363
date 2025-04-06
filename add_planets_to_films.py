import pandas as pd

# Load the films and planets CSVs
films = pd.read_csv("tables/updated_normalized_films.csv")
planets = pd.read_csv("tables/planets.csv")

# Hardcode some reasonable planet associations for each film
# Format: film_id from films.csv (not episode_id), planet_id from planets.csv
film_planet_links = [
    {"film_id": 7, "planet_id": 4},   # A New Hope -> Tatooine
    {"film_id": 7, "planet_id": 5},   # A New Hope -> Alderaan
    {"film_id": 8, "planet_id": 7},   # Empire Strikes Back -> Hoth
    {"film_id": 9, "planet_id": 4},   # Return of the Jedi -> Tatooine
    {"film_id": 10, "planet_id": 11}, # Phantom Menace -> Naboo
    {"film_id": 11, "planet_id": 11}, # Attack of the Clones -> Naboo
    {"film_id": 12, "planet_id": 12}, # Revenge of the Sith -> Coruscant
]

# Save to CSV
df_links = pd.DataFrame(film_planet_links)
df_links.to_csv("tables/film_planets.csv", index=False)
print("✅ film_planets.csv created.")
