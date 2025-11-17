import os
import time
import pandas as pd
import googlemaps
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("API_KEY_MAPS")

if API_KEY is None:
    raise ValueError("La variable d'environnement GOOGLE_MAPS_API_KEY n'est pas définie.")

INPUT_RH = "./data/staging/data_rh_transformed.csv"
OUTPUT_RH = "./data/staging/data_rh_with_distance.csv"

gmaps = googlemaps.Client(key=API_KEY)

df_rh = pd.read_csv(INPUT_RH, sep=";")

COL_HOME = "adresse_complete"
DESTINATION = os.getenv("DESTINATION")

def get_distance_and_duration(origin: str, destination: str):
    if pd.isna(origin) or pd.isna(destination):
        return None, None

    try:
        result = gmaps.distance_matrix(
            origins=[origin],
            destinations=[destination],
            mode="driving",
            units="metric",
        )

        rows = result.get("rows", [])
        if not rows:
            return None, None

        elements = rows[0].get("elements", [])
        if not elements:
            return None, None

        elem = elements[0]
        if elem.get("status") != "OK":
            return None, None

        distance_m = elem["distance"]["value"]
        duration_s = elem["duration"]["value"] 

        return distance_m, duration_s

    except Exception as e:
        print(f"Erreur API pour {origin} -> {destination} : {e}")
        return None, None
    
distances = []
durations = []

for idx, row in df_rh.iterrows():
    origin = row[COL_HOME]
    destination = DESTINATION

    distance_m, duration_s = get_distance_and_duration(origin, destination)
    distances.append(distance_m)
    durations.append(duration_s)

    time.sleep(0.1)

df_rh["distance_domicile_travail"] = distances
df_rh["duree_domicile_travail"] = durations

df_rh.to_csv(OUTPUT_RH, encoding='utf-8', sep=';', index=False)
print("Distances calculées et enregistrées dans data_rh_with_distance.csv")