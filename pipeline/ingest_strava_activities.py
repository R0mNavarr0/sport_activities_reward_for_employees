import json
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine

# ==============================
# Paramètres
# ==============================

JSONL_FILE = "./data/output/simulated_strava_activities.jsonl"

DB_USER = "postgres"
DB_PASSWORD = "postgres"
DB_HOST = "postgres"
DB_PORT = "5432"
DB_NAME = "rh_sport"

TABLE = "strava_activities"
SCHEMA = "public"


def get_engine():
    url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(url)


def load_jsonl(path: str):
    path_obj = Path(path)
    if not path_obj.exists():
        raise FileNotFoundError(f"Fichier JSONL introuvable : {path}")
    records = []
    with path_obj.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            records.append(json.loads(line))
    return records


def flatten_activities(records):
    """
    Transforme la liste de JSON Strava-like en liste de dicts à plat
    pour correspondre aux colonnes de la table strava_activities.
    """
    flat = []
    for rec in records:
        activity_id = rec.get("id")
        athlete = rec.get("athlete") or {}
        employee_id = athlete.get("id")

        flat.append({
            "activity_id": activity_id,
            "employee_id": employee_id,
            "name": rec.get("name"),
            "distance_m": rec.get("distance"),
            "moving_time_s": rec.get("moving_time"),
            "elapsed_time_s": rec.get("elapsed_time"),
            "total_elev_gain_m": rec.get("total_elevation_gain"),
            "type": rec.get("type"),
            "sport_type": rec.get("sport_type"),
            "start_date_utc": rec.get("start_date"),
            "start_date_local": rec.get("start_date_local"),
            "timezone": rec.get("timezone"),
            "utc_offset_s": rec.get("utc_offset"),
            "commute": rec.get("commute"),
            "manual": rec.get("manual"),
            "private": rec.get("private"),
            "flagged": rec.get("flagged"),
            "calories": rec.get("calories"),
            "raw_payload": json.dumps(rec, ensure_ascii=False),
        })
    return flat


def main():
    print(f"Chargement des activités depuis {JSONL_FILE} ...")
    records = load_jsonl(JSONL_FILE)
    print(f"{len(records)} activités lues.")

    flat_records = flatten_activities(records)
    df = pd.DataFrame(flat_records)

    # Conversion des dates
    df["start_date_utc"] = pd.to_datetime(df["start_date_utc"], errors="coerce", utc=True)
    df["start_date_local"] = pd.to_datetime(df["start_date_local"], errors="coerce")

    print("Aperçu des données à insérer :")
    print(df.head())

    engine = get_engine()

    df.to_sql(
        TABLE,
        engine,
        schema=SCHEMA,
        if_exists="append",
        index=False,
        method="multi",
    )

    print(f"Ingestion terminée : {len(df)} lignes insérées dans {SCHEMA}.{TABLE}.")


if __name__ == "__main__":
    main()