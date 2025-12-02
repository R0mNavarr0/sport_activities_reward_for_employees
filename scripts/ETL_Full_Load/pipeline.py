import subprocess
import sys

STEPS = [
    ("Extraction et transformation des données", ["python", "./scripts/ETL_Full_Load/Postgres/etl_data_ingestion.py"]),
    ("Ingestion dans PostGres", ["python", "./scripts/ETL_Full_Load/Postgres/ingest_to_postgre.py"]),
    ("Génération activités Strava-like", ["python", "./scripts/ETL_Full_Load/Postgres/generate_strava_activities.py"]),
    ("Ingestion activités Strava-like dans PostGres", ["python", "./scripts/ETL_Full_Load/Postgres/ingest_strava_activities.py"]),
    ("Constitution Delta Bronze Layer", ["python", "./scripts/ETL_Full_Load/Bronze_layer/bronze_from_postgres.py"]),
    ("Constitution Silver RH", ["python", "./scripts/ETL_Full_Load/Silver_layer/silver_rh.py"]),
    ("Génération demandes de distance (Kafka)",  ["python", "./scripts/ETL_Full_Load/Silver_layer/distance_build_requests.py"]),
    ("Calcul des distances domicile-travail", ["python", "./scripts/ETL_Full_Load/Silver_layer/add_distance_silver_rh.py"]),
    ("Constitution Silver Sport", ["python", "./scripts/ETL_Full_Load/Silver_layer/silver_sport_activities.py"]),
    ("Constitution Silver Strava", ["python", "./scripts/ETL_Full_Load/Silver_layer/silver_strava_activities.py"]),
    ("Constitution Gold Layer", ["python", "./scripts/ETL_Full_Load/Gold_layer/gold_build.py"])
]

def run_step(name, cmd):
    print(f"\n=== Étape: {name} ===")
    print("Commande:", " ".join(cmd))
    result = subprocess.run(cmd)
    if result.returncode != 0:
        print(f"Échec étape: {name}")
        sys.exit(result.returncode)
    print(f"OK: {name}")

def main():
    for name, cmd in STEPS:
        run_step(name, cmd)
    print("\n ETL complet terminé avec succès.")

if __name__ == "__main__":
    main()