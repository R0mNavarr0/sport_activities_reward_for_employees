import subprocess
import sys

STEPS = [
    ("Extraction et transformation des données", ["python", "./pipeline/etl_data_ingestion.py"]),
    ("Calcul distance", ["python", "./pipeline/distance_calcul.py"]),
    ("Ingestion dans PostGres", ["python", "./pipeline/ingest_to_postgre.py"]),
    ("Génération activités Strava-like", ["python", "./pipeline/generate_strava_activities.py"]),
    ("Ingestion activités Strava-like dans PostGres", ["python", "./pipeline/ingest_strava_activities.py"]),
    ("Constitution Delta Bronze Layer", ["python", "./pipeline/bronze_from_postgres.py"]),
    ("Constitution Delta Silver layer (1/3)", ["python", "./pipeline/silver_rh.py"]),
    ("Constitution Delta Silver layer (2/3)", ["python", "./pipeline/silver_sport_activities.py"]),
    ("Constitution Delta Silver layer (3/3)", ["python", "./pipeline/silver_strava_activities.py"]),
    ("Constitution Delta Gold layer", ["python", "./pipeline/gold_build.py"])
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