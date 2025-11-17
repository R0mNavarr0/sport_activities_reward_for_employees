import subprocess
import sys

STEPS = [
    ("Extraction et transformation des données", ["python", "./ETL/etl_data_ingestion.py"]),
    ("Calcul distance", ["python", "./ETL/distance_calcul.py"]),
    ("Validation des primes", ["python", "./ETL/prime_validation.py"]),
    ("Ingestion dans PostGre", ["python", "./ETL/ingest_to_postgre.py"]),
    ("Génération activités Strava-like", ["python", "./ETL/generate_strava_activities.py"]),
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