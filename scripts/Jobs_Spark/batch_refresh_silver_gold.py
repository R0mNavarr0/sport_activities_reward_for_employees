import subprocess
import sys
import time
import os

# Liste des scripts à exécuter
STEPS = [
    ("1. Silver RH", ["python", "scripts/ETL_Full_Load/Silver_layer/silver_rh.py"]),
    ("2. Kafka Req Distance", ["python", "scripts/ETL_Full_Load/Silver_layer/distance_build_requests.py"]),
    ("3. Pause (Worker Distance)", "SLEEP", 15),
    ("4. Silver Commute", ["python", "scripts/ETL_Full_Load/Silver_layer/add_distance_silver_rh.py"]),
    ("5. Silver Sport", ["python", "scripts/ETL_Full_Load/Silver_layer/silver_sport_activities.py"]),
    ("6. Silver Strava", ["python", "scripts/ETL_Full_Load/Silver_layer/silver_strava_activities.py"]),
    ("7. Gold Layer", ["python", "scripts/ETL_Full_Load/Gold_layer/gold_build.py"])
]

def run_pipeline():
    print(f"\n--- Démarrage du cycle de rafraîchissement : {time.strftime('%H:%M:%S')} ---")
    for name, cmd, *args in STEPS:
        if cmd == "SLEEP":
            duration = args[0] if args else 10
            print(f"{name} ({duration}s)...")
            time.sleep(duration)
            continue

        print(f" Lancement : {name}")
        try:
            subprocess.run(cmd, check=True)
            print(f" OK : {name}")
        except subprocess.CalledProcessError as e:
            print(f" ERREUR sur {name} (Code {e.returncode})")
            # On ne quitte pas forcément le script global si c'est une boucle infinie, 
            # mais pour un one-shot c'est mieux d'arrêter.
            # sys.exit(1) 

def main():
    mode = os.getenv("REFRESH_MODE", "ONE_SHOT")
    interval = int(os.getenv("REFRESH_INTERVAL_SECONDS", "300")) # 5 minutes par défaut

    if mode == "LOOP":
        print(f"Mode BOUCLE activé. Rafraîchissement toutes les {interval} secondes.")
        while True:
            run_pipeline()
            print(f"\nPause de {interval}s avant le prochain cycle...")
            time.sleep(interval)
    else:
        print("Mode ONE_SHOT activé.")
        run_pipeline()
        print("\nTerminé.")

if __name__ == "__main__":
    main()