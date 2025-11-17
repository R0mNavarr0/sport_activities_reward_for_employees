import os
import json
import random
from datetime import datetime, timedelta

import psycopg2
from psycopg2.extras import RealDictCursor

# ==========================
# Paramètres généraux
# ==========================

DB_USER = "postgres"
DB_PASSWORD = "postgres"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "rh_sport"

# Nombre moyen d'activités par employé pratiquant un sport
MIN_ACTIVITIES_PER_EMP = 2
MAX_ACTIVITIES_PER_EMP = 25

# Période de simulation : derniers 12 mois
MONTHS_BACK = 12

# Fichier de sortie (JSONL : 1 activité par ligne)
OUTPUT_FILE = "./data/output/simulated_strava_activities.jsonl"

# ==========================
# Connexion PostgreSQL
# ==========================

def get_connection():
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
    )

# ==========================
# Récupération des employés sportifs
# ==========================

def get_sporting_employees(conn):
    """
    Récupère les employés avec leur pratique_sport (non NULL).
    On suppose :
      - rh_employees(id, nom, prenom, ...)
      - sport_activities(id, pratique_sport)
    et un lien 1-1 par id.
    """
    query =  """
        SELECT
            e.id AS employee_id,
            e.nom,
            e.prenom,
            s.pratique_sport
        FROM rh_employees e
        JOIN sport_activities s
          ON e.id = s.id
        WHERE s.pratique_sport IS NOT NULL
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query)
        rows = cur.fetchall()
    return rows

# ==========================
# Utilitaires de simulation
# ==========================


# ==========================
# Mapping pratique_sport -> paramètres de simulation Strava
# ==========================

# Pour chaque sport déclaré, on définit :
# - type / sport_type Strava
# - un range de distance (mètres)
# - un range de durée (secondes)
SPORT_CONFIG = {
    "Runing": {  # orthographe telle que dans ta colonne
        "type": "Run",
        "sport_type": "Run",
        "dist_min": 3000,
        "dist_max": 20000,
        "time_min": 20 * 60,
        "time_max": 2 * 3600,
    },
    "Randonnée": {
        "type": "Walk",
        "sport_type": "Hike",
        "dist_min": 5000,
        "dist_max": 25000,
        "time_min": 60 * 60,
        "time_max": 6 * 3600,
    },
    "Triathlon": {
        "type": "Workout",
        "sport_type": "Triathlon",
        "dist_min": 10000,
        "dist_max": 80000,   # distance totale en "équivalent vélo"
        "time_min": 60 * 60,
        "time_max": 5 * 3600,
    },
    "Natation": {
        "type": "Swim",
        "sport_type": "Swim",
        "dist_min": 500,
        "dist_max": 4000,
        "time_min": 20 * 60,
        "time_max": 2 * 3600,
    },
    "Tennis": {
        "type": "Workout",
        "sport_type": "Tennis",
        "dist_min": 0,
        "dist_max": 0,
        "time_min": 45 * 60,
        "time_max": 3 * 3600,
    },
    "Badminton": {
        "type": "Workout",
        "sport_type": "Badminton",
        "dist_min": 0,
        "dist_max": 0,
        "time_min": 45 * 60,
        "time_max": 3 * 3600,
    },
    "Tennis de table": {
        "type": "Workout",
        "sport_type": "TableTennis",
        "dist_min": 0,
        "dist_max": 0,
        "time_min": 30 * 60,
        "time_max": 2 * 3600,
    },
    "Football": {
        "type": "Workout",
        "sport_type": "Soccer",
        "dist_min": 0,
        "dist_max": 0,
        "time_min": 60 * 60,
        "time_max": 2 * 3600,
    },
    "Basketball": {
        "type": "Workout",
        "sport_type": "Basketball",
        "dist_min": 0,
        "dist_max": 0,
        "time_min": 45 * 60,
        "time_max": 2 * 3600,
    },
    "Rugby": {
        "type": "Workout",
        "sport_type": "Rugby",
        "dist_min": 0,
        "dist_max": 0,
        "time_min": 60 * 60,
        "time_max": 2 * 3600,
    },
    "Boxe": {
        "type": "Workout",
        "sport_type": "Boxing",
        "dist_min": 0,
        "dist_max": 0,
        "time_min": 30 * 60,
        "time_max": 2 * 3600,
    },
    "Judo": {
        "type": "Workout",
        "sport_type": "MartialArts",
        "dist_min": 0,
        "dist_max": 0,
        "time_min": 30 * 60,
        "time_max": 2 * 3600,
    },
    "Escalade": {
        "type": "Workout",
        "sport_type": "RockClimbing",
        "dist_min": 0,
        "dist_max": 0,
        "time_min": 60 * 60,
        "time_max": 3 * 3600,
    },
    "Équitation": {
        "type": "Ride",
        "sport_type": "EBikeRide",  # approximation pour un POC
        "dist_min": 2000,
        "dist_max": 20000,
        "time_min": 30 * 60,
        "time_max": 3 * 3600,
    },
    "Voile": {
        "type": "Workout",
        "sport_type": "Sailing",
        "dist_min": 0,
        "dist_max": 0,
        "time_min": 60 * 60,
        "time_max": 6 * 3600,
    },
}

def random_past_datetime(months_back=12):
    """Retourne un datetime aléatoire dans les X derniers mois."""
    now = datetime.now()
    max_days = months_back * 30
    delta_days = random.randint(0, max_days)
    delta_seconds = random.randint(0, 24 * 3600)
    return now - timedelta(days=delta_days, seconds=delta_seconds)

def simulate_activity(employee_id: int, pratique_sport: str):
    """
    Génère UNE activité Strava-like en utilisant la pratique_sport déclarée.
    """
    config = SPORT_CONFIG.get(pratique_sport)

    # Si sport non mappé (cas improbable ici) -> fallback générique
    if config is None:
        config = {
            "type": "Workout",
            "sport_type": "Workout",
            "dist_min": 0,
            "dist_max": 0,
            "time_min": 30 * 60,
            "time_max": 90 * 60,
        }

    start_dt_utc = random_past_datetime(MONTHS_BACK)
    utc_offset = 3600  # ~ Europe/Paris
    start_dt_local = start_dt_utc + timedelta(seconds=utc_offset)

    distance = random.randint(config["dist_min"], config["dist_max"]) if config["dist_max"] > 0 else 0
    moving_time = random.randint(config["time_min"], config["time_max"])
    elapsed_time = moving_time + random.randint(0, 600)

    average_speed = distance / moving_time if moving_time > 0 else 0
    max_speed = average_speed * random.uniform(1.1, 1.8) if average_speed > 0 else 0

    activity_id = random.randint(10**11, 10**14)
    athlete_id = employee_id

    activity = {
        "id": activity_id,
        "resource_state": 3,
        "external_id": None,
        "upload_id": None,
        "athlete": {
            "id": athlete_id,
            "resource_state": 1
        },
        "name": f"Session {pratique_sport}",
        "distance": distance,
        "moving_time": moving_time,
        "elapsed_time": elapsed_time,
        "total_elevation_gain": random.randint(0, 800),
        "type": config["type"],
        "sport_type": config["sport_type"],
        "start_date": start_dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "start_date_local": start_dt_local.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "timezone": "(GMT+01:00) Europe/Paris",
        "utc_offset": utc_offset,
        "achievement_count": 0,
        "kudos_count": 0,
        "comment_count": 0,
        "athlete_count": 1,
        "photo_count": 0,
        "map": {
            "id": f"a{activity_id}",
            "polyline": None,
            "resource_state": 3
        },
        "trainer": False,
        "commute": False,
        "manual": True,
        "private": False,
        "flagged": False,
        "gear_id": None,
        "from_accepted_tag": None,
        "average_speed": average_speed,
        "max_speed": max_speed,
        "device_watts": False,
        "has_heartrate": False,
        "pr_count": 0,
        "total_photo_count": 0,
        "has_kudoed": False,
        "workout_type": None,
        "description": None,
        "calories": int(distance * 0.04) if distance > 0 else moving_time // 10,
        "segment_efforts": [],
    }

    return activity

# ==========================
# Génération pour tous les employés sportifs
# ==========================

def generate_activities_for_all():
    conn = get_connection()
    try:
        employees = get_sporting_employees(conn)
        print(f"{len(employees)} employés avec un sport déclaré trouvés.")

        activities = []

        for emp in employees:
            emp_id = emp["employee_id"]
            sport = emp["pratique_sport"]
            n_acts = random.randint(MIN_ACTIVITIES_PER_EMP, MAX_ACTIVITIES_PER_EMP)

            for _ in range(n_acts):
                act = simulate_activity(emp_id, sport)
                activities.append(act)

        print(f"{len(activities)} activités simulées.")

        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            for act in activities:
                f.write(json.dumps(act, ensure_ascii=False) + "\n")

        print(f"Activités écrites dans {OUTPUT_FILE}")

    finally:
        conn.close()

# ==========================
# Main
# ==========================

if __name__ == "__main__":
    generate_activities_for_all()