import json
import random
import time
from datetime import datetime, timedelta, timezone

import psycopg2
import psycopg2.extras

# ==============================
# Config Postgres
# ==============================
DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "rh_sport"
DB_USER = "postgres"
DB_PASSWORD = "postgres"

# Combien d'activités simuler (None = boucle infinie jusqu'à Ctrl+C)
NB_ACTIVITIES = 20

# Pause entre deux activités simulées (en secondes)
MIN_DELAY_S = 2
MAX_DELAY_S = 6

TIMEZONE_NAME = "(GMT+01:00) Europe/Paris"
UTC_OFFSET_S = 3600  # +1h


# ==============================
# Helpers
# ==============================

SPORT_MAPPING = {
    "Tennis": ("Workout", "Tennis"),
    "Badminton": ("Workout", "Badminton"),
    "Escalade": ("Workout", "RockClimbing"),
    "Randonnée": ("Hike", "Hike"),
    "Triathlon": ("Workout", "Triathlon"),
    "Runing": ("Run", "Run"),
    "Équitation": ("Workout", "HorseRiding"),
    "Voile": ("Workout", "Sailing"),
    "Tennis de table": ("Workout", "TableTennis"),
    "Football": ("Workout", "Soccer"),
    "Natation": ("Swim", "Swim"),
    "Judo": ("Workout", "MartialArts"),
    "Basketball": ("Workout", "Basketball"),
    "Rugby": ("Workout", "Rugby"),
    "Boxe": ("Workout", "Boxing"),
}


def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )


def fetch_sporting_employees(conn):

    query = """
        SELECT s.id AS employee_id, s.pratique_sport
        FROM sport_activities s
        JOIN rh_employees e ON e.id = s.id
        WHERE s.pratique_sport IS NOT NULL
    """
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(query)
        rows = cur.fetchall()
    return [(r["employee_id"], r["pratique_sport"]) for r in rows]


def generate_activity_for_employee(employee_id, pratique_sport):

    now_utc = datetime.now(timezone.utc)
    start_date_utc = now_utc
    start_date_local = now_utc + timedelta(seconds=UTC_OFFSET_S)

    # Type / sport_type Strava-like
    base_type, sport_type = SPORT_MAPPING.get(
        pratique_sport, ("Workout", pratique_sport or "Workout")
    )

    # Distance et durée selon le sport
    if sport_type in ("Run", "Running"):
        distance_m = random.randint(3000, 15000)  # 3–15 km
        moving_time_s = random.randint(20 * 60, 90 * 60)
    elif sport_type in ("Swim",):
        distance_m = random.randint(500, 3000)
        moving_time_s = random.randint(15 * 60, 60 * 60)
    elif sport_type in ("Hike",):
        distance_m = random.randint(5000, 20000)
        moving_time_s = random.randint(60 * 60, 4 * 60 * 60)
    else:
        # Sports “0 distance” ou indoor (tennis, judo, etc.)
        distance_m = 0
        moving_time_s = random.randint(45 * 60, 120 * 60)

    elapsed_time_s = int(moving_time_s * random.uniform(1.0, 1.2))
    total_elev_gain_m = random.randint(0, 600)
    calories = int(moving_time_s / 60 * random.uniform(6, 12))

    activity_id = random.randint(10**12, 10**14 - 1)
    name = f"Session {pratique_sport or 'Sport'}"

    payload = {
        "id": activity_id,
        "resource_state": 3,
        "external_id": None,
        "upload_id": None,
        "athlete": {
            "id": employee_id,
            "resource_state": 1,
        },
        "name": name,
        "distance": distance_m,
        "moving_time": moving_time_s,
        "elapsed_time": elapsed_time_s,
        "total_elevation_gain": total_elev_gain_m,
        "type": base_type,
        "sport_type": sport_type,
        "start_date": start_date_utc.isoformat().replace("+00:00", "Z"),
        "start_date_local": start_date_local.isoformat(),
        "timezone": TIMEZONE_NAME,
        "utc_offset": UTC_OFFSET_S,
        "achievement_count": 0,
        "kudos_count": 0,
        "comment_count": 0,
        "athlete_count": 1,
        "photo_count": 0,
        "map": {
            "id": f"a{activity_id}",
            "polyline": None,
            "resource_state": 3,
        },
        "trainer": False,
        "commute": False,
        "manual": True,
        "private": False,
        "flagged": False,
        "gear_id": None,
        "from_accepted_tag": None,
        "average_speed": distance_m / moving_time_s if moving_time_s > 0 else 0.0,
        "max_speed": 0,
        "device_watts": False,
        "has_heartrate": False,
        "pr_count": 0,
        "total_photo_count": 0,
        "has_kudoed": False,
        "workout_type": None,
        "description": None,
        "calories": calories,
        "segment_efforts": [],
    }

    return {
        "activity_id": activity_id,
        "employee_id": employee_id,
        "name": name,
        "distance_m": distance_m,
        "moving_time_s": moving_time_s,
        "elapsed_time_s": elapsed_time_s,
        "total_elev_gain_m": total_elev_gain_m,
        "type": base_type,
        "sport_type": sport_type,
        "start_date_utc": start_date_utc,
        "start_date_local": start_date_local,
        "timezone": TIMEZONE_NAME,
        "utc_offset_s": UTC_OFFSET_S,
        "commute": False,
        "manual": True,
        "private": False,
        "flagged": False,
        "calories": calories,
        "raw_payload": json.dumps(payload),
    }


def insert_activity(conn, activity):

    sql = """
        INSERT INTO strava_activities (
            activity_id,
            employee_id,
            name,
            distance_m,
            moving_time_s,
            elapsed_time_s,
            total_elev_gain_m,
            type,
            sport_type,
            start_date_utc,
            start_date_local,
            timezone,
            utc_offset_s,
            commute,
            manual,
            private,
            flagged,
            calories,
            raw_payload
        ) VALUES (
            %(activity_id)s,
            %(employee_id)s,
            %(name)s,
            %(distance_m)s,
            %(moving_time_s)s,
            %(elapsed_time_s)s,
            %(total_elev_gain_m)s,
            %(type)s,
            %(sport_type)s,
            %(start_date_utc)s,
            %(start_date_local)s,
            %(timezone)s,
            %(utc_offset_s)s,
            %(commute)s,
            %(manual)s,
            %(private)s,
            %(flagged)s,
            %(calories)s,
            %(raw_payload)s
        )
    """

    with conn.cursor() as cur:
        cur.execute(sql, activity)
    conn.commit()


def main():
    conn = get_connection()
    try:
        employees = fetch_sporting_employees(conn)
        if not employees:
            print("Aucun salarié avec pratique_sport trouvée. Vérifie la table sport_activities.")
            return

        print(f"{len(employees)} salariés sportifs trouvés.")
        print("Simulation de nouvelles activités... Ctrl+C pour arrêter.")

        count = 0
        while NB_ACTIVITIES is None or count < NB_ACTIVITIES:
            employee_id, pratique_sport = random.choice(employees)
            activity = generate_activity_for_employee(employee_id, pratique_sport)
            insert_activity(conn, activity)
            count += 1

            print(
                f"[INSERT] Nouvelle activité activity_id={activity['activity_id']} "
                f"employee_id={employee_id} sport={pratique_sport}"
            )

            delay = random.uniform(MIN_DELAY_S, MAX_DELAY_S)
            time.sleep(delay)

    except KeyboardInterrupt:
        print("\nArrêt demandé par l'utilisateur.")
    finally:
        conn.close()
        print("Connexion Postgres fermée.")


if __name__ == "__main__":
    main()