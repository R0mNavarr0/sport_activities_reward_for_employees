import json
import os
import sys
import time
from datetime import datetime
from dotenv import load_dotenv

import requests
from confluent_kafka import Consumer, KafkaException, KafkaError
import psycopg2
import psycopg2.extras

load_dotenv()

# ==============================
# Paramètres Kafka / Redpanda
# ==============================

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
TOPIC = "rh_sport.public.strava_activities"
GROUP_ID = "slack-new-activity-notifier"

# ==============================
# Paramètres Postgres
# ==============================

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

# ==============================
# Paramètres Slack
# ==============================

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

if not SLACK_WEBHOOK_URL:
    print("ERREUR: La variable d'environnement SLACK_WEBHOOK_URL n'est pas définie.")
    sys.exit(1)

# ==============================
# Postgres : cache employés
# ==============================

def load_employee_cache():

    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )
    cache = {}
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute("SELECT id, nom, prenom FROM rh_employees;")
        rows = cur.fetchall()
        for r in rows:
            cache[r["id"]] = {"nom": r["nom"], "prenom": r["prenom"]}
    conn.close()
    print(f"[POSTGRES] Cache employés chargé : {len(cache)} lignes.")
    return cache


def parse_debezium_value(value_bytes: bytes) -> dict | None:

    try:
        raw = json.loads(value_bytes.decode("utf-8"))
    except Exception as e:
        print(f"[PARSE] Impossible de décoder le message JSON: {e}")
        return None

    if "payload" in raw and isinstance(raw["payload"], dict):
        record = raw["payload"]
    else:
        record = raw 

    op = record.get("op")
    after = record.get("after")

    if op != "c" or not after:
        return None

    try:
        activity_id = after.get("activity_id")
        employee_id = after.get("employee_id")
        name = after.get("name")
        distance_m = after.get("distance_m")

        return {
            "op": op,
            "activity_id": activity_id,
            "employee_id": employee_id,
            "name": name,
            "distance_m": distance_m,
        }
    except Exception as e:
        print(f"[PARSE] Erreur d'extraction des champs 'after': {e}")
        return None


def send_slack_webhook(activity: dict, employee_cache: dict) -> None:

    distance_m = activity.get("distance_m") or 0
    distance_km = distance_m / 1000.0

    employee_id = activity.get("employee_id")
    emp_info = employee_cache.get(employee_id)
    if emp_info:
        full_name = f"{emp_info['prenom']} {emp_info['nom']}"
        first_name = emp_info["prenom"]
        last_name = emp_info["nom"]
    else:
        full_name = f"Employé {employee_id}"
        first_name = None
        last_name = None

    payload = {
        "employee_id": employee_id,
        "employee_fullname": full_name,
        "employee_firstname": first_name,
        "employee_lastname": last_name,
        "distance_km": round(distance_km, 1),
        "activity_name": activity.get("name"),
    }

    resp = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=10)
    if resp.status_code != 200:
        print(f"[SLACK] Erreur HTTP {resp.status_code}: {resp.text}")


def main():
    employee_cache = load_employee_cache()

    consumer_conf = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
        "session.timeout.ms": 45000  
    }

    consumer = Consumer(consumer_conf)
    consumer.subscribe([TOPIC])

    print(f"[KAFKA] Abonné au topic {TOPIC}. En attente de nouvelles activités...")

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                err = msg.error()
                if err.code() == KafkaError._PARTITION_EOF:
                    continue
                if err.code() == KafkaError._ALL_BROKERS_DOWN:
                    print("[KAFKA] Tous les brokers sont down, arrêt du consumer.")
                    break
                print(f"[KAFKA] Erreur : {err}")
                time.sleep(2)
                continue

            value = msg.value()
            if value is None:
                continue

            activity = parse_debezium_value(value)
            if not activity:
                continue 

            print(f"[SLACK] Envoi message pour activity_id={activity.get('activity_id')}")
            send_slack_webhook(activity, employee_cache)

            time.sleep(1)

    except KeyboardInterrupt:
        print("\nArrêt demandé par l'utilisateur.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()