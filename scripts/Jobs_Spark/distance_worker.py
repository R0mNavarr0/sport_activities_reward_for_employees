import os
import json
import time
import requests
import psycopg2
from dotenv import load_dotenv
from confluent_kafka import Consumer, KafkaError

load_dotenv()

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
TOPIC = "distance_requests"
GROUP_ID = "distance-worker"

GOOGLE_API_KEY = os.getenv("API_KEY_MAPS")

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

def compute_distance_duration(origin: str, destination: str) -> tuple[int | None, int | None, str | None]:
    url = "https://maps.googleapis.com/maps/api/distancematrix/json"
    params = {
        "origins": origin,
        "destinations": destination,
        "mode": "driving",
        "key": GOOGLE_API_KEY,
    }
    r = requests.get(url, params=params)
    data = r.json()

    try:
        elem = data["rows"][0]["elements"][0]
        status = elem["status"]
        if status != "OK":
            return None, None, status
        distance_m = elem["distance"]["value"]
        duration_s = elem["duration"]["value"]
        return distance_m, duration_s, "OK"
    except Exception as e:
        return None, None, f"ERROR: {e}"

def upsert_distance_cache(conn, origin, dest, dist, dur, status, error_msg):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO distance_cache(origin_address, destination_address, distance_m, duration_s, status, error_message)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (origin_address, destination_address, provider)
            DO UPDATE SET
                distance_m = EXCLUDED.distance_m,
                duration_s = EXCLUDED.duration_s,
                status = EXCLUDED.status,
                error_message = EXCLUDED.error_message,
                last_updated = now();
            """,
            (origin, dest, dist, dur, status, error_msg),
        )
    conn.commit()

def main():
    consumer_conf = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe([TOPIC])

    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                err = msg.error()
                if err.code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"[KAFKA] Erreur : {err}")
                time.sleep(2)
                continue

            payload = json.loads(msg.value().decode("utf-8"))
            origin = payload["origin_address"]
            dest = payload["destination_address"]

            dist, dur, status = compute_distance_duration(origin, dest)
            error_msg = None if status == "OK" else status

            upsert_distance_cache(conn, origin, dest, dist, dur, status, error_msg)
            print(f"[DIST] {origin} -> {dest} = {dist} m, {dur} s, status={status}")

    except KeyboardInterrupt:
        print("\nArrêt demandé par l'utilisateur.")
    finally:
        consumer.close()
        conn.close()

if __name__ == "__main__":
    main()