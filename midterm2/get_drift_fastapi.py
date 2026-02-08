import json
import time
import random
import threading
from collections import deque
from datetime import datetime, UTC

import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
import clickhouse_connect
from confluent_kafka import Producer, Consumer
from fastapi import FastAPI, HTTPException
import uvicorn

# ================= CONFIG =================
KAFKA_CONF = {'bootstrap.servers': "localhost:9092,localhost:19092,localhost:29092"}
TOPIC_RAW = "telemetry.events"
TOPIC_ALERTS = "telemetry.drift.alerts"
TOPIC_DLQ = "telemetry.events.dlq"

PG_URL = "host=localhost dbname=postgres user=postgres password=123999"

CH_HOST = "127.0.0.1"
CH_PORT = 8123
CH_USER = "drift_user"
CH_PASS = "drift_pass123"

WINDOW_SIZE = 20
Z_THRESHOLD = 3.0
VIOLATION_THRESHOLD = 5

# ================= SHARED STATE =================
shared_baselines = {}
rolling_windows = {}
violation_counters = {}
active_drifts = {}

# ================= FASTAPI APP =================
app = FastAPI(title="Real-Time Data Drift API")


@app.get("/status")
def get_current_drift_status():
    """
    Exposes LIVE drift status from PostgreSQL.
    Shows which sources are drifting, since when, and affected metrics.
    """
    try:
        with psycopg2.connect(PG_URL) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                            SELECT source_id,
                                   metric,
                                   status,
                                   current_value,
                                   drift_start_time,
                                   last_updated
                            FROM drift_status
                            ORDER BY last_updated DESC
                            """)
                return cur.fetchall()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")


@app.get("/status/active")
def get_only_active_drifts():
    """Returns only sources that are currently in DRIFTING state."""
    try:
        with psycopg2.connect(PG_URL) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                            SELECT *
                            FROM drift_status
                            WHERE status = 'DRIFTING'
                            """)
                return cur.fetchall()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ================= CORE LOGIC (RECOVERY & DETECTION) =================

def recover_state(ch_client):
    """Fills rolling_windows from ClickHouse on startup (State Consistency)"""
    global rolling_windows
    print("🔄 Recovering state from ClickHouse...")
    try:
        query = f"""
            SELECT source_id, metric, value
            FROM (
                SELECT source_id, metric, value,
                       row_number() OVER (PARTITION BY source_id, metric ORDER BY event_time DESC) as rn
                FROM events_raw
            ) WHERE rn <= {WINDOW_SIZE}
        """
        result = ch_client.query(query)
        if result.result_rows:
            for row in result.result_rows:
                key = (row[0], row[1])
                if key not in rolling_windows:
                    rolling_windows[key] = deque(maxlen=WINDOW_SIZE)
                rolling_windows[key].appendleft(row[2])
            print(f"✅ State recovered for {len(rolling_windows)} metrics.")
    except Exception as e:
        print(f"⚠️ State recovery failed: {e}")


def run_detector():
    """Main Kafka Consumer and Drift Detection Logic"""
    consumer = Consumer({**KAFKA_CONF, "group.id": "drift_detector_v1", "auto.offset.reset": "latest"})
    internal_producer = Producer(KAFKA_CONF)
    consumer.subscribe([TOPIC_RAW])

    try:
        ch_client = clickhouse_connect.get_client(host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASS)
        recover_state(ch_client)

        print("🚀 Detector is processing Kafka stream...")
        while True:
            msg = consumer.poll(1.0)
            if msg is None or msg.error(): continue

            data = json.loads(msg.value().decode('utf-8'))
            key, val = (data["source_id"], data["metric"]), data["value"]

            baseline = shared_baselines.get(key)
            if not baseline: continue

            # Update Rolling Window
            window = rolling_windows.setdefault(key, deque(maxlen=WINDOW_SIZE))
            window.append(val)
            if len(window) < WINDOW_SIZE: continue

            # Statistical Check (Z-Score)
            rolling_mean = sum(window) / WINDOW_SIZE
            z = abs(rolling_mean - baseline["mean"]) / baseline["std"]

            # Sustained Violation Logic
            violation_counters[key] = violation_counters.get(key, 0) + 1 if z > Z_THRESHOLD else 0

            prev_status = "DRIFTING" if key in active_drifts else "NORMAL"
            new_status = "DRIFTING" if violation_counters[key] >= VIOLATION_THRESHOLD else "NORMAL"

            # Persist Status to Postgres (for Live API)
            with psycopg2.connect(PG_URL) as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                                INSERT INTO drift_status (source_id, metric, status, current_value, drift_start_time,
                                                          last_updated)
                                VALUES (%s, %s, %s, %s, CASE WHEN %s = 'DRIFTING' THEN NOW() ELSE NULL END,
                                        NOW()) ON CONFLICT (source_id, metric) DO
                                UPDATE SET
                                    status=EXCLUDED.status,
                                    current_value=EXCLUDED.current_value,
                                    drift_start_time= CASE
                                    WHEN drift_status.status='NORMAL' AND EXCLUDED.status='DRIFTING' THEN NOW()
                                    WHEN EXCLUDED.status='NORMAL' THEN NULL
                                    ELSE drift_status.drift_start_time
                                END,
                            last_updated=NOW()
                                """, (key[0], key[1], new_status, val, new_status))

            # Trigger Events on State Transition
            if prev_status == "NORMAL" and new_status == "DRIFTING":
                active_drifts[key] = datetime.now()
                print(f"🚨 DRIFT DETECTED: {key}")
            elif prev_status == "DRIFTING" and new_status == "NORMAL":
                active_drifts.pop(key, None)
                print(f"✅ METRIC NORMALIZED: {key}")

    finally:
        consumer.close()


# ================= UTILS (GENERATOR & SYNC) =================

def data_generator_loop():
    producer = Producer(KAFKA_CONF)
    while True:
        is_drift = (int(time.time()) // 40) % 2 == 0
        for s in ["dev_Alpha", "dev_Beta"]:
            val = random.uniform(75, 95) if is_drift else random.uniform(20, 30)
            event = {"source_id": s, "metric": "cpu_usage", "value": val, "event_time": datetime.now(UTC).isoformat()}
            producer.produce(TOPIC_RAW, json.dumps(event).encode('utf-8'))
        producer.flush()
        time.sleep(1)


def baseline_sync_loop():
    global shared_baselines
    while True:
        try:
            client = clickhouse_connect.get_client(host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASS)
            result = client.query(
                "SELECT source_id, metric, avg(value), stddevPop(value) FROM events_raw WHERE event_time < now() - INTERVAL 5 MINUTE AND value < 40 GROUP BY source_id, metric")
            if result.result_rows:
                shared_baselines = {(r[0], r[1]): {"mean": r[2], "std": r[3]} for r in result.result_rows if r[3] > 0}
                print(f"🔄 Baselines synced: {len(shared_baselines)}")
        except:
            pass
        time.sleep(120)


# ================= EXECUTION =================

if __name__ == "__main__":
    # 1. Start background tasks
    threading.Thread(target=data_generator_loop, daemon=True).start()
    threading.Thread(target=baseline_sync_loop, daemon=True).start()
    threading.Thread(target=run_detector, daemon=True).start()

    # 2. Start FastAPI Server (Main Process)
    print("🌐 Starting API Server on http://0.0.0.0:8000")
    uvicorn.run(app, host="0.0.0.0", port=8000)
