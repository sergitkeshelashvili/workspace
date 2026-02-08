import json
import time
import random
import threading
from collections import deque

import psycopg2
from psycopg2.extras import execute_values
import clickhouse_connect
from confluent_kafka import Producer, Consumer
from datetime import datetime, UTC

# ================= CONFIG =================

KAFKA_CONF = {
    'bootstrap.servers': "localhost:9092,localhost:19092,localhost:29092"
}

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


# ================= STATE RECOVERY =================

def recover_state(ch_client):
    """ჩართვისას ავსებს rolling_windows-ს ClickHouse-იდან"""
    global rolling_windows
    print("🔄 Recovering state from ClickHouse...")

    try:
       
        query = f"""
            SELECT source_id, metric, value
            FROM (
                SELECT source_id, metric, value,
                       row_number() OVER (PARTITION BY source_id, metric ORDER BY event_time DESC) as rn
                FROM events_raw
            )
            WHERE rn <= {WINDOW_SIZE}
        """
        result = ch_client.query(query)

        if result.result_rows:
            count = 0
            for row in result.result_rows:
                key = (row[0], row[1])
                val = row[2]

                if key not in rolling_windows:
                    rolling_windows[key] = deque(maxlen=WINDOW_SIZE)

            
                rolling_windows[key].appendleft(val)
                count += 1
            print(f"✅ Recovered {count} data points for {len(rolling_windows)} metrics.")
        else:
            print("ℹ️ No historical data found for recovery.")

    except Exception as e:
        print(f"⚠️ State recovery failed: {e}. Starting with empty windows.")


# ================= 1. DATA GENERATOR =================

def data_generator_loop():
    producer = Producer(KAFKA_CONF)
    sources = ["dev_Alpha", "dev_Beta"]
    print("📡 Generator started")

    while True:
        is_drift = (int(time.time()) // 40) % 2 == 0
        for s in sources:
            val = random.uniform(75, 95) if is_drift else random.uniform(20, 30)
            event = {
                "source_id": s,
                "metric": "cpu_usage",
                "value": val,
                "event_time": datetime.now(UTC).isoformat()
            }
            producer.produce(TOPIC_RAW, json.dumps(event).encode('utf-8'))
        producer.flush()
        time.sleep(1)


# ================= 2. BASELINE SYNC =================

def baseline_sync_loop():
    global shared_baselines
    try:
        client = clickhouse_connect.get_client(host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASS)
        print("📊 Baseline sync connected to ClickHouse")
    except Exception as e:
        print(f"❌ Baseline sync connection failed: {e}")
        return

    while True:
        try:
            result = client.query("""
                                  SELECT source_id, metric, avg(value) AS mean_value, stddevPop(value) AS std_value
                                  FROM events_raw
                                  WHERE event_time < now() - INTERVAL 5 MINUTE AND value < 40
                                  GROUP BY source_id, metric
                                  """)
            if result.result_rows:
                valid = [(r[0], r[1], r[2], r[3], datetime.now()) for r in result.result_rows if r[3] > 0]
                if valid:
                    with psycopg2.connect(PG_URL) as conn:
                        with conn.cursor() as cur:
                            execute_values(cur, """
                                                INSERT INTO metric_baselines (source_id, metric, mean_value, stddev_value, last_calculated)
                                                VALUES %s ON CONFLICT (source_id, metric) DO
                                                UPDATE
                                                SET
                                                    mean_value = EXCLUDED.mean_value, stddev_value = EXCLUDED.stddev_value, last_calculated = EXCLUDED.last_calculated
                                                """, valid)
                    shared_baselines = {(r[0], r[1]): {"mean": r[2], "std": r[3]} for r in valid}
                    print(f"🔄 Baselines updated: {len(valid)}")
            time.sleep(120)
        except Exception as e:
            print("⚠️ Baseline sync error:", e)
            time.sleep(30)


# ================= 3. DRIFT DETECTOR =================

def run_detector():
    consumer = Consumer({**KAFKA_CONF, "group.id": "drift_detector_v1", "auto.offset.reset": "latest"})
    internal_producer = Producer(KAFKA_CONF)
    consumer.subscribe([TOPIC_RAW])
    print("🚀 Detector running")

    try:
        ch_client = clickhouse_connect.get_client(host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASS)

        # --- STATE RECOVERY ---
        recover_state(ch_client)
        # ----------------------

    except Exception as e:
        print(f"❌ Detector CH connection failed: {e}")
        return

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None: continue
            if msg.error():
                internal_producer.produce(TOPIC_DLQ, msg.value())
                continue

            try:
                data = json.loads(msg.value().decode('utf-8'))
                key = (data["source_id"], data["metric"])
                val = data["value"]

                baseline = shared_baselines.get(key)
                if not baseline: continue

                window = rolling_windows.setdefault(key, deque(maxlen=WINDOW_SIZE))
                window.append(val)

                if len(window) < WINDOW_SIZE: continue

                rolling_mean = sum(window) / WINDOW_SIZE
                z = abs(rolling_mean - baseline["mean"]) / baseline["std"]

                if z > Z_THRESHOLD:
                    violation_counters[key] = violation_counters.get(key, 0) + 1
                else:
                    violation_counters[key] = 0

                prev_status = "DRIFTING" if key in active_drifts else "NORMAL"
                new_status = "DRIFTING" if violation_counters[key] >= VIOLATION_THRESHOLD else "NORMAL"

                # Update PostgreSQL Status
                with psycopg2.connect(PG_URL) as conn:
                    with conn.cursor() as cur:
                        cur.execute("""
                                    INSERT INTO drift_status (source_id, metric, status, current_value,
                                                              drift_start_time, last_updated)
                                    VALUES (%s, %s, %s, %s, CASE WHEN %s = 'DRIFTING' THEN NOW() ELSE NULL END,
                                            NOW()) ON CONFLICT (source_id, metric) DO
                                    UPDATE SET
                                        status = EXCLUDED.status,
                                        current_value = EXCLUDED.current_value,
                                        drift_start_time = CASE
                                        WHEN drift_status.status='NORMAL' AND EXCLUDED.status='DRIFTING' THEN NOW()
                                        WHEN EXCLUDED.status='NORMAL' THEN NULL
                                        ELSE drift_status.drift_start_time
                                    END,
                                last_updated = NOW();
                                    """, (key[0], key[1], new_status, val, new_status))

                # Handle State Transitions
                if prev_status == "NORMAL" and new_status == "DRIFTING":
                    active_drifts[key] = datetime.now()
                    alert_data = {"event": "DRIFT_STARTED", "source": key[0], "metric": key[1], "z_score": round(z, 2),
                                  "timestamp": datetime.now(UTC).isoformat()}
                    internal_producer.produce(TOPIC_ALERTS, json.dumps(alert_data).encode('utf-8'))
                    ch_client.insert('drift_events',
                                     [[key[0], key[1], active_drifts[key], None, baseline["mean"], baseline["std"]]],
                                     column_names=['source_id', 'metric', 'drift_start', 'drift_end', 'baseline_mean',
                                                   'baseline_std'])
                    print(f"🚨 DRIFT STARTED: {key}")

                elif prev_status == "DRIFTING" and new_status == "NORMAL":
                    start_time = active_drifts.pop(key, None)
                    if start_time:
                        ch_client.command(
                            f"ALTER TABLE drift_events UPDATE drift_end = now() WHERE source_id = '{key[0]}' AND metric = '{key[1]}' AND drift_start = '{start_time.strftime('%Y-%m-%d %H:%M:%S')}'")
                    print(f"✅ DRIFT ENDED: {key}")

                internal_producer.flush()
            except Exception as e:
                print(f"❌ Processing error: {e}")
                internal_producer.produce(TOPIC_DLQ, msg.value())
                internal_producer.flush()
    finally:
        consumer.close()


# ================= MAIN =================

if __name__ == "__main__":
    threading.Thread(target=data_generator_loop, daemon=True).start()
    threading.Thread(target=baseline_sync_loop, daemon=True).start()
    run_detector()
