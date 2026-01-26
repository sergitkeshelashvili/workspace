from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import time

# ---- CONFIG ----

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_PATH = os.path.join(PROJECT_DIR, "remote_output.txt")

POKE_INTERVAL = 5
TIMEOUT = 60

# ---- TASK 1: SEND WORK ----
def send_work():
    """Simulates sending work to a remote system and writes output in project folder."""
    time.sleep(10)
    with open(OUTPUT_PATH, "w") as f:
        f.write("WORK_FINISHED")
    print(f"Work sent. Output written to: {OUTPUT_PATH}")


# ---- TASK 2: CUSTOM SENSOR ----
def wait_for_output():
    """Custom sensor logic: waits until OUTPUT_PATH exists and contains WORK_FINISHED"""
    start_time = time.time()
    while True:
        if os.path.exists(OUTPUT_PATH):
            with open(OUTPUT_PATH) as f:
                if f.read() == "WORK_FINISHED":
                    print("Output detected. Sensor succeeded.")
                    return
        if time.time() - start_time > TIMEOUT:
            raise TimeoutError("Sensor timed out waiting for output")
        print("Waiting for remote output...")
        time.sleep(POKE_INTERVAL)



default_args = {"owner": "S.T - DE", "retries": 0}

with DAG(
    dag_id="custom_sensor_project_folder",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["custom", "sensor"],
) as dag:

    send_work_task = PythonOperator(
        task_id="send_work_to_remote",
        python_callable=send_work,
    )

    sensor_task = PythonOperator(
        task_id="wait_for_remote_output",
        python_callable=wait_for_output,
    )

    send_work_task >> sensor_task
