from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from datetime import timedelta
import psycopg2
import logging

# --- PostgreSQL connection config
    "host": "postgres",
    "port": 5432,
    "user": "airflow",
    "password": "airflow",
    "dbname": "airflow"
}

# --- Configurable DAG parameters
BATCH_SIZE = 10000      
MAX_BATCHES = 10        
PIPELINE_NAME = "pg_copy"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="idempotent_incremental_dag",
    default_args=default_args,
    description="Incrementally copy rows from source_table to target_table",
    schedule_interval="@hourly",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=MAX_BATCHES,
)


def get_conn():
    return psycopg2.connect(**BASE_CONFIG)

# --- Task 1: Discover batches
@task
def discover_batches(batch_size=BATCH_SIZE, max_batches=MAX_BATCHES):
    conn = get_conn()
    cur = conn.cursor()

    # Lock watermark row
    cur.execute("""
        SELECT last_processed_id
        FROM etl_watermark
        WHERE pipeline_name = %s
        FOR UPDATE
    """, (PIPELINE_NAME,))
    row = cur.fetchone()
    last_id = row[0] if row else 0

    # Find max ID in source table
    cur.execute("SELECT max(id) FROM source_table WHERE id > %s", (last_id,))
    max_row = cur.fetchone()
    max_id = max_row[0] if max_row and max_row[0] else last_id

    batches = []
    current = last_id + 1
    while current <= max_id and len(batches) < max_batches:
        end = min(current + batch_size - 1, max_id)
        batches.append({"start_id": current, "end_id": end})
        current = end + 1

    conn.commit()
    cur.close()
    conn.close()

    logging.info(f"Discovered {len(batches)} batches: {batches}")
    return batches

# --- Task 2: Fetch & insert batch
@task
def fetch_and_insert(batch):
    start_id = batch["start_id"]
    end_id = batch["end_id"]
    logging.info(f"Processing batch: {start_id}-{end_id}")

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO target_table (id, col1, col2)
        SELECT id, col1, col2
        FROM source_table
        WHERE id BETWEEN %s AND %s
        ON CONFLICT (id) DO NOTHING
    """, (start_id, end_id))
    conn.commit()
    cur.close()
    conn.close()

# --- Task 3: Advance watermark
@task
def advance_watermark(batches):
    if not batches:
        logging.info("No batches to advance watermark.")
        return
    max_id = max(batch["end_id"] for batch in batches)
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO etl_watermark (pipeline_name, last_processed_id)
        VALUES (%s, %s)
        ON CONFLICT (pipeline_name)
        DO UPDATE SET last_processed_id = EXCLUDED.last_processed_id
    """, (PIPELINE_NAME, max_id))
    conn.commit()
    cur.close()
    conn.close()
    logging.info(f"Watermark advanced to {max_id}")

# --- DAG
with dag:
    # Discover new batches
    batches = discover_batches()

    # Fetch & insert in parallel
    fetch_tasks = fetch_and_insert.expand(batch=batches)

    # Advance watermark after all fetch tasks
    fetch_tasks >> advance_watermark(batches)
