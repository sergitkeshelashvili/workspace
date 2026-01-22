from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2
import logging

# --- Connection Parameters ---
BASE_CONFIG = {
    "host": "127.0.0.1",
    "port": 5432,
    "user": "postgres",
    "password": "123999",
    "dbname": "postgres"
}
TARGET_DB = "football_db"


def get_target_config():
    conf = BASE_CONFIG.copy()
    conf['dbname'] = TARGET_DB
    return conf


# --- ETL Functions ---

def extract(**context):
    """1) Extract: Source -> Bronze"""
    logging.info(f"Extracting data for Run ID: {context['run_id']}")
    logging.info(f"Logical Date: {context['ds']}")

    # Ensure Target DB exists
    conn = psycopg2.connect(**BASE_CONFIG)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{TARGET_DB}'")
    if not cur.fetchone():
        cur.execute(f"CREATE DATABASE {TARGET_DB}")
    cur.close()
    conn.close()

    # Extract from Source
    src_conn = psycopg2.connect(**BASE_CONFIG)
    src_cur = src_conn.cursor()
    src_cur.execute("SELECT event_id, match_id, team, player, event_type, event_ts FROM public.event_logs;")
    rows = src_cur.fetchall()
    src_cur.close()
    src_conn.close()

    # Load to Bronze
    tgt_conn = psycopg2.connect(**get_target_config())
    tgt_conn.autocommit = True
    cur = tgt_conn.cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS football;")
    cur.execute("""
                CREATE TABLE IF NOT EXISTS football.bronze_football_events
                (
                    event_id
                    VARCHAR
                (
                    36
                ), match_id VARCHAR
                (
                    50
                ), team VARCHAR
                (
                    100
                ),
                    player VARCHAR
                (
                    100
                ), event_type VARCHAR
                (
                    50
                ), timestamp TIMESTAMP
                    );
                """)
    cur.execute("TRUNCATE TABLE football.bronze_football_events;")
    if rows:
        cur.executemany("INSERT INTO football.bronze_football_events VALUES (%s,%s,%s,%s,%s,%s)", rows)

    count = len(rows)
    cur.close()
    tgt_conn.close()
    return count


def transform(**context):
    """2) Transform: Bronze -> Silver"""
    logging.info(f"Transforming data for task: {context['ti'].task_id}")

    conn = psycopg2.connect(**get_target_config())
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("""
                CREATE TABLE IF NOT EXISTS football.silver_football_events
                (
                    match_id
                    VARCHAR
                (
                    50
                ), team VARCHAR
                (
                    100
                ),
                    event_type VARCHAR
                (
                    50
                ), timestamp TIMESTAMP
                    );
                """)
    cur.execute("TRUNCATE TABLE football.silver_football_events;")

    cur.execute("""
                INSERT INTO football.silver_football_events (match_id, team, event_type, timestamp)
                SELECT DISTINCT match_id, team, event_type, COALESCE(timestamp, NOW())
                FROM football.bronze_football_events
                WHERE event_type IS NOT NULL;
                """)

    cur.execute("SELECT COUNT(*) FROM football.silver_football_events;")
    count = cur.fetchone()[0]
    cur.close()
    conn.close()
    return count


def load(**context):
    """3) Load: Silver -> Gold"""
    conn = psycopg2.connect(**get_target_config())
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS football.gold_football_stats;")
    cur.execute("""
                CREATE TABLE football.gold_football_stats AS
                SELECT team,
                       COUNT(*) FILTER (WHERE event_type ILIKE '%goal%') as total_goals, COUNT(*) FILTER (WHERE event_type ILIKE '%foul%') as total_fouls, COUNT(*) FILTER (WHERE event_type ILIKE '%shot%') as total_shots
                FROM football.silver_football_events
                GROUP BY team;
                """)

    cur.execute("SELECT COUNT(*) FROM football.gold_football_stats;")
    count = cur.fetchone()[0]
    cur.close()
    conn.close()
    return count


def report(**context):
    """4) Report processed rows using XCom"""
    ti = context['ti']
    b_count = ti.xcom_pull(task_ids='extract')
    s_count = ti.xcom_pull(task_ids='transform')
    g_count = ti.xcom_pull(task_ids='load')

    msg = f"""
    --- ETL REPORT ---
    Rows Processed (Bronze): {b_count}
    Rows Processed (Silver): {s_count}
    Teams in Gold Table:    {g_count}
    ------------------
    """
    logging.info(msg)
    print(msg)


# --- DAG Definition ---

default_args = {
    "owner": "S.T - DE",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10)
}

with DAG(
        dag_id="football_medallion_etl_automation",
        description="Medallion ETL Pipeline using psycopg2 and PythonOperator",
        start_date=datetime(2026, 1, 1),
        schedule="* * * * *", #every minute
        catchup=False,
        default_args=default_args,
        tags=["football_data_streaming", "etl", "analytics"],
        max_active_runs=1,
) as dag:
    t1 = PythonOperator(
        task_id="extract",
        python_callable=extract,
        provide_context=True
    )

    t2 = PythonOperator(
        task_id="transform",
        python_callable=transform,
        provide_context=True
    )

    t3 = PythonOperator(
        task_id="load",
        python_callable=load,
        provide_context=True
    )

    t4 = PythonOperator(
        task_id="report",
        python_callable=report,
        provide_context=True
    )

    t1 >> t2 >> t3 >> t4
