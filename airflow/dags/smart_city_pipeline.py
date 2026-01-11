from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2

# ================= DEFAULT ARGS =================

default_args = {
    "owner": "data-engineer",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 6),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}

# ================= DAG DEFINITION =================

dag = DAG(
    dag_id="smartcity_pipeline",
    default_args=default_args,
    description="End-to-End Smart City Traffic Big Data Pipeline",
    schedule_interval="*/10 * * * *",   # every 10 minutes
    catchup=False,
)

# ================= TASK 1 : KAFKA → HDFS =================

kafka_to_hdfs = BashOperator(
    task_id="kafka_to_hdfs",
    bash_command="""
    docker exec spark-master python3 /opt/spark-jobs/kafka_to_hdfs.py
    """,
    dag=dag,
)

# ================= TASK 2 : RAW → PROCESSED =================

raw_to_processed = BashOperator(
    task_id="raw_to_processed",
    bash_command="""
    docker exec spark-master spark-submit \
    --master spark://spark-master:7077 \
    /opt/spark-jobs/raw_to_processed.py
    """,
    dag=dag,
)

# ================= TASK 3 : PROCESSED → ANALYTICS =================

processed_to_analytics = BashOperator(
    task_id="processed_to_analytics",
    bash_command="""
    docker exec spark-master spark-submit \
    --master spark://spark-master:7077 \
    /opt/spark-jobs/processed_to_analytics.py
    """,
    dag=dag,
)

# ================= TASK 4 : ANALYTICS → POSTGRES =================

analytics_to_postgres = BashOperator(
    task_id="analytics_to_postgres",
    bash_command="""
    docker exec spark-master spark-submit \
    --master spark://spark-master:7077 \
    --jars /opt/spark-jobs/jars/postgresql-42.7.1.jar \
    /opt/spark-jobs/analytics_to_postgres.py
    """,
    dag=dag,
)

# ================= TASK 5 : VALIDATION =================

def check_postgres_data():
    conn = psycopg2.connect(
        host="postgres",
        database="traffic",
        user="traffic_user",
        password="traffic_password",
    )
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM mobility_summary")
    count = cursor.fetchone()[0]

    if count > 0:
        print(f"✅ Validation OK: {count} records found in mobility_summary")
    else:
        raise Exception("❌ Validation failed: No data in mobility_summary")

    conn.close()

verify_data = PythonOperator(
    task_id="verify_postgres_data",
    python_callable=check_postgres_data,
    dag=dag,
)

# ================= PIPELINE ORDER =================

kafka_to_hdfs >> raw_to_processed >> processed_to_analytics >> analytics_to_postgres >> verify_data
