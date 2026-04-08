from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys

# Append scripts path to sys.path for Airflow visibility
sys.path.append('/opt/airflow/scripts')

# Import core execution functions from the pipeline scripts
try:
    from ingestion.data_generator import main as run_data_gen
    from ingestion.kafka_cdc_producer import cdc_polling_consumer as run_cdc
    from processing.stream_etl_kafka_to_mysql import main as run_stream
except ImportError as e:
    print(f"Script import failed: {e}")

default_args = {
    'owner': 'shibe',
    'start_date': datetime(2026, 4, 1),
    'retries': 5,
    'retry_delay': timedelta(minutes=1),
    'execution_timeout': None,  # Allow tasks to run indefinitely for continuous processing
}

with DAG(
    '1_continuous_services_pipeline',
    default_args=default_args,
    description='Orchestration for continuous Data Generation, CDC, and Stream ETL services',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    max_active_runs=1  # Prevent overlapping execution runs
) as dag:

    # Task 1: Persistent Synthetic Data Generation into Cassandra
    task_gen = PythonOperator(
        task_id='service_data_generator',
        python_callable=run_data_gen
    )

    # Task 2: Change Data Capture (CDC) from Cassandra to Kafka
    task_cdc = PythonOperator(
        task_id='service_kafka_cdc',
        python_callable=run_cdc
    )

    # Task 3: Spark Structured Streaming from Kafka to MySQL Warehouse
    task_stream = PythonOperator(
        task_id='service_spark_streaming',
        python_callable=run_stream
    )

    # Parallel Execution: Services run independently and indefinitely
    [task_gen, task_cdc, task_stream]