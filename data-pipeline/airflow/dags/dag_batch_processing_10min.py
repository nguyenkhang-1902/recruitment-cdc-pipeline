from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys

sys.path.append('/opt/airflow/scripts')
from processing.batch_etl_cassandra_to_mysql import main as run_batch_logic

default_args = {
    'owner': 'shibe',
    'start_date': datetime(2026, 4, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    '2_batch_etl_every_10min',
    default_args=default_args,
    description='Chạy Batch ETL mỗi 10 phút để tổng hợp dữ liệu',
    schedule_interval='*/10 * * * *', # Cron: Mỗi 10 phút quét 1 lần
    catchup=False,
    max_active_runs=1 
) as dag:

    task_batch = PythonOperator(
        task_id='run_batch_processing',
        python_callable=run_batch_logic
    )

    task_batch