from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta  # <-- Đã thêm timedelta ở đây
import sys

# Thêm path để Airflow tìm thấy script của bạn
sys.path.append('/opt/airflow/scripts')

# Import các hàm main từ script của bạn
try:
    from ingestion.data_generator import main as run_data_gen
    from ingestion.kafka_cdc_producer import cdc_polling_consumer as run_cdc
    from processing.stream_etl_kafka_to_mysql import main as run_stream
except ImportError as e:
    print(f"Lỗi import script: {e}")

default_args = {
    'owner': 'shibe',
    'start_date': datetime(2026, 4, 1),
    'retries': 5,
    'retry_delay': timedelta(minutes=1),
    'execution_timeout': None, # QUAN TRỌNG: Cho phép task chạy vô hạn thời gian
}
with DAG(
    '1_continuous_services_bear_mode',
    default_args=default_args,
    description='Chạy Data Gen, CDC và Stream ETL liên tục 24/7',
    schedule_interval=None, # Bạn nhấn nút Play (Trigger) thủ công 1 lần duy nhất
    catchup=False,
    max_active_runs=1 # Đảm bảo không có 2 bản copy chạy song song
) as dag:

    # Task 1: Tạo dữ liệu giả vào Cassandra (Chạy vô tận)
    task_gen = PythonOperator(
        task_id='service_data_generator',
        python_callable=run_data_gen
    )

    # Task 2: CDC từ Cassandra sang Kafka (Chạy vô tận)
    task_cdc = PythonOperator(
        task_id='service_kafka_cdc',
        python_callable=run_cdc
    )

    # Task 3: Spark Streaming từ Kafka sang MySQL (Chạy vô tận)
    task_stream = PythonOperator(
        task_id='service_spark_streaming',
        python_callable=run_stream
    )

    # Cấu trúc song song: 3 task này chạy độc lập và không bao giờ kết thúc
    [task_gen, task_cdc, task_stream]