import random
import time
import datetime
import pandas as pd
from sqlalchemy import create_engine
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import cassandra.util

# --- 1. Cấu hình Toàn cục (Sửa Host cho Docker) ---
DB_CONFIG = {
    "host": 'mysql',      # Đổi từ localhost
    "port": '3306',
    "db_name": 'schema_name', 
    "user": 'root',
    "password": 'root'
}

CASSANDRA_CONFIG = {
    "keyspace": 'keyspace_name',
    "contact_points": ['cassandra'], # Đổi từ 127.0.0.1
    "port": 9042,
    "user": 'cassandra',
    "password": 'cassandra'
}

# --- 2. Các hàm bổ trợ (Truyền engine/session vào) ---
def get_job_data(engine):
    query = "SELECT id AS job_id, campaign_id, group_id FROM job"
    return pd.read_sql(query, engine)

def get_publisher_data(engine):
    query = "SELECT id AS publisher_id FROM company"
    return pd.read_sql(query, engine)

def generating_dummy_data(n_records, engine, session):
    try:
        publishers = get_publisher_data(engine)['publisher_id'].to_list()
        jobs_df = get_job_data(engine)
        
        job_list = jobs_df['job_id'].to_list()
        campaign_list = jobs_df['campaign_id'].to_list()
        group_list = jobs_df[jobs_df['group_id'].notnull()]['group_id'].astype(int).to_list()
    except Exception as e:
        print(f"[X] Lỗi khi lấy dữ liệu từ MySQL: {e}")
        return

    for _ in range(n_records):
        create_time = cassandra.util.uuid_from_time(datetime.datetime.now())
        bid = random.randint(0, 1)
        interact = ['click', 'conversion', 'qualified', 'unqualified']
        custom_track = random.choices(interact, weights=(70, 10, 10, 10))[0]
        
        job_id = random.choice(job_list)
        publisher_id = random.choice(publishers)
        group_id = random.choice(group_list)
        campaign_id = random.choice(campaign_list)
        ts = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        cql = """ 
            INSERT INTO tracking (create_time, bid, campaign_id, custom_track, group_id, job_id, publisher_id, ts) 
            VALUES ({}, {}, {}, '{}', {}, {}, {}, '{}')
        """.format(create_time, bid, campaign_id, custom_track, group_id, job_id, publisher_id, ts)
        
        try:
            session.execute(cql)
            print(f"[+] Inserted: Job {job_id} | Track: {custom_track} | TS: {ts}")
        except Exception as e:
            print(f"[X] Lỗi Insert Cassandra: {e}")

# --- 3. Hàm thực thi chính (Để Airflow gọi) ---
def main():
    print("🚀 Khởi tạo kết nối hệ thống...")
    
    # Kết nối MySQL
    db_url = f"mysql+mysqlconnector://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['db_name']}"
    engine = create_engine(db_url)

    # Kết nối Cassandra
    auth_provider = PlainTextAuthProvider(username=CASSANDRA_CONFIG['user'], password=CASSANDRA_CONFIG['password'])
    cluster = Cluster(CASSANDRA_CONFIG['contact_points'], port=CASSANDRA_CONFIG['port'], auth_provider=auth_provider)
    session = cluster.connect()
    session.set_keyspace(CASSANDRA_CONFIG['keyspace'])

    print("🚀 Bắt đầu tạo dữ liệu giả...")
    try:
        # Nếu dùng Airflow PythonOperator, ta có thể giới hạn số lần chạy hoặc chạy liên tục
        # Ở đây mình giữ nguyên vòng lặp True để khớp với logic cũ của bạn
        while True:
            num_to_gen = random.randint(1, 20)
            generating_dummy_data(num_to_gen, engine, session)
            time.sleep(20)
    except KeyboardInterrupt:
        print("\n[!] Dừng script. Đang đóng kết nối...")
    finally:
        cluster.shutdown()

if __name__ == "__main__":
    main()