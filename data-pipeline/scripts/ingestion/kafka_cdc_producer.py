from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import time
import pandas as pd
import json
from confluent_kafka import Producer

# Hàm callback giữ ở ngoài vì nó không khởi tạo kết nối
def delivery_report(err, msg):
    if err is not None:
        print(f"[X] Gửi tin thất bại: {err}")
    else:
        print(f"[V] Đã đẩy lên Kafka: Topic {msg.topic()}")

def cdc_polling_consumer():
    # --- 1. Cấu hình Kafka (Nhốt vào trong hàm) ---
    kafka_config = {
        'bootstrap.servers': 'broker:29092',
        'client.id': 'cdc-producer-shibe'
    }
    producer = Producer(kafka_config)

    # --- 2. Cấu hình Cassandra (Nhốt vào trong hàm) ---
    keyspace = 'keyspace_name' # Nhớ đổi đúng tên keyspace của bạn
    contact_points = ['cassandra']
    auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
    cluster = Cluster(contact_points, port=9042, auth_provider=auth_provider)
    session = cluster.connect(keyspace)

    last_cutoff = pd.Timestamp.now() - pd.Timedelta(minutes=1)
    print(f"🚀 CDC Polling started at {last_cutoff}")
    
    try:
        while True:
            current_ts_str = last_cutoff.strftime('%Y-%m-%d %H:%M:%S.%f')
            query = f"SELECT job_id, custom_track, ts FROM tracking WHERE ts > '{current_ts_str}' ALLOW FILTERING"
            rows = session.execute(query)
            new_records = list(rows)

            if new_records:
                for row in new_records:
                    payload = {
                        "job_id": row.job_id,
                        "custom_track": row.custom_track,
                        "ts": str(row.ts)
                    }
                    producer.produce(
                        topic='tracking_events', 
                        key=str(row.job_id), 
                        value=json.dumps(payload),
                        callback=delivery_report
                    )
                producer.flush()
                last_cutoff = pd.to_datetime(max(row.ts for row in new_records))
                print(f"[!] Đã đẩy {len(new_records)} bản ghi lên Kafka!")
            
            time.sleep(15)
    except KeyboardInterrupt:
        print("\n[!] Đang đóng kết nối...")
    finally:
        cluster.shutdown()

if __name__ == "__main__":
    cdc_polling_consumer()