from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import time
import pandas as pd
import json
from confluent_kafka import Producer

# --- Kafka Delivery Feedback ---
def delivery_report(err, msg):
    """Callback for asynchronous message delivery reports."""
    if err is not None:
        print(f"[ERROR] Message delivery failed: {err}")
    else:
        print(f"[INFO] Message delivered to topic: {msg.topic()} [{msg.partition()}]")

# --- Core CDC Service Logic ---
def cdc_polling_consumer():
    """Polls new records from Cassandra and streams them to Kafka Broker."""
    
    # 1. Kafka Producer Configuration (Internal Scope)
    kafka_config = {
        'bootstrap.servers': 'broker:29092',
        'client.id': 'cdc-producer-shibe',
        'retries': 5
    }
    producer = Producer(kafka_config)

    # 2. Cassandra Connection Configuration (Internal Scope)
    keyspace = 'keyspace_name'
    contact_points = ['cassandra']
    auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
    cluster = Cluster(contact_points, port=9042, auth_provider=auth_provider)
    session = cluster.connect(keyspace)

    # 3. Offset Tracking (Incremental Load Management)
    # Start polling from 1 minute before current runtime to avoid data loss
    last_cutoff = pd.Timestamp.now() - pd.Timedelta(minutes=1)
    print(f"🚀 CDC Polling Service started. Baseline Timestamp: {last_cutoff}")
    
    try:
        while True:
            # Polling Logic: Filter records newer than the last tracked timestamp
            current_ts_str = last_cutoff.strftime('%Y-%m-%d %H:%M:%S.%f')
            query = f"SELECT job_id, custom_track, ts FROM tracking WHERE ts > '{current_ts_str}' ALLOW FILTERING"
            rows = session.execute(query)
            new_records = list(rows)

            if new_records:
                for row in new_records:
                    # Construct JSON Payload
                    payload = {
                        "job_id": row.job_id,
                        "custom_track": row.custom_track,
                        "ts": str(row.ts)
                    }
                    
                    # Asynchronous Publish to Kafka
                    producer.produce(
                        topic='tracking_events', 
                        key=str(row.job_id), 
                        value=json.dumps(payload),
                        callback=delivery_report
                    )
                
                # Block until all pending messages are delivered
                producer.flush()
                
                # Update High Watermark (Last cutoff) based on latest record timestamp
                last_cutoff = pd.to_datetime(max(row.ts for row in new_records))
                print(f"[!] Successfully streamed {len(new_records)} records to Kafka Cluster.")
            
            # Polling interval to prevent resource starvation
            time.sleep(15)
            
    except KeyboardInterrupt:
        print("\n[INFO] Polling Service interruption detected. Terminating...")
    except Exception as e:
        print(f"[CRITICAL] Unexpected runtime error: {e}")
    finally:
        # Graceful shutdown of database cluster
        cluster.shutdown()

if __name__ == "__main__":
    cdc_polling_consumer()