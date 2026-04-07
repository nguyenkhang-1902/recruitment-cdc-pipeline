import random
import time
import datetime
import pandas as pd
from sqlalchemy import create_engine
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import cassandra.util

# --- Global Service Configurations ---
DB_CONFIG = {
    "host": 'mysql',
    "port": '3306',
    "db_name": 'schema_name', 
    "user": 'root',
    "password": 'root'
}

CASSANDRA_CONFIG = {
    "keyspace": 'keyspace_name',
    "contact_points": ['cassandra'],
    "port": 9042,
    "user": 'cassandra',
    "password": 'cassandra'
}

# --- Data Acquisition Helpers ---
def get_job_data(engine):
    """Fetches job metadata from relational source for mapping."""
    query = "SELECT id AS job_id, campaign_id, group_id FROM job"
    return pd.read_sql(query, engine)

def get_publisher_data(engine):
    """Fetches publisher/company IDs for synthetic data generation."""
    query = "SELECT id AS publisher_id FROM company"
    return pd.read_sql(query, engine)

# --- Core Ingestion Logic ---
def generating_dummy_data(n_records, engine, session):
    """Generates and persists synthetic tracking events into Cassandra."""
    try:
        # Load dimension data for randomized sampling
        publishers = get_publisher_data(engine)['publisher_id'].to_list()
        jobs_df = get_job_data(engine)
        
        job_list = jobs_df['job_id'].to_list()
        campaign_list = jobs_df['campaign_id'].to_list()
        group_list = jobs_df[jobs_df['group_id'].notnull()]['group_id'].astype(int).to_list()
    except Exception as e:
        print(f"[ERROR] Source metadata fetch failed: {e}")
        return

    for _ in range(n_records):
        # Generate temporal and categorical attributes
        create_time = cassandra.util.uuid_from_time(datetime.datetime.now())
        bid = random.randint(0, 1)
        interact = ['click', 'conversion', 'qualified', 'unqualified']
        custom_track = random.choices(interact, weights=(70, 10, 10, 10))[0]
        
        job_id = random.choice(job_list)
        publisher_id = random.choice(publishers)
        group_id = random.choice(group_list)
        campaign_id = random.choice(campaign_list)
        ts = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Execute CQL persistence
        cql = """ 
            INSERT INTO tracking (create_time, bid, campaign_id, custom_track, group_id, job_id, publisher_id, ts) 
            VALUES ({}, {}, {}, '{}', {}, {}, {}, '{}')
        """.format(create_time, bid, campaign_id, custom_track, group_id, job_id, publisher_id, ts)
        
        try:
            session.execute(cql)
            print(f"[SUCCESS] Record Injected: Job {job_id} | Type: {custom_track} | TS: {ts}")
        except Exception as e:
            print(f"[ERROR] Cassandra persistence failed: {e}")

# --- Service Entry Point ---
def main():
    """Main execution loop for continuous data ingestion."""
    print("🚀 Initializing system connections...")
    
    # Initialize SQLAlchemy engine for MySQL
    db_url = f"mysql+mysqlconnector://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['db_name']}"
    engine = create_engine(db_url)

    # Initialize Cassandra Cluster and Session
    auth_provider = PlainTextAuthProvider(username=CASSANDRA_CONFIG['user'], password=CASSANDRA_CONFIG['password'])
    cluster = Cluster(CASSANDRA_CONFIG['contact_points'], port=CASSANDRA_CONFIG['port'], auth_provider=auth_provider)
    session = cluster.connect()
    session.set_keyspace(CASSANDRA_CONFIG['keyspace'])

    print("🚀 Continuous data generation started...")
    try:
        # Runtime loop for simulated real-time traffic
        while True:
            num_to_gen = random.randint(1, 20)
            generating_dummy_data(num_to_gen, engine, session)
            time.sleep(20)
    except KeyboardInterrupt:
        print("\n[INFO] Graceful shutdown initiated...")
    finally:
        cluster.shutdown()

if __name__ == "__main__":
    main()