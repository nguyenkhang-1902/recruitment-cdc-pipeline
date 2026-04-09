from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
from database import get_db 
from cassandra.cluster import Cluster
from cassandra.util import uuid_from_time
from cassandra.policies import RoundRobinPolicy # Thêm chính sách này
import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Recruitment Analytics API")

cassandra_cluster = None
cassandra_session = None

def get_cassandra_session():
    global cassandra_cluster, cassandra_session
    if cassandra_session:
        return cassandra_session
    
    try:
        # Cấu hình cụ thể để tránh lỗi Protocol và load_balancing_policy
        cassandra_cluster = Cluster(
            ['cassandra'], 
            port=9042, 
            connect_timeout=10,
            protocol_version=4, # Cố định cho Cassandra 3.11
            load_balancing_policy=RoundRobinPolicy()
        )
        cassandra_session = cassandra_cluster.connect('keyspace_name')
        logger.info("Successfully connected to Cassandra.")
        return cassandra_session
    except Exception as e:
        logger.error(f"Waiting for Cassandra... {e}")
        return None

# Các endpoint giữ nguyên logic nhưng sẽ hoạt động ổn định hơn nhờ session được sửa
@app.get("/health")
def health_check():
    session = get_cassandra_session()
    return {"api_status": "operational", "cassandra_status": "connected" if session else "disconnected"}

@app.post("/api/v1/track")
async def track_action(job_id: int, action_type: str):
    session = get_cassandra_session()
    if session is None:
        raise HTTPException(status_code=503, detail="Cassandra warming up...")

    create_time = uuid_from_time(datetime.datetime.now())
    ts = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    bid = 1 if action_type == 'click' else 0
    
    query = "INSERT INTO tracking (create_time, job_id, custom_track, ts, bid) VALUES (%s, %s, %s, %s, %s)"
    try:
        session.execute(query, (create_time, job_id, action_type, ts, bid))
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Các hàm get_job_list và get_job_specific_metrics giữ nguyên