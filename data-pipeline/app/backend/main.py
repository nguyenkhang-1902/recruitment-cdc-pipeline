from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
from .database import get_db
from cassandra.cluster import Cluster
from cassandra.util import uuid_from_time
import datetime

app = FastAPI(title="Recruitment Analytics API")

# Setup Cassandra for real-time user tracking
# This connects to the same cluster as your CDC producer
try:
    cluster = Cluster(['cassandra'], port=9042)
    session = cluster.connect('keyspace_name')
except Exception as e:
    print(f"Failed to connect to Cassandra: {e}")

@app.get("/health")
def health_check():
    return {"status": "operational"}

@app.post("/api/v1/track")
async def track_action(job_id: int, action_type: str):
    """
    Triggers the CDC pipeline by inserting a new record into Cassandra.
    action_type can be: 'click', 'conversion', 'qualified', 'unqualified'
    """
    create_time = uuid_from_time(datetime.datetime.now())
    ts = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    bid = 1 if action_type == 'click' else 0 # Simple logic for spend_hour
    
    query = """
        INSERT INTO tracking (create_time, job_id, custom_track, ts, bid)
        VALUES (%s, %s, %s, %s, %s)
    """
    try:
        session.execute(query, (create_time, job_id, action_type, ts, bid))
        return {"status": "success", "event": action_type}
    except Exception as e:
        # Properly raising the exception we just discussed
        raise HTTPException(status_code=500, detail=f"Database insertion failed: {str(e)}")

@app.get("/api/v1/metrics/summary")
def get_summary(db: Session = Depends(get_db)):
    query = text("""
        SELECT sources, SUM(clicks) as total_clicks, SUM(conversion) as total_conversions
        FROM events 
        WHERE updated_at >= NOW() - INTERVAL 24 HOUR
        GROUP BY sources
    """)
    try:
        return db.execute(query).mappings().all()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))