from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
from database import get_db 
from cassandra.cluster import Cluster
from cassandra.util import uuid_from_time
import datetime
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Recruitment Analytics API")

# Global variables for Cassandra connection
cassandra_cluster = None
cassandra_session = None

def get_cassandra_session():
    """Lazy initialization of Cassandra session with retry logic."""
    global cassandra_cluster, cassandra_session
    if cassandra_session:
        return cassandra_session
    
    try:
        # Use 'cassandra' as the hostname defined in docker-compose
        cassandra_cluster = Cluster(['cassandra'], port=9042, connect_timeout=10)
        cassandra_session = cassandra_cluster.connect('keyspace_name') # Ensure keyspace exists
        logger.info("Successfully connected to Cassandra.")
        return cassandra_session
    except Exception as e:
        logger.error(f"Waiting for Cassandra... {e}")
        return None

@app.get("/health")
def health_check():
    session = get_cassandra_session()
    return {
        "api_status": "operational",
        "cassandra_status": "connected" if session else "disconnected"
    }

@app.get("/api/v1/metrics/job-list")
def get_job_list(db: Session = Depends(get_db)):
    """Fetch all unique job IDs present in the warehouse."""
    try:
        query = text("SELECT DISTINCT job_id FROM events")
        rows = db.execute(query).all()
        return [row[0] for row in rows]
    except Exception as e:
        logger.error(f"MySQL Error: {e}")
        return []

@app.get("/api/v1/metrics/job/{job_id}")
def get_job_specific_metrics(job_id: int, db: Session = Depends(get_db)):
    """Fetch aggregated metrics for a specific job ID."""
    query = text("""
        SELECT 
            COALESCE(SUM(clicks), 0) as clicks, 
            COALESCE(SUM(conversion), 0) as conversions,
            COALESCE(SUM(qualified_application), 0) as qualified
        FROM events 
        WHERE job_id = :job_id
    """)
    try:
        result = db.execute(query, {"job_id": job_id}).mappings().first()
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/v1/track")
async def track_action(job_id: int, action_type: str):
    """Logs user interaction to Cassandra."""
    session = get_cassandra_session()
    if session is None:
        raise HTTPException(
            status_code=503, 
            detail="Cassandra is still warming up or unreachable. Please try again in a moment."
        )

    create_time = uuid_from_time(datetime.datetime.now())
    ts = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    # Logic: conversion means application, click means job view
    bid = 1 if action_type == 'click' else 0
    
    query = """
        INSERT INTO tracking (create_time, job_id, custom_track, ts, bid) 
        VALUES (%s, %s, %s, %s, %s)
    """
    try:
        session.execute(query, (create_time, job_id, action_type, ts, bid))
        return {"status": "success", "job_id": job_id, "action": action_type}
    except Exception as e:
        logger.error(f"Cassandra Insert Error: {e}")
        raise HTTPException(status_code=500, detail=f"Database insertion failed: {str(e)}")