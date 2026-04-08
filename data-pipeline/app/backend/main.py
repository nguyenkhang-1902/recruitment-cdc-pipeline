from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
from database import get_db

app = FastAPI(title="Recruitment CDC Pipeline API")

@app.get("/health")
def health_check():
    """Verify API and connection status."""
    return {"status": "operational"}

@app.get("/api/v1/metrics/summary")
def get_pipeline_summary(db: Session = Depends(get_db)):
    """
    Fetch aggregated metrics comparing Kafka Streaming and Cassandra Batch sources.
    This helps in monitoring data consistency across the CDC pipeline.
    """
    query = text("""
        SELECT 
            sources, 
            SUM(clicks) as total_clicks, 
            SUM(conversion) as total_conversions,
            SUM(spend_hour) as total_spend
        FROM events 
        GROUP BY sources
    """)
    try:
        result = db.execute(query).mappings().all()
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/metrics/top-jobs")
def get_top_performing_jobs(limit: int = 10, db: Session = Depends(get_db)):
    """
    Retrieve top jobs based on engagement metrics (clicks and conversions).
    Used for the 'Top Jobs Table' in the Analytics Dashboard.
    """
    query = text("""
        SELECT 
            job_id, 
            SUM(clicks) as clicks, 
            SUM(conversion) as conversions,
            sources
        FROM events 
        GROUP BY job_id, sources
        ORDER BY clicks DESC
        LIMIT :limit
    """)
    try:
        result = db.execute(query, {"limit": limit}).mappings().all()
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))