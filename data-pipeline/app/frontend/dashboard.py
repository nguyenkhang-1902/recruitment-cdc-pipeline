import streamlit as st
import pandas as pd
import requests
import plotly.express as px
import time

# Page configuration for a professional look
st.set_page_config(page_title="Recruitment CDC Analytics", layout="wide")

# API Endpoint - Using container name for internal Docker communication
BACKEND_URL = "http://recruitment-api:8000/api/v1/metrics"

def fetch_data(endpoint):
    """Fetch data from the Backend API."""
    try:
        response = requests.get(f"{BACKEND_URL}/{endpoint}")
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        st.error(f"Connection Error: {e}")
    return None

st.title("📊 Recruitment CDC Pipeline Monitor")
st.markdown("Real-time synchronization monitoring between Cassandra (Batch) and Kafka (Streaming)")

# Layout for top-level metrics
col1, col2, col3 = st.columns(3)

summary_data = fetch_data("summary")
if summary_data:
    df_summary = pd.DataFrame(summary_data)
    
    # Calculate global totals
    total_clicks = df_summary['total_clicks'].sum()
    total_convs = df_summary['total_conversions'].sum()
    
    col1.metric("Total Clicks", f"{total_clicks:,}")
    col2.metric("Total Conversions", f"{total_convs:,}")
    col3.metric("System Health", "Operational")

    # Data Consistency Comparison Chart
    st.subheader("Data Consistency: Kafka vs Cassandra")
    fig = px.bar(
        df_summary, 
        x="sources", 
        y="total_clicks", 
        color="sources",
        title="Comparison of Ingested Clicks by Source",
        labels={"total_clicks": "Number of Clicks", "sources": "Data Source"}
    )
    st.plotly_chart(fig, use_container_width=True)

# Top Performing Jobs Table
st.subheader("Top Performing Jobs")
jobs_data = fetch_data("top-jobs?limit=10")
if jobs_data:
    df_jobs = pd.DataFrame(jobs_data)
    st.table(df_jobs)

# Auto-refresh mechanism (simulating real-time updates)
time.sleep(10)
st.rerun()