import streamlit as st
import pandas as pd
import requests
import plotly.express as px
import plotly.graph_objects as go
import time

st.set_page_config(page_title="Recruitment Data Product", layout="wide")

# Internal Docker Networking URLs
BACKEND_URL = "http://recruitment-api:8000/api/v1"

def fetch_data(endpoint):
    try:
        response = requests.get(f"{BACKEND_URL}/{endpoint}")
        return response.json() if response.status_code == 200 else None
    except: return None

# UI Header
st.title("💼 Recruitment CDC Data Product")
st.markdown("---")

# Navigation Tabs
tab_monitor, tab_portal = st.tabs(["📊 Analytics Dashboard", "🌐 Candidate Portal (Live Interaction)"])

# --- TAB 1: ANALYTICS DASHBOARD ---
with tab_monitor:
    st.header("Pipeline Real-time Metrics")
    
    summary_data = fetch_data("metrics/summary")
    if summary_data:
        df = pd.DataFrame(summary_data)
        
        # Summary Metrics
        m1, m2, m3 = st.columns(3)
        m1.metric("Total Clicks", f"{df['total_clicks'].sum():,}")
        m2.metric("Total Applications", f"{df['total_conversions'].sum():,}")
        m3.metric("System Status", "Live", delta="CDC Active")

        # Funnel Chart: Click -> Apply -> Qualified
        st.subheader("Recruitment Funnel (Conversion Rate)")
        # Calculate totals for funnel
        clicks = df['total_clicks'].sum()
        apps = df['total_conversions'].sum()
        # Mocking qualified for demo if not in summary yet, otherwise use df['total_qualified'].sum()
        qualified = apps * 0.4 
        
        fig_funnel = go.Figure(go.Funnel(
            y = ["Job Views", "Applications", "Qualified"],
            x = [clicks, apps, qualified],
            textinfo = "value+percent initial"
        ))
        st.plotly_chart(fig_funnel, use_container_width=True)

# --- TAB 2: CANDIDATE PORTAL ---
with tab_portal:
    st.header("Candidate Job Board")
    st.info("Interacting with buttons here will trigger the real-time CDC pipeline.")

    # Simulated Job List
    jobs = [
        {"id": 101, "title": "Data Engineer", "loc": "Ho Chi Minh"},
        {"id": 102, "title": "Backend Developer", "loc": "Hanoi"},
        {"id": 103, "title": "AI Researcher", "loc": "Da Nang"}
    ]

    for job in jobs:
        with st.container(border=True):
            c1, c2 = st.columns([3, 1])
            c1.subheader(job['title'])
            c1.write(f"Location: {job['loc']} | Job ID: {job['id']}")
            
            # Interaction Buttons
            if c2.button("Apply Now", key=f"app_{job['id']}"):
                requests.post(f"{BACKEND_URL}/track?job_id={job['id']}&action_type=conversion")
                st.toast(f"Application sent for {job['title']}!")
                time.sleep(0.5)
                st.rerun()

    st.warning("Note: After interacting, switch to the Analytics tab to see the live update.")

# Refresh settings
st.sidebar.markdown("### System Config")
if st.sidebar.button("Force Refresh Dashboard"):
    st.rerun()