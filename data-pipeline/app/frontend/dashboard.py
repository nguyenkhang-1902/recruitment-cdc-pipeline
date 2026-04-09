import streamlit as st
import pandas as pd
import requests
import plotly.graph_objects as go
import time

st.set_page_config(page_title="Recruitment Data Product", layout="wide")
BACKEND_URL = "http://recruitment-api:8000/api/v1"

def fetch_data(endpoint):
    try:
        response = requests.get(f"{BACKEND_URL}/{endpoint}")
        return response.json() if response.status_code == 200 else None
    except: return None

st.title("💼 Recruitment CDC Data Product")
st.markdown("---")

tab_monitor, tab_portal = st.tabs(["📊 Job Analytics", "🌐 Candidate Portal"])

with tab_monitor:
    # Fetch job list for the dropdown filter
    job_list = fetch_data("metrics/job-list")
    
    if job_list:
        selected_job = st.selectbox("Select Job ID to Inspect", options=job_list)
        
        # Fetch detailed metrics for selected job
        job_stats = fetch_data(f"metrics/job/{selected_job}")
        
        if job_stats:
            m1, m2, m3 = st.columns(3)
            m1.metric("Job Views", job_stats['clicks'] or 0)
            m2.metric("Applications", job_stats['conversions'] or 0)
            m3.metric("Qualified", job_stats['qualified'] or 0)

            # Job Specific Funnel
            st.subheader(f"Conversion Funnel for Job {selected_job}")
            fig_funnel = go.Figure(go.Funnel(
                y = ["Views", "Applications", "Qualified"],
                x = [job_stats['clicks'] or 0, job_stats['conversions'] or 0, job_stats['qualified'] or 0],
                textinfo = "value+percent initial"
            ))
            st.plotly_chart(fig_funnel, use_container_width=True)
    else:
        st.warning("No data available in warehouse yet.")

with tab_portal:
    st.header("Live Candidate Portal")
    jobs = [
        {"id": 101, "title": "Data Engineer", "loc": "Ho Chi Minh"},
        {"id": 102, "title": "Backend Developer", "loc": "Hanoi"},
        {"id": 103, "title": "AI Researcher", "loc": "Da Nang"}
    ]

    for job in jobs:
        with st.container(border=True):
            c1, c2 = st.columns([3, 1])
            c1.subheader(job['title'])
            c1.write(f"ID: {job['id']} | {job['loc']}")
            
            if c2.button("Apply Now", key=f"app_{job['id']}"):
                requests.post(f"{BACKEND_URL}/track?job_id={job['id']}&action_type=conversion")
                st.toast("Application tracked!")
                time.sleep(0.5)
                st.rerun()

st.sidebar.markdown("### Controls")
if st.sidebar.button("Refresh Data"):
    st.rerun()