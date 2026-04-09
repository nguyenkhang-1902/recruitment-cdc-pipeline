[![Six Sigma Compliant](https://img.shields.io/badge/Management-Six%20Sigma-yellowgreen)](https://en.wikipedia.org/wiki/Six_Sigma)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Role: Data Engineer](https://img.shields.io/badge/Role-Data%20Engineer-blue)](https://github.com/nguyenkhang-1902)

## 📌 Project Overview

### 📈 Business Case & Problem Statement
In modern recruitment, the "Time-to-Data" gap often leads to missed opportunities. Traditional ETL processes rely on daily batches, causing a **24-hour delay** in understanding candidate behavior. This latency results in:
* **Information Asymmetry:** Recruiters cannot see live application trends.
* **Data Silos:** Lack of synchronization between high-speed user interactions and the analytical warehouse.
* **Process Inefficiency:** Inability to detect bottlenecks in the recruitment funnel (e.g., high views but zero applications) until it's too late.

### 🎯 Six Sigma Goals
The project aims to achieve **"Lean Data Excellence"** by implementing a Real-time CDC Pipeline:
* **Reducing Data Latency:** From **24 hours** to **< 30 seconds**.
* **Data Accuracy:** Maintaining **99.9% consistency** between operational (Cassandra) and analytical (MySQL) sources.
* **Operational Control:** Automating 100% of the data flow to eliminate manual reporting errors.

### 🛠 The DMAIC Framework
* **Define:** Standardize candidate experience tracking and data integrity for every "Job Interaction" event.
* **Measure:** Implement **Change Data Capture (CDC)** to monitor Cassandra source changes. Measured by event throughput and system uptime.
* **Analyze:** Use **Spark Structured Streaming** to join real-time streams with historical job dimensions, identifying conversion bottlenecks in the funnel.
* **Improve:** Optimize performance via Kafka partitioning and Spark parallel processing, achieving a processing latency of **~10-20 seconds** per event.
* **Control:** Ensure long-term stability with **Batch Reconciliation (every 10 minutes)** to fix drift and **Checkpointing** for fault tolerance. Visual control via a real-time **Streamlit Dashboard**.

---
## 📁 Project Structure 

```plaintext
recruitment-cdc-pipeline/
├── datapipeline/
│   ├── airflow/                 
│   │   ├── dags/                
│   │   │   ├── dag_batch_processing_10min.py
│   │   │   └── dag_continuous_services.py
│   │   └── logs/                
│   ├── app/                     
│   │   ├── backend/             
│   │   │   ├── main.py
│   │   │   ├── database.py
│   │   │   └── Dockerfile
│   │   ├── frontend/            
│   │   │   ├── dashboard.py
│   │   │   └── Dockerfile
│   │   └── requirements.txt     
├── dataset/                     
│   ├── MySQL/                   
│   └── Cassandra/               
├── scripts/                     
│   ├── ingestion/               
│   │   ├── kafka_cdc_producer.py
│   │   └── data_generator.py    
│   ├── processing/              
│   │   ├── checkpoints/         
│   │   ├── stream_etl_kafka_to_mysql.py
│   │   └── batch_etl_cassandra_to_mysql.py
│   └── shared/                  
├── docker-compose.yml           
├── Dockerfile                   
├── .gitignore                   
└── README.md                    
```
``

---
### 🛠 Tech Stack

| Layer               | Technology                               | Purpose                                             |
|---------------------|------------------------------------------|-----------------------------------------------------|
| **Source Database** | **Apache Cassandra** | High-write throughput storage for user interactions |
| **Ingestion (CDC)** | **Python & Kafka Producer** | Change Data Capture to stream events from Cassandra |
| **Message Broker** | **Apache Kafka** | Real-time distributed event streaming platform       |
| **Stream Processing**| **Apache Spark Structured Streaming** | Real-time ETL, Joining, and Aggregation             |
| **Data Warehouse** | **MySQL** | Analytical storage for aggregated metrics           |
| **Orchestration** | **Apache Airflow** | Pipeline scheduling and service monitoring          |
| **API & Backend** | **FastAPI** | Serving real-time metrics and tracking endpoints    |
| **Visualization** | **Streamlit** | Real-time Dashboard & Candidate Portal              |
| **Infrastructure** | **Docker & Docker Compose** | Containerization and full-stack orchestration       |
---

### 🏗 System Architecture

The architecture follows the **Kappa Architecture** principle, focusing on real-time stream processing with a batch layer for reconciliation.

#### 1. Real-time Flow (CDC Pipeline)
* **User Interaction:** Candidate clicks/applies on the **Streamlit Portal**.
* **Event Capture:** **FastAPI** receives the request and writes to **Cassandra**.
* **CDC Logic:** A dedicated **Python Producer** polls Cassandra for new records and pushes them to **Kafka** topics.
* **Processing:** **Spark Structured Streaming** consumes events from Kafka, joins them with Job dimensions from **MySQL**, and performs real-time aggregations.
* **Sink:** Processed metrics are "upserted" back into **MySQL** Warehouse.

#### 2. Batch Reconciliation (Control Layer)
* An **Airflow DAG** triggers a **PySpark Batch** job every **10 minutes**.
* It performs a full sync between Cassandra and MySQL to ensure **99.9% data consistency**, correcting any drifts from the streaming layer.

#### 3. Data Product Layer
* The **Streamlit Dashboard** fetches live data from the **FastAPI** endpoints to visualize the recruitment funnel and job-specific metrics.
---

### 🐳 Step 1 — Start All Infrastructure
In the project root, launch all containers (Cassandra, Kafka, Spark, MySQL, Airflow, FastAPI, Streamlit):

Đây là nội dung mục **How to Run / Installation** được thiết kế chi tiết theo phong cách file README tham khảo của bạn, giúp người kế nhiệm có thể triển khai hệ thống một cách tuần tự và dễ dàng nhất.

---

### 🚀 3. How to Run / Installation (Hướng dẫn khởi chạy)

### 🐳 Step 1 — Start All Infrastructure
In the project root, launch all containers (Cassandra, Kafka, Spark, MySQL, Airflow, FastAPI, Streamlit):

```commandline
docker-compose up -d
```

Check running services:
```commandline
docker ps
```
*Wait about 60-90 seconds for Cassandra and Kafka to fully initialize before proceeding.*

---

### 🗄️ Step 2 — Initialize Database Schemas

#### 🔹 Cassandra Setup (Source)
Access the Cassandra container to create the tracking keyspace and table:
```commandline
docker exec -it cassandra cqlsh
```
Run the following commands:
```sql
CREATE KEYSPACE IF NOT EXISTS keyspace_name 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE keyspace_name.tracking (
    create_time uuid PRIMARY KEY,
    job_id int,
    custom_track text,
    ts timestamp,
    bid int
);
CREATE INDEX IF NOT EXISTS ON keyspace_name.tracking (job_id);
```

#### 🔹 MySQL Setup (Warehouse)
Access MySQL to create the analytical tables:
```commandline
docker exec -it mysql mysql -u root -p
```
*(Enter password when prompted)*
```sql
CREATE DATABASE IF NOT EXISTS recruitment_dw;
USE recruitment_dw;

CREATE TABLE job (
    id INT PRIMARY KEY,
    company_id INT,
    title VARCHAR(255)
);

CREATE TABLE events (
    job_id INT,
    clicks INT DEFAULT 0,
    conversion INT DEFAULT 0,
    qualified_application INT DEFAULT 0,
    sources VARCHAR(50),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (job_id, sources)
);

-- Seed initial Job data
INSERT INTO job (id, company_id, title) VALUES 
(101, 1, 'Data Engineer'), 
(102, 1, 'Backend Developer'), 
(103, 1, 'AI Researcher');
```

---

### ⛓️ Step 3 — Orchestrate Pipelines with Airflow
1. Access the Airflow Web UI at: `http://localhost:8080` (Login: `airflow`/`airflow`).
2. Locate the following DAGs and switch them to **ON**:
    * `1_continuous_services_pipeline`: Activates the **CDC Producer** and **Spark Streaming** services.
    * `2_batch_etl_every_10min`: Sets up the **Batch Reconciliation** layer.

---

### 🌐 Step 4 — Launch the Data Product
Access the integrated web application:
* **Analytics Dashboard & Candidate Portal:** `http://localhost:8501` (Streamlit)
* **Backend API Documentation (Swagger):** `http://localhost:8000/docs` (FastAPI)

---

### 🧪 Step 5 — Verify the Real-time Flow (Testing)
1.  Go to the **Candidate Portal** tab on the Streamlit UI.
2.  Click **"Apply Now"** on any job (e.g., Data Engineer - ID 101).
3.  Monitor the **recruitment-api** logs:
    ```commandline
    docker logs -f recruitment-api
    ```
    *You should see a `POST /api/v1/track ... 200 OK` response.*
4.  Switch back to the **Analytics Dashboard** tab.
5.  Wait **~30 seconds** for the Spark micro-batch to process. The Funnel Chart for Job 101 will update automatically.
```
