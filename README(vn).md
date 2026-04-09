# Recruitment Real-time CDC Pipeline (Data Product)

[![Six Sigma Compliant](https://img.shields.io/badge/Management-Six%20Sigma-yellowgreen)](https://en.wikipedia.org/wiki/Six_Sigma)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Role: Data Engineer](https://img.shields.io/badge/Role-Data%20Engineer-blue)](https://github.com/nguyenkhang-1902)

## 📌 Project Overview 

### 📈 Business Case & Bài toán thực tế
Trong tuyển dụng hiện đại, khoảng cách về "Thời gian có dữ liệu" thường dẫn đến việc bỏ lỡ các ứng viên tiềm năng. Các quy trình ETL truyền thống phụ thuộc vào chạy Batch hàng ngày, gây ra **độ trễ 24 giờ** trong việc hiểu hành vi ứng viên. Sự chậm trễ này dẫn đến:
* **Bất đối xứng thông tin:** Nhà tuyển dụng không thể thấy xu hướng ứng tuyển trực tiếp.
* **Cô lập dữ liệu:** Thiếu sự đồng bộ giữa tương tác tốc độ cao của người dùng và kho dữ liệu phân tích.
* **Quy trình kém hiệu quả:** Không thể phát hiện các điểm nghẽn trong phễu tuyển dụng (ví dụ: lượt xem cao nhưng không có người ứng tuyển) cho đến khi quá muộn.

### 🎯 Mục tiêu Six Sigma (Six Sigma Goals)
Dự án hướng tới đạt được **"Lean Data Excellence"** (Sự ưu việt trong dữ liệu tinh gọn) thông qua hệ thống CDC Real-time:
* **Giảm độ trễ dữ liệu:** Từ **24 giờ** xuống còn **dưới 30 giây**.
* **Độ chính xác dữ liệu:** Duy trì tính nhất quán giữa nguồn vận hành (Cassandra) và kho phân tích (MySQL).
* **Kiểm soát vận hành:** Tự động hóa 100% luồng dữ liệu để loại bỏ lỗi báo cáo thủ công.

### 🛠 Khung quy trình DMAIC
* **Define (Xác định):** Chuẩn hóa việc theo dõi trải nghiệm ứng viên và tính toàn vẹn dữ liệu cho mỗi sự kiện tương tác với Job.
* **Measure (Đo lường):** Triển khai **Change Data Capture (CDC)** để giám sát thay đổi từ nguồn Cassandra. Đo lường thông qua lưu lượng sự kiện và thời gian hoạt động của hệ thống.
* **Analyze (Phân tích):** Sử dụng **Spark Structured Streaming** để kết hợp dòng dữ liệu thời gian thực với các chiều dữ liệu lịch sử, nhằm phát hiện điểm nghẽn chuyển đổi trong phễu.
* **Improve (Cải tiến):** Tối ưu hóa hiệu suất qua cấu hình Kafka Partition và xử lý song song trong Spark, đạt độ trễ xử lý chỉ khoảng **10-20 giây** cho mỗi sự kiện.
* **Control (Kiểm soát):** Đảm bảo ổn định lâu dài bằng cơ chế **Đối soát định kỳ (mỗi 10 phút)** để sửa lỗi lệch dữ liệu và dùng **Checkpointing** để phục hồi sau sự cố. Kiểm soát trực quan qua **Dashboard Streamlit**.

---

## 📁 Project Structure (Control)

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

### 🏗 Kiến trúc hệ thống

Kiến trúc này tuân thủ nguyên lý **Kappa Architecture**, tập trung vào xử lý luồng thời gian thực kết hợp với một lớp Batch để đối soát dữ liệu.

#### 1. Luồng Real-time (CDC Pipeline)
* **Tương tác người dùng:** Ứng viên thực hiện Click/Apply trên **Streamlit Portal**.
* **Ghi nhận sự kiện:** **FastAPI** nhận yêu cầu và ghi dữ liệu vào **Cassandra**.
* **Cơ chế CDC:** **Python Producer** giám sát Cassandra và đẩy các thay đổi vào các Topic trên **Kafka**.
* **Xử lý dữ liệu:** **Spark Structured Streaming** tiêu thụ dữ liệu từ Kafka, thực hiện Join với thông tin Job từ **MySQL** và tổng hợp dữ liệu thời gian thực.
* **Lưu trữ:** Kết quả được cập nhật (Upsert) vào Kho dữ liệu **MySQL**.

#### 2. Đối soát Batch (Lớp Kiểm soát)
* **Airflow DAG** kích hoạt tiến trình **PySpark Batch** định kỳ mỗi **10 phút**.
* Thực hiện đồng bộ hóa toàn diện giữa Cassandra và MySQL để đảm bảo **độ chính xác 99.9%**, sửa chữa các sai lệch (nếu có) từ luồng Streaming.

#### 3. Lớp Sản phẩm Dữ liệu
* **Streamlit Dashboard** truy xuất dữ liệu trực tiếp từ **FastAPI** để hiển thị dashboard tuyển dụng và các chỉ số chi tiết theo từng Job ID.
---
