# Recruitment Real-time CDC Pipeline 

[![Six Sigma Compliant](https://img.shields.io/badge/Management-Six%20Sigma-yellowgreen)](https://en.wikipedia.org/wiki/Six_Sigma)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Role: Data Engineer](https://img.shields.io/badge/Role-Data%20Engineer-blue)](https://github.com/nguyenkhang-1902)
[![Apache Kafka](https://img.shields.io/badge/Streaming-Apache%20Kafka-black?style=flat&logo=apachekafka)](https://kafka.apache.org/)
[![Apache Spark](https://img.shields.io/badge/Processing-Apache%20Spark-E25A1C?style=flat&logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Apache Airflow](https://img.shields.io/badge/Orchestration-Apache%20Airflow-017CEE?style=flat&logo=apacheairflow&logoColor=white)](https://airflow.apache.org/)
[![Docker](https://img.shields.io/badge/Infrastructure-Docker-2496ED?style=flat&logo=docker&logoColor=white)](https://www.docker.com/)
[![FastAPI](https://img.shields.io/badge/API-FastAPI-009688?style=flat&logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com
## 📌 Project Overview 

### 📈 Business Case & Bài toán thực tế
Trong tuyển dụng hiện đại, khoảng cách về "Thời gian có dữ liệu" thường dẫn đến việc bỏ lỡ các ứng viên tiềm năng. Các quy trình ETL truyền thống phụ thuộc vào chạy Batch hàng ngày, gây ra **độ trễ 24 giờ** trong việc hiểu hành vi ứng viên. Sự chậm trễ này dẫn đến:
* **Bất đối xứng thông tin:** Nhà tuyển dụng không thể thấy xu hướng ứng tuyển trực tiếp.
* **Cô lập dữ liệu:** Thiếu sự đồng bộ giữa tương tác tốc độ cao của người dùng và kho dữ liệu phân tích.
* **Quy trình kém hiệu quả:** Không thể phát hiện các điểm nghẽn trong phễu tuyển dụng (ví dụ: lượt xem cao nhưng không có người ứng tuyển) cho đến khi quá muộn.

### 🎯 Mục tiêu Six Sigma 
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

Kiến trúc này tuân thủ nguyên lý **Kappa with Reconciliation**, tập trung vào xử lý luồng thời gian thực kết hợp với một lớp Batch để đối soát dữ liệu.

#### 1. Luồng Real-time
* **Tương tác người dùng:** Ứng viên thực hiện Click/Apply trên **Streamlit Portal**.
* **Ghi nhận sự kiện:** **FastAPI** nhận yêu cầu và ghi dữ liệu vào **Cassandra**.
* **Cơ chế CDC:** **Python Producer** giám sát Cassandra và đẩy các thay đổi vào các Topic trên **Kafka**.
* **Xử lý dữ liệu:** **Spark Structured Streaming** tiêu thụ dữ liệu từ Kafka, thực hiện Join với thông tin Job từ **MySQL** và tổng hợp dữ liệu thời gian thực.
* **Lưu trữ:** Kết quả được cập nhật (Upsert) vào Kho dữ liệu **MySQL**.

#### 2. Đối soát Batch
* **Airflow DAG** kích hoạt tiến trình **PySpark Batch** định kỳ mỗi **10 phút**.
* Thực hiện đồng bộ hóa toàn diện giữa Cassandra và MySQL để đảm bảo **độ chính xác 99.9%**, sửa chữa các sai lệch (nếu có) từ luồng Streaming.

#### 3. Lớp Sản phẩm Dữ liệu
* **Streamlit Dashboard** truy xuất dữ liệu trực tiếp từ **FastAPI** để hiển thị dashboard tuyển dụng và các chỉ số chi tiết theo từng Job ID.
---

### 📋 Điều kiện cần

Trước khi khởi chạy, hãy đảm bảo hệ thống của bạn đáp ứng các yêu cầu sau để vận hành ổn định (đặc biệt là Spark và Cassandra tiêu tốn khá nhiều tài nguyên):

* **Phần mềm:** * Docker & Docker Compose đã được cài đặt.
    * Git (để clone project).
* **Cấu hình phần cứng tối thiểu:**
    * **RAM:** 8GB (Khuyên dùng **16GB** để tránh treo container).
    * **CPU:** Tối thiểu 4 Cores.
    * **Dung lượng đĩa trống:** ~5GB.
* **Docker Desktop:** Cấp quyền (Resource Allocation) cho Docker sử dụng ít nhất 8GB RAM trong phần Settings.

---
### 🚀 Hướng dẫn khởi chạy

Dự án hỗ trợ 2 phương thức vận hành tùy theo mục tiêu thử nghiệm của người dùng:

---

#### 🔹 Bước 1: Khởi động hạ tầng Docker
Trước khi bắt đầu bất kỳ hướng nào, cần dựng toàn bộ hệ thống bao gồm: Cassandra, MySQL, Kafka, Spark, Airflow và FastAPI.

Tại thư mục gốc của dự án, chạy lệnh:
```bash
docker-compose up -d --build
```
Kiểm tra trạng thái các container:
```bash
docker ps
```
> **Lưu ý:** Đảm bảo tất cả các dịch vụ (`recruitment-api`, `recruitment-dashboard`, `cassandra`, `mysql`, `broker`, `airflow-scheduler`) đều ở trạng thái **Up**.

![Docker Infrastructure Status](images/docker_infrastructure_status.png)

---

#### 🔹 Bước 2: Khởi tạo dữ liệu gốc
Dự án đi kèm với bộ dữ liệu mẫu để định nghĩa các chiều dữ liệu (Dimensions) trong MySQL, phục vụ cho việc thực hiện các thao tác Join trong Spark.

1. **Truy cập MySQL container:**
   ```bash
   docker exec -it mysql mysql -u root -p123456
   ```
2. **Khởi tạo Schema và Data:**
   Nạp các file SQL có sẵn để tạo bảng và dữ liệu cho các bảng: `application`, `campaign`, `company`, `conversation`, `dev_user`, `events`, `group`, `job`, `job_location`.

![Database Schema Overview](images/database_schema_overview.png)

---

#### 🔹 Bước 3: Lựa chọn hướng vận hành

### 🛣️ Hướng 1: Giả lập dữ liệu tự động 
Phương án này sử dụng script `data_generator.py` để tự động tạo ra hàng nghìn bản ghi ngẫu nhiên, giúp kiểm tra độ chịu tải và luồng chạy của toàn bộ Pipeline mà không cần thao tác thủ công.

1. **Kích hoạt DAG trên Airflow:**
   - Truy cập Airflow UI tại: `http://localhost:8080` (Mặc định: admin/admin).
   - Tìm và bật (Unpause) DAG: `1_continuous_services_pipeline`.
2. **Cơ chế hoạt động:**
   - Task `service_data_generator` sẽ liên tục ghi dữ liệu giả lập vào bảng `tracking` của Cassandra.
   - Luồng CDC và Spark Streaming sẽ tự động bắt sự kiện và đẩy về MySQL Warehouse.
3. **Kiểm tra Dashboard:**
   - Mở Dashboard Streamlit (`localhost:8501`).
   - Tại Tab **Analytics Dashboard**, các con số sẽ tăng lên liên tục sau mỗi chu kỳ xử lý của Spark (mỗi 30 giây).

!(images/dashboard_auto_update.png)
---
#### 📊 Giám sát luồng dữ liệu 
Để theo dõi chính xác lượng dữ liệu đang chảy qua từng lớp hạ tầng, hãy sử dụng các lệnh sau trong Terminal:

* **Kiểm tra dữ liệu nguồn (Cassandra):**
    ```bash
    docker exec -it cassandra cqlsh -e "SELECT COUNT(*) FROM keyspace_name.tracking;"
    ```
    > **Kết quả mong đợi:** Trả về tổng số bản ghi gốc đã được sinh ra. Con số này là "mốc chuẩn" để đối soát với các lớp sau.

* **Kiểm tra dữ liệu trung gian (Kafka):**
    ```bash
    docker exec -it broker kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:29092 --topic tracking_events
    ```
    > **Kết quả mong đợi:** Số lượng tin nhắn (Offset) trong Kafka phải khớp hoặc xấp xỉ với Cassandra. Nếu con số này đứng yên dù Cassandra tăng, hãy kiểm tra CDC Producer.

* **Kiểm tra kho dữ liệu đích (MySQL):**
    ```bash
    docker exec -it mysql mysql -u root -proot -e "SELECT SUM(clicks) + SUM(conversion) FROM recruitment_dw.events;"
    ```
    > **Kết quả mong đợi:** Tổng số lượng bản ghi trong MySQL phải tiệm cận với Kafka (có độ trễ ~30s do chu kỳ Spark). Nếu con số này tăng lên ổn định, hệ thống đã đồng bộ hoàn toàn.
---

### 🛣️ Hướng 2: Tương tác thực tế
Sau khi đã thông luồng ở Hướng 1, có thể chuyển sang hướng này để trải nghiệm dự án như một sản phẩm thực tế, nơi dữ liệu chỉ sinh ra từ hành vi người dùng.

1. **Dừng Task giả lập:**
   - Trên Airflow UI, tạm dừng (Pause) task `service_data_generator` để làm sạch dòng dữ liệu thử nghiệm.
2. **Sử dụng Candidate Portal:**
   - Truy cập Dashboard Streamlit tại: `http://localhost:8501`.
   - Chuyển sang Tab **"Candidate Portal"**.
   - Thử nhấn nút **"Apply Now"** hoặc **"View Job"** trên các thẻ công việc (Job ID: 101, 102, 103).
3. **Quan sát luồng CDC Real-time:**
   - Mỗi cú Click sẽ gửi yêu cầu đến **FastAPI** (`recruitment-api`).
   - FastAPI ghi vào Cassandra -> Kích hoạt CDC Producer -> Đẩy vào Kafka -> Spark Streaming xử lý.
4. **Kết quả Dashboard:**
   - Quay lại Tab **"Analytics Dashboard"**, biểu đồ Phễu (Funnel) sẽ cập nhật chính xác những hành động bạn vừa thực hiện trên Portal với độ trễ < 30 giây.
5. **Đối soát định kỳ (Batch Layer):**
   - Bật DAG `2_batch_etl_every_10min` để hệ thống tự động quét và đồng bộ dữ liệu giữa Cassandra và MySQL mỗi 10 phút, đảm bảo tính chính xác tuyệt đối (99.9% consistency).

![Candidate Portal](images/candidate_portal_interface.png)

-----

### 📊 Kiểm tra dữ liệu 

#### 📡 API Documentation

Hệ thống sử dụng **FastAPI** để tự động tạo tài liệu API. Đây là giao diện giúp kiểm tra nhanh các điểm cuối (endpoints) và trạng thái kết nối của hệ thống mà không cần truy vấn database thủ công.

  * **Truy cập:** `http://localhost:8000/docs`

**Tổng quan các Endpoints:**
Giao diện Swagger hiển thị đầy đủ các luồng từ kiểm tra sức khỏe hệ thống đến việc truy xuất số liệu phân tích.
![API Overview](images/api_documentation_overview.png)

**Kiểm tra trạng thái kết nối (Health Check):**
Sử dụng endpoint `/health` để xác nhận Backend đã kết nối thành công với Cassandra cluster.

![Health Check Detail](images/api_health_check_detail.png)
-----

#### 🗄️ Truy vấn trực tiếp

Ngoài API, bạn có thể đối soát dữ liệu bằng cách truy vấn trực tiếp tại hai đầu hệ thống:

**Kiểm tra tại Cassandra:**

```sql
-- Xác nhận dữ liệu tương tác người dùng vừa tạo từ Portal đã được ghi nhận
SELECT * FROM keyspace_name.tracking WHERE job_id = 101 ALLOW FILTERING;
```
**Kiểm tra tại MySQL:**

```sql
-- Xác nhận kết quả tổng hợp cuối cùng sau khi qua Spark Streaming đã về kho
SELECT * FROM recruitment_dw.events WHERE sources = 'Kafka_Streaming' ORDER BY updated_at DESC;
```
---
### 📊 Tổng kết Dashboard

Phần cuối cùng của Pipeline là một **Sản phẩm Dữ liệu (Data Product)** hoàn chỉnh, nơi các quyết định tuyển dụng được đưa ra dựa trên dữ liệu thực tế thay vì cảm tính.
![Job 101 Analytics](images/job_analytics_dashboard_101.png)
![Job 102 Analytics](images/job_analytics_dashboard_102.png)

#### ✨ Các tính năng nổi bật đã đạt được:
* **Real-time Insights:** Các chỉ số Views, Applications và Qualified được cập nhật với độ trễ cực thấp (< 30s).
* **Drill-down Capability:** Cho phép lọc và đi sâu vào chi tiết hiệu suất của từng Job ID cụ thể.
* **Conversion Funnel:** Trực quan hóa tỷ lệ chuyển đổi qua từng giai đoạn, giúp nhà tuyển dụng nhận diện ngay lập tức các điểm nghẽn trong quy trình.
* **Stability:** Hệ thống vận hành ổn định trên nền tảng Docker, đảm bảo tính sẵn sàng cao và khả năng phục hồi dữ liệu nhờ cơ chế Checkpointing của Spark.

---

## ✍️ Author 

**Nguyen Khang** 
*Data Engineer | Big Data Enthusiast*

If you have any questions about this project, potential collaborations, or just want to talk about Data Engineering, feel free to reach out!

* **GitHub:** [@nguyenkhang-1902](https://github.com/nguyenkhang-1902)
* **LinkedIn:** [Khang Nguyễn](https://www.linkedin.com/in/khang-nguy%E1%BB%85n-5228652a0/)
* **Email:** nguyenkhang1150@gmail.com
---

