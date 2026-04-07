# Sử dụng bản 2.7.1 để nhẹ máy hơn hoặc giữ 2.10.5 tùy bạn, 
# nhưng mình khuyên dùng 2.7.1 cho ổn định với RAM hiện tại.
FROM apache/airflow:2.7.1

USER root
# Cài đặt Java 17 cho PySpark
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Thiết lập biến môi trường Java
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow
# Cài đặt thư viện và ép xung đột openlineage phải biến mất
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir \
    "apache-airflow-providers-openlineage>=1.8.0" \
    "apache-airflow-providers-apache-spark" \
    "confluent-kafka" \
    "cassandra-driver" \
    "pandas" \
    "sqlalchemy" \
    "mysql-connector-python" \
    "time-uuid