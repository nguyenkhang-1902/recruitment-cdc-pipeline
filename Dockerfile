# Use Apache Airflow as the base image (Debian-based)
FROM apache/airflow:2.7.1

# Switch to root for system-level dependencies installation
USER root

# Install OpenJDK 17 required for PySpark execution
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set Java environment variables for JVM processes
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Switch back to airflow user for Python package management
USER airflow

# Install Python dependencies and resolve provider compatibility issues
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir \
    "apache-airflow-providers-openlineage>=1.8.0" \
    "apache-airflow-providers-apache-spark" \
    "confluent-kafka" \
    "cassandra-driver" \
    "pandas" \
    "sqlalchemy" \
    "mysql-connector-python" \
    "time-uuid"