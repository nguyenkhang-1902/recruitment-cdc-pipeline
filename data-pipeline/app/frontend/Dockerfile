# Use slim version for optimized image size
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

WORKDIR /app

# Install necessary system libraries
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements from the parent directory context
COPY ../requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy frontend source code
COPY frontend/ .

# Streamlit default port
EXPOSE 8501

# Command to run Streamlit pointing to the dashboard file
CMD ["streamlit", "run", "dashboard.py", "--server.port=8501", "--server.address=0.0.0.0"]