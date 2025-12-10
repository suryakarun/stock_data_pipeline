FROM apache/airflow:2.7.1-python3.10

USER root

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Copy requirements
COPY requirements.txt /requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r /requirements.txt

# Copy scripts
COPY --chown=airflow:root scripts /opt/airflow/scripts
RUN chmod +x /opt/airflow/scripts/*
