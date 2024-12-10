# Use the official Airflow image with Python 3.11 and Airflow 2.10.1
FROM apache/airflow:2.10.1-python3.11

# Set working directory
WORKDIR /app

USER root

# Install necessary system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    ca-certificates \
    unzip \
    cmake \
    libjemalloc-dev \
    libboost-dev \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Install DuckDB CLI based on the architecture
RUN arch=$(dpkg --print-architecture) && \
    if [ "$arch" = "amd64" ]; then \
        curl -L https://github.com/duckdb/duckdb/releases/download/v1.1.1/duckdb_cli-linux-amd64.zip -o duckdb.zip; \
    elif [ "$arch" = "arm64" ]; then \
        curl -L https://github.com/duckdb/duckdb/releases/download/v1.1.1/duckdb_cli-linux-aarch64.zip -o duckdb.zip; \
    else \
        echo "Unsupported architecture: $arch"; exit 1; \
    fi && \
    unzip duckdb.zip && \
    mv duckdb /usr/local/bin/ && \
    chmod +x /usr/local/bin/duckdb && \
    rm duckdb.zip

RUN mkdir -p /opt/airflow/log/scheduler
RUN chmod -R 777 /opt/airflow/log/scheduler
RUN mkdir -p /opt/airflow/logs/scheduler
RUN chmod -R 777 /opt/airflow/logs/scheduler

RUN chown -R airflow:root /opt/airflow/logs
RUN chown -R airflow:root /opt/airflow/log

RUN chown -R airflow:root /opt/airflow/log/scheduler
RUN chown -R airflow:root /opt/airflow/logs/scheduler

RUN mkdir -p /opt/airflow/logs/scheduler/2024-12-10
RUN chmod -R 777 /opt/airflow/logs/scheduler/2024-12-10
RUN chown -R airflow:root /opt/airflow/logs/scheduler/2024-12-10

# Add airflow user to root group
RUN usermod -aG root airflow

USER airflow

# Upgrade pip to the latest version before installing PyIceberg
RUN pip install --upgrade pip

# Install Python dependencies for DuckDB, Pyarrow, dbt-duckdb, and PyIceberg with necessary extras
RUN pip install apache-airflow==2.10.1 pyarrow duckdb dbt-duckdb
RUN pip install "pyiceberg[s3fs,hive]"

# Expose Airflow webserver port
EXPOSE 8080

# Set the default command to start Airflow webserver
CMD ["airflow", "webserver"]
