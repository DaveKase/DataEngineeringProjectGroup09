# Use an official Python base image to allow Python scripts if needed
FROM python:3.9-slim

# Switch to root to set up directories and permissions
USER root

# Set working directory
WORKDIR /app

# Install necessary system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    ca-certificates \
    unzip \
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

# Install Python dependencies for DuckDB in case you need to use them from Python
RUN pip install --no-cache-dir duckdb pyarrow pyiceberg[s3fs,duckdb,hive,sql-sqlite,pyarrow]

# Set up permissions for the DuckDB data directory
RUN mkdir -p /app/mnt/tmp/duckdb_data && chmod -R 777 /app/mnt/tmp/duckdb_data 

# Set the default command to open the DuckDB CLI
CMD ["duckdb"]
