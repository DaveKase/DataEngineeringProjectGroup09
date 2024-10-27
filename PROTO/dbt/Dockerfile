# Use an official Python base image to allow Python scripts if needed
FROM python:3.9-slim

# Set working directory
WORKDIR /usr/app

# Install necessary system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    git \  
    ca-certificates \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# Install dbt and the dbt-duckdb adapter
RUN pip install --no-cache-dir dbt-core dbt-duckdb

# Create the DuckDB directory and a placeholder file
#RUN mkdir -p /usr/app/mnt/tmp/duckdb_data && \
#    touch /usr/app/mnt/tmp/duckdb_data/weather.duckdb

# Create directories for the dbt project
# RUN mkdir -p /usr/app/models && \
#     mkdir -p /usr/app/seeds && \
#     mkdir -p /usr/app/tests && \
#     mkdir -p /usr/app/macros && \
#     mkdir -p /usr/app/snapshots && \
#     mkdir -p /usr/app/analyses

# Copy your dbt project into the container
#COPY ./mnt/tmp/dbt_project /usr/app/dbt_project

# Expose the port used for the dbt web interface (optional)
EXPOSE 8080

# Default command for dbt
CMD ["dbt", "debug"]
