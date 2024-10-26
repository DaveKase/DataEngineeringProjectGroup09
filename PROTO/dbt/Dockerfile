# Use an official Python base image to allow Python scripts if needed
FROM python:3.9-slim

# Set working directory
WORKDIR /usr/app

# Install necessary system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    ca-certificates \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# Install dbt and the dbt-duckdb adapter
RUN pip install --no-cache-dir dbt-core dbt-duckdb

# Copy your dbt project into the container (you should have a dbt_project.yml)
COPY . .

# Expose the port used for the dbt web interface (optional, depending on usage)
EXPOSE 8080

# Default command for dbt (you can change this to other dbt commands, e.g., `dbt run`)
CMD ["dbt", "debug"]
