# Use the base image
FROM tabulario/iceberg-rest:1.6.0

# Switch to root to set up directories and permissions
USER root

RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Set up necessary directories and permissions
RUN mkdir -p /app/mnt/tmp/warehouse && chmod -R 777 /app/mnt/tmp/warehouse
RUN mkdir -p /iceberg/warehouse/weather && mkdir -p /iceberg/warehouse/price && mkdir -p /iceberg/warehouse/production && mkdir -p /iceberg/warehouse/consumption
RUN chmod -R 777 /iceberg/warehouse

# Switch back to the default iceberg user
USER iceberg

# Set entrypoint to start the Iceberg REST service
ENTRYPOINT ["java", "-jar", "iceberg-rest-image-all.jar"]