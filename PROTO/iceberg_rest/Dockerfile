# Use the base image
FROM tabulario/iceberg-rest:1.6.0

# Switch to root to set up directories and permissions
USER root

RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Set up necessary directories and permissions
RUN mkdir -p /app/mnt/tmp/warehouse
#RUN chmod -R 777 /app/mnt/tmp/warehouse

RUN mkdir -p /iceberg/warehouse
#RUN chmod -R 777 /iceberg/warehouse

RUN mkdir -p /iceberg/warehouse/consumption
RUN mkdir -p /iceberg/warehouse/consumption/data
RUN mkdir -p /iceberg/warehouse/consumption/metadata

RUN mkdir -p /iceberg/warehouse/price
RUN mkdir -p /iceberg/warehouse/price/data
RUN mkdir -p /iceberg/warehouse/price/metadata

RUN mkdir -p /iceberg/warehouse/production
RUN mkdir -p /iceberg/warehouse/production/data
RUN mkdir -p /iceberg/warehouse/production/metadata

RUN mkdir -p /iceberg/warehouse/weather
RUN mkdir -p /iceberg/warehouse/weather/data
RUN mkdir -p /iceberg/warehouse/weather/metadata

#RUN chmod -R 777 /iceberg/warehouse/consumption
#RUN chmod -R 777 /iceberg/warehouse/consumption/data
#RUN chmod -R 777 /iceberg/warehouse/consumption/metadata

#RUN chmod -R 777 /iceberg/warehouse/price
#RUN chmod -R 777 /iceberg/warehouse/price/data
#RUN chmod -R 777 /iceberg/warehouse/price/metadata

#RUN chmod -R 777 /iceberg/warehouse/production
#RUN chmod -R 777 /iceberg/warehouse/production/data
#RUN chmod -R 777 /iceberg/warehouse/production/metadata

#RUN chmod -R 777 /iceberg/warehouse/weather
#RUN chmod -R 777 /iceberg/warehouse/weather/data
#RUN chmod -R 777 /iceberg/warehouse/weather/metadata


# Switch back to the default iceberg user
USER iceberg

# Set entrypoint to start the Iceberg REST service
ENTRYPOINT ["java", "-jar", "iceberg-rest-image-all.jar"]