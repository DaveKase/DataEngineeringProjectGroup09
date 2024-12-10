Before you run this, make sure you have created an .env file locally. Look at the .env-sample file for guidence.

Also it seems like this is needed:
mkdir -p /iceberg/warehouse/consumption
mkdir -p /iceberg/warehouse/consumption/data
mkdir -p /iceberg/warehouse/consumption/metadata

mkdir -p /iceberg/warehouse/price
mkdir -p /iceberg/warehouse/price/data
mkdir -p /iceberg/warehouse/price/metadata

mkdir -p /iceberg/warehouse/production
mkdir -p /iceberg/warehouse/production/data
mkdir -p /iceberg/warehouse/production/metadata

mkdir -p /iceberg/warehouse/weather
mkdir -p /iceberg/warehouse/weather/data
mkdir -p /iceberg/warehouse/weather/metadata

chmod -R 777 /iceberg/warehouse/consumption
chmod -R 777 /iceberg/warehouse/consumption/data
chmod -R 777 /iceberg/warehouse/consumption/metadata

chmod -R 777 /iceberg/warehouse/price
chmod -R 777 /iceberg/warehouse/price/data
chmod -R 777 /iceberg/warehouse/price/metadata

chmod -R 777 /iceberg/warehouse/production
chmod -R 777 /iceberg/warehouse/production/data
RUN chmod -R 777 /iceberg/warehouse/production/metadata

chmod -R 777 /iceberg/warehouse/weather
chmod -R 777 /iceberg/warehouse/weather/data
chmod -R 777 /iceberg/warehouse/weather/metadata
