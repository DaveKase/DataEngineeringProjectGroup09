from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import os
import json
import pyarrow as pa
import pyarrow.json as pj
import pandas as pd
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema, NestedField
from pyiceberg.types import StringType, LongType, FloatType, TimestampType, DoubleType

# Load the weather API key from the environment variables
API_KEY = os.getenv('_WEATHER_API_KEY')

# Define country coordinates
COUNTRY_COORDINATES = {
    "Estonia": "59.35,24.80",
    "Latvia": "56.95,24.11"
}

# Base URL for the API
BASE_URL = 'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/weatherdata/history'

# Function to fetch weather data and save as JSON
def fetch_weather_data(**context):
    """
    Fetch weather data for the past day and save as JSON for each country.
    """
    end_date = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')

    for country, coordinates in COUNTRY_COORDINATES.items():
        output_dir = f'/mnt/tmp/warehouse/weather/{country.lower()}/data'
        os.makedirs(output_dir, exist_ok=True)

        params = {
            'unitGroup': 'metric',
            'contentType': 'json',
            'aggregateHours': 1,
            'dayStartTime': '0:00:00',
            'dayEndTime': '23:59:59',
            'startDateTime': f'{end_date}T00:00:00',
            'endDateTime': f'{end_date}T23:59:59',
            'locations': coordinates,
            'collectStationContribution': 'true',
            'key': API_KEY
        }

        response = requests.get(BASE_URL, params=params)

        if response.status_code == 200:
            output_file = os.path.join(output_dir, f'weather_{end_date}.json')
            with open(output_file, 'w') as f:
                json.dump(response.json(), f)
            print(f"Weather data saved for {country}: {output_file}")
        else:
            raise Exception(f"Failed to fetch weather data for {country}. Status code: {response.status_code}")

# Function to map PyArrow schema to Iceberg schema
def arrow_to_iceberg_schema(pa_schema):
    iceberg_fields = []
    for i, field in enumerate(pa_schema):
        if pa.types.is_string(field.type):
            iceberg_fields.append(NestedField(field_id=i + 1, name=field.name, field_type=StringType(), required=False))
        elif pa.types.is_int64(field.type):
            iceberg_fields.append(NestedField(field_id=i + 1, name=field.name, field_type=LongType(), required=False))
        elif pa.types.is_float32(field.type) or pa.types.is_float64(field.type):
            iceberg_fields.append(NestedField(field_id=i + 1, name=field.name, field_type=DoubleType(), required=False))
        elif pa.types.is_timestamp(field.type):
            iceberg_fields.append(NestedField(field_id=i + 1, name=field.name, field_type=TimestampType(), required=False))
        else:
            print(f"Skipping unsupported field: {field.name}")
            continue
    return Schema(*iceberg_fields)

# Function to process JSON files and save data to Iceberg
def process_and_save_to_iceberg(**context):
    """
    Process JSON files and save data to Iceberg.
    """
    end_date = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')

    catalog = load_catalog("rest", uri="http://iceberg_rest:8181")

    for country in COUNTRY_COORDINATES.keys():
        json_file_path = f"/mnt/tmp/warehouse/weather/{country.lower()}/data/weather_{end_date}.json"

        if not os.path.exists(json_file_path):
            print(f"JSON file for {country} not found: {json_file_path}")
            continue

        # Load and parse JSON file
        with open(json_file_path, 'r') as f:
            json_data = json.load(f)

        # Extract and flatten the weather data
        location_data = json_data["locations"]
        location_key = list(location_data.keys())[0]  # Extract the first location key
        values = location_data[location_key]["values"]
        station_contributions = location_data[location_key].get("stationContributions", {})

        # Convert to a Pandas DataFrame
        df = pd.DataFrame(values)

        # Include the station contributions as a single JSON-like string
        station_contributions_str = json.dumps(station_contributions)
        df["stationContributions"] = station_contributions_str

        # Use 'datetimeStr' directly for date-time field
        df["datetime"] = pd.to_datetime(df["datetimeStr"])  # ISO 8601 format
        df["datetime"] = df["datetime"].dt.tz_localize(None)  # Remove timezone
        df["datetime"] = df["datetime"].astype("datetime64[us]")  # Downcast to microsecond precision

        # Remove columns with null values
        non_null_columns = [col for col in df.columns if df[col].notnull().all()]
        df = df[non_null_columns]

        # Save to Iceberg
        iceberg_directory = f"/iceberg/warehouse/weather/{country.lower()}"
        table_name = f"weather.{country.lower()}"

        os.makedirs(iceberg_directory, exist_ok=True)

        # Convert to PyArrow table
        table = pa.Table.from_pandas(df)

        # Ensure datetime is in microseconds in PyArrow schema
        table = table.set_column(
            table.schema.get_field_index("datetime"),
            "datetime",
            pa.array(table.column("datetime").to_pylist(), pa.timestamp("us"))
        )

        try:
            iceberg_table = catalog.load_table(table_name)
            print(f"Iceberg table {table_name} exists. Appending new data.")
        except Exception as e:
            print(f"Iceberg table {table_name} does not exist. Creating table: {e}")
            iceberg_schema = arrow_to_iceberg_schema(table.schema)
            catalog.create_table(
                identifier=table_name,
                schema=iceberg_schema,
                location=iceberg_directory
            )
            iceberg_table = catalog.load_table(table_name)

        # Append data to the Iceberg table
        iceberg_table.append(table)
        print(f"Appended data to Iceberg table: {table_name}")


# Define the DAG
with DAG(
    dag_id="weather_to_iceberg",
    default_args={
        'owner': 'airflow',
        'start_date': datetime(2024, 10, 1),
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
    },
    schedule_interval='@daily',
    catchup=False
) as dag:

    # Task 1: Fetch weather data
    fetch_data = PythonOperator(
        task_id='fetch_weather_data',
        python_callable=fetch_weather_data
    )

    # Task 2: Process and save data to Iceberg
    save_data = PythonOperator(
        task_id='save_to_iceberg',
        python_callable=process_and_save_to_iceberg
    )

    fetch_data >> save_data
