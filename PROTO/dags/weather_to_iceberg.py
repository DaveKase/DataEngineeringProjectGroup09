from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import os
import json
import pandas as pd
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema, NestedField
from pyiceberg.types import StringType, LongType, FloatType, TimestampType, DoubleType

# Load the weather API key from the environment variables
API_KEY = os.getenv('_WEATHER_API_KEY')

# Base URL for the API
BASE_URL = 'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/weatherdata/history'

# Load config dates
CONFIG_FILE = '/opt/airflow/config_files/config_dates.json'
with open(CONFIG_FILE, 'r') as f:
    config_dates = json.load(f)

config_start_date = datetime.strptime(config_dates["start_date"], '%Y-%m-%d')
config_end_date = datetime.strptime(config_dates["end_date"], '%Y-%m-%d')

# Adjust the date range to be a maximum of 48 hours
if (config_end_date - config_start_date).total_seconds() > 48 * 3600:
    print("Weather data of max 48 hours can be queried. Adjusting start date accordingly.")
    config_start_date = config_end_date - timedelta(hours=48)

# Load country codes and coordinates from CSV
COUNTRY_CODES_FILE = '/opt/airflow/config_files/country_code_mapper.csv'
country_df = pd.read_csv(COUNTRY_CODES_FILE)

#test_countries = ["Estonia", "Latvia", "Lithuania"]
#filtered_country_df = country_df[country_df['Country'].isin(test_countries)]

COUNTRY_COORDINATES = {
    row['BZN']: f"{row['Latitude']},{row['Longitude']}" for _, row in country_df.iterrows()
}

# Function to fetch weather data and save as JSON
def fetch_weather_data(**context):
    """
    Fetch weather data for the configured date range and save as JSON for all countries.
    """
    start_date = config_start_date.strftime('%Y-%m-%d')
    end_date = config_end_date.strftime('%Y-%m-%d')

    output_dir = '/mnt/tmp/warehouse/weather'
    os.makedirs(output_dir, exist_ok=True)

    for _, row in country_df.iterrows():
        country_bzn = row['BZN']
        coordinates = COUNTRY_COORDINATES[country_bzn]

        params = {
            'unitGroup': 'metric',
            'contentType': 'json',
            'aggregateHours': 1,
            'dayStartTime': '0:00:00',
            'dayEndTime': '23:59:59',
            'startDateTime': f'{start_date}T00:00:00',
            'endDateTime': f'{end_date}T23:59:59',
            'locations': coordinates,
            'collectStationContribution': 'true',
            'key': API_KEY
        }

        response = requests.get(BASE_URL, params=params)

        if response.status_code == 200:
            output_file = os.path.join(output_dir, f'weather_{country_bzn.lower()}_{start_date}_to_{end_date}.json')
            with open(output_file, 'w') as f:
                json.dump(response.json(), f)
            print(f"Weather data saved for {country_bzn}: {output_file}")
        else:
            raise Exception(f"Failed to fetch weather data for {country_bzn}. Status code: {response.status_code}")

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
    Process JSON files and save data to Iceberg as a single table.
    """
    catalog = load_catalog("rest", uri="http://iceberg_rest:8181")
    table_name = "weather.data"
    iceberg_directory = "/iceberg/warehouse/weather"

    all_data = []

    for _, row in country_df.iterrows():
        country_bzn = row['BZN']
        country_name = row['Country']
        
        json_file_path = f"/mnt/tmp/warehouse/weather/weather_{country_bzn.lower()}_{config_start_date.strftime('%Y-%m-%d')}_to_{config_end_date.strftime('%Y-%m-%d')}.json"

        if not os.path.exists(json_file_path):
            print(f"JSON file for {country_bzn} not found: {json_file_path}")
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
        df["Country"] = country_name
        df["EIC"] = row["EIC"]
        df["BZN"] = row["BZN"]

        # Include the station contributions as a single JSON-like string
        station_contributions_str = json.dumps(station_contributions)
        df["stationContributions"] = station_contributions_str
        
        #print(df.columns)

        # Use 'datetimeStr' directly for date-time field
        df["datetime"] = pd.to_datetime(df["datetime"], unit='ms')  # ISO 8601 format
        df["datetime"] = df["datetime"].dt.tz_localize(None)  # Remove timezone
        df["datetime"] = df["datetime"].astype("datetime64[us]")  # Downcast to microsecond precision

        # Remove columns with null values
        non_null_columns = [col for col in df.columns if df[col].notnull().all()]
        df = df[non_null_columns]

        all_data.append(df)

    # Combine all data
    combined_df = pd.concat(all_data, ignore_index=True)

    # Save to Iceberg
    table = pa.Table.from_pandas(combined_df)

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
