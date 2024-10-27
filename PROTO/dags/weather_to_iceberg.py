from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import csv
import pandas as pd
import pyarrow as pa
import pyarrow.csv as pv
import os
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema, NestedField
from pyiceberg.types import StringType, LongType, FloatType, TimestampType, DoubleType

# Load the weather API key from the environment variables
API_KEY = os.getenv('_WEATHER_API_KEY')

# Define Estonia's coordinates
estonia_coordinates = '59.35,24.80'

# Columns to remove
columns_to_remove = ['Heat Index', 'Wind Gust', 'Wind Chill', 'Weather Type', 'Resolved Address']

# Base URL for the API
base_url = 'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/weatherdata/history'


def fetch_weather_data(**context):
    # Ensure the directory exists
    output_dir = '/app/mnt/tmp/csv_data'
    os.makedirs(output_dir, exist_ok=True)

    # Get the start and end date
    start_date = (datetime.now() - timedelta(2)).strftime('%Y-%m-%d')
    end_date = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')

    params = {
        'unitGroup': 'metric',
        'contentType': 'csv',
        'aggregateHours': 1,
        'dayStartTime': '0:00:00',
        'dayEndTime': '23:59:59',
        'startDateTime': f'{start_date}T00:00:00',
        'endDateTime': f'{end_date}T23:59:59',
        'locations': estonia_coordinates,
        'collectStationContribution': 'true',
        'key': API_KEY
    }

    # Make the request
    response = requests.get(base_url, params=params)

    if response.status_code == 200:
        csv_content = response.text.splitlines()
        csv_reader = csv.reader(csv_content)
        original_header = next(csv_reader)

        # Filter out unwanted columns
        indices_to_remove = [i for i, col in enumerate(original_header) if col in columns_to_remove]
        header = [col for i, col in enumerate(original_header) if i not in indices_to_remove]
        header.append('Country')  # Add 'Country' column

        weather_data = []
        for row in csv_reader:
            filtered_row = [col for i, col in enumerate(row) if i not in indices_to_remove]
            filtered_row.append('Estonia')  # Add country name to the row
            weather_data.append(filtered_row)

        # Save to a CSV file (temporary)
        output_file = os.path.join(output_dir, 'estonia_weather_last_3_days.csv')
        df = pd.DataFrame(weather_data, columns=header)
        df.to_csv(output_file, index=False)
        context['ti'].xcom_push(key='csv_file', value=output_file)
    else:
        raise Exception(f"Failed to fetch weather data. Status code: {response.status_code}")


def arrow_to_iceberg_schema(pa_schema):
    iceberg_fields = []
    for i, field in enumerate(pa_schema):
        if pa.types.is_string(field.type):
            iceberg_fields.append(NestedField(field_id=i + 1, name=field.name, field_type=StringType(), required=False))
        elif pa.types.is_int64(field.type):
            iceberg_fields.append(NestedField(field_id=i + 1, name=field.name, field_type=LongType(), required=False))
        elif pa.types.is_float32(field.type):
            iceberg_fields.append(NestedField(field_id=i + 1, name=field.name, field_type=FloatType(), required=False))
        elif pa.types.is_float64(field.type):
            iceberg_fields.append(NestedField(field_id=i + 1, name=field.name, field_type=DoubleType(), required=False))
        elif pa.types.is_timestamp(field.type):
            iceberg_fields.append(NestedField(field_id=i + 1, name=field.name, field_type=TimestampType(), required=False))
        elif pa.types.is_null(field.type):
            print(f"Skipping null field: {field.name}")
            continue
        else:
            raise ValueError(f"Unsupported PyArrow type: {field.type}")
    return Schema(*iceberg_fields)


def save_to_iceberg(**context):
    csv_file = context['ti'].xcom_pull(key='csv_file')

    # Ensure the Iceberg directory exists
    iceberg_directory = "/iceberg/warehouse/weather/estonia"
    os.makedirs(iceberg_directory, exist_ok=True)
    print(f"Checked/Created Iceberg directory: {iceberg_directory}")

    # Read the CSV file into a PyArrow table (inferring schema automatically)
    with open(csv_file, 'rb') as f:
        csv_options = pv.ConvertOptions(column_types=None)
        table = pv.read_csv(f, convert_options=csv_options)

    # Remove columns with null types
    non_null_columns = [col for col in table.schema.names if not pa.types.is_null(table.schema.field(col).type)]
    table = table.select(non_null_columns)

    # Load the Iceberg catalog and infer schema from the PyArrow table
    catalog = load_catalog("rest", uri="http://iceberg_rest:8181")
    table_name = "weather.estonia"

    # Check if the table exists in the catalog
    try:
        iceberg_table = catalog.load_table(table_name)
        print("Table exists. Appending new data.")
    except Exception as e:
        print("Table does not exist. Creating table:", e)
        iceberg_schema = arrow_to_iceberg_schema(table.schema)
        catalog.create_table(
            identifier=table_name,
            schema=iceberg_schema,
            location=iceberg_directory
        )
        iceberg_table = catalog.load_table(table_name)

    # Append the PyArrow table to the Iceberg table
    iceberg_table.append(table)


# Define the DAG
with DAG(
    dag_id="weather_to_iceberg",
    default_args={
        'owner': 'airflow',
        'start_date': datetime(2024, 10, 1),
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    schedule_interval='@daily',
    catchup=False
) as dag:

    # Task 1: Fetch weather data for Estonia for the last 3 days
    fetch_data = PythonOperator(
        task_id='fetch_weather_data',
        python_callable=fetch_weather_data
    )

    # Task 2: Save the weather data to Iceberg
    save_data = PythonOperator(
        task_id='save_to_iceberg',
        python_callable=save_to_iceberg
    )

    fetch_data >> save_data
