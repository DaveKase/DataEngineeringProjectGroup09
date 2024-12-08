from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
import json
import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema, NestedField
from pyiceberg.types import StringType, LongType, FloatType, TimestampType, DoubleType

df = pd.DataFrame()

def fetch_and_process():
    # Load environment variables from .env file
    load_dotenv()
    security_token = os.getenv('_ENTSOE_SECURITY_TOKEN')
    print(security_token)
    url = "https://web-api.tp.entsoe.eu/api"

    # Get bidding zones EIC code from file
    df_csv = pd.read_csv('/opt/airflow/config_files/country_code_mapper.csv')
    bidding_zones = df_csv['EIC'].dropna().str.strip().tolist()


    # Parse dates from the JSON file
    with open('/opt/airflow/config_files/config_dates.json', 'r') as f:
        config = json.load(f)
    start_date = datetime.strptime(config["start_date"], "%Y-%m-%d")
    end_date = datetime.strptime(config["end_date"], "%Y-%m-%d")
    period_duration = timedelta(days=31)  # Example: one-month intervals

    time_periods = []
    current_start = start_date
    while current_start < end_date:
        current_end = min(current_start + period_duration, end_date)
        time_periods.append((current_start.strftime("%Y%m%d%H%M"), current_end.strftime("%Y%m%d%H%M")))
        current_start = current_end

    # Namespace for XML parsing
    namespaces = {
        'ns': 'urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:3'
    }

    # Data collection
    data = []
    # Loop through bidding_zones and time periods
    for bidding_zone in bidding_zones:
        for period_start, period_end in time_periods:
            # API request parameters
            params = {
                "documentType": "A44",
                "out_Domain": bidding_zone,
                "in_Domain": bidding_zone,
                "periodStart": period_start,
                "periodEnd": period_end,
                "contract_MarketAgreement.type": 'A01',
                "securityToken": security_token,
            }

            # Send API request
            response = requests.get(url, params=params)
            if response.status_code != 200:
                print(f"Error for {bidding_zone}, {period_start}-{period_end}: {response.status_code}")
                continue

            # Parse XML response
            root = ET.fromstring(response.text)

            # Iterate over TimeSeries elements
            for time_series in root.findall('.//ns:TimeSeries', namespaces=namespaces):
                currency_unit = time_series.find('.//ns:currency_Unit.name', namespaces=namespaces).text
                price_measure_unit = time_series.find('.//ns:price_Measure_Unit.name', namespaces=namespaces).text
                resolution = time_series.find('.//ns:Period//ns:resolution', namespaces=namespaces).text
                EIC_code_out = time_series.find('.//ns:out_Domain.mRID', namespaces=namespaces).text
                start_time_str = time_series.find('.//ns:Period//ns:timeInterval//ns:start', namespaces=namespaces).text
                end_time_str = time_series.find('.//ns:Period//ns:timeInterval//ns:end', namespaces=namespaces).text
                
                # Convert to datetime objects
                start_time = datetime.strptime(start_time_str, "%Y-%m-%dT%H:%MZ")
                end_time = datetime.strptime(end_time_str, "%Y-%m-%dT%H:%MZ")

                # Extract Points
                for point in time_series.findall('.//ns:Period//ns:Point', namespaces=namespaces):
                    position = int(point.find('ns:position', namespaces=namespaces).text)
                    price = float(point.find('ns:price.amount', namespaces=namespaces).text)

                    # Append to data list
                    data.append({
                        'eic_code': EIC_code_out,
                        'price': price,
                        'currency_unit': currency_unit,
                        'price_measure_unit': price_measure_unit,
                        'resolution': resolution,
                        'position': position,
                        'start_time': start_time,
                        'end_time': end_time,
                    })

    # Convert collected data to DataFrame
    df = pd.DataFrame(data)

    # Calculate the duration_from_start
    def calculate_duration(row):
        if row['resolution'] == 'PT60M':
            return timedelta(hours=row['position'])
        elif row['resolution'] == 'PT15M':
            return timedelta(minutes=row['position'] * 15)
        else:
            return pd.NaT  #return nan if there is a new resolution

    # Apply the function to create the duration_from_start column
    df['duration_from_start'] = df.apply(calculate_duration, axis=1)
    # Calculate the datetime for each price point by adding the duration to start_time
    df['datetime'] = df['start_time'] + df['duration_from_start']
    # Drop unnecessary columns
    df = df.drop(columns=['resolution', 'position', 'start_time', 'end_time', 'duration_from_start'])
    df.to_csv('/mnt/tmp/csv_data/price.csv')
    return df


def save_to_iceberg():
    # Load the Iceberg REST catalog
    catalog = load_catalog("rest", uri="http://iceberg_rest:8181")
    table_name = "price.data"
    iceberg_directory = "/iceberg/warehouse/price"

    # Load processed data
    df_path = '/mnt/tmp/csv_data/price.csv'
    df = pd.read_csv(df_path, parse_dates=['datetime'])

    # Drop the unnecessary "Unnamed: 0" column, which is the index column in csv
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
    
    # Downcast datetime to microsecond precision
    df['datetime'] = df['datetime'].astype('datetime64[us]')

    # Convert DataFrame to PyArrow Table
    table = pa.Table.from_pandas(df)

    # Define the schema for the Iceberg table
    iceberg_schema = Schema(
        NestedField(1, 'eic_code', StringType(), required=False),
        NestedField(2, 'price', DoubleType(), required=False),
        NestedField(3, 'currency_unit', StringType(), required=False),
        NestedField(4, 'price_measure_unit', StringType(), required=False),
        NestedField(5, 'datetime', TimestampType(), required=False)
    )
    # Check if the Iceberg table exists
    try:
        iceberg_table = catalog.load_table(table_name)
        print(f"Iceberg table {table_name} exists. Appending new data.")
    except Exception as e:
        print(f"Iceberg table {table_name} does not exist. Creating it: {e}")
        catalog.create_table(
            identifier=table_name,
            schema=iceberg_schema,
            location=iceberg_directory
        )
        iceberg_table = catalog.load_table(table_name)

    # Append data to the Iceberg table
    iceberg_table.append(table)
    print(f"Data successfully appended to Iceberg table: {table_name}")


# Define the DAG
with DAG(
    dag_id="price_to_iceberg",
    default_args={
        'owner': 'airflow',
        'start_date': datetime(2024, 1, 1),
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
    },
    schedule_interval='@daily',
    catchup=False
) as dag:

    # Task 1: Fetch weather data
    fetch_data = PythonOperator(
        task_id='fetch_price_data',
        python_callable=fetch_and_process
    )

    # Task 2: Process and save data to Iceberg
    save_data = PythonOperator(
        task_id='save_to_iceberg',
        python_callable=save_to_iceberg
    )

    fetch_data >> save_data