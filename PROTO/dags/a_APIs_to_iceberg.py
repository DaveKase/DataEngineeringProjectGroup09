from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime, timedelta
import os
import json
import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema, NestedField
from pyiceberg.types import StringType, LongType, FloatType, TimestampType, DoubleType

df = pd.DataFrame()

_ENSTOE_API_KEY = os.getenv('_ENTSOE_SECURITY_TOKEN')
_WEATHER_API_KEY = os.getenv('_WEATHER_API_KEY')



def fetch_and_process_production(**context):
    # Load environment variables from .env file
    url = "https://web-api.tp.entsoe.eu/api"

    # Get bidding zones EIC code from file
    df_csv = pd.read_csv('/opt/airflow/config_files/country_code_mapper.csv')
    bidding_zones = df_csv['EIC'].dropna().str.strip().tolist()
    
    # Parse dates from the JSON file
    with open('/opt/airflow/config_files/config_dates.json', 'r') as f:
        config = json.load(f)
    start_date = datetime.strptime(config["start_date"], "%Y-%m-%d")
    end_date = datetime.strptime(config["end_date"], "%Y-%m-%d") + timedelta(days=1)  # Include the entire day
    period_duration = timedelta(days=31)  # Example: one-month intervals
    
    print(start_date, end_date)
    
    time_periods = []
    current_start = start_date
    while current_start < end_date:
        current_end = min(current_start + period_duration, end_date)
        time_periods.append((current_start.strftime("%Y%m%d%H%M"), current_end.strftime("%Y%m%d%H%M")))
        current_start = current_end

    # Namespace for XML parsing
    namespaces = {
        "ns": "urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0"
        }
    # Initialize empty DataFrame to collect all data
    data = []

    # Loop through each bidding zone and time period
    for bidding_zone in bidding_zones:
        #print(bidding_zone)
        for period_start, period_end in time_periods:
            #print(period_start)
            params = {
                "securityToken": _ENSTOE_API_KEY,
                "documentType": "A75",
                "processType": "A16",
                "in_Domain": bidding_zone,
                "periodStart": period_start,
                "periodEnd": period_end
            }

            # Send API request
            response = requests.get(url, params=params)
            if response.status_code != 200:
                print(f"Error for {bidding_zone}, {period_start}-{period_end}: {response.status_code}")
                continue
            
            # Parse the XML response
            root = ET.fromstring(response.text)
            
            for timeseries in root.findall(".//ns:TimeSeries", namespaces):
                bidding_zone_element = timeseries.find(".//ns:inBiddingZone_Domain.mRID", namespaces)
                if bidding_zone_element is None:
                    # If not found, try outBiddingZone_Domain.mRID
                    bidding_zone_element = timeseries.find(".//ns:outBiddingZone_Domain.mRID", namespaces)
                if bidding_zone_element is not None:
                    bidding_zone = bidding_zone_element.text
                else:
                    # Handle case where neither is found
                    raise ValueError("Neither inBiddingZone_Domain.mRID nor outBiddingZone_Domain.mRID found in TimeSeries!")
                resolution = timeseries.find(".//ns:resolution", namespaces).text
                start_time = pd.to_datetime(timeseries.find(".//ns:timeInterval/ns:start", namespaces).text)
                end_time = pd.to_datetime(timeseries.find(".//ns:timeInterval/ns:end", namespaces).text)
                quantity_measure_unit = timeseries.find(".//ns:quantity_Measure_Unit.name", namespaces).text
                produced_type = timeseries.find(".//ns:MktPSRType/ns:psrType", namespaces).text
                
                # Extract Points for this TimeSeries
                points = timeseries.findall(".//ns:Point", namespaces)
                
                for point in points:
                    position = int(point.find("ns:position", namespaces).text)
                    quantity = float(point.find("ns:quantity", namespaces).text)

                    # Append data for each point
                    data.append({
                        "eic_code": bidding_zone,
                        "quantity": quantity,
                        "unit": quantity_measure_unit,
                        "resolution": resolution,
                        "position": position,
                        "start_time": start_time,
                        "end_time": end_time,
                        "produced_energy_type_code": produced_type
                    })
                    
    # Create DataFrame with all data
    df = pd.DataFrame(data)
    
    # Define the dictionary
    energy_type_dict = {
        "B01": "Biomass",
        "B02": "Fossil Brown coal/Lignite",
        "B03": "Fossil Coal-derived gas",
        "B04": "Fossil Gas",
        "B05": "Fossil Hard coal",
        "B06": "Fossil Oil",
        "B07": "Fossil Oil shale",
        "B08": "Fossil Peat",
        "B09": "Geothermal",
        "B10": "Hydro Pumped Storage",
        "B11": "Hydro Run-of-river and poundage",
        "B12": "Hydro Water Reservoir",
        "B13": "Marine",
        "B14": "Nuclear",
        "B15": "Other renewable",
        "B16": "Solar",
        "B17": "Waste",
        "B18": "Wind Offshore",
        "B19": "Wind Onshore",
        "B20": "Other",
        "B25": "Energy storage",
    }
    
    # Calculate the duration_from_start
    def calculate_duration(row):
        if row['resolution'] == 'PT60M':
            return timedelta(hours=row['position'])
        elif row['resolution'] == 'PT15M':
            return timedelta(minutes=row['position'] * 15)
        else:
            return pd.NaT  # return NaT if there is a new resolution
    
    # Apply the function to create the duration_from_start column
    df['duration_from_start'] = df.apply(calculate_duration, axis=1)
    df['produced_energy_type'] = df['produced_energy_type_code'].map(energy_type_dict)

    # Calculate the datetime for each price point by adding the duration to start_time
    df['datetime'] = df['start_time'] + df['duration_from_start']

    # Drop unnecessary columns
    df = df.drop(columns=['resolution', 'position', 'start_time', 'end_time', 'duration_from_start', 'produced_energy_type_code'])
    
    df.to_csv('/mnt/tmp/csv_data/production.csv')
    
    return df


def save_production_to_iceberg(**context):
    # Load the Iceberg REST catalog
    catalog = load_catalog("rest", uri="http://iceberg_rest:8181")
    table_name = "production.data"
    iceberg_directory = "/iceberg/warehouse/production"

    # Load processed data
    df_path = '/mnt/tmp/csv_data/production.csv'
    df = pd.read_csv(df_path, parse_dates=['datetime'])

    # Drop the unnecessary "Unnamed: 0" column, which is the index column in csv
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
    
    # Remove timezone information from the datetime column
    df['datetime'] = df['datetime'].dt.tz_localize(None)
    
    # Downcast datetime to microsecond precision
    df['datetime'] = df['datetime'].astype('datetime64[us]')

    # Convert DataFrame to PyArrow Table
    table = pa.Table.from_pandas(df)

    # Define the schema for the Iceberg table
    iceberg_schema = Schema(
        NestedField(1, 'eic_code', StringType(), required=False),
        NestedField(2, 'quantity', DoubleType(), required=False),
        NestedField(3, 'unit', StringType(), required=False),
        NestedField(4, 'produced_energy_type', StringType(), required=False),
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


def fetch_and_process_price(**context):
    # Load environment variables from .env file
    url = "https://web-api.tp.entsoe.eu/api"

    # Get bidding zones EIC code from file
    df_csv = pd.read_csv('/opt/airflow/config_files/country_code_mapper.csv')
    bidding_zones = df_csv['EIC'].dropna().str.strip().tolist()


    # Parse dates from the JSON file
    with open('/opt/airflow/config_files/config_dates.json', 'r') as f:
        config = json.load(f)
    start_date = datetime.strptime(config["start_date"], "%Y-%m-%d")
    end_date = datetime.strptime(config["end_date"], "%Y-%m-%d") + timedelta(days=1)  # Include the entire day
    period_duration = timedelta(days=31)  # Example: one-month intervals
    
    print(start_date, end_date)

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
                "securityToken": _ENSTOE_API_KEY,
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


def save_price_to_iceberg(**context):
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

def fetch_and_process_consumption(**context):
    # Load environment variables from .env file
    url = "https://web-api.tp.entsoe.eu/api"

    # Get bidding zones EIC code from file
    df_csv = pd.read_csv('/opt/airflow/config_files/country_code_mapper.csv')
    bidding_zones = df_csv['EIC'].dropna().str.strip().tolist()

    # Parse dates from the JSON file
    with open('/opt/airflow/config_files/config_dates.json', 'r') as f:
        config = json.load(f)
    start_date = datetime.strptime(config["start_date"], "%Y-%m-%d")
    end_date = datetime.strptime(config["end_date"], "%Y-%m-%d") + timedelta(days=1)  # Include the entire day
    period_duration = timedelta(days=31)  # Example: one-month intervals
    
    print(start_date, end_date)

    time_periods = []
    current_start = start_date
    while current_start < end_date:
        current_end = min(current_start + period_duration, end_date)
        time_periods.append((current_start.strftime("%Y%m%d%H%M"), current_end.strftime("%Y%m%d%H%M")))
        current_start = current_end

    # Namespace for XML parsing
    namespaces = {
        "ns": "urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0"
    }

    # Data collection
    data = []
    # Loop through bidding_zones and time periods
    for bidding_zone in bidding_zones:
        for period_start, period_end in time_periods:
            params = {
                "securityToken": _ENSTOE_API_KEY,
                "documentType": "A65",
                "processType": "A16",
                "outBiddingZone_Domain": bidding_zone,
                "periodStart": period_start,
                "periodEnd": period_end
            }

            # Send API request
            response = requests.get(url, params=params)
            if response.status_code != 200:
                print(f"Error for {bidding_zone}, {period_start}-{period_end}: {response.status_code}")
                continue

            # Parse XML response
            root = ET.fromstring(response.text)

            # Iterate over TimeSeries elements
            resolution = root.find(".//ns:resolution", namespaces).text
            start_time = pd.to_datetime(root.find(".//ns:timeInterval/ns:start", namespaces).text)
            end_time = pd.to_datetime(root.find(".//ns:timeInterval/ns:end", namespaces).text)
            bidding_zone = root.find(".//ns:outBiddingZone_Domain.mRID", namespaces).text
            quantity_measure_unit = root.find(".//ns:quantity_Measure_Unit.name", namespaces).text
            points = root.findall(".//ns:Point", namespaces)
            
            for point in points:
                position = int(point.find("ns:position", namespaces).text)
                quantity = float(point.find("ns:quantity", namespaces).text)
                
                data.append({
                "eic_code": bidding_zone,
                "quantity": quantity,
                "unit": quantity_measure_unit,
                "resolution": resolution,
                "position": position,
                "start_time": start_time,
                "end_time": end_time,
                })
                
    # Create DataFrame with all data
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
    
    df.to_csv('/mnt/tmp/csv_data/consumption.csv')
    
    return df


def save_consumption_to_iceberg(**context):
    # Load the Iceberg REST catalog
    catalog = load_catalog("rest", uri="http://iceberg_rest:8181")
    table_name = "consumption.data"
    iceberg_directory = "/iceberg/warehouse/consumption"

    # Load processed data
    df_path = '/mnt/tmp/csv_data/consumption.csv'
    df = pd.read_csv(df_path, parse_dates=['datetime'])

    # Drop the unnecessary "Unnamed: 0" column, which is the index column in csv
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
    
    # Remove timezone information from the datetime column
    df['datetime'] = df['datetime'].dt.tz_localize(None)
    
    # Downcast datetime to microsecond precision
    df['datetime'] = df['datetime'].astype('datetime64[us]')

    # Convert DataFrame to PyArrow Table
    table = pa.Table.from_pandas(df)

    # Define the schema for the Iceberg table
    iceberg_schema = Schema(
        NestedField(1, 'eic_code', StringType(), required=False),
        NestedField(2, 'quantity', DoubleType(), required=False),
        NestedField(3, 'unit', StringType(), required=False),
        NestedField(4, 'datetime', TimestampType(), required=False)
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




# Function to fetch weather data and save as JSON
def fetch_weather_data(**context):
    """
    Fetch weather data for the configured date range and save as JSON for all countries.
    """
    BASE_URL = 'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/weatherdata/history'

    # Load config dates
    CONFIG_FILE = '/opt/airflow/config_files/config_dates.json'
    with open(CONFIG_FILE, 'r') as f:
        config_dates = json.load(f)

    config_start_date = datetime.strptime(config_dates["start_date"], '%Y-%m-%d')
    config_end_date = datetime.strptime(config_dates["end_date"], '%Y-%m-%d')

    # Adjust the date range to be a maximum of 48 hours, remove this if you have paid API connection
    if (config_end_date - config_start_date).total_seconds() > 48 * 3600:
        print("Weather data of max 48 hours can be queried. Adjusting start date accordingly.")
        config_start_date = config_end_date - timedelta(hours=48)

    # Load country codes and coordinates from CSV
    COUNTRY_CODES_FILE = '/opt/airflow/config_files/country_code_mapper.csv'
    country_df = pd.read_csv(COUNTRY_CODES_FILE)

    COUNTRY_COORDINATES = {
        row['BZN']: f"{row['Latitude']},{row['Longitude']}" for _, row in country_df.iterrows()
    }

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
            'key': _WEATHER_API_KEY
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
def process_and_save_weather_to_iceberg(**context):
    """
    Process JSON files and save data to Iceberg as a single table.
    """
    BASE_URL = 'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/weatherdata/history'

    # Load config dates
    CONFIG_FILE = '/opt/airflow/config_files/config_dates.json'
    with open(CONFIG_FILE, 'r') as f:
        config_dates = json.load(f)


    config_start_date = datetime.strptime(config_dates["start_date"], '%Y-%m-%d')
    config_end_date = datetime.strptime(config_dates["end_date"], '%Y-%m-%d')

    # Adjust the date range to be a maximum of 48 hours, remove this if you have paid API connection
    if (config_end_date - config_start_date).total_seconds() > 48 * 3600:
        print("Weather data of max 48 hours can be queried. Adjusting start date accordingly.")
        config_start_date = config_end_date - timedelta(hours=48)

    # Load country codes and coordinates from CSV
    COUNTRY_CODES_FILE = '/opt/airflow/config_files/country_code_mapper.csv'
    country_df = pd.read_csv(COUNTRY_CODES_FILE)

    COUNTRY_COORDINATES = {
        row['BZN']: f"{row['Latitude']},{row['Longitude']}" for _, row in country_df.iterrows()
    }

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
    dag_id="a_APIs_to_iceberg",
    default_args={
        'owner': 'airflow',
        'start_date': datetime(2024, 1, 1),
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
    },
    schedule_interval='@daily',
    catchup=False
) as dag:


    fetch_and_process_production = PythonOperator(
        task_id='fetch_and_process_production',
        python_callable=fetch_and_process_production
    )
    fetch_and_process_price = PythonOperator(
        task_id='fetch_and_process_price',
        python_callable=fetch_and_process_price
    )
    fetch_and_process_consumption = PythonOperator(
        task_id='fetch_and_process_consumption',
        python_callable=fetch_and_process_consumption
    )
    fetch_weather_data = PythonOperator(
        task_id='fetch_weather_data',
        python_callable=fetch_weather_data
    )
    save_production_to_iceberg = PythonOperator(
        task_id='save_production_to_iceberg',
        python_callable=save_production_to_iceberg
    )
    save_price_to_iceberg = PythonOperator(
        task_id='save_price_to_iceberg',
        python_callable=save_price_to_iceberg
    )
    save_consumption_to_iceberg = PythonOperator(
        task_id='save_consumption_to_iceberg',
        python_callable=save_consumption_to_iceberg
    )
    process_and_save_weather_to_iceberg = PythonOperator(
        task_id='process_and_save_weather_to_iceberg',
        python_callable=process_and_save_weather_to_iceberg
    )

    fetch_and_process_production >> save_production_to_iceberg
    fetch_and_process_price >> save_price_to_iceberg
    fetch_and_process_consumption >> save_consumption_to_iceberg
    fetch_weather_data >> process_and_save_weather_to_iceberg