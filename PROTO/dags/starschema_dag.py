'''
This DAG should take data from 4 DuckDB tables and transform them into StarSchema and save it back to DuckDB
'''

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta  # Add timedelta import
import duckdb
import os

DUCKDB_FILENAME = '/mnt/tmp/duckdb_data/star_schema_db.duckdb'

# Basically router for create_tables_task. Now we have all table creations under one DAG, instead of 6 different DAGS.
def create_tables():
    if not os.path.exists(DUCKDB_FILENAME):
        print(f"Creating DuckDB file at: {DUCKDB_FILENAME}")  # Debug statement
    
    con = duckdb.connect(DUCKDB_FILENAME)

    print("creating tables")
    create_measurements_fact_table(con)
    create_bz_dimen_table(con)
    create_energy_type_dimen_table(con)
    create_weather_condition_dimen_table(con)
    create_unit_dimen_table(con)
    create_timestamp_dimen_table(con)

    con.close()
    print("Tables created successfully")

# Takes the table name and the sql query as inputs and creates table in DuckDB
def table_creation_script(table_name, sql, con):
    print(f"Creating table {table_name}")
    
    # Ensure the table exists
    print(f"Ensuring table {table_name} exists.")  # Debug statement
    con.execute(sql)
    print(f"Table {table_name} created or exists")

'''
Creating FACT table
'''
# Creating the fact table (measurements)
def create_measurements_fact_table(con):
    table_name = "measurements_FACT"
    sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
          _id BIGINT PRIMARY KEY,
          date_time_id BIGINT NOT NULL,
          bzn_id BIGINT NOT NULL,
          consumption_qty FLOAT NOT NULL,
          consumption_unit_id BIGINT NOT NULL,
          produced_energy_x_id BIGINT NOT NULL,
          production_quantity_x FLOAT NOT NULL,
          production_unit VARCHAR NOT NULL,
          price FLOAT NOT NULL,
          price_unit_id BIGINT NOT NULL,
          weather_condition_x_id BIGINT NOT NULL,
          weather_condition_x_unit_id BIGINT NOT NULL
        )"""
    
    table_creation_script(table_name=table_name, sql=sql, con=con)

'''
Creating the dimension tables (all 5 of them)
'''
# Creating bidding zone dimension table
def create_bz_dimen_table(con):
    table_name = "bidding_zone_DIMEN"
    sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
          _id BIGINT PRIMARY KEY,
          bzn VARCHAR NOT NULL,
          country VARCHAR NOT NULL,
          eu_member INTEGER NOT NULL,
          data_resolution INTEGER NOT NULL,
          geometry VARCHAR
        )"""
    
    table_creation_script(table_name=table_name, sql=sql, con=con)

# Creating energy type dimension table
def create_energy_type_dimen_table(con):
    table_name = "energy_type_DIMEN"
    sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
          _id BIGINT PRIMARY KEY,
          energy_type_x VARCHAR NOT NULL,
          energy_type_class VARCHAR NOT NULL,
          renewable INTEGER NOT NULL,
          carbon INTEGER NOT NULL
        )"""

    table_creation_script(table_name=table_name, sql=sql, con=con)

# Creating weather condition dimension table
def create_weather_condition_dimen_table(con):
    table_name = "weather_condition_DIMEN"
    sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
          _id BIGINT PRIMARY KEY,
          condition_short VARCHAR NOT NULL,
          condition_long VARCHAR NOT NULL,
          condition_type VARCHAR NOT NULL,
          numerical INTEGER NOT NULL
        )"""
    
    table_creation_script(table_name=table_name, sql=sql, con=con)

# Creating unit dimension table
def create_unit_dimen_table(con):
    table_name = "unit_DIMEN"
    sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
          _id BIGINT PRIMARY KEY,
          unit_short VARCHAR NOT NULL,
          unit_long VARCHAR NOT NULL,
          unit_class VARCHAR NOT NULL
        )"""
    
    table_creation_script(table_name=table_name, sql=sql, con=con)

# Creating timestamp dimension table
def create_timestamp_dimen_table(con):
    table_name = "timestamp_DIMEN"
    sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
          _id BIGINT PRIMARY KEY,
          datetime TIMESTAMP NOT NULL,
          weekday VARCHAR NOT NULL,
          workingday INTEGER NOT NULL,
          weekend INTEGER NOT NULL,
          holiday INTEGER NOT NULL
        )"""
    
    table_creation_script(table_name=table_name, sql=sql, con=con)

'''
Data selection and preparation
'''
# Querying data from tables
def get_data():
  print("querying data")

  duckdb_file = '/mnt/tmp/duckdb_data/weather.duckdb'
  con = duckdb.connect(duckdb_file)
  sql = "select * from weather_cleaned;"
  weather = con.sql(sql).fetchall()
  con.close()

  duckdb_file = '/mnt/tmp/duckdb_data/consumption.duckdb'
  con = duckdb.connect(duckdb_file)
  sql = "select * from consumption_data;"
  consumption = con.sql(sql).fetchall()
  con.close()
  
  duckdb_file = '/mnt/tmp/duckdb_data/price.duckdb'
  con = duckdb.connect(duckdb_file)
  sql = "select * from price_data;"
  price = con.sql(sql).fetchall()
  con.close()

  duckdb_file = '/mnt/tmp/duckdb_data/production.duckdb'
  con = duckdb.connect(duckdb_file)
  sql = "select * from production_data;"
  production = con.sql(sql).fetchall()
  con.close()

  print("data query success!!")
  divide_data_into_starschema(weather, consumption, price, production)

# Takes the queried data and divides it into 6 tables
def divide_data_into_starschema(weather, consumption, price, production):
    print("dividing data...")
    con = duckdb.connect(DUCKDB_FILENAME)

    populate_units_table(consumption, price, production, con)
    populate_weather_condition_table(weather, con)
    populate_energy_type_table(production, con)
    populate_bidding_zone_table(con)
    
    con.close()
    print("Data division into tables was SUCCESSFULLLL!!!!")

# Populates the units_dimen table with units across other tables
def populate_units_table(consumption, price, production, con):
    units = []

    for record in consumption:
        if record[2] not in units:
            units.append(record[2])
    
    for record in price:
        if record[2] not in units:
            units.append(record[2])
        if record[3] not in units:
            units.append(record[3])
    
    for record in production:
        if record[2] not in units:
            units.append[record[2]]

    units.append("%")
    units.append("°C")
    units.append("mm")
    units.append("cm")
    units.append("km")
    units.append("kph")
    units.append("m/s")
    units.append("mb")
    units.append("mmHg")
    units.append("W/m²")
    units.append("J/m²")

    for unit in units:
        unit_class = unit_mapper(unit)[0]
        unit_long = unit_mapper(unit)[1]

        sql = f"""INSERT INTO unit_DIMEN (_id, unit_short, unit_long, unit_class) SELECT (SELECT COUNT(*) FROM unit_DIMEN) + 1, 
          \'{unit}\', \'{unit_long}\', \'{unit_class}\' WHERE NOT EXISTS (SELECT 1 FROM unit_DIMEN WHERE unit_short = \'{unit}\');"""

        con.execute(sql)
    
    print("units_DIMEN table populated with data")

# Maps the short name of a unit to a long name and class of that unit.
# I didn't think of a neater way of doing that at the moment
def unit_mapper(unit):
    unit_class = ""
    unit_long = ""

    if unit == "MAW":
        unit_class = "electricity"
        unit_long = "Mega Active Wat"
    elif unit == "MWH":
        unit_class = "electricity"
        unit_long = "megawatt-hours"
    elif unit == "EUR":
        unit_class = "price"
        unit_long = "Euro"
    elif unit == "%":
        unit_class = "weather"
        unit_long = "percentage"
    elif unit == "°C":
        unit_class = "weather"
        unit_long = "degrees Celsius"
    elif unit == "mm":
        unit_class = "weather"
        unit_long = "millimeters"
    elif unit == "cm":
        unit_class = "weather"
        unit_long = "centimeters"
    elif unit == "km":
        unit_class = "weather"
        unit_long = "kilometers"
    elif unit == "kph":
        unit_class = "weather"
        unit_long = "kilometers per hour"
    elif unit == "m/s":
        unit_class ="weather"
        unit_long = "meters per second"
    elif unit == "mb":
        unit_class = "weather"
        unit_long = "millibars"
    elif unit == "mmHg":
        unit_class = "weather"
        unit_long = "air pressure"
    elif unit == "W/m²":
        unit_class = "weather"
        unit_long = "watts per square meter"
    elif unit == "J/m²":
        unit_class = "weather"
        unit_long = "jauls per square meter"
    
    return unit_class, unit_long

# This populates weather condition dimension table.
def populate_weather_condition_table(weather, con):
    weather_conditions = []
    duckdb_file = '/mnt/tmp/duckdb_data/weather.duckdb'
    table_header_con = duckdb.connect(duckdb_file)
    table_headers = "SELECT column_name FROM information_schema.columns WHERE table_name = 'weather_cleaned';"
    header_names = table_header_con.sql(table_headers).fetchall()
    table_header_con.close()

    # Extract column names from tuples
    header_names = [name[0] for name in header_names]
    weather_dict = []

    # Loop through weather data
    for record in weather:
        record_dict = {}
        for i, value in enumerate(record):
            record_dict[header_names[i]] = value
        
        weather_dict.append(record_dict)

    for record in weather_dict:
        conditions = record['conditions']

         # Split conditions into separate values
        condition_list = [condition.strip() for condition in conditions.split(',')]

        for condition in condition_list:
           if condition not in weather_conditions:
                weather_conditions.append(condition)
    
    for header in header_names:
        if header != "datetimeStr" and header != "date_time" and header != "station_info" and header != "weathertype" and header != "conditions" and header != "country" and header != "eic_code" and header != "BZN" and header != "contributing_stations" and header != "bzn":
             if header not in weather_conditions:
                weather_conditions.append(header)

    for condition in weather_conditions:
        sql = f"""INSERT INTO weather_condition_DIMEN (_id, condition_short, condition_long, condition_type, numerical) SELECT (SELECT COUNT(*) FROM weather_condition_DIMEN) + 1, 
          \'{condition_mapper(condition)[0]}\', \'{condition_mapper(condition)[1]}\', \'{condition_mapper(condition)[2]}\', {condition_mapper(condition)[3]} 
          WHERE NOT EXISTS (SELECT 1 FROM weather_condition_DIMEN WHERE condition_short = \'{condition_mapper(condition)[0]}\');"""

        con.execute(sql)
    
    print("weather_condition_DIMEN table populated with data")

# Taking condition name and adding missing fields that are neccessary for the weather_condition dimension table
def condition_mapper(condition):
    condition_short = ""
    condition_long = ""
    condition_type = ""
    numerical = 0

    if condition == "Partially cloudy":
        condition_short = "pcloudy"
        condition_long = condition
        condition_type = "clouds"
        numerical = 0
    if condition == "Overcast":
        condition_short = "ovrcst"
        condition_long = condition
        condition_type = "clouds"
        numerical = 0
    if condition == "Light Snow":
        condition_short = "lghtsnow"
        condition_long = condition
        condition_type = "precipitation"
        numerical = 0
    if condition == "Rain":
        condition_short = "rain"
        condition_long = condition
        condition_type = "precipitation"
        numerical = 0
        numerical = 0
    if condition == "Snow":
        condition_short = "snow"
        condition_long = condition
        condition_type = "precipitation"
        numerical = 0
    if condition == "Snow Showers":
        condition_short = "snowshwrs"
        condition_long = condition
        condition_type = "precipitation"
        numerical = 0
    if condition == "Mist":
        condition_short = condition
        condition_long = condition
        condition_type = "air"
        numerical = 0
    if condition == "Light Rain And Snow":
        condition_short = "lghtrainandsnow"
        condition_long = condition
        condition_type = "precipitation"
        numerical = 0
    if condition == "Light Rain":
        condition_short = "lghtrain"
        condition_long = condition
        condition_type = "precipitation"
        numerical = 0
    if condition == "Freezing Drizzle/Freezing Rain":
        condition_short = "frdrizrain"
        condition_long = condition
        condition_type = "precipitation"
        numerical = 0
    if condition == "Sky Coverage Increasing":
        condition_short = "skycovinc"
        condition_long = condition
        condition_type = "air"
        numerical = 0
    if condition == "Light Freezing Rain":
        condition_short = "lghtfrizrain"
        condition_long = condition
        condition_type = "precipitation"
        numerical = 0
    if condition == "Drizzle":
        condition_short = "drzl"
        condition_long = condition
        condition_type = "precipitation"
        numerical = 0
    if condition == "Fog":
        condition_short = "fog"
        condition_long = condition
        condition_type = "precipitation"
        numerical = 0
    if condition == "Light Drizzle":
        condition_short = "lghtdrzl"
        condition_long = condition
        condition_type = "precipitation"
        numerical = 0
    if condition == "Clear":
        condition_short = "clr"
        condition_long = condition
        condition_type = "clouds"
        numerical = 0
    if condition == "Heavy Rain And Snow":
        condition_short = "clr"
        condition_long = condition
        condition_type = "precipitation"
        numerical = 0
    if condition == "Rain Showers":
        condition_short = "rainshwrs"
        condition_long = condition
        condition_type = "precipitation"
        numerical = 0
    if condition == "Dust storm":
        condition_short = "dststrm"
        condition_long = condition
        condition_type = "air"
        numerical = 0
    if condition == "Light Freezing Drizzle/Freezing Rain":
        condition_short = "lghtfrizdrzlandfrizrain"
        condition_long = condition
        condition_type = "precipitation"
        numerical = 0
    if condition == "Precipitation In Vicinity":
        condition_short = "prcptinvcn"
        condition_long = condition
        condition_type = "precipitation"
        numerical = 0
    if condition == "Snow And Rain Showers":
        condition_short = "snwnrainshwrs"
        condition_long = condition
        condition_type = "precipitation"
        numerical = 0
    if condition == "Ice":
        condition_short = "ice"
        condition_long = condition
        condition_type = "air"
        numerical = 0
    if condition == "Light Drizzle/Rain":
        condition_short = "lghtdrizlnrain"
        condition_long = condition
        condition_type = "air"
        numerical = 0
    if condition == "Sky Coverage Decreasing":
        condition_short = "skycovdec"
        condition_long = condition
        condition_type = "air"
        numerical = 0
    if condition == "wind_direction":
        condition_short = "wdir"
        condition_long = "wind direction"
        condition_type = "wind"
        numerical = 1
    if condition == "cloudcover":
        condition_short = "cloudcvr"
        condition_long = "cloud cover"
        condition_type = "clouds"
        numerical = 1
    if condition == "minimum_temperature":
        condition_short = "mint"
        condition_long = "minimum temperature"
        condition_type = "air"
        numerical = 1
    if condition == "precipitation":
        condition_short = "perc"
        condition_long = "precipitation"
        condition_type = "precipitation"
        numerical = 1
    if condition == "solar_radiation":
        condition_short = "soalrrad"
        condition_long = "solar radiation"
        condition_type = "solar"
        numerical = 1
    if condition == "dew_point":
        condition_short = "dew"
        condition_long = "dew point"
        condition_type = "air"
        numerical = 1
    if condition == "relative_humidity":
        condition_short = "rhmdty"
        condition_long = condition
        condition_type = "air"
        numerical = 1
    if condition == "temperature":
        condition_short = "temp"
        condition_long = condition
        condition_type = "air"
        numerical = 1
    if condition == "maximum_temperature":
        condition_short = "maxt"
        condition_long = "maximum temperature"
        condition_type = "air"
        numerical = 1
    if condition == "visibility":
        condition_short = condition
        condition_long = condition
        condition_type = "air"
        numerical = 1
    if condition == "wind_speed":
        condition_short = "wspd"
        condition_long = "wind speed"
        condition_type = "air"
        numerical = 1
    if condition == "solar_energy":
        condition_short = "solaren"
        condition_long = "solar energy"
        condition_type = "air"
        numerical = 1
    if condition == "sea_level_pressure":
        condition_short = "slevprs"
        condition_long = "sealevel pressure"
        condition_type = "air"
        numerical = 1
    if condition == "windchill":
        condition_short = "wndchl"
        condition_long = condition
        condition_type = "air"
        numerical = 1
    if condition == "wgust":
        condition_short = condition
        condition_long = "wind gust"
        condition_type = "air"
        numerical = 1
    if condition == "snowdepth":
        condition_short = condition
        condition_long = "snow depth"
        condition_type = "precipitation"
        numerical = 1

    return condition_short, condition_long, condition_type, numerical

# Populating energy type table with data from production table
def populate_energy_type_table(production, con):
    energy_types = []

    for record in production:
        if record[3] not in energy_types:
            energy_types.append(record[3])

    for energy_type in energy_types:
        sql = f"""INSERT INTO energy_type_DIMEN (_id, energy_type_x, energy_type_class, renewable, carbon) SELECT (SELECT COUNT(*) FROM energy_type_DIMEN) + 1, 
          \'{energy_type}\', \'{energy_type_mapper(energy_type)[0]}\', {energy_type_mapper(energy_type)[1]}, {energy_type_mapper(energy_type)[2]} 
          WHERE NOT EXISTS (SELECT 1 FROM energy_type_DIMEN WHERE energy_type_x = \'{energy_type}\');"""
        
        con.execute(sql)
    
    print("energy_type_DIMEN table populated with data")

# Mapping missing values to energy type
def energy_type_mapper(energy_type):
    energy_type_class = ""
    renewable = 0
    carbon = 0

    if energy_type == "Biomass":
        energy_type_class = "green"
        renewable = 1
        carbon = 0
    if energy_type == "Fossil Coal-derived gas":
        energy_type_class = "fossil"
        renewable = 0
        carbon = 1
    if energy_type == "Fossil Gas":
        energy_type_class = "fossil"
        renewable = 0
        carbon = 1
    if energy_type == "Fossil Oil shale":
        energy_type_class = "fossil"
        renewable = 0
        carbon = 1
    if energy_type == "Fossil Peat":
        energy_type_class = "fossil"
        renewable = 0
        carbon = 1
    if energy_type == "Hydro Run-of-river and poundage":
        energy_type_class = "green"
        renewable = 1
        carbon = 0
    if energy_type == "Other":
        energy_type_class = "other"
        renewable = 0
        carbon = 0
    if energy_type == "Other renewable":
        energy_type_class = "green"
        renewable = 1
        carbon = 0
    if energy_type == "Solar":
        energy_type_class = "green"
        renewable = 1
        carbon = 0
    if energy_type == "Waste":
        energy_type_class = "other"
        renewable = 0
        carbon = 0
    if energy_type == "Wind Onshore":
        energy_type_class = "green"
        renewable = 1
        carbon = 0
    if energy_type == "Wind Offshore":
        energy_type_class = "green"
        renewable = 1
        carbon = 0
    if energy_type == "Hydro Pumped Storage":
        energy_type_class = "green"
        renewable = 1
        carbon = 0
    if energy_type == "Fossil Brown coal/Lignite":
        energy_type_class = "fossil"
        renewable = 0
        carbon = 1
    if energy_type == "Fossil Hard coal":
        energy_type_class = "fossil"
        renewable = 0
        carbon = 1
    if energy_type == "Fossil Oil":
        energy_type_class = "fossil"
        renewable = 0
        carbon = 1
    if energy_type == "Hydro Water Reservoir":
        energy_type_class = "green"
        renewable = 1
        carbon = 0
    if energy_type == "Geothermal":
        energy_type_class = "green"
        renewable = 1
        carbon = 0
    if energy_type == "Marine":
        energy_type_class = "green"
        renewable = 1
        carbon = 0
    if energy_type == "Nuclear":
        energy_type_class = "other"
        renewable = 0
        carbon = 0
    
    return energy_type_class, renewable, carbon

# Populating bidding zone table with required data
def populate_bidding_zone_table(con):
    bzn_to_country = {
        "EE": "Estonia", "LV": "Latvia", "LT": "Lithuania", "FI": "Finland",
        "SE1": "Sweden", "SE2": "Sweden", "SE3": "Sweden", "SE4": "Sweden", 
        "NO1": "Norway", "NO2": "Norway", "NO3": "Norway", "NO4": "Norway", "NO5": "Norway",
        "DK1": "Denmark", "DK2": "Denmark", "DE-LU": "Germany", "PL": "Poland"
    }

    for bzn in bzn_to_country:
        country = bzn_to_country[bzn]
        print(country)
        resolution = 60

        if country == "Poland" or country == "Lithuania" or country == "Germany" or country == "Finland":
            resolution = 15

        sql = f"""
            INSERT INTO bidding_zone_DIMEN (_id, bzn, country, eu_member, data_resolution, geometry) SELECT (SELECT COUNT(*) FROM bidding_zone_DIMEN) + 1, '{bzn}', '{country}', 1, {resolution}, 'geom' 
            WHERE NOT EXISTS (SELECT 1 FROM bidding_zone_DIMEN WHERE bzn = '{bzn}');
        """
        
        con.execute(sql)

    print("energy_type_DIMEN table populated with data")

'''
DAG definitions and running order
'''
# Define the DAG
with DAG(
    dag_id="starschema_transform",  # Name of the DAG
    default_args={
        'owner': 'airflow',
        'start_date': datetime(2024, 12, 7),  # Starting date of the DAG
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # Use timedelta here
    },
    schedule_interval='@daily',  # This will run the DAG once a day
    catchup=False
) as dag:
    create_tables_task = PythonOperator(
        task_id = 'create_tables_task',
        python_callable = create_tables
    )

    query_data_task = PythonOperator(
        task_id = 'query_data_task',
        python_callable = get_data
    )

    # Define task dependencies (order of execution)
    create_tables_task >>  query_data_task