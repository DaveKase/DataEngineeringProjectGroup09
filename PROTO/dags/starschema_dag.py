'''
This DAG should take data from 4 DuckDB tables and transform them into StarSchema and save it back to DuckDB
'''

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta  # Add timedelta import
import duckdb
import os

# Basically router for create_tables_task. Now we have all table creations under one DAG, instead of 6 different DAGS.
def create_tables():
    print("creating tables")
    create_measurements_fact_table()
    create_bz_dimen_table()
    create_energy_type_dimen_table()
    create_weather_condition_dimen_table()
    create_unit_dimen_table()
    create_timestamp_dimen_table()
    print("Tables created successfully")

# Takes the table name and the sql query as inputs and creates table in DuckDB
def table_creation_script(table_name, sql):
    print(f"Creating table {table_name}")
    duckdb_file = '/mnt/tmp/duckdb_data/' + table_name + '.duckdb'

    if not os.path.exists(duckdb_file):
        print(f"Creating DuckDB file at: {duckdb_file}")  # Debug statement
    
    con = duckdb.connect(duckdb_file)

    # Ensure the table exists
    print(f"Ensuring table {table_name} exists.")  # Debug statement
    con.execute(sql)
    con.close()
    print(f"Table {table_name} created or exists")

'''
Creating FACT table
'''

# Creating the fact table (measurements)
def create_measurements_fact_table():
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
    
    table_creation_script(table_name=table_name, sql=sql)

'''
Creating the dimension tables (all 5 of them)
'''

# Creating bidding zone dimension table
def create_bz_dimen_table():
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
    
    table_creation_script(table_name=table_name, sql=sql)

# Creating energy type dimension table
def create_energy_type_dimen_table():
    table_name = "energy_type_DIMEN"
    sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
          _id BIGINT PRIMARY KEY,
          energy_type_x VARCHAR NOT NULL,
          energy_type_class VARCHAR NOT NULL,
          renewable INTEGER NOT NULL,
          carbon INTEGER NOT NULL
        )"""

    table_creation_script(table_name=table_name, sql=sql)

# Creating weather condition dimension table
def create_weather_condition_dimen_table():
    table_name = "weather_condition_DIMEN"
    sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
          _id BIGINT PRIMARY KEY,
          condition_short VARCHAR NOT NULL,
          condition_long VARCHAR NOT NULL,
          condition_type VARCHAR NOT NULL,
          numerical INTEGER NOT NULL
        )"""
    
    table_creation_script(table_name=table_name, sql=sql)

# Creating unit dimension table
def create_unit_dimen_table():
    table_name = "unit_DIMEN"
    sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
          _id BIGINT PRIMARY KEY,
          unit_short VARCHAR NOT NULL,
          unit_long VARCHAR NOT NULL,
          unit_class VARCHAR NOT NULL
        )"""
    
    table_creation_script(table_name=table_name, sql=sql)

# Creating timestamp dimension table
def create_timestamp_dimen_table():
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
    
    table_creation_script(table_name=table_name, sql=sql)

'''
Data selection and preparation
'''

# Querying data from tables
def get_data():
  print("querying data")

  duckdb_file = '/mnt/tmp/duckdb_data/weather.duckdb'
  con = duckdb.connect(duckdb_file)
  sql = "select * from weather_data;"
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
    populate_units_table(consumption, price, production)

# Populates the units_dimen table with units across other tables
def populate_units_table(consumption, price, production):
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

    for unit in units:
        unit_class = ""
        unit_long = ""

        if unit == "MAW":
            unit_class = "electricity"
            unit_long = "MAW"
        elif unit == "MWH":
            unit_class = "electricity"
            unit_long = "megawatt-hours"
        elif unit == "EUR":
            unit_class = "price"
            unit_long = "Euro"

        sql = f"""INSERT INTO unit_DIMEN (_id, unit_short, unit_long, unit_class) SELECT (SELECT COUNT(*) FROM unit_DIMEN) + 1, 
          \'{unit}\', \'{unit_long}\', \'{unit_class}\' WHERE NOT EXISTS (SELECT 1 FROM unit_DIMEN WHERE unit_short = \'{unit}\');"""

        duckdb_file = '/mnt/tmp/duckdb_data/unit_DIMEN.duckdb'
        con = duckdb.connect(duckdb_file)
        con.execute(sql)
        con.close()
    
    print("units_DIMEN table populated with data")

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
        task_id = 'create_tables',
        python_callable = create_tables
    )

    query_data_task = PythonOperator(
        task_id = 'query_data_task',
        python_callable = get_data
    )

    # Define task dependencies (order of execution)
    create_tables_task >>  query_data_task