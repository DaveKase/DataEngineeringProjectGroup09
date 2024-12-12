'''
This DAG should take data from 4 DuckDB databases and transform them into StarSchema and save it back to DuckDB

At the moment, it's just testing if it shows up in the DAGs view and if it works at all
'''

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta  # Add timedelta import
import duckdb
import os

#DUCKDB_FILE = '/mnt/tmp/duckdb_data/measurements_fact.duckdb'

# Simple function for testing
def print_hello():
    print("Hello, Airflow!")

def print_goodbye():
    print("Goodbye, Airflow!")


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
    hello_task = PythonOperator(
        task_id='print_hello',
        python_callable=print_hello
    )

    goodbye_task = PythonOperator(
        task_id='print_goodbye',  # Corrected task_id
        python_callable=print_goodbye
    )

    create_measurements_table_task = PythonOperator(
        task_id = 'create_measurement_fact_table',
        python_callable = create_measurements_fact_table
    )

    create_bz_dimen_table_task = PythonOperator(
        task_id = 'create_bidding_zone_dimension_table',
        python_callable = create_bz_dimen_table
    )

    create_energy_type_table_task = PythonOperator(
        task_id = 'create_energy_type_dimension_table',
        python_callable = create_energy_type_dimen_table
    )

    create_weather_condition_table_task = PythonOperator(
        task_id = 'create_weather_condition_dimension_table',
        python_callable = create_weather_condition_dimen_table
    )

    create_unit_table_task = PythonOperator(
        task_id = 'create_unit_dimension_table',
        python_callable = create_unit_dimen_table
    )

    create_timestamp_table_task = PythonOperator(
        task_id = 'create_timestamp_dimension_table',
        python_callable = create_timestamp_dimen_table
    )

    # Define task dependencies (order of execution)
    hello_task >> create_measurements_table_task >> create_bz_dimen_table_task >> create_energy_type_table_task >> create_weather_condition_table_task >> create_unit_table_task >> create_timestamp_table_task >> goodbye_task