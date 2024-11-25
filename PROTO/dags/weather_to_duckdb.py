from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import duckdb
import os

# Paths and connection details
DUCKDB_FILE = '/mnt/tmp/duckdb_data/weather.duckdb'
PARQUET_BASE_DIR = '/mnt/tmp/warehouse/weather/'  # Base directory for Parquet files
COUNTRIES = ['estonia', 'latvia']  # List of countries to process

def load_data_to_duckdb(country, **kwargs):
    print(f"Processing data for country: {country}")  # Debug statement
    
    # Define the path for the country's Parquet files
    parquet_dir = os.path.join(PARQUET_BASE_DIR, country, 'data')
    print(f"Checking files in: {parquet_dir}")  # Debug statement
    
    # List files in the directory
    if not os.path.exists(parquet_dir):
        raise FileNotFoundError(f"Directory does not exist: {parquet_dir}")

    files = [f for f in os.listdir(parquet_dir) if f.endswith('.parquet')]
    print(f"Found files: {files}")  # Debug statement
    if not files:
        raise FileNotFoundError(f"No Parquet files found for country: {country}.")
    
    # Get the latest file
    latest_file = max(files, key=lambda f: os.path.getmtime(os.path.join(parquet_dir, f)))
    latest_file_path = os.path.join(parquet_dir, latest_file)
    print(f"Loading data from the latest file: {latest_file_path}")  # Debug statement

    # Create DuckDB connection, create the file if it doesn't exist
    if not os.path.exists(DUCKDB_FILE):
        print(f"Creating DuckDB file at: {DUCKDB_FILE}")  # Debug statement
    con = duckdb.connect(DUCKDB_FILE)

    # Ensure the table exists
    table_name = f"weather_{country}"
    print(f"Ensuring table {table_name} exists.")  # Debug statement
    con.execute(f"""
    CREATE TABLE IF NOT EXISTS {table_name} AS 
    SELECT * FROM read_parquet('{latest_file_path}')
    LIMIT 0;  -- Create an empty table with the same schema
    """)
    
    # Insert new data into the table
    print(f"Inserting data from {latest_file_path} into {table_name}.")  # Debug statement
    con.execute(f"""
    INSERT INTO {table_name}
    SELECT * FROM read_parquet('{latest_file_path}');
    """)
    print(f"Data inserted into {table_name}.")  # Debug statement
    
    con.close()

# Airflow DAG definition
with DAG(
    dag_id="iceberg_to_duckdb",
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "start_date": datetime(2023, 10, 1),
    },
    schedule_interval="@daily",
    catchup=False,
) as dag:
    
    last_task = None

    for country in COUNTRIES:
        load_data_task = PythonOperator(
            task_id=f"load_data_to_duckdb_{country}",
            python_callable=load_data_to_duckdb,
            op_kwargs={"country": country},
        )

        if last_task:
            last_task >> load_data_task  # Set dependency
        last_task = load_data_task  # Update last_task for the next iteration

