from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import duckdb
import os

# Paths and connection details
DUCKDB_FILE = '/mnt/tmp/duckdb_data/consumption.duckdb'
PARQUET_BASE_DIR = '/mnt/tmp/warehouse/consumption/'  # Base directory for Parquet files

def load_latest_parquet_to_duckdb(**kwargs):
    print("Processing the latest Parquet file.")  # Debug statement

    # Ensure the base directory exists
    if not os.path.exists(PARQUET_BASE_DIR):
        raise FileNotFoundError(f"Base directory does not exist: {PARQUET_BASE_DIR}")

    # List all Parquet files in the base directory recursively
    all_parquet_files = []
    for root, _, files in os.walk(PARQUET_BASE_DIR):
        for file in files:
            if file.endswith('.parquet'):
                all_parquet_files.append(os.path.join(root, file))

    print(f"Found Parquet files: {all_parquet_files}")  # Debug statement
    if not all_parquet_files:
        raise FileNotFoundError(f"No Parquet files found in the directory: {PARQUET_BASE_DIR}")

    # Find the latest Parquet file based on modification time
    latest_file = max(all_parquet_files, key=os.path.getmtime)
    print(f"Loading data from the latest file: {latest_file}")  # Debug statement

    # Create DuckDB connection, create the file if it doesn't exist
    if not os.path.exists(DUCKDB_FILE):
        print(f"Creating DuckDB file at: {DUCKDB_FILE}")  # Debug statement
    con = duckdb.connect(DUCKDB_FILE)

    # Ensure the table exists
    table_name = "consumption_data"
    print(f"Ensuring table {table_name} exists.")  # Debug statement
    con.execute(f"""
    CREATE TABLE IF NOT EXISTS {table_name} AS 
    SELECT * FROM read_parquet('{latest_file}')
    LIMIT 0;  -- Create an empty table with the same schema
    """)
    
    # Insert new data into the table
    print(f"Inserting data from {latest_file} into {table_name}.")  # Debug statement
    con.execute(f"""
    INSERT INTO {table_name}
    SELECT * FROM read_parquet('{latest_file}');
    """)
    print(f"Data inserted into {table_name}.")  # Debug statement

    con.close()

# Airflow DAG definition
with DAG(
    dag_id="iceberg_consumption_to_duckdb",
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "start_date": datetime(2023, 10, 1),
    },
    schedule_interval="@daily",
    catchup=False,
) as dag:
    
    load_data_task = PythonOperator(
        task_id="iceberg_consumption_to_duckdb",
        python_callable=load_latest_parquet_to_duckdb,
    )
