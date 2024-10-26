from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import duckdb
import os

# Paths and connection details
DUCKDB_FILE = '/mnt/tmp/duckdb_data/weather.duckdb'
PARQUET_DIR = '/mnt/tmp/warehouse/weather/estonia/data/'  # Directory containing Parquet files

def load_recent_data_to_duckdb(**kwargs):
    # Print the current working directory
    current_directory = os.getcwd()
    print(f"Current working directory: {current_directory}")  # Debug statement
    
    # Define the path to check
    parquet_dir = PARQUET_DIR
    print(f"Checking files in: {parquet_dir}")  # Debug statement
    
    # List files in the directory
    if not os.path.exists(parquet_dir):
        raise FileNotFoundError(f"Directory does not exist: {parquet_dir}")

    files = [f for f in os.listdir(parquet_dir) if f.endswith('.parquet')]
    print(f"Found files: {files}")  # Debug statement
    if not files:
        raise FileNotFoundError("No Parquet files found in the directory.")
    
    # Get the latest file
    latest_file = max(files, key=lambda f: os.path.getmtime(os.path.join(parquet_dir, f)))
    latest_file_path = os.path.join(parquet_dir, latest_file)
    print(f"Loading data from the latest file: {latest_file_path}")  # Debug statement

    # Create DuckDB connection, create the file if it doesn't exist
    if not os.path.exists(DUCKDB_FILE):
        print(f"Creating DuckDB file at: {DUCKDB_FILE}")  # Debug statement
    con = duckdb.connect(DUCKDB_FILE)

    # Load data from the latest Parquet file
    print("Executing SQL to create table weather_estonia.")  # Debug statement
    con.execute(f"""
    CREATE TABLE IF NOT EXISTS weather_estonia AS 
    SELECT * FROM read_parquet('{latest_file_path}');
    """)
    print("Table weather_estonia created or already exists.")  # Debug statement
    
    # Optional: Uncomment to filter data for the last 7 days
    # end_date = datetime.now()
    # start_date = end_date - timedelta(days=7)
    # print(f"Filtering data for the last 7 days: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")  # Debug statement
    
    # filtered_query = f"""
    # CREATE TABLE IF NOT EXISTS weather_estonia_last_7_days AS
    # SELECT * FROM weather_estonia
    # WHERE Date_x20time BETWEEN '{start_date.strftime('%Y-%m-%d')}' AND '{end_date.strftime('%Y-%m-%d')}';
    # """
    
    # con.execute(filtered_query)
    # print("Table weather_estonia_last_7_days created or already exists.")  # Debug statement
    con.close()

# Airflow DAG definition
with DAG(
    dag_id="load_iceberg_to_duckdb",
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "start_date": datetime(2023, 10, 1),
    },
    schedule_interval="@daily",
    catchup=False,
) as dag:
    
    # Task: Load recent data from Iceberg to DuckDB
    load_data_task = PythonOperator(
        task_id="load_data_to_duckdb",
        python_callable=load_recent_data_to_duckdb,
    )

    load_data_task
