from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import duckdb
import os

def combine_duckdb_databases():
    """
    Combines all DuckDB databases into a single database, handling only tables without alias prefixes.
    """
    # Define database paths
    db_dir = '/mnt/tmp/duckdb_data'
    combined_db_path = os.path.join(db_dir, 'combined_data.duckdb')

    # Ensure the directory exists
    os.makedirs(db_dir, exist_ok=True)

    # Initialize the combined database
    print(f"Initializing combined database at {combined_db_path}")
    conn = duckdb.connect(combined_db_path)

    # List of DuckDB databases to combine
    databases = [
        ('consumption.duckdb', 'consumption_data'),
        ('price.duckdb', 'price_data'),
        ('production.duckdb', 'production_data'),
        ('weather.duckdb', 'weather_data')  # Weather only processes the table now
    ]

    for db_info in databases:
        db = db_info[0]
        db_path = os.path.join(db_dir, db)
        alias = db.split('.')[0]  # Use the database name (e.g., 'consumption') as alias

        if os.path.exists(db_path):
            # Attach the existing database
            print(f"Attaching database: {db_path}")
            conn.execute(f"ATTACH DATABASE '{db_path}' AS {alias}")

            # Handle the main table
            main_table = db_info[1]
            if main_table:
                print(f"Copying table: {main_table} from {alias} into combined database")
                conn.execute(f"""
                CREATE OR REPLACE TABLE {main_table} AS 
                SELECT * FROM {alias}.{main_table};
                """)
        else:
            print(f"Database not found: {db_path}")

    conn.close()
    print("Combination of DuckDB databases completed.")

# Define the Airflow DAG
with DAG(
    dag_id="combine_duckdb_databases",
    default_args={
        "owner": "airflow",
        "start_date": datetime(2024, 1, 1),
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    schedule_interval=None,
    catchup=False,
) as dag:

    combine_task = PythonOperator(
        task_id="combine_duckdb_databases",
        python_callable=combine_duckdb_databases,
    )
