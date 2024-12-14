from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import shutil
import os

# Function to clear data directories
def clear_data_directories():
    """
    Deletes all files and folders inside specified directories except `.gitkeep` files.
    """
    directories_to_clear = [
        "/mnt/tmp/duckdb_data/",
        "/mnt/tmp/warehouse/weather/",
        "/mnt/tmp/warehouse/price/",  # Add production and consumption directories as well when they are present
    ]

    for directory in directories_to_clear:
        if os.path.exists(directory):
            for root, dirs, files in os.walk(directory):
                for file in files:
                    if file != ".gitkeep":  # Keep `.gitkeep` files
                        file_path = os.path.join(root, file)
                        os.remove(file_path)
                for dir_name in dirs:
                    dir_path = os.path.join(root, dir_name)
                    shutil.rmtree(dir_path)
            print(f"Cleared directory: {directory}")
        else:
            print(f"Directory does not exist: {directory}")


# Define the DAG
with DAG(
    dag_id="MASTER_DAG",
    default_args={
        "owner": "airflow",
        "start_date": datetime(2024, 12, 1),
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    schedule_interval=None,  # Run manually or trigger as needed
    catchup=False,
) as dag:

    # Task 1: Clear data directories
    clear_directories_task = PythonOperator(
        task_id="clear_data_directories",
        python_callable=clear_data_directories,
    )

    # Task 2: Trigger dependent DAGs
    trigger_dag1 = TriggerDagRunOperator(
        task_id="trigger_weather_to_iceberg",
        trigger_dag_id="weather_to_iceberg",  
        wait_for_completion=True,
        execution_date="{{ ts }}"  # Automatically gets the current execution timestamp
    )

    trigger_dag2 = TriggerDagRunOperator(
        task_id="trigger_price_to_iceberg",
        trigger_dag_id="price_to_iceberg",  
        wait_for_completion=True,
        execution_date="{{ ts }}" 
    )

    trigger_dag3 = TriggerDagRunOperator(
        task_id="trigger_weather_to_duckdb",
        trigger_dag_id="iceberg_weather_to_duckdb",  
        wait_for_completion=True,
        execution_date="{{ ts }}" 
    )
    
    trigger_dag4 = TriggerDagRunOperator(
        task_id="trigger_price_to_duckdb",
        trigger_dag_id="iceberg_price_to_duckdb",  
        wait_for_completion=True,
        execution_date="{{ ts }}" 
    )
    
    trigger_dag5 = TriggerDagRunOperator(
        task_id="transform_weather",
        trigger_dag_id="dbt_weather_clean",  
        wait_for_completion=True,
        execution_date="{{ ts }}" 
    )

    # Define the execution order
    # clear_directories_task >> [trigger_dag1, trigger_dag2] >> [trigger_dag3, trigger_dag4] >> trigger_dag5
    clear_directories_task >> trigger_dag1
    clear_directories_task >> trigger_dag2
    trigger_dag1 >> trigger_dag3
    trigger_dag2 >> trigger_dag4
    trigger_dag3 >> trigger_dag5
    trigger_dag4 >> trigger_dag5