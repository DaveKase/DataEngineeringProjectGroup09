from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Define the DAG
with DAG(
    dag_id="dbt_weather_clean",
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "start_date": datetime(2023, 10, 1),
    },
    schedule_interval="@daily",
    catchup=False,
) as dag:
    
    # Task: Run DBT model
    run_dbt_task = BashOperator(
        task_id="clean_weather_dbt_model",
        bash_command="docker exec -i dbt dbt run --models weather_cleaned",  # Run the DBT command inside the dbt container
        dag=dag,
    )

    run_dbt_task
