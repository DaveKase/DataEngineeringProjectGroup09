from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Define the DAG
with DAG(
    dag_id="dbt_add_bzn_codes",
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "start_date": datetime(2023, 10, 1),
    },
    schedule_interval="@daily",
    catchup=False,
) as dag:
    
    # Task: Run DBT models
    run_consumption_cleaned = BashOperator(
        task_id="run_consumption_cleaned_model",
        bash_command="docker exec -i dbt dbt run --models consumption_cleaned",
    )

    run_price_cleaned = BashOperator(
        task_id="run_price_cleaned_model",
        bash_command="docker exec -i dbt dbt run --models price_cleaned",
    )

    run_production_cleaned = BashOperator(
        task_id="run_production_cleaned_model",
        bash_command="docker exec -i dbt dbt run --models production_cleaned",
    )

    # Define task dependencies
    run_consumption_cleaned >> run_price_cleaned >> run_production_cleaned
