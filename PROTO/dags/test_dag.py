from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta  # Add timedelta import

# Simple function for testing
def print_hello():
    print("Hello, Airflow!")

def print_goodbye():
    print("Goodbye, Airflow!")

# Define the DAG
with DAG(
    dag_id="test_dag",  # Name of the DAG
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

# Define task dependencies (order of execution)
hello_task >> goodbye_task