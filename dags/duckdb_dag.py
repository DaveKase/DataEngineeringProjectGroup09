from airflow.decorators import dag, task
from pendulum import datetime
import duckdb
import pandas as pd

@dag(start_date=datetime(2023, 1, 1), schedule_interval=None, catchup=False)
def duckdb_dag():
    @task
    def create_table():
        conn = duckdb.connect('my_database.db')
        df = pd.DataFrame({'name': ['Alice', 'Bob'], 'age': [25, 30]})
        conn.execute("CREATE TABLE IF NOT EXISTS people AS SELECT * FROM df")
        conn.close()

    create_table()

duckdb_dag()
