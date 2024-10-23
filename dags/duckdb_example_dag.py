# from airflow.decorators import dag, task
# from pendulum import datetime
# import duckdb
# import pandas as pd

# @dag(start_date=datetime(2023, 1, 1), schedule_interval=None, catchup=False)
# def duckdb_example_dag():
#     @task
#     def create_pandas_df():
#         data = {"colors": ["blue", "red", "yellow"], "numbers": [2, 3, 4]}
#         return pd.DataFrame(data)

#     @task
#     def write_to_duckdb(df):
#         conn = duckdb.connect('/data/my_duckdb.db')
#         conn.execute("CREATE TABLE IF NOT EXISTS my_table AS SELECT * FROM df")

#     df = create_pandas_df()
#     write_to_duckdb(df)

# duckdb_example_dag()
