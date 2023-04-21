from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from common_package.common_module import run_select \

def load_all_cities_air_data():
    call_func_sql = "select transform.load_data();"
    
    result_row_counts = run_select(call_func_sql)[0][0]
    print(result_row_counts)

with DAG(dag_id="transform_cities_air_data",
         start_date=datetime(2021,1,1),
         schedule_interval="15 * * * *",
         catchup=False) as dag:
    
    task_load_all_cities_air_data = PythonOperator(
        task_id="load_all_cities_air_data",
        python_callable=load_all_cities_air_data)

task_load_all_cities_air_data
