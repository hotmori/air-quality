from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.models import Variable
import requests, json
from common_package.common_module import run_select, \
                                         run_inserts, \
                                         get_openweather_key, \
                                         BASE_URL_OPENWEATHER_CURRENT

def finalize():
    print("All is done")


with DAG(dag_id="transform_cities_air_data",
         start_date=datetime(2021,1,1),
         schedule_interval="5 * * * *",
         catchup=False) as dag:

   
    task_finalize = PythonOperator(
        task_id="finalize",
        python_callable=finalize)

task_finalize
