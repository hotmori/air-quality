from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.models import Variable
import requests, json
from common_package import run_select

def finalize():
    print("All is done")

def get_cities_air_data_missed_ranges():
    missed_ranges = run_select("select min_ts_hour, max_ts_hour, city_id \
                                from vcities_air_data_missed_ranges \
                                order by city_id")
    print ("missed_ranges", missed_ranges)
    return None

with DAG(dag_id="staging_cities_air_data",
         start_date=datetime(2021,1,1),
         schedule_interval="10 * * * *",
         catchup=False) as dag:
    
    task_get_cities_air_data_missed_ranges = PythonOperator(
        task_id="get_cities_air_data_missed_ranges",
        python_callable=get_cities_air_data_missed_ranges)    

    task_finalize = PythonOperator(
        task_id="finalize",
        python_callable=finalize)
    
task_get_cities_air_data_missed_ranges >> task_finalize