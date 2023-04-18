from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.models import Variable
import requests, json
from common_package.common_module import run_select

def finalize():
    print("All is done")

def get_cities_air_data_missed_ranges():
    missed_ranges = run_select("select city_id, min_ts_hour, max_ts_hour  \
                                from staging.vcities_air_data_missed_ranges \
                                order by city_id")
    print ("missed_ranges", missed_ranges)
    result = {}
    for range in missed_ranges:
        city_id = range[0]
        dt_start = range[1].strftime("%d/%m/%Y %H:%M:%S")
        dt_end = range[2].strftime("%d/%m/%Y %H:%M:%S")
        ux_min_ts_hour = range[1].timestamp()        
        ux_max_ts_hour = range[2].timestamp()


        print(f'city_id: {city_id}, start: {dt_start}, end: {dt_end}')
        result[city_id]={"ux_min_ts_hour": ux_min_ts_hour, "ux_max_ts_hour": ux_max_ts_hour}

    print ("result: ", result)
    return result

with DAG(dag_id="staging_cities_air_data_history",
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