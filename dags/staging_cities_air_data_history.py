from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.models import Variable
import requests, json
from common_package.common_module import run_select, \
                                         get_openweather_key, \
                                         BASE_URL_OPENWEATHER_HISTORY

def finalize():
    print("All is done")

def get_cities_air_data_missed_ranges():
    missed_ranges = run_select("select vr.city_id, vr.min_ts_hour, vr.max_ts_hour, vc.longitude, vc.latitude  \
                                from staging.vcities_air_data_missed_ranges vr \
                                join staging.vcities_coordinates vc on vc.city_id = vr.city_id\
                                order by vr.city_id")
    print ("missed_ranges", missed_ranges)
    result_missed_ranges = {}
    for range in missed_ranges:
        city_id = range[0]
        dt_start = range[1].strftime("%d/%m/%Y %H:%M:%S")
        dt_end = range[2].strftime("%d/%m/%Y %H:%M:%S")
        ux_start = range[1].timestamp()        
        ux_end = range[2].timestamp()
        longitude = range[3]
        latitude = range[4]

        print(f'city_id: {city_id}, start: {dt_start}, end: {dt_end}')
        result_missed_ranges[city_id]={"ux_start": ux_start,
                                       "ux_end": ux_end,
                                       "longitude": longitude,
                                       "latitude": latitude}

    print ("result: ", result_missed_ranges)
    return result_missed_ranges


def get_city_history_data(city_id, ux_start, ux_end, latitude, longitude):
    base_url = BASE_URL_OPENWEATHER_HISTORY
    key = get_openweather_key()



    request_url = f'{base_url}lat={latitude}&lon={longitude}&appid={key}'
    response = requests.get(request_url)
    status_code = response.status_code
    if status_code != 200:
        raise ValueError('Openweather API connection error status code:', status_code, " message: ", response)

    json_data = response.json()

    metric_list = json_data["list"]
    aqi = metric_list[0]["main"]["aqi"]
    components =  metric_list[0]["components"]
    component_co = components["co"]
    component_no = components["no"]
    component_no2 = components["no2"]
    component_o3 = components["o3"]
    component_so2 = components["so2"]
    component_pm2_5 = components["pm2_5"]
    component_pm10 = components["pm10"]
    component_nh3 = components["nh3"]
    ux_timestamp = metric_list[0]["dt"]    
    return None

def get_history_data(**kwargs):
    ti = kwargs['ti']
    ranges = ti.xcom_pull(key='return_value', task_ids='get_cities_air_data_missed_ranges')


    return None

with DAG(dag_id="staging_cities_air_data_history",
         start_date=datetime(2021,1,1),
         schedule_interval="10 * * * *",
         catchup=False) as dag:
    
    task_get_cities_air_data_missed_ranges = PythonOperator(
        task_id="get_cities_air_data_missed_ranges",
        python_callable=get_cities_air_data_missed_ranges)
    
    task_get_history_data = PythonOperator(
        task_id="get_history_data",
        python_callable=get_history_data)    

    task_finalize = PythonOperator(
        task_id="finalize",
        python_callable=finalize)
    
task_get_cities_air_data_missed_ranges >> task_finalize