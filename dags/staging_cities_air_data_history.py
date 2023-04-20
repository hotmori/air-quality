from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException
from datetime import datetime
from airflow.models import Variable
import requests, json
from common_package.common_module import run_select, \
                                         run_inserts, \
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
        ux_start = int(range[1].timestamp())
        ux_end = int(range[2].timestamp())
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
    request_url = f'{base_url}lat={latitude}&lon={longitude}&start={ux_start}&end={ux_end}&appid={key}'
    #print("request_url", request_url)
    response = requests.get(request_url)
    status_code = response.status_code
    if status_code != 200:
        raise ValueError('Openweather API connection error status code:', status_code, " message: ", response)

    json_data = response.json()

    records = json_data["list"]
    result_history_data = {}
    for rec in records:

        aqi = rec["main"]["aqi"]
        components =  rec["components"]
        component_co = components["co"]
        component_no = components["no"]
        component_no2 = components["no2"]
        component_o3 = components["o3"]
        component_so2 = components["so2"]
        component_pm2_5 = components["pm2_5"]
        component_pm10 = components["pm10"]
        component_nh3 = components["nh3"]
        ux_timestamp = rec["dt"]
        result_history_data[ux_timestamp] = {"aqi": aqi,
                                             "component_co": component_co,
                                             "component_no": component_no,
                                             "component_no2": component_no2,
                                             "component_o3": component_o3,
                                             "component_so2": component_so2,
                                             "component_pm2_5": component_pm2_5,
                                             "component_pm10": component_pm10,
                                             "component_nh3": component_nh3
                                             }
    return result_history_data

def generate_inserts(city_id, city_history_air):
    sql_ins = """insert into staging.cities_air(city_id, \
                                                ts, \
                                                aqi, \
                                                co, \
                                                no, \
                                                no2, \
                                                o3, \
                                                so2, \
                                                pm2_5, \
                                                pm10, \
                                                nh3, \
                                                source_flg \
                                                ) """
    sql_vals = ""
    #print (city_id, "city_history_air " , city_history_air)
    for i, ux_timestamp in enumerate(city_history_air):
    
        sql_vals += (',' if i > 0 else '') + f'({city_id},  \
                       to_timestamp({ux_timestamp}), \
                       {city_history_air[ux_timestamp]["aqi"]}, \
                       {city_history_air[ux_timestamp]["component_co"]}, \
                       {city_history_air[ux_timestamp]["component_no"]}, \
                       {city_history_air[ux_timestamp]["component_no2"]}, \
                       {city_history_air[ux_timestamp]["component_o3"]}, \
                       {city_history_air[ux_timestamp]["component_so2"]}, \
                       {city_history_air[ux_timestamp]["component_pm2_5"]},\
                       {city_history_air[ux_timestamp]["component_pm10"]},\
                       {city_history_air[ux_timestamp]["component_nh3"]}, \
                       \'H\'\
                       )\n'
    
    result_sql = f'{sql_ins} values {sql_vals};'
    result_sql = " ".join(result_sql.split())
    #print ("result_sql: ", result_sql)

    return result_sql

def save_city_history_data(city_id, city_air_history):
    insert_sql = generate_inserts(city_id, city_air_history)
    run_inserts(insert_sql)
    return None

def process_cities_history_data(**kwargs):
    ti = kwargs['ti']
    cities_ranges = ti.xcom_pull(key='return_value', task_ids='get_cities_air_data_missed_ranges')
    if not cities_ranges:
        raise AirflowSkipException    
    for city in cities_ranges:
        city_air_history = get_city_history_data(city_id=city, \
                                        ux_start = cities_ranges[city]["ux_start"], \
                                        ux_end = cities_ranges[city]["ux_end"], \
                                        latitude = cities_ranges[city]["latitude"], \
                                        longitude = cities_ranges[city]["longitude"]\
                                            )
        save_city_history_data(city, city_air_history)
        #print("history: ", city_air_history)



    return None

with DAG(dag_id="staging_cities_air_data_history",
         start_date=datetime(2021,1,1),
         schedule_interval="10 * * * *",
         catchup=False) as dag:
    
    task_get_cities_air_data_missed_ranges = PythonOperator(
        task_id="get_cities_air_data_missed_ranges",
        python_callable=get_cities_air_data_missed_ranges)
    
    task_process_cities_history_data = PythonOperator(
        task_id="process_cities_history_data",
        python_callable=process_cities_history_data)    

    task_finalize = PythonOperator(
        task_id="finalize",
        python_callable=finalize)
    
task_get_cities_air_data_missed_ranges >> task_process_cities_history_data >> task_finalize