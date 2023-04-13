from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime
from airflow.models import Variable
import requests, json

def finalize():
    print("All is done")
        

def get_cities():
    request = "select city_id, longitude, latitude from staging.cities"
    pg_hook = PostgresHook(postgre_conn_id = "postgres_default")
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(request)
    cities = cursor.fetchall()
    city_dict = {}

    for city in cities:
        city_dict[city[0]] = {"longitude": city[1], "latitude": city[2]}

    return city_dict

def get_city_air_data(latitude, longitude):
    base_url = 'http://api.openweathermap.org/data/2.5/air_pollution?'
    key = Variable.get("openweather_k")
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

    city_air_data = {
                   "ux_timestamp": ux_timestamp,
                   "aqi": aqi,
                   "component_co": component_co,
                   "component_no" : component_no,
                   "component_no2" : component_no2,
                   "component_o3" : component_o3,
                   "component_so2" : component_so2,
                   "component_pm2_5" : component_pm2_5,
                   "component_pm10" : component_pm10,
                   "component_nh3" : component_nh3}

    return city_air_data

def get_openweather_data():
    cities = get_cities()
    cities_air_data = {}
    for city in cities:
        print("city_id: ", city, "data: ", cities[city], "lon:", cities[city]["longitude"])
        city_air_data = get_city_air_data(latitude = cities[city]["latitude"], longitude=cities[city]["longitude"] )
        cities_air_data[city] = city_air_data
        
    print("cities_air_data", cities_air_data)


    sql_inserts = generate_inserts(cities_air_data)
    return sql_inserts

def generate_inserts(cities_air_data):
    sql_ins = """insert into staging.cities_air(city_id, \
                                                ts, \
                                                aqi, \
                                                co, \
                                                no, \
                                                no2, \
                                                o3, \
                                                pm2_5, \
                                                pm10, \
                                                nh3 \
                                                ) """
    sql_vals = ""
    for i, city_air_data in enumerate(cities_air_data):

        sql_vals += (',' if i > 0 else '') + f'({city_air_data},  \
                       to_timestamp({cities_air_data[city_air_data]["ux_timestamp"]}), \
                       {cities_air_data[city_air_data]["aqi"]}, \
                       {cities_air_data[city_air_data]["component_co"]}, \
                       {cities_air_data[city_air_data]["component_no"]}, \
                       {cities_air_data[city_air_data]["component_no2"]}, \
                       {cities_air_data[city_air_data]["component_o3"]}, \
                       {cities_air_data[city_air_data]["component_pm2_5"]},\
                       {cities_air_data[city_air_data]["component_pm10"]},\
                       {cities_air_data[city_air_data]["component_nh3"]}\
                       )\n'
    
    result_sql = f'{sql_ins} values {sql_vals};'
    result_sql = " ".join(result_sql.split())
    print ("result_sql: ", result_sql)


    pg_hook = PostgresHook(postgre_conn_id = "postgres_default")
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(result_sql)
    return result_sql

with DAG(dag_id="load_staging_data",
         start_date=datetime(2021,1,1),
         schedule_interval="5 * * * *",
         catchup=False) as dag:

    task_get_openweather_data = PythonOperator(
        task_id="get_openweather_data",
        python_callable=get_openweather_data)

    task_connect_postgres_db = PostgresOperator(task_id = "connect_postgres_db",
                                                postgres_conn_id="postgres_default",
                                                sql = "SELECT 1 x;")

    task_finalize = PythonOperator(
        task_id="finalize",
        python_callable=finalize)

task_get_openweather_data >> task_connect_postgres_db #>> task_save_data
