from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.models import Variable
import requests, json
from common_package.common_module import get_db_connection, BASE_URL_OPENWEATHER_CURRENT

def finalize():
    print("All is done")

def get_cities():
    request = "select c.city_id, cc.longitude, cc.latitude \
              from staging.vcities c \
              join staging.cities_coordinates cc \
                on c.city_id = cc.city_id \
              "
    connection = get_db_connection()
    cursor = connection.cursor()
    cursor.execute(request)
   
    cities = cursor.fetchall()
    cursor.close()
    connection.close()
    city_dict = {}

    for city in cities:
        city_dict[city[0]] = {"longitude": city[1], "latitude": city[2]}

    return city_dict

def get_city_air_data(latitude, longitude):
    base_url = BASE_URL_OPENWEATHER_CURRENT
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

def get_cities_air_data():
    cities = get_cities()
    cities_air_data = {}
    for city in cities:
        print("city_id: ", city, "data: ", cities[city], "lon:", cities[city]["longitude"])
        city_air_data = get_city_air_data(latitude = cities[city]["latitude"], longitude=cities[city]["longitude"] )
        cities_air_data[city] = city_air_data
        
    print("cities_air_data", cities_air_data)
    return cities_air_data

def save_cities_air_data(**kwargs):
    ti = kwargs['ti']
    cities_air_data = ti.xcom_pull(key='return_value', task_ids='get_cities_air_data')
    print("cities_air_data: ", cities_air_data)
    sql_inserts = generate_inserts(cities_air_data)
    connection = get_db_connection()
    cursor = connection.cursor()
    cursor.execute(sql_inserts)
    cursor.close()
    connection.close()


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
    
    result_sql = f'{sql_ins} values {sql_vals}; commit;'
    result_sql = " ".join(result_sql.split())
    print ("result_sql: ", result_sql)
    return result_sql

with DAG(dag_id="staging_cities_air_data",
         start_date=datetime(2021,1,1),
         schedule_interval="5 * * * *",
         catchup=False) as dag:

    task_get_cities_air_data = PythonOperator(
        task_id="get_cities_air_data",
        python_callable=get_cities_air_data)
    
    task_save_cities_air_data = PythonOperator(
        task_id="save_cities_air_data",
        python_callable=save_cities_air_data)

    task_finalize = PythonOperator(
        task_id="finalize",
        python_callable=finalize)

task_get_cities_air_data >> task_save_cities_air_data >> task_finalize
