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
    request = "select * from staging.cities"
    pg_hook = PostgresHook(postgre_conn_id = "postgres_default")
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(request)
    cities = cursor.fetchall()
    for city in cities:
        print("City: {0} - {1}".format(city[0], city[1]))
    return cities

def get_openweather_data():
    cities = get_cities()
    url = 'http://api.openweathermap.org/data/2.5/air_pollution?lat=59.9343&lon=30.3351'
    key = Variable.get("openweather_k")
    request_url = f'{url}&appid={key}'
    print("request_url", request_url)

    x = requests.get(request_url)
    status_code = x.status_code
    print("status_code: ",status_code)
    if status_code != 200:
        raise ValueError('Openweather API connection error status code:', status_code, " message: ", x.message)

    json_data = x.json()

    #json_data = json.loads('{"coord": {"lon": 30.3351, "lat": 59.9343}, "list": [{"main": {"aqi": 3}, "components": {"co": 894.55, "no": 81.36, "no2": 49.35, "o3": 11.27, "so2": 101.09, "pm2_5": 17.8, "pm10": 25.09, "nh3": 1.44}, "dt": 1681204196}]}')

    print("json_data: ", json_data)

    latitude = json_data["coord"]["lat"]
    longitude = json_data["coord"]["lon"]
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


    print("latitude: ", latitude)
    print("longitude: ", longitude)

    print("metric_listx: ", metric_list)
    print("aqi: ", aqi)
    print("component_co: ", component_co)
    print("component_nh3: ", component_nh3)
    print("dt: ", ux_timestamp)
    print(datetime.utcfromtimestamp(ux_timestamp).strftime('%Y-%m-%d %H:%M:%S'))
 
    openweather_result = {"ux_timestamp": ux_timestamp,
                          "latitude": latitude,
                          "longitude": longitude,
                          "aqi": aqi,
                          "component_co": component_co,
                          "component_no" : component_no,
                          "component_no2" : component_no2,
                          "component_o3" : component_o3,
                          "component_so2" : component_so2,
                          "component_pm2_5" : component_pm2_5,
                          "component_pm10" : component_pm10,
                          "component_nh3" : component_nh3
                         }

    return openweather_result

with DAG(dag_id="load_staging_data",
         start_date=datetime(2021,1,1),
         schedule_interval="*/5 * * * *",
         catchup=False) as dag:

    task_get_openweather_data = PythonOperator(
        task_id="get_openweather_data",
        python_callable=get_openweather_data)

    task_connect_postgres_db = PostgresOperator(task_id = "connect_postgres_db",
                                                postgres_conn_id="postgres_default",
                                                sql = "SELECT 1 x;")

    task_save_data = PostgresOperator(task_id = "save_data_postgres_db",
                                      postgres_conn_id="postgres_default",
                                      sql = """insert into staging.cities_air(city_id, \
                                                                        ts, \
                                                                        aqi, \
                                                                        co, \
                                                                        no, \
                                                                        no2, \
                                                                        o3, \
                                                                        pm2_5, \
                                                                        pm10, \
                                                                        nh3 \
                                                                        )
                                               values('1', \
                                                      to_timestamp('{{ ti.xcom_pull(key='return_value', task_ids='get_openweather_data')['ux_timestamp'] }}'), \
                                                                   '{{ ti.xcom_pull(key='return_value', task_ids='get_openweather_data')['aqi'] }}', \
                                                                   '{{ ti.xcom_pull(key='return_value', task_ids='get_openweather_data')['component_co'] }}', \
                                                                   '{{ ti.xcom_pull(key='return_value', task_ids='get_openweather_data')['component_no'] }}', \
                                                                   '{{ ti.xcom_pull(key='return_value', task_ids='get_openweather_data')['component_no2'] }}', \
                                                                   '{{ ti.xcom_pull(key='return_value', task_ids='get_openweather_data')['component_o3'] }}', \
                                                                   '{{ ti.xcom_pull(key='return_value', task_ids='get_openweather_data')['component_pm2_5'] }}', \
                                                                   '{{ ti.xcom_pull(key='return_value', task_ids='get_openweather_data')['component_pm10'] }}', \
                                                                   '{{ ti.xcom_pull(key='return_value', task_ids='get_openweather_data')['component_nh3'] }}' \
                                                                   );
                                            """,
                                      params = {},
                                      autocommit = True)

    task_finalize = PythonOperator(
        task_id="finalize",
        python_callable=finalize)

task_get_openweather_data >> task_connect_postgres_db >> task_save_data
