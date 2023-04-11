from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime
from airflow.models import Variable
import requests

def finalize():
    print("All is done")

def get_openweather_data():
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
    #message = json_data["message"]
  

    #example:  {'coord': {'lon': 30.3351, 'lat': 59.9343}, 'list': [{'main': {'aqi': 3}, 'components': {'co': 894.55, 'no': 81.36, 'no2': 49.35, 'o3': 11.27, 'so2': 101.09, 'pm2_5': 17.8, 'pm10': 25.09, 'nh3': 1.44}, 'dt': 1681204196}]}

    print("json_data: ",json_data)
    #if message != "success":
    #    raise ValueError('ISS API response error message:', message)
    
    #latitude = json_data["iss_position"]["latitude"]
    #longitude = json_data["iss_position"]["longitude"]
    #timestamp = json_data["timestamp"]
    #iss_result = {"ux_timestamp":timestamp, "latitude": latitude, "longitude": longitude, "message": message}

    return 1
    


with DAG(dag_id="load_staging_data",
         start_date=datetime(2021,1,1),
         schedule_interval="*/5 * * * *",
         catchup=False) as dag:
    
    task_get_openweather_data = PythonOperator(
        task_id="get_openweather_data",
        python_callable=get_openweather_data)
    
   

task_get_openweather_data
