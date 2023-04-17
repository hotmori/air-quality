from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime
from airflow.models import Variable
import requests, json



def get_cities_with_missed_coordinates():
    return None

def get_city_coordinates():
    city = 'Saint Petersburg'
    country = 'Russia'
    base_url = 'https://api.api-ninjas.com/v1/geocoding?'
    key = Variable.get("ninjas_k")
    request_url = f'{base_url}city={city}&country={country}'
    response = requests.get(request_url, headers={'X-Api-Key': key})
    
    if response.status_code == requests.codes.ok:
        print(response.text)
    else:
        print("Error:", response.status_code, response.text)
        raise ValueError('Ninjas API connection error status code:', response.status_code, " message: ", response.text)

    cities_list = response.json()
    # Examples 
    #cities_list = json.loads("[{\"name\": \"Saint Petersburg\", \"latitude\": 59.938732, \"longitude\": 30.316229, \"country\": \"RU\", \"state\": \"Saint Petersburg\"}]")
    #cities_list = json.loads("[{\"name\": \"London\", \"latitude\": 51.5073219, \"longitude\": -0.1276474, \"country\": \"GB\", \"state\": \"England\"}, \
    # {\"name\": \"City of London\", \"latitude\": 51.5156177, \"longitude\": -0.0919983, \"country\": \"GB\", \"state\": \"England\"}, \
    # {\"name\": \"Chelsea\", \"latitude\": 51.4875167, \"longitude\": -0.1687007, \"country\": \"GB\", \"state\": \"England\"}, \
    # {\"name\": \"Vauxhall\", \"latitude\": 51.4874834, \"longitude\": -0.1229297, \"country\": \"GB\", \"state\": \"England\"} \
    #]")

    latitude = cities_list[0]["latitude"]
    longitude = cities_list[0]["longitude"]

    return {"latitude" : latitude, "longitude" : longitude}



with DAG(dag_id="staging_cities",
         start_date=datetime(2021,1,1),
         schedule_interval="5 * * * *",
         catchup=False) as dag:
    
    task_update_cities = PostgresOperator(task_id = "update_cities",
                                          postgres_conn_id="postgres_default",
                                          sql = "sql/dml_cities.sql")

    #task_get_city_coordinates = PythonOperator(
    #    task_id="get_city_coordinates",
    #    python_callable=get_city_coordinates)    
    


task_update_cities    