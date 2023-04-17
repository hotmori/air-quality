from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime
from airflow.models import Variable
import requests, json


def get_db_connection():
    pg_hook = PostgresHook(postgre_conn_id = "postgres_default")
    connection = pg_hook.get_conn()
    return connection

def get_cities_with_empty_coordinates():
    request = "select city_id, name, country \
                 from staging.cities c \
                where not exists (select null \
                                    from staging.cities_coordinates cc \
                                   where cc.city_id = c.city_id) \
    "
    connection = get_db_connection()
    cursor = connection.cursor()
    cursor.execute(request)
   
    cities = cursor.fetchall()
    cursor.close()
    connection.close()
    print("cities: ", cities)
    return cities

def get_cities_coordinates(**kwargs):
    ti = kwargs['ti']
    cities_with_empty_coordinates = ti.xcom_pull(key='return_value', task_ids='get_cities_with_empty_coordinates')
    cities_with_coordinates = {}
    for city in cities_with_empty_coordinates:
        city_id = city[0]
        name = city[1]
        country = city[2]    
        coordinates = get_city_coordinates(name,country)
        print("city: ", city, "coordinates: ", coordinates)
        cities_with_coordinates[city_id] = coordinates
    return cities_with_coordinates

def get_city_coordinates(city, country):
    print("city_name: ", city, "country_name: ", country)
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

def generate_inserts(cities_coordinates_data):
    sql_ins = """insert into staging.cities_coordinates(city_id, \
                                                longitude, \
                                                latitude \
                                                ) """
    sql_vals = ""
    for i, city_coord_data in enumerate(cities_coordinates_data):

        sql_vals += (',' if i > 0 else '') + f'({city_coord_data},  \
                       {cities_coordinates_data[city_coord_data]["longitude"]}, \
                       {cities_coordinates_data[city_coord_data]["latitude"]} \
                       )\n'
    
    result_sql = f'{sql_ins} values {sql_vals}; commit;'
    result_sql = " ".join(result_sql.split())
    print ("result_sql: ", result_sql)
    return result_sql

def save_cities_coordinates_data(**kwargs):
    ti = kwargs['ti']
    cities_coordinates_data = ti.xcom_pull(key='return_value', task_ids='get_cities_coordinates')
    if cities_coordinates_data:
        print("cities_coordinates_data: ", cities_coordinates_data)
        sql_inserts = generate_inserts(cities_coordinates_data)
        connection = get_db_connection()
        cursor = connection.cursor()
        cursor.execute(sql_inserts)
        cursor.close()
        connection.close()


with DAG(dag_id="staging_cities",
         start_date=datetime(2021,1,1),
         schedule_interval="*/5 * * * *",
         catchup=False) as dag:
    
    task_update_cities = PostgresOperator(task_id = "update_cities",
                                          postgres_conn_id="postgres_default",
                                          sql = "sql/dml_cities.sql")
    
    task_get_cities_with_empty_coordinates = PythonOperator(
        task_id="get_cities_with_empty_coordinates",
        python_callable=get_cities_with_empty_coordinates)

    task_get_cities_coordinates = PythonOperator(
        task_id="get_cities_coordinates",
        python_callable=get_cities_coordinates)
    
    task_save_cities_coordinates_data  = PythonOperator(
        task_id="save_cities_coordinates_data",
        python_callable=save_cities_coordinates_data )
    


task_update_cities >>  task_get_cities_with_empty_coordinates >> task_get_cities_coordinates >> task_save_cities_coordinates_data