from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable

BASE_URL_OPENWEATHER_CURRENT = 'http://api.openweathermap.org/data/2.5/air_pollution?'
BASE_URL_OPENWEATHER_HISTORY = 'http://api.openweathermap.org/data/2.5/air_pollution/history?'
BASE_URL_OPENWEATHER_FORECAST = 'http://api.openweathermap.org/data/2.5/air_pollution/forecast?'
BASE_URL_NINJAS_GEO = 'https://api.api-ninjas.com/v1/geocoding?'


def get_db_connection():
    pg_hook = PostgresHook(postgre_conn_id = "postgres_default")
    connection = pg_hook.get_conn()
    return connection

def get_db_replica_connection():
    pg_hook = PostgresHook(postgre_conn_id = "postgres_replica")
    connection = pg_hook.get_conn()
    return connection

def run_select_replica(sql_select):
    result_sql = " ".join(sql_select.split()) #remove extra blanks
    connection = get_db_replica_connection()
    cursor = connection.cursor()
    cursor.execute(result_sql)
   
    result = cursor.fetchall()
    cursor.close()
    connection.close()
    return result

def run_select(sql_select):
    result_sql = " ".join(sql_select.split()) #remove extra blanks
    connection = get_db_connection()
    cursor = connection.cursor()
    cursor.execute(result_sql)
   
    result = cursor.fetchall()
    cursor.close()
    connection.commit()
    connection.close()
    return result

def run_inserts(sql_inserts):
    result_sql = sql_inserts
    connection = get_db_connection()
    cursor = connection.cursor()
    cursor.execute(result_sql)
    connection.commit()
    cursor.close()
    connection.close()

def get_openweather_key():
    key = Variable.get("openweather_k")
    return key

def get_ninjas_key():
    key = Variable.get("ninjas_k")
    return key