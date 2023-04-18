from airflow.hooks.postgres_hook import PostgresHook

BASE_URL_OPENWEATHER_CURRENT = 'http://api.openweathermap.org/data/2.5/air_pollution?'
BASE_URL_OPENWEATHER_HISTORY = 'http://api.openweathermap.org/data/2.5/air_pollution/history?'
BASE_URL_OPENWEATHER_FORECAST = 'http://api.openweathermap.org/data/2.5/air_pollution/forecast?'
BASE_URL_NINJAS_GEO = 'https://api.api-ninjas.com/v1/geocoding?'


def get_db_connection():
    pg_hook = PostgresHook(postgre_conn_id = "postgres_default")
    connection = pg_hook.get_conn()
    return connection

def run_select(sql_select):
    formatted_sql = " ".join(sql_select.split()) #remove extra blanks
    connection = get_db_connection()
    cursor = connection.cursor()
    cursor.execute(formatted_sql)
   
    result = cursor.fetchall()
    cursor.close()
    connection.close()
    return result