from airflow.hooks.postgres_hook import PostgresHook

def get_db_connection():
    pg_hook = PostgresHook(postgre_conn_id = "postgres_default")
    connection = pg_hook.get_conn()
    return connection