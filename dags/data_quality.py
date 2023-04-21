from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow import AirflowException
from datetime import datetime
from common_package.common_module import run_select_replica



    
def check_staging_missed_per_hour_data():
    sql_select = "select country, name, cnt \
                    from data_quality.vstaging_cities_air_missed_per_hour_data\
                    order by country, name\
                  "
    check_data = run_select_replica(sql_select)  
    if check_data:
        for city in check_data:
            print( "Country: ", city[0], "city: ", city[1], "cnt: ", city[2])
        raise  AirflowException(f"There cities that have gaps in data:{check_data}")
    
def get_reporting_delivery_metrics():
    sql_select = "select data_delay_in_minutes \
    from data_quality.vreporting_data_delivery_metrics"
    
    data_delay_in_minutes = run_select_replica(sql_select)[0][0]
    return {"data_delay_in_minutes": data_delay_in_minutes}

def check_reporting_data_delay():
    metrics = get_reporting_delivery_metrics()

    if metrics["data_delay_in_minutes"]/60 > 2:
        raise AirflowException("Delay is more than two hours")

def check_reporting_cities_air_data_count():
    sql_select = "select country_name, \
                         city_name, \
                         cnt, \
                         cnt_expected \
                    from data_quality.vreporting_cities_air_data_count \
                    where cnt <> cnt_expected"
    check_data = run_select_replica(sql_select)
    
    if check_data:
       for city in check_data:
           print( "Country: ", city[0], "city: ", city[1], "cnt: ", city[2], "cnt_expected:", city[3] )
       raise  AirflowException(f"There cities that have gaps in data")       

with DAG(dag_id="data_quality",
         start_date=datetime(2021,1,1),
         schedule_interval="30 * * * *",
         catchup=False) as dag:
    
    task_check_staging_missed_per_hour_data = PythonOperator(
        task_id="check_staging_missed_per_hour_data",
        python_callable=check_staging_missed_per_hour_data)
        
    task_check_reporting_data_delay = PythonOperator(
        task_id="check_reporting_data_delay",
        python_callable=check_reporting_data_delay)
   
    task_check_reporting_cities_air_data_count = PythonOperator(
        task_id="check_reporting_cities_air_data_count",
        python_callable=check_reporting_cities_air_data_count)

[task_check_staging_missed_per_hour_data, task_check_reporting_data_delay, task_check_reporting_cities_air_data_count]
