# air-quality
Air quality analytical solution using: Docker, Postgres, Airflow, Metabase

## Create postgres db
```
make create-postgres
```
```
deploy all db objects in 'database' dir manually :(
``` 
## Create airflow
```
make create-airflow
```
```
setup postgres connection with id: postgres_default, host: postgresql-master, schema/db: my_database, user: my_user, password: my_password, port: 5432
```
```
set variable for OpenWeather API key: 'openweather_k'
```
```
set variable for Ninjas GEO API key: 'ninjas_k'
```
```
enable all dags
```
## Create metabase

```
make create-metabase
```
```
setup connection to postgres db to reporting schema
```
