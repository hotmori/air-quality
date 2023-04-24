# air-quality
Air quality analytical solution using: Docker, Postgres, Airflow, Metabase

## Postgres db
```
make create-postgres
```

- deploy all db objects in 'database' dir... manually :(

## Airflow
```
make create-airflow
```
Setup postgres connection
- with id: postgres_default, host: postgresql-master, schema/db: my_database, user: my_user, password: my_password, port: 5432
- [optional] with id: postgres_replica, host: postgresql-slave, schema/db: my_database, user: my_user, password: my_password, port: 5432

Setup variables
- for OpenWeather API key: 'openweather_k'
- for Ninjas GEO API key: 'ninjas_k'
- Enable all dags

## Metabase

```
make create-metabase
```
- setup connection to postgres db to postgresql-slave host to reporting schema
