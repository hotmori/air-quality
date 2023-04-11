create-airflow:
	mkdir -p ./dags ./logs ./plugins
	echo -e "AIRFLOW_UID=$(id -u)" > .env
	docker-compose -f docker-compose-airflow.yaml up -d

create-postgres:
	docker-compose -f docker-compose-postgres.yaml up -d

create-metabase:
	docker-compose -f docker-compose-metabase.yaml up -d	