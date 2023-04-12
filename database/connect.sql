connect to master:
psql --username=my_user  --host=postgresql-master --dbname=my_database
my_password

connect to slave:
psql --username=repl_user  --host=postgresql-slave --dbname=my_database
repl_password

connect to metabase db:
psql --username=mb_user --host=metabase-postgres-db --dbname=mb_database
mb_password