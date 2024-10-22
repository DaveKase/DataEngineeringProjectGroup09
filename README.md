To set up this in your local machine, first, you have to have Docker Desktop (or equivelant for your OS) installed.

To initialize the database, run:
docker-compose run airflow airflow db init

To start the services, run 
docker-compose up
