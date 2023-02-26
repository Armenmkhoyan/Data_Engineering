#### In this project, for orchestration used Airflow, and for not installing it on the machine, 
need to use a Docker container. Instruction on how to run Airflow on Docker is attached below.

##### Initializing Environment    
    mkdir ./dags ./logs ./plugins
    echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env

#### database migrations and create the first user account. To do it, run.
    docker-compose up airflow-init

#### Running Airflow
    docker-compose up


#### Cleaning up
##### To stop and delete containers, delete volumes with database data and download images, run:
    docker-compose down --volumes --rmi all

##### For shutting down docker containers and removing them
    docker-compose down --v

##### Managing Postgres container
    docker-compose up -d --no-deps --build postgres