# Agel Assignment 

*********************************************
## Design
![](/agel_design.png)


*********************************************
## Launching the program
1. Clone this repositary to your PC to your ``$HOME`` directory
2. There are 2 options to run the proposed pipeline:
- a) Firstly, you have installed Apache Airflow locally, then you have to only copy **Agel/dags/*** folder to your Airflow **/dags/** folder
- a) Secondly, set new variables in Airflow: ``DATASET_URL=https://archive.ics.uci.edu/static/public/296/diabetes+130-us+hospitals+for+years+1999-2008.zip``
and ``AGEL_DIR=/AGEL`` in Airflow Webserver UI
- a) Reload Airflow webserver to add refresh new DAGs
- a) Finally, launch **AgelETL** DAG in Apache airflow webserver UI
- b) Using **docker compose** tool to build and run all required services - see below

*********************************************
## a) Setting Postgres DB in Dockerfile for running only DB postgres for Airflow as a backend DB - instead of sqlite
*********************************************
Navigate to the downloaded `/Agel` directory

First build base image using:
``docker build -t airflow_base:1.2 .``

After successful building of the image, try to run Postgres database in docker container
To run postgres database backend for airflow run:  
```docker compose run -it -p 5432:5432 --name postgres postgres ```

Now in the opened terminal type, or edit **airflow.cfg** and set variable named **sql_alchemy_conn** with the constant below:
``EXPORT AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://postgres:postgres@postgres:5432/airflow_db``  
``airflow scheduler``

Now open the 2nd terminal and type:  
``airflow webserver``  

After all this commands airflow should start immediately.
Airflow webserver by default listens at port **8080**, so open  your web browser and type this URL:  
``localhost:8080``

After opening URL at the webserver address, the login form will appear:  
Now for username type: **airflow** and for password **airflow**  

After successful login, Airflow UI webserver will show.   
There are some DAGs, from which there is also the required DAG called ``AgelETL``.
You can launch it in order to run the whole pipeline for Agel Assignment.



*********************************************
## b) Docker Compose
********************************************
If you would like to run all services in docker containers, run following commands:  
Make sure you are in /Agel directory.  
``docker build -t airflow_base:1.2 .``  
``docker compose build --parallel``   
``docker compose run``  

Now, the services are continuously starting, and you can type to your web browser a URL for Airflow webserver,
which by default is in compose.yml file configured from port **8080** to **8000**.

So the URL for web server is located as ``localhost:8000``
The username and password are the same as above.
Run ETL DAG pipeline.

In order to stop the containers, run following command:  
``docker compose down``

