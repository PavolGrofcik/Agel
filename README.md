# Agel Assignment 

*********************************************
## Design
![](/agel_design.png)


*********************************************
## Launching the program
1. Clone this repositary to your PC ``$HOME`` directory
2. There are 2 options to run the proposed pipeline:
- a) using your Local Apache Airflow installation
- b) using Docker Compose to run all services in containers

*********************************************
## a) Using your Local Apache Airflow installation
*********************************************
Navigate to the cloned `/Agel` repositary

Now, copy content of `./dags` folder containg DAGs file to your airflow/dags folder using command below.
Replace path to your local Airflow folder
``cp ./dags/* /$HOME/airflow/dags``

After successful copy of dags files, you have to create 2 new variables in Airflow Webserver->Variables:
These variables are used in the pipeline named **AgelETL**  
``DATASET_URL=https://archive.ics.uci.edu/static/public/296/diabetes+130-us+hospitals+for+years+1999-2008.zip``  
 ``AGEL_DIR=/AGEL`` in Airflow Webserver UI


After you have added these variables, you can refresh Airflow webserver UI and launch DAG called **AgelETL**.  
It should start the pipeline soon, and you should see results of the pipeline flow.   
Final preprocessed dataset will be located after successful run in **/Agel/scripts/data_final/** with name:
**data_processed_YYYY-MM-DD.parquet** with automatically added date info of the processing.

It is also expected to run this DAG on a daily frequency, so for each day will be created new **processed_data** with date **YYYY_MM_DD**, aimed for the next steps: e.g. feature selection and training AI models...




*********************************************
## b) Docker Compose
********************************************
Firstly, make sure you have installed **Docker**  and **Docker compose**

If you would like to run all services in docker containers, run following commands:  
Also, make sure you are in /Agel directory.  
``docker build -t airflow_base:1.2 .``  
``docker compose build --parallel``   
``docker compose run``  

Now, the services should be continuously starting, and you can type to your web browser an URL for the Airflow webserver,
which by default is in compose.yml file configured from port **8080** to **8000**.  
To list running services (containers), use command:
``docker ps ``  


So to access Airflow webserver type following address ``localhost:8000`` to your web browser.  
The username and password for login are `postgres` and `postgres` (for test use case only!)  
Now you should see DAG pipeline called **AgelETL** and you can launch it manually.

In order to stop the containers, run following command:  
``docker compose down``

