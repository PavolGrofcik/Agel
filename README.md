# Agel Assignment 

*********************************************
## Launching the program
*********************************************
## Dockerfile for running only DB postgres for Airflow as a backend DB
*********************************************
Navigate to the downloaded `/Agel` directory

To run postgres database backend for airflow run:  
```docker compose run -it -p 5432:5432 --name postgres postgres ```

Now open 2 terminals (Ctrl + Alt + T) and type:   
``EXPORT AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://postgres:postgres@postgres:5432/airflow_db``  
``airflow scheduler``   
``airflow webserver``  

Airflow webserver by default listens at port **8080**, so open  your web browser and type this URL:  
``localhost:8080``

After opening URL at the webserver address, the login form will appear:  
Now for username type: **airflow** and for password **airflow**  

After successful login, Airflow UI will show.   
There are some DAGs, from which there is also a requried DAG called ``AgelETL``.


You can launch it in order to run the whole pipeline for Agel Assignment.



*********************************************
## Docker Compose
********************************************
If you would like to run all services in docker containers, run following commands:  
``docker compose build --parallel``  
``docker compose run``

Now, the services are continuously starting and you can type to your web browser an URL for Airflow webserver,
which by default is in compose.yml file configured from port **8080** to **8000**.

So the URL for web server is located as ``localhost:8000``
The username and password are the same as above.

In order to stop the containers, run following command:  
``docker compose down``

