FROM ubuntu:22.04 AS base


WORKDIR /Agel
COPY / .

RUN apt update -y && \
   apt install python3.10 -y && \
   apt install python3-pip -y && \
   apt install vim -y && \
   apt install unzip -y && \
   pip3 install dask distributed && \
   pip3 install -r ./requirements.txt && \
   pip3 install apache-airflow-providers-postgres

#Set $HOME variable
ENV HOME=/ AIRFLOW_HOME=/airflow/ AGEL_DIR=/Agel

