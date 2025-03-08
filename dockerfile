# FROM ubuntu:22.04 AS base
#
#
# WORKDIR /Agel
# COPY / .
#
# RUN apt update -y && \
#    apt install python3.10 -y &&\
#    apt install python3-pip -y && \
#    apt install vim -y && \
#    bash init.sh


FROM airflow_base:1.2

ENV AGEL_DIR=/Agel AIRFLOW_DIR=/root/airflow
ENTRYPOINT ["airflow", "standalone"]
