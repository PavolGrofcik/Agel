FROM ubuntu:22.04 AS base


WORKDIR /Agel
COPY / .

RUN apt update -y && \
    apt install python3.10 -y &&\
    apt install python3-pip -y && \
    pip3 install -r ./requirements.txt && \
    pip3 install pyarrow && \
    python3 -m pip install "dask[distributed]" && \
    airflow db init && \
    airflow users create --username admin --password admin --firstname test  --lastname test --role Admin --email test@test.org && \
    bash init.sh

ENTRYPOINT "startup.sh run"

##Use previous image
#FROM app:1.6
#
##Config airflow
#RUN airflow db init && \
#    airflow users create --username admin --password admin --firstname test  --lastname test --role Admin #--email test@test.org && \
#    bash init.sh
#
##Run airflow in background when container starts to run