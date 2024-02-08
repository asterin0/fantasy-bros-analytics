FROM apache/airflow:slim-latest-python3.9

WORKDIR /opt/airflow

ARG DEV=false

# Copying project files
COPY ./fantasyBros /opt/airflow/fantasyBros
COPY ./requirements.dev.txt /opt/airflow/requirements.dev.txt
COPY ./requirements.txt /opt/airflow/requirements.txt
COPY ./airflow/dags /opt/airflow/dags
COPY ./dbt /opt/airflow/dbt
COPY ./tests /opt/airflow/tests
COPY ./airflow/config/initdb.sql /opt/airflow/initdb.sql
COPY ./airflow/config/airflow.cfg /opt/airflow/airflow.cfg
COPY ./airflow/config/webserver_config.py /opt/airflow/webserver_config.py
COPY ./airflow/config/entrypoint.sh /opt/airflow/entrypoint.sh

# Install git as root user for dbt compatibility
USER root
RUN apt-get -y update && \
    apt-get -y install git
    
# Granting airflow users read-write ownership to project files
RUN chmod -R 777 /opt/airflow/.

# Switching to airflow user for airflow-init server compatibility and installing packages
USER airflow
RUN pip install -r /opt/airflow/requirements.txt && \
    if [ $DEV = "true" ]; then \
    pip install -r /opt/airflow/requirements.dev.txt ; \
    fi && \
    pip install /opt/airflow/fantasyBros

ENTRYPOINT [ "/opt/airflow/entrypoint.sh" ]