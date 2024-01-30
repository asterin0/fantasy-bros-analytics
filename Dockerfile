FROM apache/airflow:slim-latest-python3.9

WORKDIR /opt/airflow

# Copying project files
COPY ./fantasyBros /opt/airflow/fantasyBros
COPY ./requirementsDev.txt /opt/airflow/requirementsDev.txt
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

# Switching to airflow user for airflow-init server compatibility and installing 
USER airflow
RUN pip install -r /opt/airflow/requirementsDev.txt

# Installing data scraper scripts as package and adding to path
RUN pip install /opt/airflow/fantasyBros
# RUN python3 /opt/airflow/fantasyBros/setup.py install --user
# ENV PYTHONPATH "${PYTHONPATH}:/opt/airflow/fantasyBros"

#EXPOSE 8008

ENTRYPOINT [ "/opt/airflow/entrypoint.sh" ]