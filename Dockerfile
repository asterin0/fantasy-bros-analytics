FROM apache/airflow:slim-latest-python3.9

# Copying project files
COPY ./fantasyBros ./fantasyBros
COPY ./requirementsDev.txt ./requirementsDev.txt

# Install git as root user for dbt compatibility
USER root
RUN apt-get -y update && \
    apt-get -y install git
    
# Granting airflow user read-write ownership to project files
RUN chmod -R 777 ./

# Switching to airflow user for airflow-init server compatibility and installing 
USER airflow
RUN pip install -r requirementsDev.txt

# Installing data scraper scripts as package and adding to path
RUN pip install ./fantasyBros