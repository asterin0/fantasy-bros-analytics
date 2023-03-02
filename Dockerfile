FROM apache/airflow:slim-latest-python3.9

WORKDIR /Projects/fantasy-bros-analytics

# Copying project files
COPY . .

# Install git as root user for dbt compatibility
USER root
RUN apt-get -y update && \
    apt-get -y install git
    
# Granting airflow user read-write ownership to project files
RUN chmod -R 777 ./

# Switching to airflow user for airflow-init server compatibility and installing 
USER airflow
RUN pip install -r requirements.txt

# Installing data ingestion scripts as package and adding to path
RUN python3 app/setup.py install --user
ENV PYTHONPATH "${PYTHONPATH}:/app"
