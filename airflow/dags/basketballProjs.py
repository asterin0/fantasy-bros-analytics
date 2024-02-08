import os
from datetime import datetime, timedelta

import pendulum
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from airflow import DAG
from fantasyBros.scripts.scrapeFantasyPros import (
    getBasketballProjections,
    getProBasketballReferenceStats,
)
from fantasyBros.utils.dataLakeToDb import scrapeDataFromS3, sendDataToPostgres
from fantasyBros.utils.dfToDataLake import loadDataToS3

# Config
awsS3AccessKey = os.environ.get("AWS_ACCESS_KEY")
awsS3SecretKey = os.environ.get("AWS_SECRET_KEY")
awsBucketName = os.environ.get("AWS_BUCKET")
postgresUser = os.environ.get("FANTASYBROS__POSTGRES_USER")
postgresPswrd = os.environ.get("FANTASYBROS__POSTGRES_PASSWORD")
postgresHost = os.environ.get("FANTASYBROS__POSTGRES_HOST")
postgresPort = os.environ.get("FANTASYBROS__POSTGRES_PORT")
postgresDatabase = os.environ.get("FANTASYBROS__POSTGRES_DB")

endpoints = [
    "overall",
    "pg",
    "sg",
    "sf",
    "pf",
    "c",
    "g",
    "f",
    "proBasketballRefStats",
]


# Setting default arguments for dag
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": pendulum.today("UTC"),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "schedule_interval": "0 13 * * *",
}

# Initializing dag
dag = DAG(
    "basketballProjs",
    default_args=default_args,
    schedule="@daily",
)


# Scraping fantasyPros projections and sending to local folder for all positions
def webScrapeProjs():
    """
    Scraping projections for each position
    """
    for p in endpoints:
        landingDirectory = f"basketball/ros/{p}/"
        filename = p + "_" + datetime.now().strftime("%m-%d-%Y") + ".csv"
        filePath = landingDirectory + filename

        print(f"Scraping projections for {p}s")

        # Selecting scraper function based on endpoint value
        if p == "proBasketballRefStats":
            projsDf = getProBasketballReferenceStats("2024")
        else:
            projsDf = getBasketballProjections(pos=p)

        # Sending data to MinIO client
        loadDataToS3(
            projsDf,
            awsAccessKeyId=awsS3AccessKey,
            awsSecretAccessKey=awsS3SecretKey,
            bucketName=awsBucketName,
            landingDirectory=filePath,
        )

        print(f"{p}s sent successfully to S3 bucket!")


# Setting webScrapeProjs as Airflow Task
scrapeDataAndLoadToDataLake = PythonOperator(
    task_id="scrapeDataAndLoadToDataLake",
    python_callable=webScrapeProjs,
    dag=dag,
)


# Sending projections from MinIO data lake to Postgres Database
def loadProjsFromDataLakeToPostgres():
    """
    Extracting projections from data lake and sending to local database
    """
    for p in endpoints:
        fileDirectory = f"basketball/ros/{p}/"
        filename = p + "_" + datetime.now().strftime("%m-%d-%Y") + ".csv"
        filePath = fileDirectory + filename

        # Grabbing bucket object from MinIO and saving as Pandas DataFrame
        projsDf = scrapeDataFromS3(
            awsAccessKeyId=awsS3AccessKey,
            awsSecretAccessKey=awsS3SecretKey,
            bucketName=awsBucketName,
            fileDirectory=filePath,
        )

        # Sending Pandas DataFrame to 'staging' schema in Postgres DB
        sendDataToPostgres(
            df=projsDf,
            tableName=p,
            dbUser=postgresUser,
            dbPswrd=postgresPswrd,
            dbHost=postgresHost,
            dbPort=postgresPort,
            dbName=postgresDatabase,
        )


# Setting loadProjsFromDataLakeToPostgres as Airflow Task
loadProjsFromDataLakeToStageTable = PythonOperator(
    task_id="loadProjsFromDataLakeToStageTable",
    python_callable=loadProjsFromDataLakeToPostgres,
    dag=dag,
)


# Bash command for dim_players dbt model
playersDimModel = BashOperator(
    task_id="playersDimModel",
    bash_command="cd /opt/airflow/dbt/fantasyBrosDbt && dbt run --select basketball.dim_basketball_players --profiles-dir . --target fantasyBros",
    dag=dag,
)


# Bash command for dim_players_history dbt model
playersHistoryDimModel = BashOperator(
    task_id="playersHistoryDimModel",
    bash_command="cd /opt/airflow/dbt/fantasyBrosDbt && dbt run --select basketball.dim_basketball_players_history --profiles-dir . --target fantasyBros",
    dag=dag,
)


# Bash command for dim_benchmarks dbt model
benchmarksDimModel = BashOperator(
    task_id="benchmarksDimModel",
    bash_command="cd /opt/airflow/dbt/fantasyBrosDbt && dbt run --select basketball.dim_basketball_benchmarks --profiles-dir . --target fantasyBros",
    dag=dag,
)


# Bash command for fact_valuations dbt model
valuationsFactModel = BashOperator(
    task_id="valuationsFactModel",
    bash_command="cd /opt/airflow/dbt/fantasyBrosDbt && dbt run --select basketball.fct_basketball_valuations --profiles-dir . --target fantasyBros",
    dag=dag,
)


# Setting task order for ELT workflow
(
    scrapeDataAndLoadToDataLake
    >> loadProjsFromDataLakeToStageTable
    >> playersDimModel
    >> [playersHistoryDimModel, benchmarksDimModel]
    >> valuationsFactModel
)
