import os
import pendulum
import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import date, timedelta, datetime

from fantasyBros.scripts.scrapeFantasyPros import (
    getFootballProjections,
    getEspnPlayers,
)
from fantasyBros.utils.dfToDataLakeLocal import (
    loadDataFrameToLocalFolder,
    loadDataToMinioBucket,
)
from fantasyBros.utils.dataLakeToDbLocal import (
    scrapeDataFromMinioBucket,
    sendDataToPostgres,
)


# Config
minioAccessKey = os.environ.get("AIRFLOW__CONN__MINIO_ROOT_USER")
minioSecretKey = os.environ.get("AIRFLOW__CONN__MINIO_ROOT_PASSWORD")
postgresUser = os.environ.get("AIRFLOW__CONN__POSTGRES_LOCAL_USER")
postgresPswrd = os.environ.get("AIRFLOW__CONN__POSTGRES_LOCAL_PASSWORD")
postgresHost = os.environ.get("AIRFLOW__CONN__POSTGRES_LOCAL_HOST")
postgresPort = os.environ.get("AIRFLOW__CONN__POSTGRES_LOCAL_PORT")
postgresDatabase = os.environ.get("AIRFLOW__CONN__POSTGRES_LOCAL_DB")
endpoints = ["qb", "rb", "wr", "te", "espnLeague"]
espnYear = int(os.environ.get("ESPN_YEAR"))  # ESPN League Credentials
espnLeagueId = os.environ.get("ESPN_LEAGUEID")
espnSwid = os.environ.get("ESPN_SWID")
espnS2 = os.environ.get("ESPN_S2")


# Setting default arguments for dag
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": pendulum.today("UTC"),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

# Initializing dag
dag = DAG(
    "footballProjsDev",
    default_args=default_args,
    schedule="@daily",
    is_paused_upon_creation=True,
)


# Scraping fantasyPros projections and sending to local folder for all positions
def webScrapeProjs():
    """
    Scraping projections for each position
    """
    for p in endpoints:
        landingDirectory = f"football/ros/{p}/"
        filename = p + "_" + datetime.now().strftime("%m-%d-%Y") + ".csv"
        filePath = landingDirectory + filename

        print(f"Scraping projections for {p}s")

        # Selecting scraper function based on endpoint value
        if p == "espnLeague":
            projsDf = getEspnPlayers(
                leagueId=espnLeagueId,
                leagueYear=espnYear,
                espnS2=espnS2,
                espnSwid=espnSwid,
            )
        else:
            projsDf = getFootballProjections(pos=p)

        # Sending data to MinIO client
        loadDataToMinioBucket(
            projsDf,
            minioAccessKey=minioAccessKey,
            minioSecretKey=minioSecretKey,
            bucketName="fantasy-bros-analytics",
            landingDirectory=filePath,
        )

        print(f"{p}s sent successfully to MinIO bucket!")


# Setting webScrapeProjs as Airflow Task
scrapeDataAndLoadToDataLake = PythonOperator(
    task_id="scrapeDataAndLoadToDataLake", python_callable=webScrapeProjs, dag=dag
)


# Sending projections from MinIO data lake to Postgres Database
def loadProjsFromDataLakeToPostgres():
    """
    Extracting projections from data lake and sending to local database
    """
    for p in endpoints:
        fileDirectory = f"football/ros/{p}/"
        filename = p + "_" + datetime.now().strftime("%m-%d-%Y") + ".csv"
        filePath = fileDirectory + filename

        # Grabbing bucket object from MinIO and saving as Pandas DataFrame
        projsDf = scrapeDataFromMinioBucket(
            minioAccessKey=minioAccessKey,
            minioSecretKey=minioSecretKey,
            bucketName="fantasy-bros-analytics",
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
    bash_command="cd dbt/fantasyBrosDbt && dbt run --select football.dim_players --profiles-dir .",
    dag=dag,
)


# Bash command for dim_players_history dbt model
fantasyTeamsDimModel = BashOperator(
    task_id="fantasyTeamsDimModel",
    bash_command="cd dbt/fantasyBrosDbt && dbt run --select football.dim_fantasy_teams --profiles-dir .",
    dag=dag,
)


# Bash command for dim_players_history dbt model
playersHistoryDimModel = BashOperator(
    task_id="playersHistoryDimModel",
    bash_command="cd dbt/fantasyBrosDbt && dbt run --select football.dim_players_history --profiles-dir .",
    dag=dag,
)


# Bash command for dim_benchmarks dbt model
benchmarksDimModel = BashOperator(
    task_id="benchmarksDimModel",
    bash_command="cd dbt/fantasyBrosDbt && dbt run --select football.dim_benchmarks --profiles-dir .",
    dag=dag,
)


# Bash command for fact_valuations dbt model
valuationsFactModel = BashOperator(
    task_id="valuationsFactModel",
    bash_command="cd dbt/fantasyBrosDbt && dbt run --select football.fct_valuations --profiles-dir .",
    dag=dag,
)


# Setting task order for ELT workflow
(
    scrapeDataAndLoadToDataLake
    >> loadProjsFromDataLakeToStageTable
    >> [playersDimModel, fantasyTeamsDimModel]
    >> playersHistoryDimModel
    >> benchmarksDimModel
    >> valuationsFactModel
)
