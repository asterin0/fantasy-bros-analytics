import boto3
import pandas as pd

from fantasyBros.utils.devSetupLocal import createLocalEngine, fieldProcessing
from minio import Minio


def scrapeDataFromMinioBucket(
    minioAccessKey, minioSecretKey, bucketName, fileDirectory
):
    """
    Grabs data from MinIO S3 bucket
    """
    try:
        # Creating client with the MinIO server playground
        client = Minio(
            "minio1:9000",
            access_key=minioAccessKey,
            secret_key=minioSecretKey,
            secure=False,
        )

        # Grabbing csv file from MinIO bucket
        bucketObject = client.get_object(
            bucketName,
            fileDirectory,
        )

        # Converting MinIO bucket object into Pandas DataFrame
        df = pd.read_csv(bucketObject)

        return df

    except Exception as e:
        print("Error in extracting file from MinIO bucket: ", str(e))


def scrapeDataFromS3(
    awsAccessKeyId, awsSecretAccessKey, bucketName, fileDirectory
):
    """
    Grabs data from S3 bucket as Pandas DataFrame
    """
    try:
        # Creating s3 client
        s3_session = boto3.Session(
            aws_access_key_id=awsAccessKeyId,
            aws_secret_access_key=awsSecretAccessKey,
        )

        # Sending dataframe to s3 client
        s3_resource = s3_session.resource("s3")
        s3_object = s3_resource.Object(bucketName, fileDirectory).get()

        # Converting S3 bucket object into Pandas DataFrame
        df = pd.read_csv(s3_object["Body"])

        return df

    except Exception as e:
        print("Error in extracting file from S3 bucket: ", str(e))


def sendDataToPostgres(
    df: pd.DataFrame,
    tableName: str,
    dbUser: str,
    dbPswrd: str,
    dbHost: str,
    dbPort: str,
    dbName: str,
):
    """
    Sending dataframe to local Postgres database
    """
    engine = createLocalEngine(
        user=dbUser, pswrd=dbPswrd, host=dbHost, port=dbPort, db=dbName
    )
    conn = engine.connect()

    # Collecting SQL data types associated with DataFrame columns
    metaData = fieldProcessing(endpoint=tableName)

    # Check if table already exists, and emptying table if so
    tableExistCheck = conn.execute(
        f"""SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE  table_schema = 'staging'
                AND    table_name   = '{tableName}'
                );"""
    ).fetchone()[0]

    if tableExistCheck:
        conn.execute(f'TRUNCATE TABLE "staging"."{tableName}"')

    # Sending DataFrame to Postgres DB
    print(f"Sending {tableName} data to postgres")

    try:
        df.to_sql(
            name=tableName,
            con=engine,
            schema="staging",
            if_exists="append",
            method="multi",
            dtype=metaData.dTypes,
        )

        print(f"Successfully sent {tableName} data to postgres!")

    except Exception as e:
        print("Could not send data to postgres: ", str(e))
