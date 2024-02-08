import os
from datetime import datetime
from io import BytesIO, StringIO

import boto3
import pandas as pd

from minio import Minio


def loadDataFrameToLocalFolder(
    dataframe: pd.DataFrame, landingDirectory: str, fileName: str
):
    """
    Saving dataframe in parquet format and saving down to local folder
    """
    try:
        print("Saving projections down...")
        initTime = datetime.now()

        df = dataframe

        if not os.path.exists(landingDirectory):
            os.makedirs(landingDirectory)

        df.to_csv(landingDirectory + fileName)

        finTime = datetime.now()

        print(
            "Projections successfully saved! Execution time: ",
            finTime - initTime,
        )

    except Exception as e:
        print("Error in sending dataframe to landing directory: ", str(e))


def loadDataToMinioBucket(
    dataframe, minioAccessKey, minioSecretKey, bucketName, landingDirectory
):
    """
    Sends dataframe to MinIO S3 bucket
    """
    try:
        # Creating client with the MinIO server playground
        client = Minio(
            "minio1:9000",
            access_key=minioAccessKey,
            secret_key=minioSecretKey,
            secure=False,
        )

        # Make fantasy-bros-analytics bucket if it doesn't already exist
        print("Creating bucket if it doesn't already exist")
        found = client.bucket_exists("fantasy-bros-analytics")
        if not found:
            client.make_bucket("fantasy-bros-analytics")
            print(f"Created {bucketName} bucket")
        else:
            print(f"Bucket '{bucketName}' already exists")

        # Preparing pandas dataframe and extracting metadata to be sent to MinIO client
        df = dataframe
        csv_bytes = df.to_csv(index=False).encode("utf-8")
        csv_buffer = BytesIO(csv_bytes)

        print("Sending projections to bucket")

        # Sending dataframe to MinIO client
        client.put_object(
            bucketName,
            landingDirectory,
            data=csv_buffer,
            length=len(csv_bytes),
            content_type="application/csv",
        )
        print("Projections successfully sent!")
    except Exception as e:
        print("Error in sending dataframe to MinIO bucket: ", str(e))


def loadDataToS3(
    dataframe, bucketName, awsAccessKeyId, awsSecretAccessKey, landingDirectory
):
    """
    Sends dataframe to S3 bucket
    """
    try:
        # Creating s3 client
        s3_session = boto3.Session(
            aws_access_key_id=awsAccessKeyId,
            aws_secret_access_key=awsSecretAccessKey,
        )

        # Preparing pandas dataframe and extracting metadata to be sent to boto3 client
        df = dataframe
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)

        print("Sending projections to bucket")

        # Sending dataframe to s3 client
        s3_resource = s3_session.resource("s3")
        s3_resource.Object(bucketName, landingDirectory).put(
            Body=csv_buffer.getvalue()
        )

        print("Projections successfully sent!")
    except Exception as e:
        print("Error in sending dataframe to S3: ", str(e))
