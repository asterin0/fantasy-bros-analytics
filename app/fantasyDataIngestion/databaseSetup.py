import os
import psycopg2
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Loading DB credentials as environmental variables
load_dotenv()

# Creating connection string using config template and creating sqlalchemy engine
def createEngine():

    username = os.environ.get("POSTGRES_USER")
    password = os.environ.get("POSTGRES_PASSWORD")
    host = os.environ.get("POSTGRES_HOST")
    port = os.environ.get("POSTGRES_PORT")
    database = os.environ.get("POSTGRES_DB")

    connectionString = (
        f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}"
    )

    # Define sql alchemy engine
    return create_engine(connectionString)


if __name__ == "__main__":
    print(createEngine())
