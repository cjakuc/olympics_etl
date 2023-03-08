from prefect import flow, task
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
import os
import typing
from utilities import extract_data, load_data, read_local_csvs
from olympics_transform import transform_olympics_data, process_olympics_data

load_dotenv()
POSTGRES_USER = os.environ["POSTGRES_USER"]
POSTGRES_DB = os.environ["POSTGRES_DB"]
POSTGRES_PASSWORD = os.environ["POSTGRES_PASSWORD"]
POSTGRES_PORT = os.environ["POSTGRES_PORT"]
POSTGRES_SERVER = os.environ["POSTGRES_SERVER"]
ENGINE = create_engine(f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_SERVER}:{POSTGRES_PORT}/{POSTGRES_DB}')

@flow
def run_pipeline(
    bucket_name: str
    , subfolder: str
    , local_path: str
    ):
    """
    Prefect flow to control all other flows. Performs extract, transform, 
    and load on files stored in AWS S3 bucket and loads them to a database.

    Parameters
    ----------
    bucket_name : str
        AWS S3 bucket name
    subfolder : str
        sub-directory within the S3 bucket
    local_path: str
        Local directory to store the raw CSVs
    """
    extract_data(
        bucket_name=bucket_name
        , subfolder=subfolder
        , local_path=local_path
        ) # flow
    df_dict = transform_olympics_data(local_read_path=local_path) # flow
    load_data(df_dict=df_dict, engine=ENGINE) # flow

if __name__ == '__main__':
    bucket_name = 'raw-csv-storage'
    subfolder = 'raw-olympics'
    local_path = 'data/raw'
    run_pipeline(
        bucket_name=bucket_name
        , subfolder=subfolder
        , local_path=local_path)
