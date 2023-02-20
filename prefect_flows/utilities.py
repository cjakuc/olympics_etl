from prefect import flow, task
from prefect.blocks.core import Block
from prefect_aws.s3 import S3Bucket
from infrastructure.awscredentials import aws_creds
import pandas as pd
import typing

@flow
def extract_data(
    bucket_name: str
    , filename: str
):
    df = pull_aws_files(
        bucket_name = bucket_name
        , filename = filename
        )
    return df

@flow
def load_data():
    pass

@task
def pull_aws_files(
    bucket_name: str
    , filename: str
    , subfolder: str) -> pd.DataFrame:

    s3_bucket_block = S3Bucket(
        bucket_name = bucket_name
        , aws_credentials = aws_creds
        , basepath = subfolder
    )
    file = s3_bucket_block.read_path(path=f"{filename}")
    # TODO: support other files besides those supported by pd.read_csv()
    df = pd.read_csv(file)
    
    return df

@task
def read_local_csvs(
    local_read_path: str
    , filenames: list[str]) -> typing.Dict[str, pd.DataFrame]:
    pass

@task
def write_local_csvs():
    pass

@task
def connect_to_pgsql():
    pass


if __name__ == '__main__':
    
    s3_bucket_block_name = 'raw-csv-storage'
    bucket_name = 'raw-csv-storage'
    subfolder = 'raw-olympics'
    filename = 'regions'
    df = extract_data(
        bucket_name=bucket_name
        , filename=filename
        , subfolder=subfolder
        )
    breakpoint()