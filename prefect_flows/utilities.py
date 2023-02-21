from prefect import flow, task
from prefect.blocks.core import Block
from prefect_aws.s3 import S3Bucket
from prefect.tasks import task_input_hash
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import Engine
from sqlalchemy.inspection import inspect
from dotenv import load_dotenv
import pandas as pd
import typing
import os
import shutil
import uuid
from datetime import timedelta
from textwrap import dedent
from infrastructure.awscredentials import aws_creds

@flow
def extract_data(
    bucket_name: str
    , subfolder: str
    , local_path: str
    ):
    """
    Prefect flow to extract CSV files from a specified AWS bucket and 
    deposit in a local directory.

    Parameters
    ----------
    bucket_name : str
        AWS S3 bucket name
    subfolder : str
        sub-directory within the S3 bucket
    local_path : str
        Local directory to store the raw CSVs
    """

    pull_aws_files(
        bucket_name = bucket_name
        , subfolder = subfolder
        , local_path = local_path
        )
    
    return

@flow
def load_data(
    df_dict: dict
    , engine
    ):
    """
    Loads data from a dictionary of Pandas DataFrames into a database instance

    Parameters
    ----------
    df_dict : Dict[str, pd.DataFrame]
        Keys are table names, values are DataFrames to be inserted into database
    engine : sqlalchemy.engine.Engine
        SQLAlchemy engine to use to insert into the database
    """

    for tbl in df_dict.keys():
        upsert_df(df=df_dict[tbl], table_name=tbl, engine=engine)

    return

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def pull_aws_files(
    bucket_name: str
    , subfolder: str
    , local_path: str
    ):
    """
    Pulls CSV from a specified AWS S3 bucket, from specifed subfolder, and puts 
    them into the specified local directory.

    Parameters
    ----------
    bucket_name : str
        AWS S3 bucket name
    subfolder : str
        sub-directory within the S3 bucket
    local_path: str
        Local directory to store the raw CSVs
    """
    
    s3_bucket = S3Bucket(
        bucket_name = bucket_name
        , aws_credentials = aws_creds
        , basepath = subfolder
    )
    obj_list = s3_bucket.list_objects()
    # Empty the local directory, if it already has files or directories
    for path in os.listdir(local_path):
        if os.path.isfile(path):
            os.unlink(os.path.join(root, path))
        elif os.path.isdir(path):
            shutil.rmtree(os.path.join(root, path))
    # The first obj in the list is the subfolder of the bucket ('raw_olympics')
    for obj in obj_list[1:]:
        filename = obj['Key'].split('/')[-1]
        # There could be other directories in the subfolder
        # let's just do CSV files for now
        if not filename.endswith('.csv'):
            continue
        s3_bucket.download_object_to_path(
            from_path=f"{subfolder}/{filename}"
            , to_path=f"{local_path}/{filename}"
            )
    # if there are no CSV files downloaded, throw an error
    if os.listdir(local_path) == []:
        raise ValueError(
            dedent(f"""
            There are no CSV files in the bucket, '{bucket_name}', 
            in '{subfolder}'
            """
            ))

    return

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def read_local_csvs(
    local_read_path: str
    ) -> typing.Dict[str, pd.DataFrame]:
    """
    Pulls CSVs from a local directory and returns them in a dictionary 
    where the keys are the filenames without the file extension and 
    the values are the CSVs turned into Pandas DataFrames.

    Parameters
    ----------
    local_path: str
        Local directory to store the raw CSVs
    
    Returns
    ----------
    df_dict: Dict[str, pd.DataFrame]
        Keys are the filenames without the file extension and 
        the values are the CSVs turned into Pandas DataFrames
    """

    files_list = []
    for path in os.listdir(local_read_path):
        if path.endswith('.csv'):
            files_list.append(path)
    if files_list == []:
        raise ValueError(
            dedent(f"""
            There are no CSV files in '{local_read_path}'
            """
            ))

    df_dict = dict()
    for f in files_list:
        f_remove_ext = '.'.join(f.split('.')[:-1])
        df_dict[f_remove_ext] = pd.read_csv(f"{local_read_path}/{f}", index_col=0)
    
    return df_dict

def upsert_df(
    df: pd.DataFrame
    , table_name: str
    , engine
    ):
    """
    Implements the equivalent of pd.DataFrame.to_sql(..., if_exists='update')
    (which does not exist). Creates or updates the db records based on the
    dataframe records.
    Conflicts to determine update are based on the dataframes index.
    This will set unique keys constraint on the table equal to the index names
    1. Create a temp table from the dataframe
    2. Insert/update from temp table into table_name

    Parameters
    ----------
    df: pd.DataFrame
        Pandas DataFrame of data to upsert
    table_name: str
        Table name to upsert into
    engine : sqlalchemy.engine.Engine
        SQLAlchemy engine to use to insert into the database
    """

    # If the table does not exist, we should just use to_sql to create it
    if not engine.execute(
        f"""SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE  table_schema = 'public'
            AND    table_name   = '{table_name}');
            """
    ).first()[0]:
        raise KeyError(
            f"""Table named '{table_name}' does not exist. 
            Make sure prefect_flows/manage_db.py has been run"""
            )
        
    # If it already exists...
    temp_table_name = f"temp_{uuid.uuid4().hex[:6]}"
    df.to_sql(temp_table_name, engine, index=False)

    meta = MetaData()
    table = Table(table_name, meta, autoload=True, autoload_with=engine)
    index = [pk_column.name for pk_column in table.primary_key.columns.values()]
    index_sql_txt = ', '.join([f'"{i}"' for i in index])
    columns = list(df.columns)
    headers_sql_txt = ", ".join(
        [f'"{i}"' for i in columns]
    )

    # col1 = exluded.col1, col2=excluded.col2
    update_column_stmt = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in columns])

    # Compose and execute upsert query
    query_upsert = f"""
    INSERT INTO "{table_name}" ({headers_sql_txt}) 
    SELECT {headers_sql_txt} FROM "{temp_table_name}"
    ON CONFLICT ({index_sql_txt}) DO UPDATE 
    SET {update_column_stmt};
    """
    engine.execute(query_upsert)
    engine.execute(f"DROP TABLE {temp_table_name}")

    return
