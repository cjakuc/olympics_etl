from prefect import flow, task
from prefect.tasks import task_input_hash
import pandas as pd
import os
from datetime import timedelta
import typing
import hashlib
from utilities import extract_data, read_local_csvs

@flow
def transform_olympics_data(
    local_read_path: str
    ) -> typing.Dict[str, pd.DataFrame]:
    """
    Transforms the raw CSV files stored in a local directory into 
    a dictionary where keys are table names, values are DataFrames. 
    Returns the dictionary.

    Parameters
    ----------
    local_read_path : str
        Local directory to retrieve the raw CSVs

    Returns
    -------
    df_dict: typing.Dict[str, pd.DataFrame]
        Keys are table names, values are DataFrames to be inserted into database
    """
    df_dict = read_local_csvs(local_read_path=local_read_path) # task
    df_dict = process_olympics_data(df_dict=df_dict) # task
    return df_dict

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def process_olympics_data(
    df_dict: typing.Dict[str, pd.DataFrame]
    ) -> typing.Dict[str, pd.DataFrame]:
    """
    Transforms the dictionary of DataFrames passed in into 
    a dictionary where keys are table names, values are DataFrames. 
    Returns the dictionary.

    Parameters
    ----------
    df_dict : typing.Dict[str, pd.DataFrame]
        Keys are the filenames without the file extension and 
        the values are the CSVs turned into Pandas DataFrames

    Returns
    -------
    df_dict: typing.Dict[str, pd.DataFrame]
        Keys are table names, values are DataFrames to be inserted into database
    """

    # Drop duplicates, where all columns are equal
    for tbl in df_dict.keys():
        df_dict[tbl] = df_dict[tbl].drop_duplicates()
    regions_df = df_dict['regions']
    # Combine athletes DFs into one
    winter = df_dict['athletes_winter_games']
    summer = df_dict['athletes_summer_games']
    all_seasons = pd.concat([summer, winter])
    # Make a DF for just the athletes
    # This would drop some athletes of the same nation if they have the same name
    # This would have multiple records for athletes that change sex or nation
    athlete_cols = ['Name', 'Sex', 'NOC', 'Team']
    athletes_df = all_seasons.drop_duplicates(athlete_cols)[athlete_cols]
    # Create an ID for athletes that is deterministic and we can use later
    athletes_df['AthleteId'] = athletes_df.apply(lambda row:  hashlib.md5((str(row)).encode()).hexdigest(), axis=1)
    athlete_cols = ["AthleteId"] + athlete_cols
    athletes_df = athletes_df[athlete_cols]
    # Create an ID for event results that is deterministic and we can use later
    events_cols = ['Year', 'Season', 'City', 'Sport', 'Event', 'Medal', 'AthleteId']
    events_df = pd.merge(all_seasons, athletes_df, on=['Name', 'Sex', 'NOC'])[events_cols]
    events_df['EventResultId'] = events_df.apply(lambda row:  hashlib.md5((str(row)).encode()).hexdigest(), axis=1)
    events_cols = ['EventResultId'] + events_cols
    events_df = events_df[events_cols]

    final_dict = {
        'regions': regions_df
        , 'athletes': athletes_df
        , 'event_results': events_df
        }
    for df in final_dict.keys():
        final_dict[df].columns =  map(lambda x: str(x).upper(), final_dict[df].columns)
    
    return final_dict
