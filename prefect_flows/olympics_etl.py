from prefect import flow, task
import pandas as pd
import os
from utilities import extract_data, load_data

@flow
def run_pipeline():
    extract_data() # flow
    transform_olympics_data() # flow
    load_data() # flow

@flow
def transform_olympics_data():
    process_olympics_data()
    pass

@task
def process_olympics_data():
    pass
