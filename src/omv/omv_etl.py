"""
*** OREGON Motor Voter Data ***
    Responsible for getting the data, validating that the data can be processed,
    initial cleaning (removing of unwanted records) and casting of columns into
    the right format.
"""
import sys
sys.path.append(r'../src')
import os
import time
from zipfile import ZipFile
import glob
from datetime import datetime
import pandas as pd
from sqlalchemy.sql import text
from sqlalchemy.orm import sessionmaker
import traceback
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from sqlalchemy.dialects.postgresql import insert


from common_functions.common import get_traceback, get_timing, timing_decorator
from common_functions.file_operations import read_extract, write_load, read_extract_multiple

from utils.arg_parser import get_date, get_sample

from utils.config import RAW_DATA_PATH, PROCESSED_DATA_PATH, FINAL_DATA_PATH, state, file_date, sample, initialize_pandarallel
from data_contracts.omv_data_contract import DATA_CONTRACT, TABLENAME, dtype_mapping, final_columns, MotorVoter, Base
from utils.database import Database, fetch_existing_table, fetch_sql
from utils.file_operations import validate_dataframe
from utils.dataframe_operations import diff_dataframe
from utils.transformations import convert_date_format
from utils.search import index_documents

def _clean(df):
    print("Cleaning data")
    # de dupe
    df = df.drop_duplicates()

    print(f"...after cleaning {len(df)} records remain")

    return df.reset_index(drop=True)

def _transform(df):
    print('Transforming data...')
    df['file_date'] = file_date.strftime('%Y-%m-%d')
    df['state'] = state.lower()
    df = df[final_columns]
    return df.reset_index(drop=True)

def _load_database(current_df):
    print("....writing to database")

    database = "oregon_voter_files"
    db_connection = Database(database)
    engine = db_connection.get_engine()
    # Create the table if it doesn't exist
    Base.metadata.create_all(bind=engine, checkfirst=True)

    table_name = f"{TABLENAME.lower()}"
    existing_records = fetch_existing_table(engine, table_name, final_columns)

    insert_df = diff_dataframe(df_new=current_df, df_existing=existing_records, on_columns=['state', 'county', 'state_voter_id'])
    # drop file_date_y
    insert_df = insert_df.drop(columns=['file_date_y'], errors='ignore')
    # rename file_date_x to file_date
    insert_df = insert_df.rename(columns={'file_date_x': 'file_date'})
    print(f"Found {len(insert_df)} new records to insert")
    results = insert_df.to_sql(table_name, engine, if_exists='append', index=False, dtype=dtype_mapping, method='multi', chunksize=50)

def main():
    df = pd.DataFrame()
    # get dataframe with try except on file name
    try:
        df = read_extract_multiple(df, os.path.join(RAW_DATA_PATH, file_date.strftime('%Y_%m_%d'), 'omv'))
    except Exception as e:
        print(f"Error reading file {file_date.strftime('%Y-%m-%d')}")
        print(get_traceback(e))
        exit()
    # if len is 0 exit()
    if len(df) == 0:
        print(f"No records found for {file_date.strftime('%Y-%m-%d')}")
        exit()
    # validate the dataframe with try except
    try:
        df = validate_dataframe(df, DATA_CONTRACT)
    except Exception as e:
        print(f"Validation failed for {file_date.strftime('%Y-%m-%d')}")
        print(get_traceback(e))
        exit()
    df = _clean(df)
    if sample:
        print(f"...taking a sample of {sample}")
        df = df.sample(n=sample)
    df = _transform(df)
    df = write_load(df, os.path.join(PROCESSED_DATA_PATH, f"{file_date.strftime('%Y.%m.%d')}.{TABLENAME.lower()}.gzip"))
    df = write_load(df, os.path.join(FINAL_DATA_PATH, f"{file_date.strftime('%Y.%m.%d')}.{TABLENAME.lower()}.gzip"))
    # convert election_date to datetime
    df['file_date'] = pd.to_datetime(df['file_date'])
    _load_database(df)

    print(f"File Processed {len(df)} records")

if __name__ == "__main__":
    process_time = time.time()
    try:
        print(f"Processing raw {TABLENAME} file on {file_date.strftime('%Y-%m-%d')}")
        main()
    except Exception as e:
        print('------Start--------')
        print(get_traceback(e))
        print('------End--------')
    get_timing(process_time)
