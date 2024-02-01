"""
*** OREGON Precinct Data ***
    Responsible for getting the data, validating that the data can be processed,
    initial cleaning (removing of unwanted records) and casting of columns into
    the right format.
"""
import sys
sys.path.append(r'../src')
import os
import time
from datetime import datetime
import pandas as pd

from common_functions.common import get_traceback, get_timing
from common_functions.file_operations import read_extract, write_load, read_extract_multiple

from utils.arg_parser import get_date, get_sample

from utils.config import RAW_DATA_PATH, PROCESSED_DATA_PATH, FINAL_DATA_PATH, WORKING_DATA_PATH, state, file_date, sample
from data_contracts.precinct_data_contract import DATA_CONTRACT, TABLENAME, dtype_mapping, final_columns
from utils.database import Database

def _clean(df):
    print("Cleaning data")
    # de dupe
    df = df.drop_duplicates()

    print(f"...after cleaning {len(df)} records remain")

    return df.reset_index(drop=True)

def _transform(df):
    print('Transforming data...')
    df['precinct_link'] = df['district_county'] + '-' + df['precinct_number'] + '-' + df['precinct_split']
    df['district_link'] = df['district_county'] + '-' + df['district_type'] + '-' + df['district_name']
    df['file_date'] = file_date.strftime('%Y-%m-%d')
    df['state'] = state.lower()
    df = df[final_columns]
    return df.reset_index(drop=True)

def _load_database(df) -> pd.DataFrame:
    print("....writing to database")

    database = "votetracker"
    db_connection = Database(database)
    engine = db_connection.get_engine()
    table_name = f"{TABLENAME.lower()}-{file_date.strftime('%Y-%m-%d')}"

    # df.to_sql(table_name, con=engine, index=False, if_exists='replace')
    df.to_sql(table_name, con=engine, index=False, if_exists='replace', dtype=dtype_mapping)
    return df.reset_index(drop=True)

def _create_indices():
    # Example usage
    database = "votetracker"
    db_connection = Database(database)
    table_name = f"{TABLENAME.lower()}-{file_date.strftime('%Y-%m-%d')}"
    db_connection.create_index(table_name, ["district_link"])
    db_connection.create_index(table_name, ["precinct_link"])
    db_connection.create_index(table_name, ["district_type", "district_name"])

def _create_view():
    database = "votetracker"
    db_connection = Database(database)

    table_name = f"{TABLENAME.lower()}-{file_date.strftime('%Y-%m-%d')}"
    db_connection.create_view(f"{TABLENAME.lower()}-current", table_name)

def main():
    df = pd.DataFrame()
    df = read_extract_multiple(df, os.path.join(RAW_DATA_PATH, file_date.strftime('%Y_%m_%d'), TABLENAME.lower()))
    df = validate_dataframe(df, DATA_CONTRACT)
    df = _clean(df)
    if sample:
        print(f"...taking a sample of {sample}")
        df = df.sample(n=sample)
    df = _transform(df)
    df = write_load(df, os.path.join(PROCESSED_DATA_PATH, f"{file_date.strftime('%Y.%m.%d')}.{TABLENAME.lower()}.gzip"))
    df = write_load(df, os.path.join(FINAL_DATA_PATH, f"{file_date.strftime('%Y.%m.%d')}.{TABLENAME.lower()}.gzip"))
    df = _load_database(df)
    _create_indices()
    _create_view()

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
