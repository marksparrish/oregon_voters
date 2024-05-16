"""
*** OREGON History Data ***
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


# from common_functions.common import get_traceback, get_timing, timing_decorator
from common_functions.file_operations import read_extract, write_load, read_extract_multiple

from utils.arg_parser import get_date, get_sample

from utils.config import LOGSTASH_DATA_PATH, DATA_FILES, state, file_date, sample, initialize_pandarallel
from data_contracts.daily_voted_data_contract import DATA_CONTRACT, TABLENAME, dtype_mapping, final_columns, DailyVoted, Base
from utils.database import Database, fetch_existing_table, fetch_sql
from utils.file_operations import validate_dataframe
from utils.dataframe_operations import diff_dataframe
from utils.transformations import convert_date_format
from utils.search import index_documents

initialize_pandarallel()

def _extract(df):
    print("Extracting Data...")
    # get most recent file
    # file_path =   f"/Volumes/nfs-data/voter_data/oregon/daily_voted/raw/2023-05-16/Voted & Not Voted 5-23-2022.zip"
    file_path =     os.path.join(DATA_FILES, state.lower(), 'daily_voted', 'raw', f"{file_date.strftime('%Y-%m-%d')}/*.zip")
    lst_files = glob.glob(file_path)
    lst_files.sort(key=os.path.getmtime)
    file_path = lst_files[-1:][0]
    names = ZipFile(file=file_path).namelist()
    for n in names:
        if os.path.basename(n):
            if 'readme' not in n:
                print(n)
                temp_df = pd.read_csv(ZipFile(file=file_path).open(n), sep='\t', dtype=str, on_bad_lines='skip')
                # Ex-VoterNotVoted-gbergerson-2024-05-14-72459_YAMHILL.txt
                # get the last part of the file name by underscore
                county = os.path.basename(n).split('_')[-1].split('.')[0]
                print(county)
                temp_df['county'] = county
                df = pd.concat([df, temp_df])

    return df.reset_index(drop=True)

def _clean(df):
    print("Cleaning data")
    # de dupe
    df = df.drop_duplicates(subset=['county', 'state_voter_id', 'voted_on_date'], keep='last')
    # remove invalid state_voter_id
    df = df[df['state_voter_id'] != 'ACP']

    print(f"...after cleaning {len(df)} records remain")

    return df.reset_index(drop=True)

def _transform(df):
    print('Transforming data...')
    df['state'] = state.upper()
    df['election_date'] = file_date.strftime('%Y-%m-%d')
    df['voted'] = 'YES'
    df['voted_on_date'] = df['voted_on_date'].parallel_apply(convert_date_format)

    df = df[final_columns]
    return df.reset_index(drop=True)

def _load(df):
    print('Loading data', end =" ")
    df = _load_file(df)
    df = _load_index(df)

    return df.reset_index(drop=True)

def _load_file(df):
    file_path = os.path.join(DATA_FILES, state.lower(), 'daily_voted', 'processed', f"{file_date.strftime('%Y.%m.%d')}.{TABLENAME.lower()}.gzip")
    # file_path = f"/voterdata/{state}/daily_voted/processed/{file_date.strftime('%Y.%m.%d')}.{TABLENAME}.gzip"
    print(f"...writing processed data to {file_path}", end =" ")
    df.to_parquet(file_path, index=False, compression='gzip')
    print("...done")

def _load_index(df) -> None:
    print('...writing to index')
    df['id'] = df['state'] + '-' + df['election_date'] + '-' + df['state_voter_id']
    # set the index to _id column
    df = df.set_index('id', drop=False)
    index_name = f"votetracker-{state.lower()}-{TABLENAME.lower()}"
    index_documents(df, index_name)
    print("...done")

# Assuming Database class and other necessary imports and configurations are defined elsewhere

def _load_history(current_df, engine):
    table_name = f"{TABLENAME.lower()}_history"
    sql = f"SELECT * FROM {table_name} where election_date = '{file_date.strftime('%Y-%m-%d')}'"
    existing_records = fetch_sql(engine, sql)

    # convert election_date to datetime
    existing_records['election_date'] = pd.to_datetime(existing_records['election_date'])
    #convert voted_on_date to datetime
    existing_records['voted_on_date'] = pd.to_datetime(existing_records['voted_on_date'])

    insert_df = diff_dataframe(df_new=current_df, df_existing=existing_records, on_columns=final_columns)
    print(f"Found {len(insert_df)} new records to insert")
    results = insert_df.to_sql(table_name, engine, if_exists='append', index=False, dtype=dtype_mapping, method='multi', chunksize=5000)

def _load_current(current_df, engine):
    table_name = f"{TABLENAME.lower()}_{file_date.strftime('%Y_%m_%d')}"
    existing_records = fetch_existing_table(engine, table_name, final_columns)

    insert_df = diff_dataframe(df_new=current_df, df_existing=existing_records, on_columns=final_columns)
    print(f"Found {len(insert_df)} new records to insert")
    results = insert_df.to_sql(table_name, engine, if_exists='append', index=False, dtype=dtype_mapping, method='multi', chunksize=5000)

def _load_database(df_orginal, database) -> pd.DataFrame:
    """
    Load data from a DataFrame into the specified database.

    Parameters:
    df (pd.DataFrame): DataFrame containing the data to be loaded.
    database (str): Database connection string.

    Returns:
    pd.DataFrame: DataFrame with the loaded data.
    """
    db_connection = Database(database)
    print(f"Writing to database {database}")
    engine = db_connection.get_engine()

    # shape df with final columns
    df = df_orginal[final_columns].copy()

    # Create the table if it doesn't exist
    Base.metadata.create_all(bind=engine, checkfirst=True)

    # convert election_date to datetime
    df['election_date'] = pd.to_datetime(df['election_date'])
    #convert voted_on_date to datetime
    df['voted_on_date'] = pd.to_datetime(df['voted_on_date'])

    # _load current records
    _load_current(df, engine)
    # _load history records
    _load_history(df, engine)

# @timing_decorator
def main():
    database = "oregon_voter_vote_history"
    df = pd.DataFrame()
    df = _extract(df)
    print(f"...extracted {len(df)} records")
    df = validate_dataframe(df, DATA_CONTRACT)
    print(f"...validated {len(df)} records")
    df = _clean(df)
    print(f"...cleaned {len(df)} records")
    if sample:
        print(f"...taking a sample of {sample}")
        df = df.sample(n=sample)
    df = _transform(df)
    print(f"...transformed {len(df)} records")
    _load_file(df)
    _load_index(df)
    _load_database(df, database)

    print(f"File Processed {len(df)} records")

def get_traceback(e):
    lines = traceback.format_exception(type(e), e, e.__traceback__)
    return ''.join(lines)

if __name__ == "__main__":
    try:
        print(f"Processing raw history file on {file_date.strftime('%Y-%m-%d')}")
        main()
    except Exception as e:
        print('------Start--------')
        print(get_traceback(e))
        print('------End--------')
