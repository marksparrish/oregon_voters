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

from utils.config import LOGSTASH_DATA_PATH, DATA_FILES, state, file_date, sample, initialize_pandarallel, PROCESSED_DATA_PATH
from data_contracts.votehistory_data_contract import DATA_CONTRACT, TABLENAME, dtype_mapping, final_columns, ElectionVoting, Base
from utils.database import Database, fetch_existing_table, fetch_sql
from utils.file_operations import validate_dataframe
from utils.dataframe_operations import diff_dataframe
from utils.transformations import convert_date_format
from utils.search import index_documents

def _transform(df) -> pd.DataFrame:
    # we only want these columns
    df = df[final_columns]
    # convert election_date to datetime
    df['election_date'] = pd.to_datetime(df['election_date'])
    return df.reset_index(drop=True)

def _load(df) -> pd.DataFrame:
    print("Loading data")
    print(df.head())
    print(df.columns)
    print(df.info())
    print("...done")
    return df.reset_index(drop=True)

def main():
    df = pd.DataFrame()
    df = read_extract(df, os.path.join(PROCESSED_DATA_PATH, f"{file_date.strftime('%Y.%m.%d')}.{TABLENAME.lower()}.gzip"))
    if sample > 0:
        print(f"...taking a sample of {sample}")
        df = df.sample(n=sample)
    df = _transform(df)
    df = _load(df)

    print(f"File Processed {len(df)} records")

if __name__ == "__main__":
    process_time = time.time()
    try:
        print(f"Processing processed voter file on {file_date.strftime('%Y-%m-%d')}")
        main()
    except Exception as e:
        print('------Start--------')
        print(get_traceback(e))
        print('------End--------')
    get_timing(process_time)
