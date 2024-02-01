"""
This file will store the final voter file in a compressed file,
database and elasticsearch index.
Database will be named votetracker_{state}_voters_{file_date}
Index will be named votetracker_{state}_voters_{file_date}
"""

import sys
sys.path.append(r'../src')
import os
import time
from datetime import datetime
import pandas as pd
import numpy as np
import pyarrow.parquet as pq


from common_functions.common import get_traceback, get_timing
from common_functions.file_operations import read_extract, write_load

from utils.config import RAW_DATA_PATH, PROCESSED_DATA_PATH, FINAL_DATA_PATH, WORKING_DATA_PATH, state, file_date, sample
from utils.database import Database

from data_contracts.voterfile_data_contract import DATA_CONTRACT, TABLENAME, dtype_mapping, final_columns


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
    db_connection.create_index(table_name, ["state_voter_id"])
    db_connection.create_index(table_name, ["precinct_link"])
    db_connection.create_index(table_name, ["physical_id"])

def _create_view():
    database = "votetracker"
    db_connection = Database(database)

    table_name = f"{TABLENAME.lower()}-{file_date.strftime('%Y-%m-%d')}"
    db_connection.create_view(f"{TABLENAME.lower()}-current", table_name)

def main():
    df = pd.DataFrame()
    df = read_extract(df, os.path.join(FINAL_DATA_PATH, f"{file_date.strftime('%Y.%m.%d')}.{TABLENAME.lower()}.gzip"))

    if sample > 0:
        print(f"...taking a sample of {sample}")
        df = df.sample(n=sample)
    _load_database(df)
    _create_indices()
    _create_view()
    print(f"File Processed {len(df)} records")

if __name__ == "__main__":
    process_time = time.time()
    try:
        print(f"Starting {os.path.basename(__file__)}")
        main()
    except Exception as e:
        print('------Start--------')
        print(get_traceback(e))
        print('------End--------')
    get_timing(process_time)
