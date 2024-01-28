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

from utils.arg_parser import get_date, get_sample

from utils.config import RAW_DATA_PATH, PROCESSED_DATA_PATH, FINAL_DATA_PATH, WORKING_DATA_PATH, STATE
from voterfile_data_contract import DATA_CONTRACT, TABLENAME, dtype_mapping, final_columns
from utils.database import Database

def _transform(df) -> pd.DataFrame:
    print("Performing Final Data Transformtion...")
    file_date = get_date()
    df['file_date'] = file_date.strftime('%Y-%m-%d')
    df['state'] = STATE.lower()
    # add column address_type and set to 'residential'
    df['address_type'] = 'residential'
    # update adress_type to 'apartment' if physical_unit_type is empty, null or a blank string
    df.loc[df['physical_unit_type'] != '', 'address_type'] = 'apartment'

    # ensure birthdate is a date
    # if only year is present, set to Jan 1 of that year
    df['birthdate'] = df['birthdate'].astype(str)
    df.loc[df['birthdate'].str.len() == 4, 'birthdate'] = df['birthdate'] + '-01-01'

    # ensure PropertyLatitude and PropertyLongitude are numeric
    df['PropertyLatitude'] = pd.to_numeric(df['PropertyLatitude'], errors='coerce')
    df['PropertyLongitude'] = pd.to_numeric(df['PropertyLongitude'], errors='coerce')

    # Create a mask for rows where 'results' is 'Not Found'
    mask = df['results'] == 'Not Found'

    # Update 'PropertyAddressFull' for rows matching the mask
    df.loc[mask, 'physical_id'] = 'Not-Found-' + df['physical_address_1'] + ' ' + df['physical_address_2']
    df.loc[mask, 'PropertyAddressFull'] = df['physical_address_1'] + ' ' + df['physical_address_2']
    df.loc[mask, 'PropertyAddressCity'] = df['physical_city']
    df.loc[mask, 'PropertyAddressState'] = df['physical_state']
    df.loc[mask, 'PropertyAddressZIP'] = df['physical_zip_code']

    mask = df['results'] == 'Homeless'

    # Update 'PropertyAddressFull' for rows matching the mask
    df.loc[mask, 'physical_id'] = 'Homeless'
    df.loc[mask, 'PropertyAddressFull'] = 'Homeless'
    df.loc[mask, 'PropertyAddressCity'] = 'Homeless'
    df.loc[mask, 'PropertyAddressState'] = 'Homeless'
    df.loc[mask, 'PropertyAddressZIP'] = 'Homeless'
    df.loc[mask, 'address_type'] = 'Homeless'

    # drop columns
    df = df[final_columns]

    return df.reset_index(drop=True)

def _load_database(df) -> pd.DataFrame:
    print("....writing to database")

    file_date = get_date()
    database = "votetracker"
    db_connection = Database(database)
    engine = db_connection.get_engine()
    table_name = f"voters-{file_date.strftime('%Y-%m-%d')}"

    # df.to_sql(table_name, con=engine, index=False, if_exists='replace')
    df.to_sql(table_name, con=engine, index=False, if_exists='replace', dtype=dtype_mapping)
    return df.reset_index(drop=True)

def main():
    file_date = get_date()
    sample = get_sample()

    df = pd.DataFrame()
    df = read_extract(df, os.path.join(FINAL_DATA_PATH, f"{file_date.strftime('%Y.%m.%d')}.{TABLENAME.lower()}.gzip"))

    if sample > 0:
        print(f"...taking a sample of {sample}")
        df = df.sample(n=sample)
    df = _transform(df)
    df = _load_database(df)

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
