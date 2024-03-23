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
from datetime import datetime
import pandas as pd

import glob

from common_functions.common import get_traceback, get_timing, cast_date, timing_decorator
from common_functions.file_operations import read_extract, write_load, read_extract_multiple

from utils.config import RAW_DATA_PATH, PROCESSED_DATA_PATH, FINAL_DATA_PATH, WORKING_DATA_PATH, LOGSTASH_DATA_PATH, state, file_date, sample
from utils.database import Database
from utils.dataframe_operations import validate_dataframe
from utils.transformations import convert_date_format

from data_contracts.votehistory_data_contract import DATA_CONTRACT, TABLENAME, dtype_mapping, final_columns

def _clean(df):
    print("Cleaning data")
    # de dupe
    df = df.drop_duplicates()
    # remove invalid state_voter_id
    df = df[df['state_voter_id'] != 'ACP']

    print(f"...after cleaning {len(df)} records remain")

    return df.reset_index(drop=True)

def _transform(df):
    print('Transforming data...', end =" ")
    for h in list(df):
        print(f"renaming or dropping {h}...", end =" ")
        try:
            new_header = datetime.strptime(h, "%m/%d/%Y").strftime("%Y-%m-%d")
            df = df.rename(columns = {h: new_header})
            print(f"...renaming to {new_header}")
            df[new_header] = df[new_header].fillna('ITV')
            df.loc[df[new_header].str.contains('-'), new_header] = 'ITV'
            df[new_header] = df[new_header].str.upper()
        except ValueError as e:
            if h == "state_voter_id":
                print(f"skipping {h}")
            else:
                print(f"...droping {h}")
                df = df.drop(columns=[h])
    df = pd.melt(df, id_vars=['state_voter_id'],  value_name='voted', var_name='election_date')
    print("Droping voters who are ITV (ineligible to vote)", end =" ")
    df = df[df['voted'] != 'ITV']
    print("...done")
    print("Adding state column")
    df['state'] = f"{state}".upper()
    df['votes_early_days'] = 0
    df['voted_on_date'] = df['election_date']

    df = df[['state', 'state_voter_id', 'election_date', 'voted', 'votes_early_days', 'voted_on_date']]
    return df.reset_index(drop=True)

def _load(df):
    print('Loading data', end =" ")
    # /Volumes/Data/voter_data/oregon/voter_lists/processed
    file_path = f"/voterdata/{state}/voter_lists/processed/{file_date.strftime('%Y.%m.%d')}.{tablename}.gzip"
    print(f"...writing processed data to {file_path}", end =" ")
    df.to_parquet(file_path, index=False, compression='gzip')
    print("...done")
    return df.reset_index(drop=True)

def main():
    print(f"Processing raw vote history file on {file_date.strftime('%Y-%m-%d')}")

    df = pd.DataFrame()
    df = read_extract_multiple(df, os.path.join(RAW_DATA_PATH, file_date.strftime('%Y_%m_%d'), 'history'))
    # rename VOTER_ID to state_voter_id
    df = df.rename(columns = {'VOTER_ID': 'state_voter_id'})
    df = _clean(df)
    df = _transform(df)
    # if sample:
    #     print(f"...taking a sample of {sample}")
    #     df = df.sample(n=sample)
    # df = write_load(df, os.path.join(PROCESSED_DATA_PATH, f"{file_date.strftime('%Y.%m.%d')}.{TABLENAME.lower()}.gzip"))

    print(df)

    print(f"File Processed {len(df)} records")

if __name__ == "__main__":
    process_time = time.time()
    try:
        print(f"Processing raw history file on {file_date.strftime('%Y-%m-%d')}")
        main()
    except Exception as e:
        print('------Start--------')
        print(get_traceback(e))
        print('------End--------')
    get_timing(process_time)
