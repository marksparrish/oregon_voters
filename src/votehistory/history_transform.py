"""
*** OREGON Precinct Data ***
    Responsible for getting the data, validating that the data can be processed,
    initial cleaning (removing of unwanted records) and casting of columns into
    the right format.
"""
import os
import sys
import time
from datetime import datetime

sys.path.append('/app')

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from config.cli import FILE_DATE, SAMPLE
from config.common import get_timing, get_traceback

from history import STATE, TABLENAME

file_date = FILE_DATE
sample = SAMPLE
state = STATE
tablename = TABLENAME

def _extract(df):
    print("Extracting Data", end =" ")
    # get file names
    file_path = f"/voterdata/{state}/voter_lists/processed/{file_date.strftime('%Y.%m.%d')}.{tablename}.gzip"
    df = pq.read_table(source=file_path).to_pandas()
    print("...done")
    return df.reset_index(drop=True)

def _transform(df):
    print("Transforming data into election file")
    # we only want elections data so we drop all columns except for state_voter_id
    # and rename the columns to a proper datetime
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
    file_path = f"/voterdata/{state}/voter_lists/final/{file_date.strftime('%Y.%m.%d')}.{tablename}.gzip"
    print(f"...writing processed data to {file_path}", end =" ")
    df.to_parquet(file_path, index=False, compression='gzip')
    print("...done")
    return df.reset_index(drop=True)

def main():
    df = pd.DataFrame()
    df = _extract(df)
    if sample:
        print(f"...taking a sample of {sample}")
        df = df.sample(n=sample)
    df = _transform(df)
    df = _load(df)
    # print(df)
    print(f"File Processed {len(df)} records")

if __name__ == "__main__":
    process_time = time.time()
    try:
        print(f"Processing raw {tablename} file on {file_date.strftime('%Y-%m-%d')}")
        main()
    except Exception as e:
        print('------Start--------')
        print(get_traceback(e))
        print('------End--------')
    get_timing(process_time)
