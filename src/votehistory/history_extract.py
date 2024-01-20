"""
*** OREGON History Data ***
    Responsible for getting the data, validating that the data can be processed,
    initial cleaning (removing of unwanted records) and casting of columns into
    the right format.
"""
import os
import sys
import time

sys.path.append('/app')

import pandas as pd
from config.cli import FILE_DATE, SAMPLE
from config.common import get_timing, get_traceback

from history import DATA_CONTRACT, STATE, TABLENAME

file_date = FILE_DATE
sample = SAMPLE
state = STATE
tablename = TABLENAME

def _extract(df):
    print("Extracting Data...")
    # get file names
    file_path = f"/voterdata/{state}/voter_lists/raw/{file_date.strftime('%Y_%m_%d')}/{tablename}"
    print(file_path)
    for file in os.listdir(file_path):
        if file.endswith(".txt"):
            print(os.path.join(file_path, file))
            temp_df = pd.read_csv(os.path.join(file_path, file), sep='\t', dtype=str, low_memory=False, encoding_errors='ignore', on_bad_lines='skip')
            df = pd.concat([df, temp_df], ignore_index=True)
    return df.reset_index(drop=True)

def _clean(df):
    print("Cleaning data")
    # de dupe
    df = df.drop_duplicates()
    # remove invalid state_voter_id
    df = df[df['state_voter_id'] != 'ACP']

    print(f"...after cleaning {len(df)} records remain")

    return df.reset_index(drop=True)

def _validate(df):
    print("...validating", end =" ")

    # we only have to validate one column - state_voter_id
    print("...renaming voter id column", end =" ")

    df_columns = df.columns
    for key, possible_headers in DATA_CONTRACT.items():
        # keep if any header in headers is in the df.columns
        in_df = False
        for heading in possible_headers:
            # capture heading as the column we need in the validated df (vdf)
            if heading in df_columns:
                in_df = heading

        if in_df:
            df = df.rename(columns = {in_df: key})
        else:
            raise ValueError(f"Broken Contract!@! for {key, possible_headers, df_columns}")
    print("...done")
    return df.reset_index(drop=True)

def _transform(df):
    print('Transforming data...', end =" ")

    print("...done")
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
    df = pd.DataFrame()
    df = _extract(df)
    df = _validate(df)
    df = _clean(df)
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
        print(f"Processing raw history file on {file_date.strftime('%Y-%m-%d')}")
        main()
    except Exception as e:
        print('------Start--------')
        print(get_traceback(e))
        print('------End--------')
    get_timing(process_time)
