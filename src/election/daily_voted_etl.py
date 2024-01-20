"""
*** OREGON History Data ***
    Responsible for getting the data, validating that the data can be processed,
    initial cleaning (removing of unwanted records) and casting of columns into
    the right format.
"""
import glob
import os
import sys
import time

sys.path.append('/app')
from zipfile import ZipFile

import pandas as pd
from config.cli import FILE_DATE, SAMPLE
from config.common import get_timing, get_traceback

from daily_voted import DATA_CONTRACT, STATE, TABLENAME

file_date = FILE_DATE
sample = SAMPLE
state = STATE
tablename = TABLENAME

def _extract(df):
    print("Extracting Data...")
    # get most recent file
    # file_path =   f"/voterdata/{state}/daily_voted/raw/2022-05-17/Voted & Not Voted 5-23-2022.zip"
    file_path =     f"/voterdata/{state}/daily_voted/raw/{file_date.strftime('%Y-%m-%d')}/*.zip"
    lst_files = glob.glob(file_path)
    lst_files.sort(key=os.path.getmtime)
    file_path = lst_files[-1:][0]
    print(file_path)
    names = ZipFile(file=file_path).namelist()
    for n in names:
        if os.path.basename(n):
            if 'readme' not in n:
                # print(n)
                temp_df = pd.read_csv(ZipFile(file=file_path).open(n), sep='\t', dtype=str, on_bad_lines='skip')
                df = pd.concat([df, temp_df])
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
    df = df.rename(columns={"RECEIVE_DATE": "voted_on_date"})
    df['state'] = 'OREGON'
    df['election_date'] = file_date.strftime('%Y-%m-%d')
    df['voted'] = 'YES'
    df['voted_on_date'] = pd.to_datetime(df['voted_on_date'],format='%m-%d-%Y').dt.strftime('%Y-%m-%d')

    df[['A','B']] = df[['election_date','voted_on_date']].apply(pd.to_datetime)
    df['votes_early_days'] = (df['A'] - df['B']).dt.days

    df = df[['state', 'state_voter_id', 'election_date', 'voted', 'votes_early_days', 'voted_on_date']]
    print("...done")
    df = df.dropna()
    return df.reset_index(drop=True)

def _load(df):
    print('Loading data', end =" ")
    df = _load_file(df)
    df = _load_index(df)

    return df.reset_index(drop=True)

def _load_file(df):
    # /Volumes/Data/voter_data/oregon/voter_lists/processed
    file_path = f"/voterdata/{state}/daily_voted/processed/{file_date.strftime('%Y.%m.%d')}.{tablename}.gzip"
    print(f"...writing processed data to {file_path}", end =" ")
    df.to_parquet(file_path, index=False, compression='gzip')
    print("...done")
    return df.reset_index(drop=True)

def _load_index(df) -> pd.DataFrame:
    file_path = f"/logstash/history/{state}-{tablename}-{file_date.strftime('%Y.%m.%d')}.csv"
    print(f"...writing processed data to logstash {file_path}", end =" ")
    df.to_csv(file_path, mode='a', index=False, header=False)

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
