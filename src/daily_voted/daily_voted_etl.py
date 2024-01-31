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

from common_functions.common import get_traceback, get_timing, timing_decorator
from common_functions.file_operations import read_extract, write_load, read_extract_multiple

from utils.arg_parser import get_date, get_sample

from utils.config import LOGSTASH_DATA_PATH, DATA_FILES, state, file_date, sample, initialize_pandarallel
from data_contracts.daily_voted_data_contract import DATA_CONTRACT, TABLENAME, dtype_mapping, final_columns
from utils.database import Database
from utils.file_operations import validate_dataframe
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

def _transform(df):
    print('Transforming data...', end =" ")
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
    # /Volumes/Data/voter_data/oregon/voter_lists/processed
    file_path = f"/voterdata/{state}/daily_voted/processed/{file_date.strftime('%Y.%m.%d')}.{TABLENAME}.gzip"
    print(f"...writing processed data to {file_path}", end =" ")
    df.to_parquet(file_path, index=False, compression='gzip')
    print("...done")
    return df.reset_index(drop=True)

def _load_index(df) -> None:
    print('...writing to index')
    df['id'] = df['state'] + '-' + df['election_date'] + '-' + df['state_voter_id']
    # set the index to _id column
    df = df.set_index('id', drop=False)
    index_name = f"votetracker-{state.lower()}-{TABLENAME.lower()}"
    index_documents(df, index_name)
    print("...done")

@timing_decorator
def main():
    df = pd.DataFrame()
    df = _extract(df)
    df = validate_dataframe(df, DATA_CONTRACT)
    df = _clean(df)
    if sample:
        print(f"...taking a sample of {sample}")
        df = df.sample(n=sample)
    df = _transform(df)
    # df = _load(df)
    _load_index(df)

    print(f"File Processed {len(df)} records")

if __name__ == "__main__":
    try:
        print(f"Processing raw history file on {file_date.strftime('%Y-%m-%d')}")
        main()
    except Exception as e:
        print('------Start--------')
        print(get_traceback(e))
        print('------End--------')
