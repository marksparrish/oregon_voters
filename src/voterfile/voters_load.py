"""
This file will store the final voter file in a compressed file,
database and elasticsearch index.
Database will be named votetracker_{state}_voters_{file_date}
Index will be named votetracker_{state}_voters_{file_date}
"""
import sys
sys.path.append(r'../src')
import time
from datetime import datetime

sys.path.append('/app')

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from config import STATE
from config.cli import FILE_DATE, SAMPLE
from config.common import get_timing, get_traceback

file_date = FILE_DATE
sample = SAMPLE
table_name = 'voters'
index_name = 'voters'
state = STATE

def _extract(df) -> pd.DataFrame:
    print("Extracting Data...")
    # get file names
    file_path = f"/data/{state}/voter_lists/final/{file_date.strftime('%Y.%m.%d')}.{table_name}.gzip"
    print(file_path)
    df = pq.read_table(source=file_path).to_pandas()

    return df.reset_index(drop=True)

def _transform(df) -> pd.DataFrame:
    df.drop(columns=['whole_address', 'whole_commas', 'whole_street'], inplace=True)
    df.drop(columns=['mail_address_3', 'mail_address_4'], inplace=True)
    df['file_date'] = file_date.strftime('%Y-%m-%d')
    df['state'] = state
    return df.reset_index(drop=True)

def _load(df) -> pd.DataFrame:
    print('Loading')
    print('...writing to index')
    curr_dt = datetime.now()
    timestamp = int(round(curr_dt.timestamp()))
    file = f"/logstash/{table_name}/{timestamp}.csv"
    # print(df.columns)
    df.to_csv(file, index=False, header=False)

    return df.reset_index(drop=True)

def main():
    df = pd.DataFrame()
    df = _extract(df)
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
