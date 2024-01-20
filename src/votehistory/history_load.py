"""
This file will store the final voter file in a compressed file,
database and elasticsearch index.
Database will be named votetracker_{state}_voters_{file_date}
"""
import sys
import time
from datetime import datetime

sys.path.append('/app')

import pandas as pd
import pyarrow.parquet as pq
from config import STATE
from config.cli import FILE_DATE, SAMPLE
from config.common import get_traceback, get_timing

from history import STATE, TABLENAME

file_date = FILE_DATE
sample = SAMPLE
table_name = 'history'
index_name = 'history'
state = STATE

def _extract(df) -> pd.DataFrame:
    print("Extracting Data...", end=" ")
    # get file names
    file_path = f"/voterdata/{state}/voter_lists/final/{file_date.strftime('%Y.%m.%d')}.{table_name}.gzip"
    print(file_path)
    df = pq.read_table(source=file_path).to_pandas()
    print("...done")
    return df.reset_index(drop=True)

def _transform(df) -> pd.DataFrame:
    # we only want these columns
    df = df[['state', 'state_voter_id', 'election_date', 'voted', 'votes_early_days', 'voted_on_date']]
    return df.reset_index(drop=True)

def _load(df) -> pd.DataFrame:
    print("Loading data")
    curr_dt = datetime.now()
    timestamp = int(round(curr_dt.timestamp()))
    file_path = f"/logstash/{table_name}/{timestamp}.csv"
    df.to_csv(file_path, index=False, header=False)

    print("...done")
    return df.reset_index(drop=True)

def main():
    df = pd.DataFrame()
    df = _extract(df)
    if sample > 0:
        print(f"...taking a sample of {sample}")
        df = df.sample(n=sample)
    df = _transform(df)
    df = _load(df)
    print(df)

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
