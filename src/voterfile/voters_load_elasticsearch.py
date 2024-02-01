"""
This script processes a final voter file, writing it to a logstash pipeline to be sent to an Elasticsearch index.
The database and index are named using the format: votetracker_{state}_voters_{file_date}.
"""

import sys
sys.path.append(r'../src')
import os
import time
from datetime import datetime
import pandas as pd

from common_functions.common import get_traceback, get_timing
from common_functions.file_operations import read_extract, write_load

from utils.config import RAW_DATA_PATH, PROCESSED_DATA_PATH, FINAL_DATA_PATH, WORKING_DATA_PATH, LOGSTASH_DATA_PATH, state, file_date, sample
from utils.database import Database

from data_contracts.voterfile_data_contract import DATA_CONTRACT, TABLENAME, dtype_mapping, final_columns

def _load(df) -> pd.DataFrame:
    """
    Loads the DataFrame into an Elasticsearch index and returns the modified DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame to be loaded.

    Returns:
        pd.DataFrame: The modified DataFrame after loading.
    """
    print('Loading')
    print('...writing to index')
    curr_dt = datetime.now()
    timestamp = int(round(curr_dt.timestamp()))
    file = f"{LOGSTASH_DATA_PATH}/{TABLENAME.lower()}/{timestamp}.csv"
    df.to_csv(file, index=False, header=False)

    return df.reset_index(drop=True)

def main():
    """
    Main function to process the voter file.
    """
    df = pd.DataFrame()
    file_path = os.path.join(FINAL_DATA_PATH, f"{file_date.strftime('%Y.%m.%d')}.{TABLENAME.lower()}.gzip")
    df = read_extract(df, file_path)

    if sample > 0:
        print(f"...taking a sample of {sample}")
        df = df.sample(n=sample)
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
