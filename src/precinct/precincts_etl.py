"""
*** OREGON Precinct Data ***
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
from common_functions.common import get_traceback, get_timing
from utils.arg_parser import get_date_and_sample
from utils.config import DATA_FILES
from precinct_data_contract import DATA_CONTRACT, STATE, TABLENAME

# Get the date and sample values using the imported function
file_date, sample = get_date_and_sample()

raw_data_path = os.path.join(DATA_FILES, STATE.lower(), 'voter_lists', 'raw', file_date.strftime('%Y_%m_%d'), TABLENAME.lower())
processed_data_path = os.path.join(DATA_FILES, STATE.lower(), 'voter_lists', 'processed', f"{file_date.strftime('%Y.%m.%d')}.{TABLENAME.lower()}.gzip")
final_data_path = os.path.join(DATA_FILES, STATE.lower(), 'voter_lists', 'final', f"{file_date.strftime('%Y.%m.%d')}.{TABLENAME.lower()}.gzip")

def _extract(df) -> pd.DataFrame:
    print("Extracting Data...")
    # get file names

    print(from_data_path)
    for file in os.listdir(from_data_path):
        if file.endswith(".txt"):
            print(os.path.join(raw_data_path, file))
            temp_df = pd.read_csv(os.path.join(from_data_path, file), sep='\t', dtype=str, low_memory=False, encoding_errors='ignore', on_bad_lines='skip')
            df = pd.concat([df, temp_df], ignore_index=True)

    return df.reset_index(drop=True)

def _clean(df):
    print("Cleaning data")
    # de dupe
    df = df.drop_duplicates()

    print(f"...after cleaning {len(df)} records remain")

    return df.reset_index(drop=True)

def _validate(df):
    print("...validating", end =" ")
    vdf = pd.DataFrame(data=None)
    df_columns = df.columns
    for key, possible_headers in DATA_CONTRACT.items():
        # keep if any header in headers is in the df.columns
        in_df = False
        for heading in possible_headers:
            # capture heading as the column we need in the validated df (vdf)
            if heading in df_columns:
                in_df = heading

        if in_df:
            vdf[key] = df[in_df].str.strip()
        else:
            raise ValueError(f"Broken Contract!@! for {key, possible_headers, df_columns}")
    print("...done")
    return vdf.reset_index(drop=True)

def _transform(df):
    print('Transforming data...')
    df['precinct_link'] = df['disrict_county'] + '-' + df['precinct_number'] + '-' + df['precinct_split']
    df['district_link'] = df['disrict_county'] + '-' + df['district_type'] + '-' + df['district_name']
    df['file_date'] = file_date.strftime('%Y-%m-%d')
    df['state'] = state
    return df.reset_index(drop=True)

def _load(df):
    print('Loading data...')
    print("...saving processed data")

    df.to_parquet(processed_data_path, index=False, compression='gzip')
    df.to_parquet(final_data_path, index=False, compression='gzip')

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

    print(f"File Processed {len(df)} records")

if __name__ == "__main__":
    process_time = time.time()
    try:
        print(f"Processing raw {TABLENAME} file on {file_date.strftime('%Y-%m-%d')}")
        main()
    except Exception as e:
        print('------Start--------')
        print(get_traceback(e))
        print('------End--------')
    get_timing(process_time)
