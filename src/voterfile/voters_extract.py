"""
*** OREGON Voter Data ***
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

from data_contracts.voterfile_data_contract import DATA_CONTRACT, TABLENAME, dtype_mapping, final_columns, ACTIVE_VOTERS_CODES

# Get the date and sample values using the imported function

def _clean(df):
    print(f"Cleaning data - {len(df)}")
    # You can continue working with the modified DataFrame 'df'

    # remove bad records, inactive voters and voters with no precinct
    mask = df['state_voter_id'] != 'ACP'
    df = df[mask]
    mask = df['voter_status'].isin(ACTIVE_VOTERS_CODES)
    df = df[mask]
    mask = ~df['precinct'].isna()
    df = df[mask]

    # df['registration_date'] = df['registration_date'].parallel_apply(convert_date_format)
    df['registration_date'] = pd.to_datetime(df['registration_date'], format='%m-%d-%Y', errors='coerce')

    # cast back to string with a format of 'YYYY-MM-DD'
    if df['registration_date'].dtype == 'datetime64[ns]':
        df['registration_date'] = df['registration_date'].dt.strftime('%Y-%m-%d').astype(str)
    else:
        print("Conversion to datetime failed. Check the format and data.")

    # print(df['registration_date'].head())
    # print(df['registration_date'].tail())
    # exit()
    # De-dupe and sort the DataFrame in one step
    df = df.sort_values(by='registration_date', ascending=False).drop_duplicates(subset='state_voter_id', keep='first').reset_index(drop=True)

    print(f"...after cleaning {len(df)} records remain")

    return df.reset_index(drop=True)

def _transform(df) -> pd.DataFrame:
    print("Initial Transformations....")
    # Define the confidential birth year
    # Convert the 'BIRTH_DATE' column to integers
    # Replace 'BIRTH_DATE' with 1850 for confidential voters
    date2 = datetime(2017, 3, 4)
    current_year = pd.Timestamp.now().year
    if file_date < date2:
        df['birthdate'] = pd.to_datetime(df['birthdate'], errors='coerce')
        default_date = datetime(1850, 1, 1)
        df['birthdate'] = df['birthdate'].fillna(default_date)
        df['birth_year'] = df['birthdate'].astype(str).str[:4].astype(int)
        df['age'] = current_year - df['birth_year']
        df.drop(columns=['birth_year'], inplace=True)
    else:
        generic_birth_year = 1850
        df.loc[df['confidential'] == 'confidential', 'birthdate'] = generic_birth_year
        df.loc[df['birthdate'].str.contains('X'), 'birthdate'] = generic_birth_year
        df['birthdate'] = df['birthdate'].astype(int, errors='ignore')
        df.loc[df['birthdate'] < generic_birth_year, 'birthdate'] = generic_birth_year
        # Calculate age by subtracting birth year from the current year
        df['age'] = current_year - df['birthdate']


    df['precinct_link'] = df['county'] + '-' + df['precinct'] + '-' + df['split']

    return df.drop_duplicates(subset="state_voter_id", keep="first").reset_index(drop=True)

def main():
    print(f"Processing raw voter file on {file_date.strftime('%Y-%m-%d')}")

    df = pd.DataFrame()
    df = read_extract_multiple(df, os.path.join(RAW_DATA_PATH, file_date.strftime('%Y_%m_%d'), TABLENAME.lower()))
    df = validate_dataframe(df, DATA_CONTRACT)
    df = _clean(df)
    if sample > 0:
        print(f"...taking a sample of {sample}")
        df = df.sample(n=sample)
    df = _transform(df)
    # Nan to empty string
    df = df.fillna('')
    df = write_load(df, os.path.join(PROCESSED_DATA_PATH, f"{file_date.strftime('%Y.%m.%d')}.{TABLENAME.lower()}.gzip"))

    print(f"File Processed {len(df)} records")
    print(f"File Processed {len(df.columns)} columns")
    for col in df.columns:
        print(col)

if __name__ == "__main__":
    process_time = time.time()
    try:
        main()
    except Exception as e:
        print('------Start--------')
        print(get_traceback(e))
        print('------End--------')
    get_timing(process_time)
