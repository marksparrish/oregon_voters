"""
*** OREGON Voter Data ***
    Responsible for getting the data, validating that the data can be processed,
    initial cleaning (removing of unwanted records) and casting of columns into
    the right format.
"""
import sys
import os
import time
from datetime import datetime
import pandas as pd
from  common_functions.common import get_traceback, get_timing
from utils.data_contract import DATA_CONTRACT
from utils.arg_parser import get_date_and_sample
from utils.config import STATE, TABLENAME, DATA_FILES

# Get the date and sample values using the imported function
file_date, sample = get_date_and_sample()

data_file_path = DATA_FILES
state_folder = STATE.lower()
table_name = TABLENAME.lower()

from_data_path = os.path.join(data_file_path, state_folder, 'voter_lists', 'raw', file_date.strftime('%Y_%m_%d'), table_name)
to_data_path = os.path.join(data_file_path, state_folder, 'voter_lists', 'processed', f"{file_date.strftime('%Y.%m.%d')}.{table_name}.gzip")

def _extract(df) -> pd.DataFrame:
    print("Extracting Data...")
    # get file names

    print(from_data_path)
    for file in os.listdir(from_data_path):
        if file.endswith(".txt"):
            print(os.path.join(from_data_path, file))
            temp_df = pd.read_csv(os.path.join(from_data_path, file), sep='\t', dtype=str, low_memory=False, encoding_errors='ignore', on_bad_lines='skip')
            df = pd.concat([df, temp_df], ignore_index=True)

    return df.reset_index(drop=True)

def _validate(df):
    print("Validating...", end=" ")

    vdf = pd.DataFrame()

    for key, possible_headers in DATA_CONTRACT.items():
        matching_columns = df.columns[df.columns.isin(possible_headers)]

        if len(matching_columns) > 0:
            vdf[key] = df[matching_columns[0]].str.strip()
        else:
            raise ValueError(f"Broken Contract! Missing columns for {key}: {possible_headers}")

    print("Done")
    return vdf.reset_index(drop=True)


def _clean(df):
    print(f"Cleaning data - {len(df)}")
    # You can continue working with the modified DataFrame 'df'

    # remove bad records, inactive voters and voters with no precinct
    mask = df['state_voter_id'] != 'ACP'
    df = df[mask]
    mask = df['voter_status'] != 'I'
    df = df[mask]
    mask = ~df['precinct'].isna()
    df = df[mask]

    # Define the default date value
    default_date = '1900-01-01'
    # Convert 'registration_date' column to datetime with the original format
    date_format = '%Y-%m-%d'
    # Try to parse the dates, and create a mask for invalid dates (NaT)
    df['registration_date'] = pd.to_datetime(df['registration_date'], format=date_format, errors='coerce')
    # Replace invalid dates with the default date
    df['registration_date'].fillna(default_date, inplace=True)
    # De-dupe and sort the DataFrame in one step
    df = df.sort_values(by='registration_date', ascending=False).drop_duplicates(subset='state_voter_id', keep='first').reset_index(drop=True)

    print(f"...after cleaning {len(df)} records remain")

    return df.reset_index(drop=True)

def _transform(df) -> pd.DataFrame:
     print(f"Initial Transformations....")
    # Define the confidential birth year
    # Convert the 'BIRTH_DATE' column to integers
    # Replace 'BIRTH_DATE' with 1850 for confidential voters
    generic_birth_year = 1850
    df.loc[df['confidential'] == 'confidential', 'birthdate'] = generic_birth_year
    df.loc[df['birthdate'].str.contains('X'), 'birthdate'] = generic_birth_year
    df['birthdate'] = df['birthdate'].astype(int, errors='ignore')
    df.loc[df['birthdate'] < generic_birth_year, 'birthdate'] = generic_birth_year
    current_year = pd.Timestamp.now().year
    # Calculate age by subtracting birth year from the current year
    df['age'] = current_year - df['birthdate']
    df['precinct_link'] = df['county'] + '-' + df['precinct'] + '-' + df['split']

    return df.drop_duplicates(subset="state_voter_id", keep="first").reset_index(drop=True)

def _load(df) -> pd.DataFrame:
    print('Loading data...')
    print("...saving processed data")
    df.to_parquet(to_data_path, index=False, compression='gzip')

    return df.reset_index(drop=True)

def main():
    df = pd.DataFrame()
    df = _extract(df)
    df = _validate(df)
    df = _clean(df)
    if sample > 0:
        print(f"...taking a sample of {sample}")
        df = df.sample(n=sample)
    df = _transform(df)
    df = _load(df)

    print(f"File Processed {len(df)} records")
    print(f"File Processed {len(df.columns)} columns")
    for col in df.columns:
        print(col)

if __name__ == "__main__":
    process_time = time.time()
    try:
        print(f"Processing raw voter file on {file_date.strftime('%Y-%m-%d')}")
        main()
    except Exception as e:
        print('------Start--------')
        print(get_traceback(e))
        print('------End--------')
    get_timing(process_time)
