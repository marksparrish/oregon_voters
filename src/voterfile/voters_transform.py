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
import pyarrow.parquet as pq

from common_functions.common import get_traceback, get_timing
from common_functions.physical_address import standardize_address
from utils.data_contract import DATA_CONTRACT
from utils.arg_parser import get_date_and_sample
from utils.config import STATE, TABLENAME, DATA_FILES

# Get the date and sample values using the imported function
file_date, sample = get_date_and_sample()

data_file_path = DATA_FILES
state_folder = STATE.lower()
table_name = TABLENAME.lower()

from_data_path = os.path.join(data_file_path, state_folder, 'voter_lists', 'processed', f"{file_date.strftime('%Y.%m.%d')}.{table_name}.gzip")
to_data_path = os.path.join(data_file_path, state_folder, 'voter_lists', 'final', f"{file_date.strftime('%Y.%m.%d')}.{table_name}.gzip")

def _extract(df) -> pd.DataFrame:
    print("Extracting Data...")
    # get file names
    df = pq.read_table(source=from_data_path).to_pandas()
    return df.reset_index(drop=True)

# Define a function to apply 'standardize_address' to each row
def apply_standardize_address(row):
    # Create a list of address columns you want to standardize
    address_columns = [
        row['physical_address_1'],
        row['physical_address_2'],
        row['physical_city'],
        row['physical_state'],
        row['physical_zip_code']
    ]
    address_columns = ['' if x is None else x for x in address_columns]
    # Call your 'standardize_address' function with the list of address columns
    return standardize_address(address_columns)

def _transform(df) -> pd.DataFrame:
    """
    Transform addresses (mail and physical) into standard formats
    Create hash from standard addresses
    Match which addresses are already geocoded (in the places table)
    Write unmatched addresses to file adding to places table and for geocoding (only physical)
    Write processed voters to file having the right columns
    """
    print("...standardizing physical address")
    # Standardize physical address
    result = df.apply(apply_standardize_address, axis=1)
    # Create new columns for the results
    df['StandardizedAddress'] = [result[i][0] for i in range(len(result))]
    df['AddressType'] = [result[i][1] for i in range(len(result))]


    # print("...standardizing mailing address")
    # # Standardize mailing address
    # mail_columns = ['mail_address_1', 'mail_address_2', 'mail_address_3', 'mail_address_4', 'mail_city', 'mail_state', 'mail_zip_code']
    # df['mail_address_whole'] = df[mail_columns].apply(standardize_address, axis=1, result_type='expand')

    # print("...creating hashes for physical and mail addresses")
    # # for confidential addresses we hash the precinct_link
    # df["physical_address_whole"] = np.where(df["confidential"] == "YES", df['precinct_link'], df['physical_address_whole'])
    # df['physical_id'] = df['physical_address_whole'].apply(create_hash)
    # df['mail_id'] = df['mail_address_whole'].apply(create_hash)

    return df.reset_index(drop=True)

def _load(df) -> pd.DataFrame:
    print('Loading data...')
    print("...saving processed data")
    file_path = f"/data/{STATE}/voter_lists/final/{file_date.strftime('%Y.%m.%d')}.{TABLENAME}.gzip"
    df.to_parquet(file_path, index=False, compression='gzip')

    return df.reset_index(drop=True)

def main():
    df = pd.DataFrame()
    df = _extract(df)
    if sample:
        print(f"...taking a sample of {sample}")
        df = df.sample(n=sample)
    df = _transform(df)
    # df = _load(df)

    print(f"File Processed {len(df)} records")
    print(f"File Processed {len(df.columns)} columns")
    for col in df.columns:
        print(col)

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
