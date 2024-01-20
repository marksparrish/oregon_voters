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
import numpy as np
import pyarrow.parquet as pq
from pandarallel import pandarallel

from common_functions.common import get_traceback, get_timing
from common_functions.physical_address import standardize_address
from common_functions.file_operations import read_extract, write_load
from utils.search import search_client
from utils.arg_parser import get_date_and_sample

from voterfile_data_contract import DATA_CONTRACT, STATE, TABLENAME
from utils.config import DATA_FILES


# Initialize pandarallel
pandarallel.initialize(progress_bar=True)

def truncate_words(address, max_length=10):
    # Split the address into words and truncate each word to max_length
    truncated_words = [word[:max_length] for word in address.split()]
    # Join the truncated words back into a string
    return ' '.join(truncated_words)

# Define a function to apply 'standardize_address' to each row
def search_for_address(address):
    index_name = "attom"

    # address = truncate_words(address)

    search_results = search_client.search_address(index_name, address)

    if len(search_results['hits']['hits']) == 1:
        hit = search_results['hits']['hits'][0]['_source']
        return pd.Series({
            "results": "Success",
            "phsyical_id": hit.get("[ATTOM ID]"),
            "PropertyAddressFull": hit.get("PropertyAddressFull"),
            "PropertyAddressHouseNumber": hit.get("PropertyAddressHouseNumber"),
            "PropertyAddressStreetDirection": hit.get("PropertyAddressStreetDirection"),
            "PropertyAddressStreetName": hit.get("PropertyAddressStreetName"),
            "PropertyAddressStreetSuffix": hit.get("PropertyAddressStreetSuffix"),
            "PropertyAddressCity": hit.get("PropertyAddressCity"),
            "PropertyAddressState": hit.get("PropertyAddressState"),
            "PropertyAddressZIP": hit.get("PropertyAddressZIP"),
            "PropertyAddressZIP4": hit.get("PropertyAddressZIP4"),
            "PropertyAddressCRRT": hit.get("PropertyAddressCRRT"),
            "PropertyLatitude": hit.get("PropertyLatitude"),
            "PropertyLongitude": hit.get("PropertyLongitude")
        })

    if len(search_results['hits']['hits']) > 1:
        return pd.Series({
            "results": "Too Many Results",
            "phsyical_id": "",
            "PropertyAddressFull": "",
            "PropertyAddressHouseNumber": "",
            "PropertyAddressStreetDirection": "",
            "PropertyAddressStreetName": "",
            "PropertyAddressStreetSuffix": "",
            "PropertyAddressCity": "",
            "PropertyAddressState": "",
            "PropertyAddressZIP": "",
            "PropertyAddressZIP4": "",
            "PropertyAddressCRRT": "",
            "PropertyLatitude": "",
            "PropertyLongitude": ""
        })

    return pd.Series({
        "results": "No Results",
        "phsyical_id": "",
        "PropertyAddressFull": "",
        "PropertyAddressHouseNumber": "",
        "PropertyAddressStreetDirection": "",
        "PropertyAddressStreetName": "",
        "PropertyAddressStreetSuffix": "",
        "PropertyAddressCity": "",
        "PropertyAddressState": "",
        "PropertyAddressZIP": "",
        "PropertyAddressZIP4": "",
        "PropertyAddressCRRT": "",
        "PropertyLatitude": "",
        "PropertyLongitude": ""
    })

def _transform(df) -> pd.DataFrame:
    """
    Transform addresses (mail and physical) into standard formats
    Create hash from standard addresses
    Match which addresses are already geocoded (in the places table)
    Write unmatched addresses to file adding to places table and for geocoding (only physical)
    Write processed voters to file having the right columns
    """
    print("...standardizing physical address")
    # find address
    df['find_address'] = df['physical_house_number'].astype(str) + ' ' + df['physical_street_name'].astype(str) + ' ' + df['physical_zip_code'].astype(str)
    address_df = df['find_address'].parallel_apply(search_for_address)
    df = df.join(address_df)
    # Define conditions

    # df['flattened_response'] = df['found_address'].parallel_apply(flatten_response)

    # # Convert the flattened responses to a DataFrame
    # df_flattened = pd.json_normalize(df['flattened_response'])

    # # Concatenate the new DataFrame with the original DataFrame
    # df = pd.concat([df, df_flattened], axis=1)

    # # Create a boolean mask for rows with "Too Many Results" and a non-empty 'physical_unit_type'
    # mask = (df['found_address'] == "Too Many Results") & df['physical_unit_type'].notna()
    # # Use the mask to assign values in the new column
    # df['apartment_flag'] = "Not Apartment"  # Default value
    # df.loc[mask, 'apartment_flag'] = "Possible Apartment"

    # df = df.drop('find_address', axis=1)
    return df.reset_index(drop=True)


def main():
    # Get the date and sample values using the imported function
    file_date, sample = get_date_and_sample()
    print(f"Processing processed voter file on {file_date.strftime('%Y-%m-%d')}")

    raw_data_path = os.path.join(DATA_FILES, STATE.lower(), 'voter_lists', 'raw', file_date.strftime('%Y_%m_%d'), TABLENAME.lower())
    processed_data_path = os.path.join(DATA_FILES, STATE.lower(), 'voter_lists', 'processed', f"{file_date.strftime('%Y.%m.%d')}.{TABLENAME.lower()}.gzip")
    final_data_path = os.path.join(DATA_FILES, STATE.lower(), 'voter_lists', 'final', f"{file_date.strftime('%Y.%m.%d')}.{TABLENAME.lower()}.gzip")

    df = pd.DataFrame()
    df = read_extract(df, processed_data_path)
    if sample:
        print(f"...taking a sample of {sample}")
        df = df.sample(n=sample)
    df = _transform(df)
    df = write_load(df, final_data_path)

    print(f"File Processed {len(df)} records")
    # for col in df.columns:
    #     print(col)
    # print(df.head())
    print(df['results'].value_counts())


if __name__ == "__main__":
    process_time = time.time()
    try:
        main()
    except Exception as e:
        print('------Start--------')
        print(get_traceback(e))
        print('------End--------')
    get_timing(process_time)
