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
from utils.search import search_client, process_search_results
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

def search_for_address(address):
    index_name = "places"
    search_results = search_client.search_address(index_name, address)
    return process_search_results(search_results)

def exact_match_address(house_number, street_name, zip_code):
    index_name = "places"
    search_results = search_client.exact_match_address(index_name, house_number, street_name, zip_code)
    return process_search_results(search_results)


def _transform(df) -> pd.DataFrame:
    """
    Transform addresses (mail and physical) into standard formats
    Create hash from standard addresses
    Match which addresses are already geocoded (in the places table)
    Write unmatched addresses to file adding to places table and for geocoding (only physical)
    Write processed voters to file having the right columns
    """

    # fill na with empty string
    df = df.fillna('')
    print("...standardizing physical address")
    # find address
    df['find_address'] = df['physical_house_number'].astype(str) + ' ' + df['physical_street_name'].astype(str) + ' ' + df['physical_zip_code'].astype(str)
    address_df = df['find_address'].parallel_apply(search_for_address)
    df = df.join(address_df)

    # Add Confidetial to Results where the address is confidential
    # Confidential address are not geocoded and have a physical_id equal to the precinct_link
    df.loc[df['confidential'] == 'Confidential', 'results'] = 'Confidential'
    df.loc[df['confidential'] == 'Confidential', 'physical_id'] = df['precinct_link']

    # frist pass on too many results
    # see if there is an exact match on the house number, street and zip code
    mask = (df['results'] == "Too Many Results")
    df[["results",
        "phsyical_id",
        "PropertyAddressFull",
        "PropertyAddressHouseNumber",
        "PropertyAddressStreetDirection",
        "PropertyAddressStreetName",
        "PropertyAddressStreetSuffix",
        "PropertyAddressCity",
        "PropertyAddressState",
        "PropertyAddressZIP",
        "PropertyAddressZIP4",
        "PropertyAddressCRRT",
        "PropertyLatitude",
        "PropertyLongitude"]] = df.parallel_apply(lambda x: exact_match_address(x['physical_house_number'], x['physical_street_name'], x['physical_zip_code']) if mask[x.name] else
            (x["results"],
            x["phsyical_id"],
            x["PropertyAddressFull"],
            x["PropertyAddressHouseNumber"],
            x["PropertyAddressStreetDirection"],
            x["PropertyAddressStreetName"],
            x["PropertyAddressStreetSuffix"],
            x["PropertyAddressCity"],
            x["PropertyAddressState"],
            x["PropertyAddressZIP"],
            x["PropertyAddressZIP4"],
            x["PropertyAddressCRRT"],
            x["PropertyLatitude"],
            x["PropertyLongitude"]), axis=1, result_type="expand")

    # deal with addresses that have a unit number (Condos, Apartments, etc)
    # need to reapply the mask becuase the df has changed
    mask = (df['results'] == "Too Many Results")
    df['find_address'] = df['physical_house_number'].astype(str) + ' ' + df['physical_street_name'].astype(str) + ' ' + df['physical_zip_code'].astype(str) + ' ' + df['physical_unit_number'].astype(str)
    df[["results",
        "phsyical_id",
        "PropertyAddressFull",
        "PropertyAddressHouseNumber",
        "PropertyAddressStreetDirection",
        "PropertyAddressStreetName",
        "PropertyAddressStreetSuffix",
        "PropertyAddressCity",
        "PropertyAddressState",
        "PropertyAddressZIP",
        "PropertyAddressZIP4",
        "PropertyAddressCRRT",
        "PropertyLatitude",
        "PropertyLongitude"]] = df.parallel_apply(lambda x: search_for_address(x['find_address']) if mask[x.name] else
            (x["results"],
            x["phsyical_id"],
            x["PropertyAddressFull"],
            x["PropertyAddressHouseNumber"],
            x["PropertyAddressStreetDirection"],
            x["PropertyAddressStreetName"],
            x["PropertyAddressStreetSuffix"],
            x["PropertyAddressCity"],
            x["PropertyAddressState"],
            x["PropertyAddressZIP"],
            x["PropertyAddressZIP4"],
            x["PropertyAddressCRRT"],
            x["PropertyLatitude"],
            x["PropertyLongitude"]), axis=1, result_type="expand")

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
    print('')
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
