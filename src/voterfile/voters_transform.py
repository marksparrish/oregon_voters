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


from common_functions.common import get_traceback, get_timing
from common_functions.physical_address import standardize_address
from common_functions.file_operations import read_extract, write_load
from utils.search import search_client, process_search_results

from voterfile_data_contract import DATA_CONTRACT, TABLENAME
from utils.config import RAW_DATA_PATH, PROCESSED_DATA_PATH, FINAL_DATA_PATH, WORKING_DATA_PATH, state, file_date, sample, iteration
from utils.transformations import initialize_pandarallel, join_columns, mark_homeless_addresses

initialize_pandarallel()

def truncate_words(address, max_length=10):
    # Split the address into words and truncate each word to max_length
    truncated_words = [word[:max_length] for word in address.split()]
    # Join the truncated words back into a string
    return ' '.join(truncated_words)

def search_for_address(address):
    index_name = "places"
    search_results = search_client.search_address(index_name, address)
    return process_search_results(search_results)

def search_for_apartments(address):
    index_name = "places-previous"
    search_results = search_client.search_unit_address(index_name, address)
    return process_search_results(search_results)


def search_exact_match_address(house_number, street_name, zip_code):
    index_name = "places"
    search_results = search_client.exact_match_address(index_name, house_number, street_name, zip_code)
    return process_search_results(search_results)

def update_address_fields(df, mask, search_function):
    columns_to_update = ["results", "physical_id", "PropertyAddressFull", "PropertyAddressHouseNumber", "PropertyAddressStreetDirection", "PropertyAddressStreetName", "PropertyAddressStreetSuffix", "PropertyAddressCity", "PropertyAddressState", "PropertyAddressZIP", "PropertyAddressZIP4", "PropertyAddressCRRT", "PropertyLatitude", "PropertyLongitude"]
    df.loc[mask, columns_to_update] = df[mask].apply(lambda x: search_function(x['find_address']), axis=1, result_type="expand")

def _transform_pass_01(df, columns_to_update) -> pd.DataFrame:
    for column in columns_to_update:
        df[column] = None

    df['results'] = 'Not Found'

    print("...marking confidential addresses")
    confidential_mask = df['confidential'] == 'Confidential'
    df.loc[confidential_mask, 'results'] = 'Confidential'
    df.loc[confidential_mask, 'physical_id'] = df['precinct_link']

    print("...marking homeless addresses")
    # Homeless addresses
    tests = [
        ('startswith', '00'),
        ('contains', '@'),
        ('contains', 'PARKING '),
        ('contains', 'Lot '),
        ('contains', 'AROUND '),
        ('contains', ' & '),
        ('contains', 'SAFE CAMP'),
        ('contains', 'CORNER OF '),
        ('contains', 'BETWEEN '),
        ('startswith', 'NEAR '),
        ('startswith', '0 '),
        ('startswith', 'HOSELESS '),
        ('startswith', 'HOMELESS '),
        ('startswith', 'BEHIND '),
    ]

    # Call the function
    df = mark_homeless_addresses(df, tests)

    mask = df['physical_address_2'].str.contains(' AND ')
    df.loc[mask, 'results'] = 'Homeless'

    print(df['results'].value_counts())
    return df

def _transform_pass_02(df, columns_to_update) -> pd.DataFrame:
    mask = df['results'] == 'Not Found'
    df.loc[mask, columns_to_update] = df[mask].parallel_apply(lambda x: search_for_address(x['find_address']), axis=1, result_type="expand")
    print(df['results'].value_counts())
    return df

def _transform_pass_03(df, columns_to_update) -> pd.DataFrame:
    # deal with appartments in the apartment index
    mask = df['results'] == 'Not Found'
    # df['find_address'] = df['physical_house_number'].astype(str) + ' ' + df['physical_street_name'].astype(str) + ' ' + df['physical_zip_code'].astype(str) + ' ' + df['physical_unit_number'].astype(str)
    df.loc[mask, columns_to_update] = df[mask].parallel_apply(lambda x: search_for_apartments(x['find_address']), axis=1, result_type="expand")
    print(df['results'].value_counts())
    return df

def _transform_pass_04(df, columns_to_update) -> pd.DataFrame:
    # deal with appartments in the apartment index
    mask = df['results'] == 'Not Found'
    # df['find_address'] = df['physical_house_number'].astype(str) + ' ' + df['physical_street_name'].astype(str) + ' ' + df['physical_zip_code'].astype(str) + ' ' + df['physical_unit_number'].astype(str)
    df.loc[mask, columns_to_update] = df[mask].parallel_apply(lambda x: search_for_apartments(x['find_address']), axis=1, result_type="expand")
    print(df['results'].value_counts())
    return df

def _transform_pass_05(df, columns_to_update) -> pd.DataFrame:
    mask = df['results'] == 'Not Found'
    df.loc[mask, columns_to_update] = df[mask].parallel_apply(lambda x: search_exact_match_address(x['physical_house_number'], x['physical_street_name'], x['physical_zip_code']), axis=1, result_type="expand")
    print(df['results'].value_counts())
    return df

def _transform_pass_06(df, columns_to_update) -> pd.DataFrame:
    mask = df['results'] == 'Not Found'
    df.loc[mask, columns_to_update] = df[mask].parallel_apply(lambda x: search_for_address(x['find_address']), axis=1, result_type="expand")
    print(df['results'].value_counts())
    mask = df['results'] == 'Not Found'
    # df['find_address'] = df['physical_house_number'].astype(str) + ' ' + df['physical_street_name'].astype(str) + ' ' + df['physical_zip_code'].astype(str) + ' ' + df['physical_unit_number'].astype(str)
    df.loc[mask, columns_to_update] = df[mask].parallel_apply(lambda x: search_for_apartments(x['find_address']), axis=1, result_type="expand")
    print(df['results'].value_counts())
    return df

def _transform_pass_07(df, columns_to_update) -> pd.DataFrame:
    mask = df['results'] == 'Not Found'
    df.loc[mask, columns_to_update] = df[mask].parallel_apply(lambda x: search_for_address(x['find_address']), axis=1, result_type="expand")
    print(df['results'].value_counts())
    mask = df['results'] == 'Not Found'
    # df['find_address'] = df['physical_house_number'].astype(str) + ' ' + df['physical_street_name'].astype(str) + ' ' + df['physical_zip_code'].astype(str) + ' ' + df['physical_unit_number'].astype(str)
    df.loc[mask, columns_to_update] = df[mask].parallel_apply(lambda x: search_for_apartments(x['find_address']), axis=1, result_type="expand")
    print(df['results'].value_counts())
    return df

def _transform_pass_final(df, columns_to_update) -> pd.DataFrame:
    return df

def _transform(df, iteration) -> pd.DataFrame:
    """
    Transform addresses (mail and physical) into standard formats
    Create hash from standard addresses
    Match which addresses are already geocoded (in the places table)
    Write unmatched addresses to file adding to places table and for geocoding (only physical)
    Write processed voters to file having the right columns
    """

    # fill na with empty string
    df = df.fillna('')
    columns_to_update = ["results", "physical_id", "PropertyAddressFull", "PropertyAddressHouseNumber", "PropertyAddressStreetDirection", "PropertyAddressStreetName", "PropertyAddressStreetSuffix", "PropertyAddressCity", "PropertyAddressState", "PropertyAddressZIP", "PropertyAddressZIP4", "PropertyAddressCRRT", "PropertyLatitude", "PropertyLongitude"]


    # we are only going run the exact iteration or if it is 0 then we will run all iterations
    if iteration == 0 or iteration == 1:
        interation_pass = '01'
        print(f"...pass { interation_pass }")
        df = _transform_pass_01(df, columns_to_update)
        # we are going write the results of the first pass to a file with the date and iteration
        # df.to_parquet(os.path.join(WORKING_DATA_PATH, f"{TABLENAME}-places-iteration-{ interation_pass }.parquet"), compression='gzip')

    if iteration == 0 or iteration == 2:
        df['find_address'] = df['physical_house_number'].astype(str) + ' ' + df['physical_street_pre_direction'].astype(str) + ' ' + df['physical_street_name'].astype(str) + ' ' + df['physical_zip_code'].astype(str) + ' ' + df['physical_unit_number'].astype(str)
        interation_pass = '02'
        print(f"...pass { interation_pass }")
        df = _transform_pass_02(df, columns_to_update)
        df.to_parquet(os.path.join(WORKING_DATA_PATH, f"{TABLENAME}-places-iteration-{ interation_pass }.parquet"), compression='gzip')

    if iteration == 0 or iteration == 3:
        interation_pass = '03'
        print(f"...pass { interation_pass }")
        df = _transform_pass_03(df, columns_to_update)
        df.to_parquet(os.path.join(WORKING_DATA_PATH, f"{TABLENAME}-places-iteration-{ interation_pass }.parquet"), compression='gzip')

    if iteration == 0 or iteration == 4:
        df['find_address'] = df['find_address'] + ' ' + df['physical_unit_type'].astype(str)
        interation_pass = '04'
        print(f"...pass { interation_pass }")
        df = _transform_pass_04(df, columns_to_update)
        df.to_parquet(os.path.join(WORKING_DATA_PATH, f"{TABLENAME}-places-iteration-{ interation_pass }.parquet"), compression='gzip')

    if iteration == 0 or iteration == 5:
        interation_pass = '05'
        print(f"...pass { interation_pass }")
        df = _transform_pass_05(df, columns_to_update)
        df.to_parquet(os.path.join(WORKING_DATA_PATH, f"{TABLENAME}-places-iteration-{ interation_pass }.parquet"), compression='gzip')

    if iteration == 0 or iteration == 6:
        df['find_address'] = df['find_address'] + ' ' + df['physical_street_suffix'].astype(str)
        interation_pass = '06'
        print(f"...pass { interation_pass }")
        df = _transform_pass_06(df, columns_to_update)
        df.to_parquet(os.path.join(WORKING_DATA_PATH, f"{TABLENAME}-places-iteration-{ interation_pass }.parquet"), compression='gzip')

    if iteration == 0 or iteration == 7:
        df['find_address'] = df['physical_address_2'] + ' ' + df['physical_zip_code'].astype(str)
        interation_pass = '07'
        print(f"...pass { interation_pass }")
        df = _transform_pass_07(df, columns_to_update)
        df.to_parquet(os.path.join(WORKING_DATA_PATH, f"{TABLENAME}-places-iteration-{ interation_pass }.parquet"), compression='gzip')

    # only run the final pass if we are running all iterations
    if iteration == 0:
        print(f"...final pass")
        df = _transform_pass_final(df, columns_to_update)

    # df = df.drop('find_address', axis=1)
    return df.reset_index(drop=True)


def main():
    # Get the date and sample values using the imported function
    print(f"Processing processed voter file on {file_date.strftime('%Y-%m-%d')}")

    df = pd.DataFrame()

    # read the process file or the working file iteration if it exists
    if iteration < 2:
        print(f"...reading processed file")
        df = read_extract(df, os.path.join(PROCESSED_DATA_PATH, f"{file_date.strftime('%Y.%m.%d')}.{TABLENAME.lower()}.gzip"))
    else:
        print(f"...reading working file iteration {(iteration - 1):02d}")
        df = read_extract(df, os.path.join(WORKING_DATA_PATH, f"{TABLENAME}-places-iteration-{(iteration - 1):02d}.parquet"))

    if sample:
        print(f"...taking a sample of {sample}")
        df = df.sample(n=sample)
    df = _transform(df, iteration)

    # we only write the final file if we are running all iterations
    if iteration == 0:
        df = write_load(df, os.path.join(FINAL_DATA_PATH, f"{file_date.strftime('%Y.%m.%d')}.{TABLENAME.lower()}.gzip"))
    print('')
    print(f"File Processed {len(df)} records")


if __name__ == "__main__":
    process_time = time.time()
    try:
        main()
    except Exception as e:
        print('------Start--------')
        print(get_traceback(e))
        print('------End--------')
    get_timing(process_time)
