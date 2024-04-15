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
import gender_guesser.detector as gender


from common_functions.common import get_traceback, get_timing
from common_functions.physical_address import standardize_address
from common_functions.file_operations import read_extract, write_load

from data_contracts.voterfile_data_contract import DATA_CONTRACT, TABLENAME, final_columns
from utils.config import RAW_DATA_PATH, PROCESSED_DATA_PATH, FINAL_DATA_PATH, WORKING_DATA_PATH, state, file_date, sample, iteration, initialize_pandarallel

from voterfile.transformations.address_passes import _transform_pass_01, _transform_pass_02, _transform_pass_03, _transform_pass_04, _transform_pass_05, _transform_pass_06, _transform_pass_07, _transform_pass_final

gd = gender.Detector(case_sensitive=False)

def _transform_address(df, iteration) -> pd.DataFrame:
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

def _transform_main(df, iteration) -> pd.DataFrame:
    print("Performing Final Data Transformtion...")

    df['file_date'] = file_date.strftime('%Y-%m-%d')
    df['state'] = state.lower()
    # add column address_type and set to 'residential'
    df['address_type'] = 'residential'
    # update adress_type to 'apartment' if physical_unit_type is empty, null or a blank string
    df.loc[df['physical_unit_type'] != '', 'address_type'] = 'apartment'

    # ensure birthdate is a date
    # if only year is present, set to Jan 1 of that year
    df['birthdate'] = df['birthdate'].astype(str)
    df.loc[df['birthdate'].str.len() == 4, 'birthdate'] = df['birthdate'] + '-01-01'

    # ensure PropertyLatitude and PropertyLongitude are numeric
    df['PropertyLatitude'] = pd.to_numeric(df['PropertyLatitude'], errors='coerce')
    df['PropertyLongitude'] = pd.to_numeric(df['PropertyLongitude'], errors='coerce')

    # in PropertyLatitude and PropertyLongitude are null or NaN, set to 0
    df['PropertyLatitude'] = df['PropertyLatitude'].fillna(0)
    df['PropertyLongitude'] = df['PropertyLongitude'].fillna(0)

    # Create a mask for rows where 'results' is 'Not Found'
    mask = df['results'] == 'Not Found'

    # Update 'PropertyAddressFull' for rows matching the mask
    df.loc[mask, 'physical_id'] = 'Not-Found-' + df['physical_address_1'] + ' ' + df['physical_address_2']
    df.loc[mask, 'PropertyAddressFull'] = df['physical_address_1'] + ' ' + df['physical_address_2']
    df.loc[mask, 'PropertyAddressCity'] = df['physical_city']
    df.loc[mask, 'PropertyAddressState'] = df['physical_state']
    df.loc[mask, 'PropertyAddressZIP'] = df['physical_zip_code']

    mask = df['results'] == 'Homeless'

    # Update 'PropertyAddressFull' for rows matching the mask
    df.loc[mask, 'physical_id'] = 'Homeless'
    df.loc[mask, 'PropertyAddressFull'] = 'Homeless'
    df.loc[mask, 'PropertyAddressCity'] = 'Homeless'
    df.loc[mask, 'PropertyAddressState'] = 'Homeless'
    df.loc[mask, 'PropertyAddressZIP'] = 'Homeless'
    df.loc[mask, 'address_type'] = 'Homeless'

    # add gender column
    print("...guessing gender based on first and middle names")
    # df['gender'] = df.parallel_apply(lambda row: assume_gender(row['name_first'], row['name_middle']), axis=1)
    df['gender'] = df['name_first'].map(lambda x: gd.get_gender(x, country='usa'))

    # create a mask based on gender being unknown, andy
    # then guess based on middle name
    mask = df['gender'].isin(['unknown', 'andy'])
    df.loc[mask, 'gender'] = df['name_middle'].map(lambda x: gd.get_gender(x, country='usa'))

    # create the same mask and then change their gender value to unknown
    mask = df['gender'].isin(['unknown', 'andy'])
    df.loc[mask, 'gender'] = 'unknown'

    # create a mask based on mostly_male
    mask = df['gender'] == 'mostly_male'
    df.loc[mask, 'gender'] = 'male'

    # create a mask based on mostly_female
    mask = df['gender'] == 'mostly_female'
    df.loc[mask, 'gender'] = 'female'

    df['mail_id'] = df['mail_address_1'] + ' ' + df['mail_zip_code']

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
    df = _transform_address(df, iteration)
    df = _transform_main(df, iteration)
    # we only write the final file if we are running all iterations

    # non_numeric = df[pd.to_numeric(df['PropertyLatitude'], errors='coerce').isna()]
    # print(non_numeric[['PropertyLatitude','PropertyLongitude']])
    # exit()
    if iteration == 0:
        df = write_load(df, os.path.join(FINAL_DATA_PATH, f"{file_date.strftime('%Y.%m.%d')}.{TABLENAME.lower()}.gzip"))
    print('')
    print(f"File Processed {len(df)} records")
    # print(df)



if __name__ == "__main__":
    process_time = time.time()
    try:
        main()
    except Exception as e:
        print('------Start--------')
        print(get_traceback(e))
        print('------End--------')
    get_timing(process_time)
