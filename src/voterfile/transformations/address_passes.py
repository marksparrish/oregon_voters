import pandas as pd

from utils.search import search_client, process_search_results, search_for_address, search_for_apartments, search_exact_match_address
from utils.config import RAW_DATA_PATH, PROCESSED_DATA_PATH, FINAL_DATA_PATH, WORKING_DATA_PATH, state, file_date, sample, iteration, initialize_pandarallel


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
