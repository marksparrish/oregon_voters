import sys
import time

sys.path.append('/app')

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from config.cli import FILE_DATE, SAMPLE
from config.common import (create_hash, get_timing, get_traceback, reduce_join,
                           standardize_address)
from pandarallel import pandarallel

from voters import DATA_CONTRACT, DATA_TRANSFORMATIONS, STATE, TABLENAME

pandarallel.initialize(progress_bar=True, verbose=0)
file_date = FILE_DATE
sample = SAMPLE

def _extract(df) -> pd.DataFrame:
    print("Extracting Data...")
    # get file names
    file_path = f"/data/{STATE}/voter_lists/processed/{file_date.strftime('%Y.%m.%d')}.{TABLENAME}.gzip"
    df = pq.read_table(source=file_path).to_pandas()
    return df.reset_index(drop=True)

def _transform(df) -> pd.DataFrame:
    """
    Transform addresses (mail and physical) into standard formats
    Create hash from standard addresses
    Match which addresses are already geocoded (in the places table)
    Write unmatched addresses to file adding to places table and for geocoding (only physical)
    Write processed voters to file having the right columns
    """
    print("...standardizing physical address")
    columns = ['physical_address_1', 'physical_address_2']
    df['whole_street'] = reduce_join(df, columns=columns)
    columns = ['whole_street', 'physical_city', 'physical_state']
    df['whole_commas'] = reduce_join(df, columns=columns, sep=', ')
    columns = ['whole_commas', 'physical_zip_code']
    df['whole_address'] = reduce_join(df, columns=columns)

    final_columns = ['physical_address_1', 'physical_address_2', 'physical_city', 'physical_state', 'physical_zip_code','physical_address_type']
    raw_columns = ['whole_address']
    df[final_columns] = df[raw_columns].parallel_apply(standardize_address, axis=1, result_type='expand')

    columns = ['physical_address_1', 'physical_address_2', 'physical_city', 'physical_state', 'physical_zip_code']
    df['physical_address_whole'] = reduce_join(df, columns=columns)

    print("...standardizing mailing address")
    columns = ['mail_address_1', 'mail_address_2', 'mail_address_3', 'mail_address_4']
    df['whole_street'] = reduce_join(df, columns=columns)
    columns = ['whole_street', 'mail_city', 'mail_state']
    df['whole_commas'] = reduce_join(df, columns=columns, sep=', ')
    columns = ['whole_commas', 'mail_zip_code']
    df['whole_address'] = reduce_join(df, columns=columns)

    final_columns = ['mail_address_1', 'mail_address_2', 'mail_city', 'mail_state', 'mail_zip_code','mail_address_type']
    raw_columns = ['whole_address']
    df[final_columns] = df[raw_columns].parallel_apply(standardize_address, axis=1, result_type='expand')

    columns = ['mail_address_1', 'mail_address_2', 'mail_city', 'mail_state', 'mail_zip_code']
    df['mail_address_whole'] = reduce_join(df, columns=columns)

    print("...creating hashes for physical and mail addresses")
    # for confidential addresses we hash the precinct_link
    df["physical_address_whole"] = np.where(df["confidential"] == "YES", df['precinct_link'], df['physical_address_whole'])
    df['physical_id'] = df['physical_address_whole'].apply(create_hash)
    df['mail_id'] = df['mail_address_whole'].apply(create_hash)

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
