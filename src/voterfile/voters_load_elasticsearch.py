"""
This script processes a final voter file, writing it to a logstash pipeline to be sent to an Elasticsearch index.
The database and index are named using the format: votetracker_{state}_voters_{file_date}.
"""

import sys
sys.path.append(r'../src')
import os
import time
from datetime import datetime
import pandas as pd

from common_functions.common import get_traceback, get_timing
from common_functions.file_operations import read_extract, write_load

from utils.config import RAW_DATA_PATH, PROCESSED_DATA_PATH, FINAL_DATA_PATH, WORKING_DATA_PATH, LOGSTASH_DATA_PATH, state, file_date, sample
from utils.search import search_client, index_documents


from data_contracts.voterfile_data_contract import DATA_CONTRACT, TABLENAME, dtype_mapping, final_columns

def _load(df) -> pd.DataFrame:
    """
    Loads the DataFrame into an Elasticsearch index and returns the modified DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame to be loaded.

    Returns:
        pd.DataFrame: The modified DataFrame after loading.
    """
    print('Loading')
    # Create the index name
    index_name = f"votetracker-{state}-voters-{file_date.strftime('%Y-%m-%d')}"
    # change in df index to state_voter_id but keep the column
    df = df.set_index('state_voter_id', drop=False)
    # set NaN to empty string
    df = df.fillna('')
    index_documents(df, index_name)

    return df.reset_index(drop=True)

def voter_votehistory(state_voter_id):
    """
    Search for a voter's vote history in the Elasticsearch index.

    Args:
        state_voter_id (str): The state voter ID to search for.

    Returns:
        dict: The search results as a dictionary.
    """
    index_name = f"votetracker-{state}-voter-elections"
    response = search_client.voter_votehistory(index_name, state_voter_id)
    total = response['hits']['total']['value']

    if total > 0:
        print(total, state_voter_id)


    return (0,0,'','')

def _transform(df) -> pd.DataFrame:
    """
    Transforms the DataFrame to match the final data contract and returns the modified DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame to be transformed.

    Returns:
        pd.DataFrame: The modified DataFrame after transformation.
    """
    print(f'Transforming {len(df)} ')
    df[['voter_ranking','search_ranking', 'elections_voted', 'elections_not_voted']] = df.apply(lambda x: voter_votehistory(x['state_voter_id']), axis=1, result_type='expand')
    # look up the votes the voter has cast
    return df

def output_row_as_json(df):
    """
    Outputs each row of the DataFrame as a JSON document.

    Args:
        df (pd.DataFrame): The DataFrame to be processed.
    """
    # create unique file name using a timestamp
    file_name = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
    # Iterate over DataFrame rows
    with open(f'/Volumes/nfs-data/logstash/voters/{file_name}.json', 'a') as f:
        for index, row in df.iterrows():
            # Convert the row to a JSON string
            json_str = row.to_json()
            # Output the JSON string
            f.write(json_str + '\n')

def main():
    """
    Main function to process the voter file.
    """
    df = pd.DataFrame()
    file_path = os.path.join(FINAL_DATA_PATH, f"{file_date.strftime('%Y.%m.%d')}.{TABLENAME.lower()}.gzip")
    df = read_extract(df, file_path)

    if sample > 0:
        print(f"...taking a sample of {sample}")
        df = df.sample(n=sample)
    df['registration_date'] = df['registration_date'].replace('nan', file_date.strftime('%Y-%m-%d'))
    # set Nan to empty string
    df = df.fillna('')
    output_row_as_json(df)
    # df = _transform(df)
    # df = _load(df)

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
