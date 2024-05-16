import sys
sys.path.append(r'../src')
import os
import time
from datetime import datetime
import pandas as pd
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from sqlalchemy.dialects.postgresql import insert

from common_functions.common import get_traceback, get_timing, timing_decorator
from common_functions.file_operations import read_extract, write_load, read_extract_multiple

from utils.arg_parser import get_date, get_sample

from utils.config import LOGSTASH_DATA_PATH, DATA_FILES, state, file_date, sample, initialize_pandarallel, PROCESSED_DATA_PATH
from data_contracts.votehistory_data_contract import DATA_CONTRACT, TABLENAME, dtype_mapping, final_columns, ElectionVoting, Base, create_dynamic_class
from utils.database import Database, fetch_existing_table, fetch_sql
from utils.file_operations import validate_dataframe
from utils.dataframe_operations import diff_dataframe
from utils.transformations import convert_date_format
from utils.search import index_documents

def _transform(df) -> pd.DataFrame:
    # we only want these columns
    df = df[final_columns]
    # convert election_date to datetime
    df['election_date'] = pd.to_datetime(df['election_date'])
    return df.reset_index(drop=True)

def _load(df) -> pd.DataFrame:
    print("Loading data")
    # get the distinct election dates
    election_dates = df['election_date'].unique()
    database = "oregon_voter_vote_history"
    db_connection = Database(database)
    print(f"Writing to database {database}")
    engine = db_connection.get_engine()
    # loop through the election dates and load the data into the database
    for election_date in election_dates:
        print(f"Loading data for election date {election_date}")
        # get the data for the election date
        election_df = df[df['election_date'] == election_date]

        # get the table name
        election_date_str = pd.to_datetime(election_date).strftime('%Y_%m_%d')

        # Dynamically create the class based on the election date
        DynamicElectionVoting = create_dynamic_class(election_date_str)

        # Create the table if it doesn't exist
        Base.metadata.create_all(bind=engine, checkfirst=True)

        # Check the dynamically set table name
        table_name = DynamicElectionVoting.__tablename__

        # check if the data is already in the database
        existing_data = fetch_existing_table(engine=engine, sql_table=table_name, final_columns=final_columns)
        # drop the id column
        existing_data = existing_data.drop(columns=['id'], errors='ignore')
        # check if the data is already in the database
        insert_df = diff_dataframe(df_new=election_df, df_existing=existing_data, on_columns=final_columns)
        print(f"Found {len(insert_df)} new records to insert")
        results = insert_df.to_sql(table_name, engine, if_exists='append', index=False, dtype=dtype_mapping, method='multi', chunksize=5000)

    return df.reset_index(drop=True)

def main():
    df = pd.DataFrame()
    df = read_extract(df, os.path.join(PROCESSED_DATA_PATH, f"{file_date.strftime('%Y.%m.%d')}.{TABLENAME.lower()}.gzip"))
    if sample > 0:
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
