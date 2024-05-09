"""
*** OREGON History Data ***
    Responsible for getting the data, validating that the data can be processed,
    initial cleaning (removing of unwanted records) and casting of columns into
    the right format.
"""
import sys
sys.path.append(r'../src')
import os
import time
from zipfile import ZipFile
import glob
from datetime import datetime
import pandas as pd
from sqlalchemy.sql import text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError

from common_functions.common import get_traceback, get_timing, timing_decorator
from common_functions.file_operations import read_extract, write_load, read_extract_multiple

from utils.arg_parser import get_date, get_sample

from utils.config import LOGSTASH_DATA_PATH, DATA_FILES, state, file_date, sample, initialize_pandarallel
from data_contracts.daily_voted_data_contract import DATA_CONTRACT, TABLENAME, dtype_mapping, final_columns, DailyVoted, Base, create_daily_voted_table
from utils.database import Database
from utils.file_operations import validate_dataframe
from utils.transformations import convert_date_format
from utils.search import index_documents

initialize_pandarallel()

def _extract(df):
    print("Extracting Data...")
    # get most recent file
    # file_path =   f"/Volumes/nfs-data/voter_data/oregon/daily_voted/raw/2023-05-16/Voted & Not Voted 5-23-2022.zip"
    file_path =     os.path.join(DATA_FILES, state.lower(), 'daily_voted', 'raw', f"{file_date.strftime('%Y-%m-%d')}/*.zip")
    lst_files = glob.glob(file_path)
    lst_files.sort(key=os.path.getmtime)
    file_path = lst_files[-1:][0]
    names = ZipFile(file=file_path).namelist()
    for n in names:
        if os.path.basename(n):
            if 'readme' not in n:
                print(n)
                temp_df = pd.read_csv(ZipFile(file=file_path).open(n), sep='\t', dtype=str, on_bad_lines='skip')
                df = pd.concat([df, temp_df])
    return df.reset_index(drop=True)

def _clean(df):
    print("Cleaning data")
    # de dupe
    df = df.drop_duplicates()
    # remove invalid state_voter_id
    df = df[df['state_voter_id'] != 'ACP']

    print(f"...after cleaning {len(df)} records remain")

    return df.reset_index(drop=True)

def _transform(df):
    print('Transforming data...')
    df['state'] = state.upper()
    df['election_date'] = file_date.strftime('%Y-%m-%d')
    df['voted'] = 'YES'
    df['voted_on_date'] = df['voted_on_date'].parallel_apply(convert_date_format)

    df = df[final_columns]
    return df.reset_index(drop=True)

def _load(df):
    print('Loading data', end =" ")
    df = _load_file(df)
    df = _load_index(df)

    return df.reset_index(drop=True)

def _load_file(df):
    file_path = os.path.join(DATA_FILES, state.lower(), 'daily_voted', 'processed', f"{file_date.strftime('%Y.%m.%d')}.{TABLENAME.lower()}.gzip")
    # file_path = f"/voterdata/{state}/daily_voted/processed/{file_date.strftime('%Y.%m.%d')}.{TABLENAME}.gzip"
    print(f"...writing processed data to {file_path}", end =" ")
    df.to_parquet(file_path, index=False, compression='gzip')
    print("...done")

def _load_index(df) -> None:
    print('...writing to index')
    df['id'] = df['state'] + '-' + df['election_date'] + '-' + df['state_voter_id']
    # set the index to _id column
    df = df.set_index('id', drop=False)
    index_name = f"votetracker-{state.lower()}-{TABLENAME.lower()}"
    index_documents(df, index_name)
    print("...done")

# Assuming Database class and other necessary imports and configurations are defined elsewhere

def _load_database(df, database) -> pd.DataFrame:
    db_connection = Database(database)
    print(f"Writing to database {database}")
    engine = db_connection.get_engine()
    Session = sessionmaker(bind=engine)
    session = Session()

    table_name = f"{TABLENAME.lower()}_{file_date.strftime('%Y_%m_%d')}"

    # Create the table if it doesn't exist
    create_daily_voted_table(engine)
    _create_trigger(database)

    try:
        batch_size = 5000  # Define the size of each batch
        for index, row in df.iterrows():
            daily_voted = DailyVoted(
                state=row['state'],
                election_date=row['election_date'],
                state_voter_id=row['state_voter_id'],
                ballot_id=row['ballot_id'],
                ballot_style=row['ballot_style'],
                voted_on_date=row['voted_on_date']
            )
            session.add(daily_voted)

            # Commit every batch_size records
            if (index + 1) % batch_size == 0:
                session.commit()
                session.close()  # Close the session to clear memory and start a new one
                session = Session()  # Open a new session
                print(f"Committed {index + 1} rows of {len(df)}", end='\r', flush=True)
        session.commit()  # Commit any remaining records
        print(f"Committed all {len(df)} rows       ")  # Clear the line after final commit
    except SQLAlchemyError as e:
        session.rollback()
        print(f"An error occurred: {e}")
    finally:
        session.close()
    print(f"Committed {len(df)} rows of {len(df)}")
    return df.reset_index(drop=True)

def _create_indices(database):
    # Example usage
    print("Creating indices")
    db_connection = Database(database)
    table_name = f"{TABLENAME.lower()}_{file_date.strftime('%Y_%m_%d')}"
    db_connection.create_index(table_name, ["state_voter_id"])

def _create_trigger(database):
    print("Creating trigger")
    db_connection = Database(database)
    table_name = f"{TABLENAME.lower()}_{file_date.strftime('%Y_%m_%d')}"
    trigger_statement = "DROP TRIGGER IF EXISTS after_daily_insert;"
    db_connection.create_trigger(trigger_statement)
    trigger_statement = f"""
        CREATE TRIGGER after_daily_insert
        AFTER INSERT ON {table_name}
        FOR EACH ROW
        BEGIN
            INSERT IGNORE INTO daily_voted_history (state, election_date, state_voter_id, ballot_style, ballot_id, voted_on_date)
            VALUES (NEW.state, NEW.election_date, NEW.state_voter_id, NEW.ballot_style, NEW.ballot_id, NEW.voted_on_date);
        END;
    """
    db_connection.create_trigger(trigger_statement)

@timing_decorator
def main():
    database = "oregon_voter_vote_history"
    df = pd.DataFrame()
    df = _extract(df)
    print(f"...extracted {len(df)} records")
    df = validate_dataframe(df, DATA_CONTRACT)
    print(f"...validated {len(df)} records")
    df = _clean(df)
    print(f"...cleaned {len(df)} records")
    if sample:
        print(f"...taking a sample of {sample}")
        df = df.sample(n=sample)
    df = _transform(df)
    print(f"...transformed {len(df)} records")
    _load_file(df)
    _load_index(df)
    _load_database(df, database)

    print(f"File Processed {len(df)} records")

if __name__ == "__main__":
    try:
        print(f"Processing raw history file on {file_date.strftime('%Y-%m-%d')}")
        main()
    except Exception as e:
        print('------Start--------')
        print(get_traceback(e))
        print('------End--------')
