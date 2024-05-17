import sys
sys.path.append(r'../src')
import os
import re
import logging
from common_functions.common import timing_decorator
from utils.config import RAW_DATA_PATH, PROCESSED_DATA_PATH, FINAL_DATA_PATH, WORKING_DATA_PATH, LOGSTASH_DATA_PATH, state, file_date, sample

def get_folder_names(directory, pattern):
    folder_names = []
    for item in os.listdir(directory):
        if os.path.isdir(os.path.join(directory, item)) and re.match(pattern, item):
            folder_names.append(item)
    return folder_names

def setup_logging():
    logging.basicConfig(filename='voter_data_extraction.log', level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')
@timing_decorator
def main():
    directory = RAW_DATA_PATH
    print(f"Processing {directory}")
    pattern = r'\d{4}_\d{2}_\d{2}'  # Pattern for yyyy_mm_dd
    folder_names = get_folder_names(directory, pattern)
    print(f"Processing {len(folder_names)} folders")
    setup_logging()

    for folder_name in sorted(folder_names):
        date_format = folder_name.replace('_', '-')
        command = f'python votehistory/history_load.py --date={date_format}'
        # load = f'python voterfile/voters_load.py --date={date_format}'
        logging.info(f"Executing: {command}")
        return_code = os.system(command)
        if return_code != 0:
            logging.error(f"Command failed with return code {return_code}: {command}")
        else:
            logging.info(f"Command executed successfully: {command}")

if __name__ == "__main__":
    main()
