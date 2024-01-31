import os
import re
import logging
from common_functions.common import timing_decorator

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
    directory = '/Volumes/nfs-data/voter_data/oregon/daily_voted/raw'
    pattern = r'\d{4}-\d{2}-\d{2}'  # Pattern for yyyy_mm_dd
    folder_names = get_folder_names(directory, pattern)

    setup_logging()

    for folder_name in folder_names:
        date_format = folder_name.replace('_', '-')
        command = f'python daily_voted/daily_voted_etl.py --date={date_format}'
        # load = f'python voterfile/voters_load.py --date={date_format}'
        logging.info(f"Executing: {command}")
        return_code = os.system(command)
        if return_code != 0:
            logging.error(f"Command failed with return code {return_code}: {command}")
        else:
            logging.info(f"Command executed successfully: {command}")

if __name__ == "__main__":
    main()
