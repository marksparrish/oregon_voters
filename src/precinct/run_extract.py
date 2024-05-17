import os
sys.path.append(r'../src')
import re
import logging
from utils.config import DATA_FILES
from precinct_data_contract import STATE

def get_folder_names(directory, pattern):
    folder_names = []
    for item in os.listdir(directory):
        if os.path.isdir(os.path.join(directory, item)) and re.match(pattern, item):
            folder_names.append(item)
    return folder_names

def setup_logging():
    logging.basicConfig(filename='voter_data_extraction.log', level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    directory = os.path.join(DATA_FILES, STATE.lower(), 'voter_lists', 'raw')
    pattern = r'\d{4}_\d{2}_\d{2}'  # Pattern for yyyy_mm_dd
    folder_names = get_folder_names(directory, pattern)

    setup_logging()

    for folder_name in sorted(folder_names):
        date_format = folder_name.replace('_', '-')
        command = f'python precinct/precinct_extract.py --date={date_format}'
        logging.info(f"Executing: {command}")
        return_code = os.system(command)
        if return_code != 0:
            logging.error(f"Command failed with return code {return_code}: {command}")
        else:
            logging.info(f"Command executed successfully: {command}")

if __name__ == "__main__":
    main()
