import argparse
from datetime import datetime

def parse_args():
    parser = argparse.ArgumentParser(description='Process voter data')
    parser.add_argument('--date', required=True, help='File date in YYYY-MM-DD format')
    parser.add_argument('--sample', type=int, default=0, help='Sample option with a default of 0')
    return parser.parse_args()

def get_date_and_sample():
    args = parse_args()
    file_date_str = args.date
    file_date = datetime.strptime(file_date_str, '%Y-%m-%d')
    sample = args.sample
    return file_date, sample
