import argparse
from datetime import datetime

def parse_args():
    parser = argparse.ArgumentParser(description='Process voter data')
    parser.add_argument('--date', required=True, help='File date in YYYY-MM-DD format')
    parser.add_argument('--iteration', type=int, default=0, help='Which pass to run with a default of 0 (all passes)')
    parser.add_argument('--sample', type=int, default=0, help='Sample option with a default of 0')
    parser.add_argument('--link', action='store_true', help="Set a boolean flag.")
    return parser.parse_args()

def get_date():
    args = parse_args()
    file_date_str = args.date
    file_date = datetime.strptime(file_date_str, '%Y-%m-%d')
    return file_date

def get_sample():
    args = parse_args()
    return args.sample

def get_iteration():
    args = parse_args()
    return args.iteration

def get_link():
    args = parse_args()
    return args.link
