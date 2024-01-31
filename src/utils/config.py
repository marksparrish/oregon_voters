# utils/config.py
import os
from dotenv import load_dotenv
from utils.arg_parser import get_date, get_sample, get_iteration
# Initialize pandarallel
from pandarallel import pandarallel

def initialize_pandarallel():
    pandarallel.initialize(progress_bar=True, use_memory_fs=False)

# Specify the path to the .env file at the top level of your project
dotenv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '..', '.env')
# Load environment variables from the specified .env file
load_dotenv(dotenv_path)

# Global Variables
state = os.environ.get("STATE", "OREGON").lower() # defined in .env file
file_date = get_date() # defined in utils/arg_parser.py cli argument
sample = get_sample() # defined in utils/arg_parser.py cli argument
iteration = get_iteration() # defined in utils/arg_parser.py cli argument

# Define default constants

DATA_FILES = os.environ.get("DATA_FILES", "/Volumes/nfs-data/voter_data")

# Define elasticsearch constants
ES_HOST=os.environ.get("ES_HOST", "https://localhost:9200")
ES_USERNAME=os.environ.get("ES_USERNAME", "elastic")
ES_PASSWORD=os.environ.get("ES_PASSWORD", "")
CA_CERT_PATH=os.environ.get("CA_CERT_PATH", "")

# File Paths
LOGSTASH_DATA_PATH = os.environ.get("LOGSTASH_FILES", "/Volumes/nfs-data/logstash")
RAW_DATA_PATH = os.path.join(DATA_FILES, state, 'voter_lists', 'raw')
PROCESSED_DATA_PATH = os.path.join(DATA_FILES, state, 'voter_lists', 'processed')
FINAL_DATA_PATH = os.path.join(DATA_FILES, state, 'voter_lists', 'final')
WORKING_DATA_PATH = os.path.join(DATA_FILES, state, 'voter_lists', 'working')

# Database Connections
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_USERNAME = os.environ.get("DB_USERNAME", "root")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "password")
DB_PORT = os.environ.get("DB_PORT", "3306")
