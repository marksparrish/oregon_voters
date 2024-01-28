# utils/config.py
import os
from dotenv import load_dotenv

# Specify the path to the .env file at the top level of your project
dotenv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '..', '.env')
# Load environment variables from the specified .env file
load_dotenv(dotenv_path)

# Define default constants
STATE = os.environ.get("STATE", "OREGON")
DATA_FILES = os.environ.get("DATA_FILES", "/Volumes/nfs-data/voter_data")

# Define elasticsearch constants
ES_HOST=os.environ.get("ES_HOST", "https://localhost:9200")
ES_USERNAME=os.environ.get("ES_USERNAME", "elastic")
ES_PASSWORD=os.environ.get("ES_PASSWORD", "")
CA_CERT_PATH=os.environ.get("CA_CERT_PATH", "")

# File Paths
RAW_DATA_PATH = os.path.join(DATA_FILES, STATE.lower(), 'voter_lists', 'raw')
PROCESSED_DATA_PATH = os.path.join(DATA_FILES, STATE.lower(), 'voter_lists', 'processed')
FINAL_DATA_PATH = os.path.join(DATA_FILES, STATE.lower(), 'voter_lists', 'final')
WORKING_DATA_PATH = os.path.join(DATA_FILES, STATE.lower(), 'voter_lists', 'working')

# Database Connections
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_USERNAME = os.environ.get("DB_USERNAME", "root")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "password")
DB_PORT = os.environ.get("DB_PORT", "3306")
