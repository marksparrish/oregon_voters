# utils/config.py
import os
from dotenv import load_dotenv

# Specify the path to the .env file at the top level of your project
dotenv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')

# Load environment variables from the specified .env file
load_dotenv(dotenv_path)

# Define default constants
STATE = os.environ.get("STATE", "OREGON")
TABLENAME = os.environ.get("TABLENAME", "voters")
DATA_FILES = os.environ.get("DATA_FILES", "/Volumes/nfs-data/voter_data")

# Define elasticsearch constants
ES_HOST=os.environ.get("ES_HOST", "https://localhost:9200")
ES_USERNAME=os.environ.get("ES_USERNAME", "elastic")
ES_PASSWORD=os.environ.get("ES_PASSWORD", "")
CA_CERT_PATH=os.environ.get("CA_CERT_PATH", "")
