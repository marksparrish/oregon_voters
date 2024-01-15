import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk, BulkIndexError
import tqdm
import math
import json
import glob
import re
import argparse

parser = argparse.ArgumentParser(description='Process ATTOM data.')
parser.add_argument('--year', type=int, required=True, help='Year of the data to process')
parser.add_argument('--month', type=str, required=True, help='Month of the data to process')

args = parser.parse_args()

index_year = args.year
index_month = args.month

es_host = "https://pip-search:9200"

es_username = "elastic"  # Elasticsearch username
# es_password = "+pWVzY*WcaUImmyo7C5Q"  # Elasticsearch password
es_password = "b7wtbd2t"

index_name = "attom"
pipeline_name = "attom-pipeline"

# data_file_path = f"/home/jovyan/work/data/attom/{index_year}"
data_file_path = f"/Volumes/nfs-data/attom/{index_year}/{index_month}"

ca_cert_path = "/Volumes/nfs-data/certs/elasticsearch/ca.crt"  # Path to your CA certificate (.crt file)
# ca_cert_path = "/home/jovyan/work/data/certs/elasticsearch/ca.crt"

header_file_path = 'attom.header.txt'

json_mappings = "json_mappings/attom.json"
