from common_functions.search import MyElasticsearch
from utils.config import ES_HOST, ES_USERNAME, ES_PASSWORD, CA_CERT_PATH
import pandas as pd
from elasticsearch.helpers import streaming_bulk, BulkIndexError
import tqdm
import json

# Example usage:
es_host = ES_HOST
es_username = ES_USERNAME
es_password = ES_PASSWORD
ca_cert_path = CA_CERT_PATH

class MyExtendedElasticsearch(MyElasticsearch):
    def __init__(self, host, cert_path, username, password):
        super().__init__(host, cert_path, username, password)

    def __enter__(self):
        # Initialize the Elasticsearch client connection here
        # Assuming self.client is the Elasticsearch client instance
        self.client = self.create_client()  # Placeholder for actual client creation method
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Close the Elasticsearch client connection here
        # Assuming self.client has a close method
        if hasattr(self.client, 'close'):
            self.client.close()

    def create_client(self):
        # Placeholder method to create and return the Elasticsearch client
        # This should include authentication and any other setup required
        return super().create_client()
    def search_address(self, index_name, address_query):
        """
        Search for an address in the Elasticsearch index.

        Args:
            index_name (str): The name of the Elasticsearch index to search.
            address_query (str): The address query to search for.

        Returns:
            dict: The search results as a dictionary.
        """
        # Create an Elasticsearch query DSL for address search
        query = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "simple_query_string": {
                                "query": address_query,

                                "fields": ["PropertyAddress*"],
                                "default_operator": "AND",
                                "analyzer": "standard"
                            }
                        }
                    ]
                }
            },
            "collapse": {
                "field": "[ATTOM ID].keyword"
            }
        }
        response = self.client.options(basic_auth=(self.username, self.password)).search(index=index_name, body=query)
        return response

    def search_unit_address(self, index_name, address_query):
        """
        Search for an address in the Elasticsearch index.

        Args:
            index_name (str): The name of the Elasticsearch index to search.
            address_query (str): The address query to search for.

        Returns:
            dict: The search results as a dictionary.
        """
        # Create an Elasticsearch query DSL for address search
        query = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "simple_query_string": {
                                "query": address_query,

                                "fields": ["PropertyAddress*"],
                                "default_operator": "AND",
                                "analyzer": "standard"
                            }
                        }
                    ]
                }
            }
        }
        response = self.client.options(basic_auth=(self.username, self.password)).search(index=index_name, body=query)
        return response

    def exact_match_address(self, index_name, house_number, street_name, zip_code):
        """
        Search for an address in the Elasticsearch index.

        Args:
            index_name (str): The name of the Elasticsearch index to search.
            address_query (str): The address query to search for.

        Returns:
            dict: The search results as a dictionary.
        """
        # Create an Elasticsearch query DSL for address search
        query = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "term": {
                                "PropertyAddressHouseNumber.raw": house_number
                            }
                        },
                        {
                            "term": {
                                "PropertyAddressStreetName.raw": street_name
                            }
                        },
                        {
                            "term": {
                                "PropertyAddressZIP.raw": zip_code
                            }
                        }
                    ]
                }
            }
        }
        response = self.client.options(basic_auth=(self.username, self.password)).search(index=index_name, body=query)
        return response

search_client = MyExtendedElasticsearch(es_host, ca_cert_path, es_username, es_password)

def search_for_address(address):
    index_name = "places"
    search_results = search_client.search_address(index_name, address)
    return process_search_results(search_results)

def search_for_apartments(address):
    index_name = "places-previous"
    search_results = search_client.search_unit_address(index_name, address)
    return process_search_results(search_results)


def search_exact_match_address(house_number, street_name, zip_code):
    index_name = "places"
    search_results = search_client.exact_match_address(index_name, house_number, street_name, zip_code)
    return process_search_results(search_results)

def generate_actions(df):
    for index, row in df.iterrows():
        yield {
            "_id": index,
            "_source": row.to_dict(),
        }

def index_documents(df, index_name):
    """Bulk index the dataframe into Elasticsearch."""
    number_of_docs = len(df)
    progress = tqdm.tqdm(unit="docs", total=number_of_docs)
    successes = 0
    failures = 0

    try:
        for ok, action in streaming_bulk(
            search_client.client.options(basic_auth=(es_username, es_password)),
            index=index_name, actions=generate_actions(df),
        ):
            progress.update(1)
            if ok:
                successes += 1
            else:
                failures += 1
                # Log the failed action
                with open('failed_documents.log', 'a') as log_file:
                    log_file.write(json.dumps(action) + '\n')

    except BulkIndexError as e:
        for i, error in enumerate(e.errors):
            print(f"Error {i+1}: {error}")
            print('---------------------')
            failures += 1
            # Log the error
            with open('failed_documents.log', 'a') as log_file:
                log_file.write(json.dumps(error) + '\n')
        print(f"Error indexing documents: {e}")

    print("Indexed %d/%d documents" % (successes, number_of_docs))
    if failures > 0:
        print(f"Failed to index {failures} document(s). Check 'failed_documents.log' for details.")

def process_search_results(search_results):
    hits_count = len(search_results['hits']['hits'])

    if hits_count == 1:
        hit = search_results['hits']['hits'][0]
        return handle_single_result(hit)

    return handle_not_found()

def handle_single_result(hit):
    physical_id = hit["_id"]
    return pd.Series({
        "results": "Success",
        "physical_id": physical_id,
        "PropertyAddressFull": hit['_source'].get("PropertyAddressFull"),
        "PropertyAddressHouseNumber": hit['_source'].get("PropertyAddressHouseNumber"),
        "PropertyAddressStreetDirection": hit['_source'].get("PropertyAddressStreetDirection"),
        "PropertyAddressStreetName": hit['_source'].get("PropertyAddressStreetName"),
        "PropertyAddressStreetSuffix": hit['_source'].get("PropertyAddressStreetSuffix"),
        "PropertyAddressCity": hit['_source'].get("PropertyAddressCity"),
        "PropertyAddressState": hit['_source'].get("PropertyAddressState"),
        "PropertyAddressZIP": hit['_source'].get("PropertyAddressZIP"),
        "PropertyAddressZIP4": hit['_source'].get("PropertyAddressZIP4"),
        "PropertyAddressCRRT": hit['_source'].get("PropertyAddressCRRT"),
        "PropertyLatitude": str(hit['_source'].get("PropertyLatitude")),
        "PropertyLongitude": str(hit['_source'].get("PropertyLongitude"))
    })

def handle_not_found():
    return pd.Series({
        "results": "Not Found",
        "physical_id": "",
        "PropertyAddressFull": "",
        "PropertyAddressHouseNumber": "",
        "PropertyAddressStreetDirection": "",
        "PropertyAddressStreetName": "",
        "PropertyAddressStreetSuffix": "",
        "PropertyAddressCity": "",
        "PropertyAddressState": "",
        "PropertyAddressZIP": "",
        "PropertyAddressZIP4": "",
        "PropertyAddressCRRT": "",
        "PropertyLatitude": "",
        "PropertyLongitude": ""
    })
