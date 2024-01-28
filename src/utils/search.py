from common_functions.search import MyElasticsearch
from utils.config import ES_HOST, ES_USERNAME, ES_PASSWORD, CA_CERT_PATH
import pandas as pd

# Example usage:
es_host = ES_HOST
es_username = ES_USERNAME
es_password = ES_PASSWORD
ca_cert_path = CA_CERT_PATH

class MyExtendedElasticsearch(MyElasticsearch):
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

search_client = MyExtendedElasticsearch(es_host, ca_cert_path, es_username, es_password)
