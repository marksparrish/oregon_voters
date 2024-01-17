from common_functions.search import MyElasticsearch
from utils.config import ES_HOST, ES_USERNAME, ES_PASSWORD, CA_CERT_PATH

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
                "field": "[ATTOM ID]"
            }
        }
        response = self.client.options(basic_auth=(self.username, self.password)).search(index=index_name, body=query)
        return response

search_client = MyExtendedElasticsearch(es_host, ca_cert_path, es_username, es_password)
