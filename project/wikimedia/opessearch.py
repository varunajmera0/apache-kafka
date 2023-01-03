import logging

from opensearchpy import OpenSearch
from opensearchpy.helpers import bulk

# Create the client with SSL/TLS enabled, but hostname verification disabled.


class ElasticSearch:
    def __init__(self):
        self.client = OpenSearch("https://j2be1ialsp:1quud7ku1@dish-search-5800983340.us-east-1.bonsaisearch.net:443",use_ssl = True)
        self.index_body = {
                      'settings': {
                          #https://www.elastic.co/guide/en/elasticsearch/reference/current/ignore-malformed.html#ignore-malformed-setting
                          "index.mapping.ignore_malformed": True, #The index.mapping.ignore_malformed setting can be set on the index level to ignore malformed content globally across all allowed mapping types. Mapping types that don’t support the setting will ignore it if set on the index level.
                          'index': {
                          'number_of_shards': 4
                        }
                      }
                    }

        self.index_name = 'wikimedia'
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__file__)

    def _init(self, index_name=None, index_body=None):
        if index_body is not None:
            self.index_body = index_body
        if index_name is not None:
            self.index_name = index_name

    def create_index(self, index_name=None, index_body=None):
        self._init(index_name, index_body)
        index_status = self.client.indices.exists(self.index_name)
        if not index_status:
            response = self.client.indices.create(self.index_name, body=self.index_body)
            self.logger.info('\nCreating index:')
            return response
        else:
            return f"{index_name} index already exists!"

    def create_document(self, document,id=None, index_name=None, index_body=None):
        self._init(index_name, index_body)
        try:
            if id is not None:
                self.logger.info('\nAdding document:')
                return self.client.index(
                    index=self.index_name,
                    body=document,
                    id=id,
                    refresh=True
                )
            self.logger.info('\nAdding document:')
            return self.client.index(
                index=self.index_name,
                body=document,
                refresh=True
            )
        except Exception as e:
            self.logger.info('Error while adding document: ' + str(e))
            return


    def bulk_create(self, bulk_data):
        try:
            response = bulk(self.client, bulk_data)
            print("response", response)
            self.client.indices.refresh(index=self.index_name)
            return self.client.cat.count(index=self.index_name, format="json")
        except Exception as e:
            self.logger.info('Bulk document failed: ' + str(e))
            return