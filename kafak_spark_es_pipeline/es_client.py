import logging
from pprint import pprint
from elasticsearch import Elasticsearch

class ES_client():
    def __init__(self):
        self.con = Elasticsearch([{'host': 'localhost', 'port': 9200}])
        if self.con.ping():
            print('Connected')
        else:
            print('connection failed')


    def is_connectted(self):
        return self.con.ping()


    def create_index(self, index_name, schema): 
        created = False
        # index settings
        settings = schema
        try:
            if not self.con.indices.exists(index_name):
                # Ignore 400 means to ignore "Index Already Exist" error.
                if schema == None:
                    self.con.indices.create(index=index_name, ignore=400)
                else:
                    self.con.indices.create(index=index_name, ignore=400, body=settings)
                print('Created Index')
            created = True
        except Exception as ex:
            print(str(ex))
        finally:
            return created


    def store_record(self, index_name, record):
        is_stored = True
        try:
            outcome = self.con.index(index=index_name, body=record)
            print(outcome)
        except Exception as ex:
            print('Error in indexing data')
            print(str(ex))
            is_stored = False
        finally:
            return is_stored


    def search(self, index_name, search):
        res = self.con.search(index=index_name, body=search)
        pprint(res)


