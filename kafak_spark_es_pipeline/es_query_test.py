import logging
from pprint import pprint
from elasticsearch import Elasticsearch
from news_schema import schema, schema_label
from es_client import ES_client
from kafka import KafkaProducer, KafkaConsumer
from time import sleep  
import json


    # write in
es = ES_client()
    
# es.create_index('test', schema)
# for msg in consumer:
#     sleep(2)
#     result = json.loads(msg.value)
#     out = es.store_record('test', result)
#     print('Data indexed successfully')

# query
# first argument(can be none) means which data you want, the whole record or specific info in record
# search_object = {'query': {'match': {'author': 'Amy Tennery'}}}
# # search_object = {'_source': ['title'], 'query': {'match': {'author': 'Matthew Green'}}}

# es.search('label_news_test2', json.dumps(search_object))


# script = es.con.indices.stats('label_news_test2')
# pprint(script)


from elasticsearch.helpers import scan
es_response = scan(
    es.con,
    index='test',
    query={"query": { "match_all" : {}}}
)

for item in es_response:
    print(json.dumps(item))


# es.search('label_news_test2', json.dumps(search_object))




