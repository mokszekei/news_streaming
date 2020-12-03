import logging
from pprint import pprint
from elasticsearch import Elasticsearch
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'common'))

from news_schema import schema, schema_label
from es_client import ES_client
from kafka import KafkaProducer, KafkaConsumer
from time import sleep  
import json

def main():
    topic_name = 'parsed_news'
    consumer = KafkaConsumer(topic_name, auto_offset_reset='earliest',
                             bootstrap_servers='localhost:9092', consumer_timeout_ms=1000)
    
    # write in
    es = ES_client()
    if not es.is_connectted():
        return
    es.create_index('test', schema)
    for msg in consumer:
        sleep(2)
        result = json.loads(msg.value)
        out = es.store_record('test', result)
        print('Data indexed successfully')

    # query
    # first argument(can be none) means which data you want, the whole record or specific info in record
    # search_object = {'query': {'match': {'author': 'Amy Tennery'}}}
    search_object = {'_source': ['title'], 'query': {'match': {'author': 'Amy Tennery'}}}
    es.search('test', json.dumps(search_object))

if __name__ == '__main__':
    main()

