import logging
from pprint import pprint
from elasticsearch import Elasticsearch


# ISSUE: doc in different format can still write in
def create_index(es_object, index_name):
    created = False
    # index settings
    settings = {
        "settings": {
            "number_of_shards": 1, # the number of partitions that will keep the data of this Index. 
                                   # If you are running a cluster of multiple Elastic nodes then entire 
                                    # data is split across them. In simple words, if there are 5 shards then 
                                    # entire data is available across 5 shards and ElasticSearch cluster can 
                                    # serve requests from any of its node.
            "number_of_replicas": 0
        },
        "mappings": {
            "dynamic": "strict",  ## The automatic detection and addition of new fields is called dynamic mapping. 
                                  ## The dynamic mapping rules can be customised to suit your purposes with: 
            "properties": {
                "source": {
                    "type": "nested",
                    "properties": {
                        "step": {"type": "text"}
                    }
                },
                "author": {
                    "type": "text"
                },
                "description": {
                    "type": "text"
                },
                "url": {
                    "type": "text"
                },
                "publishedAt": {
                    "type": "date"   ## If date_detection is enabled (default), then new string fields are checked 
                                     ## to see whether their contents match any of the date patterns 
                                     ## specified in dynamic_date_formats
                    "format": "yyyy-MM-dd'T'HH:mm:ssZ"   # if don't specify format, use defult format
                },
                '2020-09-24T23:08:00Z'
                "content": {
                    "type": "text"
                },
            },
        }
    }


    try:
        if not es_object.indices.exists(index_name):
            # Ignore 400 means to ignore "Index Already Exist" error.
            es_object.indices.create(index=index_name, ignore=400, body=settings)
            print('Created Index')
        created = True
    except Exception as ex:
        print(str(ex))
    finally:
        return created


def store_record(elastic_object, index_name, record):
    is_stored = True
    try:
        outcome = elastic_object.index(index=index_name, body=record)
        print(outcome)
    except Exception as ex:
        print('Error in indexing data')
        print(str(ex))
        is_stored = False
    finally:
        return is_stored


def connect_elasticsearch():
    _es = None
    _es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    if _es.ping():
        print('Connected')
    else:
        print('connection failed')
    return _es


def search(es_object, index_name, search):
    res = es_object.search(index=index_name, body=search)
    pprint(res)


if __name__ == '__main__':
    
    topic_name = 'parsed_news'
    consumer = KafkaConsumer(topic_name, auto_offset_reset='earliest',
                             bootstrap_servers='localhost:9092', consumer_timeout_ms=1000)
    
    # write in
    es = connect_elasticsearch()
    create_index(es, 'test'):
    for msg in consumer:
        sleep(2)
        result = json.loads(msg.value)
        out = store_record(es, 'test', result)
        print('Data indexed successfully')

    # query
    es = connect_elasticsearch()
    if es is not None:
    # first argument(can be none) means which data you want, the whole record or specific info in record
#    search_object = {'query': {'match': {'author': 'Amy Tennery'}}}
    search_object = {'_source': ['title'], 'query': {'match': {'author': 'Amy Tennery'}}}
    search(es, 'test', json.dumps(search_object))
