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

import datetime as dt
import pytz
from dateutil import parser

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

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
# es_response = scan(
#     es.con,
#     index='test',
#     query={"query": { "match_all" : {}}}
# )

yesterday = dt.date.today() - dt.timedelta(hours=54)
yesterday_str = yesterday.isoformat()

es = ES_client()
es_response = scan(
    es.con,
    index="test",
    query={"query": {
    "bool": {
      "filter": [
        {
          "range": {
            'publishedAt': {
              "gt": yesterday_str,
              "format": "yyyy-MM-dd"
            }
          }
        }
      ]
    }
  }}
)

def connect_text(text_list):
	text = ''
	for i in text_list:
		text = text + i
	return text

def get_cos_similarity(news1,news2):

	article_1 = connect_text(news1["content"])
	article_2 = connect_text(news2["content"])
	corpus = [article_1,article_2]
	vectorizer = TfidfVectorizer()
	trsfm = vectorizer.fit_transform(corpus)
	similarity = cosine_similarity(trsfm[0:1], trsfm)
	return similarity[0][1]

def Sort_Tuple(tup):  
  
    return(sorted(tup, key = lambda x: x[1],reverse = True))   



search_object = {'query': {'match': {'_id': 'sHehPXUBc36yaXmaN-m_'}}}
doc = es.search('test', json.dumps(search_object))
doc_final = doc['hits']['hits'][0]['_source']



news_similarity_list = []
recommended_news = []
for item in es_response:
	# pprint(item)
	news2 = item['_source']
	similarity = get_cos_similarity(doc_final,news2)
	news_similarity_list.append((news2,similarity))

sorted_list = Sort_Tuple(news_similarity_list)
for item in sorted_list[:5]:
	recommended_news.append(item[0])

pprint(recommended_news[0])
# id = sHehPXUBc36yaXmaN-m_
# id_str = str(id)
# search_object = {'query': {'match': {'_id': 'snehPXUBc36yaXmaQOkV'}}}
# doc = es.search('test', json.dumps(search_object))
# pprint(doc['hits']['hits'][0])


