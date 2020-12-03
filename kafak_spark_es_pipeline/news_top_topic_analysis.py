from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import os
import json
import pyspark
import pandas as pd
import numpy as np
from pyspark import StorageLevel

import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'common'))

from news_classes import class_map
from es_client import ES_client
from news_schema import schema, schema_label, schema_label2
import requests
from nltk.corpus import stopwords

PYSPARK_SUBMIT_ARGS = '--jars /Users/mosiqi/news_streaming/kafak_spark_es_pipeline/spark-streaming-kafka-0-8-assembly_2.11-2.4.6.jar pyspark-shell'

os.environ['PYSPARK_SUBMIT_ARGS'] = PYSPARK_SUBMIT_ARGS


def get_top_keywords(ori_json,window_length, sliding_interval):
	stop_words = stopwords.words('english')

	top_topics = ori_json.flatMap(lambda article: article['content']) \
	.reduce(lambda item1, item2: item1 + item2) \
	.flatMap(lambda line: line.split(" ")) \
	.filter(lambda x: x.lower() not in stop_words) \
	.map(lambda word: (word, 1)) \
	.reduceByKeyAndWindow(lambda x, y: x + y, lambda x, y: x - y, window_length, sliding_interval)

	sorted_topics = top_topics.map(lambda kv: (kv[1], kv[0])) \
	.transform(lambda rdd: rdd.sortByKey(False)) \
	.map(lambda kv: (kv[1], kv[0])) 

	return sorted_topics

def send_to_beckend(rdd):
	if not rdd.isEmpty():
		element = rdd.collect()
		print(element[10:50])
		taken = element[10:50]
		labels = []
		count = []
		for kv in taken:
			labels.append(kv[0])
			count.append(kv[1])
			request_data = {'labels': str(labels), 'count': str(count)}
			url = 'http://127.0.0.1:5000/update_top_topics'
			response = requests.post(url , data=request_data)


if __name__ == "__main__":

	sc = SparkContext(appName="PythonStreamingKafka")
	ssc = StreamingContext(sc, 1) # 1 second window

	ssc.checkpoint("newscheck")

	directKafkaStream = KafkaUtils.createDirectStream(
	    ssc, 
	    ["test"],  ## topic
	    # ["parsed_news"],
	    {"metadata.broker.list": "localhost:9092"}
	)

	ori_json = directKafkaStream.map(lambda x: json.loads(x[1])) 
	ori_json.persist(StorageLevel.MEMORY_AND_DISK)

	# hot words count in 15 mins
	window_length = 15*60
	sliding_interval = 6  # how often the data is updated

	# top_topics won't be stored in ES, send to flask directly.
	top_topics = get_top_keywords(ori_json,window_length, sliding_interval) \
	.foreachRDD(send_to_beckend)

	#Starting Spark context
	ssc.start()
	ssc.awaitTermination()
