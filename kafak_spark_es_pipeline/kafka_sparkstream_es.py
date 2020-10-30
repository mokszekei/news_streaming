from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import os
import json
import pyspark
# from pyspark.sql import SQLContext
import pandas as pd
import numpy as np
from pyspark import StorageLevel

from classifier import classifier as clsf
from news_classes import class_map
from es_client import ES_client
from news_schema import schema, schema_label, schema_label2

PYSPARK_SUBMIT_ARGS = '--jars /Users/mosiqi/onlineinternship/streamapp/spark-streaming-kafka-0-8-assembly_2.11-2.4.6.jar pyspark-shell'

os.environ['PYSPARK_SUBMIT_ARGS'] = PYSPARK_SUBMIT_ARGS

es = ES_client()
es.create_index('label_news_test3', schema_label2)


def create_df (text):
    df = pd.DataFrame(np.array([[text]]),columns=['text'])
    return df


def get_prediction(df):
	model = clsf.read_model('./model/custom_model.pickle')
	pred = model.predict(df['text'])
	return pred[0] # class id, still need to transform to 


def add_label(json):
	content = json['content']
	text = ''
	for i in content:
		text = text + i
	df = create_df(text)
	pred = get_prediction(df)
	label = class_map[str(pred)]
	json['label']=label
	return json


def store_es(rdd):
	string = rdd.collect()
	print(string)
	if string:
		json = {
		"source":string[0]['source'],
		"author":string[0]['author'],
		"title":string[0]['title'],
		"description":string[0]['description'],
		"url":string[0]['url'],
		"urlToImage":string[0]['urlToImage'],
		"publishedAt":string[0]['publishedAt'],
		"content":string[0]['content'],
		"label":string[0]['label']
		}
		es.store_record('test', json)


def get_top_keywords(ori_json,window_length, sliding_interval):
	top_topics = ori_json.flatMap(lambda article: article['content']) \
	.reduce(lambda item1, item2: item1 + item2) \
	.flatMap(lambda line: line.split(" ")) \
	.map(lambda word: (word, 1)) \
	.reduceByKeyAndWindow(lambda x, y: x + y, lambda x, y: x - y, window_length, sliding_interval)

	sorted_topics = top_topics.map(lambda kv: (kv[1], kv[0])) \
	.transform(lambda rdd: rdd.sortByKey(False)) \
	.map(lambda kv: (kv[1], kv[0])) 

	return sorted_topics

#####################################################

##### spark 实现2个功能：predict，top topic统计 ########

#####################################################

if __name__ == "__main__":

	sc = SparkContext(appName="PythonStreamingKafka")
	ssc = StreamingContext(sc, 1) # 1 second window

	ssc.checkpoint("newscheck")

	directKafkaStream = KafkaUtils.createDirectStream(
	    ssc, 
	    ["test"],  ## topic
	    {"metadata.broker.list": "localhost:9092"}
	)

	ori_json = directKafkaStream.map(lambda x: json.loads(x[1])) 

	ori_json.persist(StorageLevel.MEMORY_AND_DISK)
	# add label 
	# 加了checkpoint后，这部分会出错,两个功能无法同时进行？
	label_news = ori_json.map(add_label) \
	                      .foreachRDD(store_es)


	# hot words count in 15 mins
	window_length = 15*60
	sliding_interval = 6  # how often the data is updated

	def take_and_print(rdd):
		element = rdd.collect()
		print(element[40:90])

	top_topics = get_top_keywords(ori_json,window_length, sliding_interval) \
	.foreachRDD(take_and_print)


	# top_topics won't be stored in ES, send to flask directly.
	# server = 'http://localhost:5001/'
	# send_top_to_dashboard(top_topics, server + 'update_most_used_words')


	#Starting Spark context
	ssc.start()
	ssc.awaitTermination()
