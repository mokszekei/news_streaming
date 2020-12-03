from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
import pyspark
# from pyspark.sql import SQLContext
import pandas as pd
import numpy as np
from pyspark import StorageLevel

import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'common'))

from classifier import classifier
from news_classes import class_map
from es_client import ES_client
from news_schema import schema, schema_label, schema_label2

PYSPARK_SUBMIT_ARGS = '--jars /Users/mosiqi/news_streaming/kafak_spark_es_pipeline/spark-streaming-kafka-0-8-assembly_2.11-2.4.6.jar pyspark-shell'

os.environ['PYSPARK_SUBMIT_ARGS'] = PYSPARK_SUBMIT_ARGS



def create_df (text):
    df = pd.DataFrame(np.array([[text]]),columns=['text'])
    return df


def get_prediction(df):
	clsf = classifier
	model = clsf.read_model('../topic_predicion_model/model/custom_model.pickle')
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



if __name__ == "__main__":

	es = ES_client()
	es.create_index('final_news', schema_label2)

	sc = SparkContext(appName="PythonStreamingKafka")
	ssc = StreamingContext(sc, 1) # 1 second window

	directKafkaStream = KafkaUtils.createDirectStream(
	    ssc, 
	    # ["test"],  ## topic
	    ["parsed_news"],
	    {"metadata.broker.list": "localhost:9092"}
	)

	ori_json = directKafkaStream.map(lambda x: json.loads(x[1])) 

	label_news = ori_json.map(add_label) \
	                      .foreachRDD(store_es)

	ssc.start()
	ssc.awaitTermination()
