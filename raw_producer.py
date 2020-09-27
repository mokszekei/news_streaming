from time import sleep  
from bs4 import BeautifulSoup
import requests
import json
import pandas as pd
from datetime import datetime
from kafka import KafkaProducer


def generate_urls(date, sources):
    """
    Generate urls for today's News for multiple sources
    """
    
    paths = ['http://newsapi.org/v2/everything?language=en&sources=' + source + '&from=' + date 
             + '&to=' + date+ '&apiKey=266ebc4ba15241eba220f81ba83a3ee9' for source in sources]
    return paths


def connect_kafka_producer():
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers='localhost:9092')
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer


def publish_message(producer_instance, topic_name, key, value):
    try:
        key_bytes = bytes(key, encoding='utf-8')
        value_bytes = bytes(value, encoding='utf-8')
        producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
        producer_instance.flush()
        print('Message published successfully.')
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))


def auto_producer_raw(urls,producer_instance):
    """
    Produce extracted News data to topic "raw_news" automatically 
    """
    producer = producer_instance;
    for url in urls:
        response = requests.get(url)
        result=response.json()
        for article in result['articles']:
        	value = json.dumps(article)
        	publish_message(producer, 'raw_news', 'raw', value)
    if producer is not None:
        producer.close()


if __name__ == '__main__':

    date = datetime.now().strftime("%Y-%m-%d")
    sources = ['fox-news', 'techcrunch', 'reuters']
    urls = generate_urls(date, sources)
    #sleep(600)
    kafka_producer = connect_kafka_producer()
    auto_producer_raw(urls,kafka_producer)

