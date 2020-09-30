from time import sleep  
from bs4 import BeautifulSoup
import requests
import json
import pandas as pd
from datetime import datetime
from kafka import KafkaProducer
from kafka import KafkaConsumer
import redis
import hashlib

class Redis():
    def __init__(self):
        self.con = redis.Redis(
            host='', #ip地址
            port=6379, #端口号，默认为6379
            db=0,
            decode_responses=True #设置为True存的数据格式就是str类型
        )
    def add(self,key,data):
        if self.con.setnx(key, data):
            print("Successfully add")
            return 1
        else:
            print("Data already exist")
            return 0


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


def connect_kafka_producer():
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers='localhost:9092')
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer


def parse (messenger):
    news_piece = json.loads(messenger.value)
    # del news_piece['urlToImage']
    source = news_piece['source']['id']
    extractor = extractors[source]
    news_piece['content'] = extractor(news_piece['url'])
    return news_piece


def extract_article_techcrunch (url):
    r=requests.get(url)
    soup = BeautifulSoup(r.text, "lxml")
    article=[]
    table = soup.findAll('div',attrs={"class":"article-content"})
    for x in table[0].findAll('p',attrs={'class': None}):
        article.append(x.text)
    return article


def extract_article_reuters (url):
    r=requests.get(url)
    soup = BeautifulSoup(r.text, "lxml")
    article=[]
    table = soup.findAll('p',attrs={"class":"Paragraph-paragraph-2Bgue ArticleBody-para-TD_9x"})
    for x in table:
        article.append(x.text)
    return article


def extract_article_fox_news (url):
    r=requests.get(url)
    soup = BeautifulSoup(r.text, "lxml")
    article=[]
    table = soup.findAll('p',attrs={"class":'speakable'})
    for x in table:
        article.append(x.text)
    table = soup.findAll('p',attrs={"class":None})
    for x in table:
        if x.find('strong') == None:
            article.append(x.text)
    return article


def check_duplicate(msg,redis):
    re=redis
    title = json.loads(msg.value)['title']
    encode = hashlib.md5(title.encode()).hexdigest()
    return re.add(encode,"")


if __name__ == '__main__':

    extractors = {'reuters' : extract_article_reuters,
	'fox-news' : extract_article_fox_news,
	'techcrunch' : extract_article_techcrunch}
    parsed_records = []
    topic_name = 'raw_news'
    parsed_topic_name = 'parsed_news'
    re = Redis()
    consumer = KafkaConsumer(topic_name, auto_offset_reset='earliest',
		bootstrap_servers='localhost:9092', consumer_timeout_ms=1000)

    for msg in consumer:
        if check_duplicate(msg,re):
            news_piece = parse(msg)
            parsed_records.append(news_piece)

    consumer.close()
    sleep(2)

    if len(parsed_records) > 0:
        print('Publishing records..')
        producer = connect_kafka_producer()
        for rec in parsed_records:
            sleep(0.5)
            publish_message(producer, parsed_topic_name, 'parsed', json.dumps(rec))
    