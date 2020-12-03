from elasticsearch.helpers import scan
import datetime as dt
import pytz
from dateutil import parser
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from es_client import ES_client


class Recommender:

	def __init__(self):
		self.news_similarity_list = []
		self.recommended_news = []

	def connect_text(self,text_list):
		text = ''
		for i in text_list:
			text = text + i
		return text

	def get_cos_similarity(self,news1,news2):

		article_1 = self.connect_text(news1["content"])
		article_2 = self.connect_text(news2["content"])
		corpus = [article_1,article_2]
		vectorizer = TfidfVectorizer()
		trsfm = vectorizer.fit_transform(corpus)
		similarity = cosine_similarity(trsfm[0:1], trsfm)
		return similarity[0][1]


	def get_recommendation(self,news):

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

		for item in es_response:
			news2 = item['_source']
			similarity = self.get_cos_similarity(news,news2)
			self.news_similarity_list.append((news2,similarity))
			# print(similarity)
		
		self.news_similarity_list = sorted(self.news_similarity_list, key = lambda x: x[1],reverse = True)

		for item in self.news_similarity_list[:5]:
			self.recommended_news.append(item[0])
		return self.recommended_news



