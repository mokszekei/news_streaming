from flask import Flask, jsonify, request, json
from flask import render_template
import ast
from elasticsearch.helpers import scan
import datetime as dt
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'common'))
from es_client import ES_client
from recommender import Recommender


app = Flask(__name__)

top_topics = {'labels': [], 'count': []}

es = ES_client()


@app.route("/")
def get_dashboard_page():
	global top_topics
	return render_template(
		'dashboard.html',
		top_topics=top_topics)


@app.route('/refresh_top_topics')
def refresh_top_topics():
	global top_topics

	return jsonify(
		sLabel=top_topics['labels'],
		sCount=top_topics['count'])


@app.route('/update_top_topics', methods=['POST'])
def update_top_topics():
	global top_topics
	if not request.form not in request.form:
		return "error", 400

	top_topics['labels'] = ast.literal_eval(request.form['labels'])
	top_topics['count'] = ast.literal_eval(request.form['count'])

	return "success", 201



@app.route('/news')
def news():
	Two_day_ago = dt.date.today() - dt.timedelta(hours=72)
	Two_day_ago = Two_day_ago.isoformat()

	es_response = scan(
    	es.con,
    	index='test',
    	query={"query": {
			    "bool": {
			      "filter": [
			        {
			          "range": {
			            'publishedAt': {
			              "gt": Two_day_ago,
			              "format": "yyyy-MM-dd"
			            }
			          }
			        }
			      ]
			    }
			  }}
    	)

	results = []
	for item in es_response:	
		results.append(item)
		if len(results) >=15:
			break
	new = results

	return render_template('news.html', new=new)


@app.route('/news/<id>/', methods=['GET', 'POST'])
def content(id):
	search_object = {'query': {'match': {'_id': id}}}
	doc = es.search('test', json.dumps(search_object))
	doc_final = doc['hits']['hits'][0]['_source']
	recommender = Recommender()
	recom_list = recommender.get_recommendation(doc_final)
	doc_final["extra"] = recom_list

	return render_template('content.html', doc=doc_final)


@app.route('/news/', methods=['POST'])
def search():
	## performing a full-text search, including options for fuzzy matching
	keys = request.form['key_word']

	es_response = scan(
    	es.con,
    	index='test',
    	query={"query": {'match': {'content': {"query": keys}}}}
    	)

	results = []
	for item in es_response:	
		results.append(item)
		if len(results) >=15:
			break
	new = results
	return render_template('news.html', new=new)


if __name__ == "__main__":
	app.run(host='localhost', port=5000)