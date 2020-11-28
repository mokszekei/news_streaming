from flask import Flask, jsonify, request
from flask import render_template
import ast

app = Flask(__name__)

top_topics = {'labels': [], 'count': []}


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


if __name__ == "__main__":
	app.run(host='localhost', port=5000)