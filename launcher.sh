#!/bin/bash

cd ../elastic
docker-compose up -d

cd ../redis-docker
docker-compose up -d

cd ../onlineinternship
docker-compose up -d


cd ../news_streaming/kafak_spark_es_pipeline
python raw_producer.py &
python consume_parse_produce.py &
python news_top_topic_analysis.py &
python news_topic_prediction.py &
python ../web_app/app.py