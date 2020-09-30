from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import os
import json

# 1. specify 'PYSPARK_SUBMIT_ARGS'. Tell spark where to find this .jar package
# 2. use spark-submit --jar .... in terminal. Submit the package to spark
# both works!
os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars /Users/mosiqi/onlineinternship/streamapp/spark-streaming-kafka-0-8-assembly_2.11-2.4.6.jar pyspark-shell'

sc = SparkContext(appName="PythonStreamingKafka")
ssc = StreamingContext(sc, 1) # 1 second window
# Spark Streaming works by creating batches of events at certain time intervals, as configured by the user,
# and delivering them for processing at another specified time interval.

directKafkaStream = KafkaUtils.createDirectStream(
    ssc, 
    ["test"],  ## topic
    {"metadata.broker.list": "localhost:9092"}
)

# directKafkaStream.pprint()
# first field is Key, we need value, so x[1]
# lines = directKafkaStream.map(lambda x: x[1])
# lines.pprint()

# Extract article content (type is still 'list')
content = directKafkaStream.map(lambda x: json.loads(x[1])) \
						   .flatMap(lambda article: article['content'])
content.pprint()

# Count words
# 如果content用 map 来提取，以下会出错，因为content是list： AttributeError: 'list' object has no attribute 'split'
# 但flatMap可以，因为它把各个list的词都map了遍，再合成。
counts = content.flatMap(lambda line: line.split(" ")) \
              .map(lambda word: (word, 1)) \
              .reduceByKey(lambda a, b: a+b)
counts.pprint()

# convert streaming JSON to df

# schema = StructType().add('created_at', StringType(), False).add('id_str', StringType(), False)


#Starting Spark context
ssc.start()
ssc.awaitTermination()