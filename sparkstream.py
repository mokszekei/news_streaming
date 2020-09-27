from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import os

# To initialize PySpark, 
# import findspark
# findspark.init('/usr/local/Cellar/apache-spark/3.0.1')
# findspark.init()

# Streaming context
#os.environ['SPARK_HOME'] = '/usr/local/Cellar/apache-spark/3.0.1'
os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars /usr/local/Cellar/apache-spark/3.0.1/spark-streaming-kafka-0-8-assembly_2.11-2.4.6.jar pyspark-shell'

sc = SparkContext(appName="PythonStreamingKafka")
ssc = StreamingContext(sc, 1) # 1 second window

# Spark Streaming works by creating batches of events at certain time intervals, as configured by the user,
# and delivering them for processing at another specified time interval.

directKafkaStream = KafkaUtils.createDirectStream(
    ssc, 
    ["parsed"],  ## topic
    {"metadata.broker.list": "localhost:9092"}
)

directKafkaStream.pprint()
#Starting Spark context
ssc.start()
ssc.awaitTermination()