# - read from any kafka
# - do computation
# - write back to kafka

import atexit
import logging
import json
import sys
import time

from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession, Row

from pyspark.sql.functions import explode
from pyspark.sql.functions import split, decode
logger_format = '%(asctime)-15s %(message)s'
logging.basicConfig(format=logger_format)
logger = logging.getLogger('stream-processing')
logger.setLevel(logging.INFO)

topic = None
target_topic = None
brokers = None
kafka_producer = None


if __name__ == "__main__":
# - create SparkContext and StreamingContext
    # sc = SparkContext("local[2]", "StockAveragePrice")
    # sc.setLogLevel('ERROR')
    # ssc = StreamingContext(sc, 5)

    spark = SparkSession.builder.appName('spark_word2vec').getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    topic = 'quickstart-events'
    bootstrap_servers = 'localhost:9092'
    
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("subscribe", topic) \
        .load()
    # df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") 

    # Decode and Split the lines into words
    def get_text_info(x):
        # spark sql json deserialize 
        # json_schema = spark.read.json(words.rdd.map(lambda row: row.json)).schema
        text_map = from_json(x, "MAP<STRING,STRING>")
        text_map = from_json(text_map["data"], "MAP<STRING,STRING>")
        return text_map["text"]
    
    from pyspark.sql.functions import from_json, col
    words = df.withColumn('value', decode(df.value, 'UTF-8'))  #.map(get_info)
    words = words.withColumn('value', get_text_info(col('value')))

    words = words.select(
        explode(
            split(words.value, " ")
        ).alias("word")
    )

    # Generate running word count
    wordCounts = words.groupBy("word").count()

    query = wordCounts \
        .writeStream \
        .outputMode("complete") \
        .format("console") \
        .start()

    query.awaitTermination()
    exit()