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

#
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

    topic = 'quickstart-events'
    bootstrap_servers = 'localhost:9092'
    
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("subscribe", topic) \
        .load()
    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

       


    print(df, type(df))
    def print_json(x):
        print(type(x), x)
    # df.rdd.map(print_json).collect()

    query1 = df.writeStream \
        .format("console") \
        .start() 
    
    print(dir(query1))
    print(type(query1), "!!!!")
    query1.awaitTermination()
    exit()
    tmp = query1.rdd.foreach(print_json).collect()
        
    

    # query1.awaitTermination()
    spark.sparkContext.start()
    spark.sparkContext.awaitTermination()