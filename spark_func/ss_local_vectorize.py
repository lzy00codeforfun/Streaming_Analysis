# - read from any kafka
# - do computation
# - write back to kafka

import atexit
import logging
import json
import sys
import time

from pyspark.sql.types import StringType

from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import explode, transform
from pyspark.sql.functions import split, col, udf
# from fasttext_wrapper import model, transform_w2v

topic = None
target_topic = None
brokers = None
kafka_producer = None


if __name__ == "__main__":

    spark = SparkSession.builder.appName('spark_word2vec').getOrCreate()

    # Create DataFrame representing the stream of input lines from connection to localhost:9999
    lines = spark \
        .readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", 9999) \
        .load()

    vector_map = {
        "hello": [1,2,3,4,5],
        "world": [4.3, 5.3, 43.5, 0.0, 1.34]
    }
    @udf(returnType=StringType())
    def get_vectorized(x):  
        # global model
        print(type(x), x)
        words = x.split(" ")
        print(words)
        # return words
        vector_lines = []
        for i in words:
            if i in vector_map:
                vector_lines.append(vector_map[i])
            else:
                print(i, 'not exists')
                vector_lines.append([0.0])
        print(vector_lines)
        return vector_lines
    #     

    # lines.withColumn("vector", get_vectorized(col("value"))) 
    # lines.select(get_vectorized("value")).show()
    # lines = lines.select(get_vectorized("value"))
    lines = lines.withColumn("vectorized", get_vectorized("value"))
     


    query = lines \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

    query.awaitTermination()
    
    exit()