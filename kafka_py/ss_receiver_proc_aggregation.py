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
from pyspark.sql.functions import aggregate, explode, window
from pyspark.sql.functions import split, decode, udf
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
    
    @udf(returnType=StringType())
    def getHashTag(text):
        # hashtag & http
        # st = time.time()
        text = text.split(" ")
        hashtag_list = []
        for word in text:
            if word.startswith("#"):
                hashtag_list.append(word)
        
        hashtag = " ".join(hashtag_list)
        # print('hashtag time', time.time()-st)
        return hashtag

    from text_processor_engine import engtextproc
    @udf(returnType=StringType())
    def pre_processing_text(text):
        # RT & @ & hashtag & http
        text = text.split(" ")
        text = [word for word in text if word != "RT" or word[0] != "@" \
            or not word.startswith("#") or not word.startswith("http")]
    
        # not english words
        text = " ".join(text)
        final_words = engtextproc.process(text)
        return " ".join(final_words)

    # vector_map = engtextproc.get_vector_map()
    # engtextproc.load_vector_model()
    # vector_model = engtextproc.get_vector_model()
    @udf(returnType=StringType())
    def get_vector(x):  
        # global model
        print(type(x), x)
        words = x.split(" ")
        print(words)
        # return words
        vector_lines = []
        # for i in words:
        #     if i in vector_model:
        #         vector_lines.append(vector_model[i])
        #     else:
        #         print(i, 'not exists')
        #         vector_lines.append([0.0])
        # print(vector_lines)
        return vector_lines
        
    from pyspark.sql.functions import from_json, col
    # decode
    words = df.withColumn('value', decode(df.value, 'UTF-8'))  #.map(get_info)
    # deserialize
    words = words.withColumn('value', get_text_info(col('value')))
    # get hashtag
    words = words.withColumn('hashtag', getHashTag('value'))
    # preprocess text
    words = words.withColumn('pre-processed text', pre_processing_text('value'))
    # vectorize text
    words = words.withColumn('vectorization', get_vector('pre-processed text'))

    # words = words.select(
    #     explode(
    #         split(words.value, " ")
    #     ).alias("word")
    # )

    # # Generate running word count
    # wordCounts = words.groupBy("word").count()

    #bwy split hashtag & aggregation
    words = words.filter(words.hashtag != "")

    words = words.withColumn(
        "hashtag",
        explode(
            split(words.hashtag, " ")
        )
    )

    wordCounts = words \
        .groupBy(window(words.timestamp, "20 seconds", "5 seconds"),"hashtag") \
        .count()

    # aggregation_query = words \
    #     .writeStream \
    #     .outputMode("update") \
    #     .format("kafka") \
    #     .option("checkpointLocation", "./wordcount_checkpoint") \
    #     .option("kafka.bootstrap.servers", bootstrap_servers) \
    #     .option("topic", "word_count") \
    #     .start()

    aggregation_query = wordCounts \
        .writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", False) \
        .start()

    aggregation_query.awaitTermination()

    # query = words \
    #     .writeStream \
    #     .outputMode("append") \
    #     .format("console") \
    #     .start()

    # query.awaitTermination()
    exit()