# method 1
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SQLContext
import sys
import requests

# spark config
conf = SparkConf()
conf.setMaster("local[4]").setAppName("StreamingAPIWithTwitter")

# spark cxt
scxt = SparkContext(conf=conf)
# scxt.setLogLevel("ERROR")

# spark streaming cxt, interval size 2 seconds
sscxt = StreamingContext(scxt, 1)
sscxt.checkpoint("checkpoint_StreamingTwitter")

# read from port 9009
dataStream = sscxt.socketTextStream("localhost", 10086)

words = dataStream.flatMap(lambda line: line.split(" "))
print(words)
print('end')


def aggregate_tags_count(new_values, total_sum):
	return sum(new_values) + (total_sum or 0)

def get_sql_context_instance(spark_context):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)    
    return globals()['sqlContextSingletonInstance']

def process_rdd(time, rdd):
    print("----------- %s -----------" % str(time))
    try:
        # Get spark sql singleton context from the current context
        sql_context = get_sql_context_instance(rdd.context)
        # convert the RDD to Row RDD
        row_rdd = rdd.map(lambda w: Row(hashtag=w[0], hashtag_count=w[1]))
        # create a DF from the Row RDD
        hashtags_df = sql_context.createDataFrame(row_rdd)
        # Register the dataframe as table
        hashtags_df.registerTempTable("hashtags")
        # get the top 10 hashtags from the table using SQL and print them
        hashtag_counts_df = sql_context.sql("select hashtag, hashtag_count from hashtags order by hashtag_count desc limit 10")
        hashtag_counts_df.show()
        # call this method to prepare top 10 hashtags DF and send them
        send_df_to_dashboard(hashtag_counts_df)
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)

def send_df_to_dashboard(df):
	# extract the hashtags from dataframe and convert them into array
	top_tags = [str(t.hashtag) for t in df.select("hashtag").collect()]
	# extract the counts from dataframe and convert them into array
	tags_count = [p.hashtag_count for p in df.select("hashtag_count").collect()]
	# initialize and send the data through REST API
	url = 'http://localhost:5001/updateData'
	request_data = {'label': str(top_tags), 'data': str(tags_count)}
	response = requests.post(url, data=request_data)

# filter the words to get only hashtags, then map each hashtag to be a pair of (hashtag,1)
hashtags = words.filter(lambda w: '#' in w).map(lambda x: (x, 1))
# adding the count of each hashtag to its last count
tags_totals = hashtags.updateStateByKey(aggregate_tags_count)
# do processing for each RDD generated in each interval
tags_totals.foreachRDD(process_rdd)
# start the streaming computation
sscxt.start()
# wait for the streaming to finish
sscxt.awaitTermination()

# from pyspark import SparkContext, SparkConf
# from pyspark.sql.session import SparkSession
# conf = SparkConf().setAppName('test_parquet')
# sc = SparkContext('local', 'test', conf=conf)
# spark = SparkSession(sc)
# # parquetFile = r"hdfs://host:port/Felix_test/test_data.parquet"
# parquetFile = r"data.gz.parquet"
# df = spark.read.parquet(parquetFile)
# print(df.first())