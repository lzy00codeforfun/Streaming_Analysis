from pyspark.ml.feature import Word2Vec
import pyspark

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, Row

# Input data: Each row is a bag of words from a sentence or document.
spark = SparkSession.builder.appName('spark_word2vec').getOrCreate()
# dataframe
documentDF = spark.createDataFrame([
    ("Hi I heard about Spark".split(" "), ),
    ("I wish Java could use case classes".split(" "), ),
    ("Logistic regression models are neat".split(" "), ),
    ("Linear regression models are dirty".split(" "), )
], ["text"])

# rdd
# sc = SparkContext("local", "PySpark Word Count Exmaple")
# documentDF_rdd = sc.parallelize([
#     "Hi I heard about Spark".split(" "),
#     "I wish Java could use case classes".split(" "),
#     "Logistic regression models are neat".split(" "),
#     "Linear regression models are dirty".split(" "), 
# ])

# Learn a mapping from words to Vectors.
from pyspark.sql.functions import col, udf
import fasttext_wrapper

udf_predict_language = udf(fasttext_wrapper.transform_w2v)
spark.sparkContext.addFile('fasttext_twitter_raw.bin')
spark.sparkContext.addPyFile('fasttext_wrapper.py')
result = documentDF.withColumn('vector', udf_predict_language(col('text')))

# rdd
result = documentDF.map(transform_w2v)
result = documentDF.map(print_iter)
result.collect()
for row in result.collect():
    text, vector = row
    print("Text: [%s] => \nVector: %s\n" % (", ".join(text), str(vector)))