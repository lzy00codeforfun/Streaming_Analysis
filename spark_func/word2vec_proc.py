from pyspark.ml.feature import Word2Vec
import pyspark

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, Row

# # Input data: Each row is a bag of words from a sentence or document.
# spark = SparkSession.builder.appName('spark_word2vec').getOrCreate()
# # dataframe
# documentDF = spark.createDataFrame([
#     ("Hi I heard about Spark".split(" "), ),
#     ("I wish Java could use case classes".split(" "), ),
#     ("Logistic regression models are neat".split(" "), ),
#     ("Linear regression models are dirty".split(" "), )
# ], ["text"])

# rdd
sc = SparkContext("local", "PySpark Word Count Exmaple")
documentDF_rdd = sc.parallelize([
    "Hi I heard about Spark".split(" "),
    "I wish Java could use case classes".split(" "),
    "Logistic regression models are neat".split(" "),
    "Linear regression models are dirty".split(" "), 
])

# Learn a mapping from words to Vectors.
import fasttext

model = fasttext.load_model('fasttext_twitter_raw.bin')

def transform_w2v(x):
    return model[x]

def transform_w2v(x):
    w2v_list = []
    for i in x:
        if i in model:
            w2v_list.append(model[i])
    return w2v_list


def transform_w2v(x):
    w2v_list = []
    for i in x:
        if i in model:
            print(i)
            w2v_list.append(i)
    return w2v_list


def print_iter(x):
    print(x, type(x))
    for i in x:
        if i in model:
            print(i, type(i))
# df
result = documentDF.rdd.map(transform_w2v)
result = documentDF.rdd.map(print_iter)

# rdd
result = documentDF_rdd.map(transform_w2v)
result = documentDF_rdd.map(print_iter)
result.collect()
for row in result.collect():
    text, vector = row
    print("Text: [%s] => \nVector: %s\n" % (", ".join(text), str(vector)))