from pyspark.ml.feature import Word2Vec
import pyspark

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, Row

# Input data: Each row is a bag of words from a sentence or document.
spark = SparkSession.builder.appName('spark_word2vec').getOrCreate()
documentDF = spark.createDataFrame([
    ("Hi I heard about Spark".split(" "), ),
    ("I wish Java could use case classes".split(" "), ),
    ("Logistic regression models are neat".split(" "), ),
    ("Linear regression models are dirty".split(" "), )
], ["text"])

# Learn a mapping from words to Vectors.
word2Vec = Word2Vec(vectorSize=3, minCount=0, inputCol="text", outputCol="result")
model = word2Vec.fit(documentDF)

result = model.transform(documentDF)
for row in result.collect():
    text, vector = row
    print("Text: [%s] => \nVector: %s\n" % (", ".join(text), str(vector)))