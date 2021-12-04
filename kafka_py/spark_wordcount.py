# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import explode, udf
from pyspark.sql.functions import split, window, col
from pyspark.sql.functions import current_timestamp, date_format

if __name__ == "__main__":
    spark = SparkSession.builder.appName("WordCount")
    # print(dir(spark))
    spark = SparkSession.builder.appName("WordCount").getOrCreate()
    # print(dir(spark))
    spark.sparkContext.setLogLevel("WARN")

    topic = 'quickstart-events'
    bootstrap_servers = 'localhost:9092'

    df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", bootstrap_servers) \
            .option("subscribe", topic) \
            .load()
    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    # print("???" * 100)
    # print(dir(df))
    words = df.select(
        explode(
            split(df.value, " ")
        ).alias("word"), "timestamp"
    )
    # .where(df.timestamp.cast("long") < current_timestamp().cast("long") - 5)
    # words = df.withColumn()

    # Generate running word count
    # wordCounts = words.groupBy(window(words.timestamp, "5 seconds"),"word").count()
    wordCounts = words \
            .withWatermark("timestamp", "10 seconds") \
            .groupBy(window(words.timestamp, "20 seconds", "5 seconds"),"word") \
            .count()

    wordCounts = wordCounts.withColumn("begin_timestamp", wordCounts.window.start)
            # .groupBy(window(words.timestamp, "5 seconds", "1 seconds"),"word") \
    # DataFrame dosen't support item assignment
    # wordCounts["window"] = wordCounts["window"][-1:-10]
    # wordCounts.withColumn("window", date_format(wordCounts.window, "HH:mm:ss"))
    # wordCounts = words.groupBy("word").count()

    # print("???" * 100)
    # print(wordCounts)

    # print("???" * 100)
    # print(df)
    # print("???" * 100)

    # @udf(returnType=StringType())
    # def printCol(x):
    #     print("udf print", x)
    #     x = str(x)
    #     return x

    # wordCounts=wordCounts.select("window", "word", printCol("window"))

    # query = wordCounts \
    #     .writeStream \
    #     .outputMode("complete") \
    #     .format("console") \
    #     .option("truncate", False) \
    #     .option("numRows", 100) \
    #     .start()
        # .trigger(processingTime = "5 seconds") \

    # wordCounts.writeStream \
    #         .option("checkpointLocation", "./WordCountCheckpoint") \
    #         .toTable("word_count")

    # spark.read.table("myTable").show()


    query = wordCounts \
        .selectExpr("CAST(window.start AS STRING) AS key", "to_json(struct(*)) AS value") \
        .writeStream \
        .outputMode("update") \
        .format("kafka") \
        .option("checkpointLocation", "./wordcount_checkpoint") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("topic", "word_count") \
        .start()


    # query = wordCounts \
    #     .writeStream \
    #     .outputMode("append") \
    #     .format("json") \
    #     .option("path", "./result.json") \
    #     .option("checkpointLocation", "./checkpoint.txt") \
    #     .start()

    query.awaitTermination()




    print("END_" * 300)

    # query1 = df.writeStream \
    #     .format("console") \
    #     .start()
    # print(query1)
    # print(dir(query1))
    # query1.awaitTermination()
    # #tmp = query1.rdd.foreach(lambda x: print(x)).collect()
    # print("???" * 100)
