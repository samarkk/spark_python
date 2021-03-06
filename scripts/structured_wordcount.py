import os
import sys
os.environ['SPARK_HOME']='/home/cloudera/spark230hadoop26'
os.environ['PYLIB']=os.environ['SPARK_HOME']+'/python/lib'
sys.path.insert(0,os.environ['PYLIB']+'/py4j-0.10.6-src.zip')
sys.path.insert(1,os.environ['PYLIB']+'/pyspark.zip')


from pyspark.sql import SparkSession
import pandas as pd
import numpy as np

spark = SparkSession.builder.appName('StreamingWordCount') \
.config('spark.warehouse.dir','/apps/hive/warehouse') \
.config('spark.sql.shuffle.partitions', 2) \
.enableHiveSupport().getOrCreate()

# Having set the driver and driver options we should have spark representing spark session 
# available straight away
spark.version

from pyspark.sql.functions import * 

spark.conf.set('spark.sql.shuffle.partitions', 2)

lines = spark \
.readStream \
.format("socket") \
.option("host", "localhost") \
.option("port", 9999) \
.load() \
.withColumn("timestamp", current_timestamp())

words = lines.toDF('word', 'timestamp').select(
explode(split('word', ' ')).alias('word'), 'timestamp'
)

query = words.groupBy('word').count().writeStream.outputMode('complete').format('console')

query.start().awaitTermination()


wordCount = words \
.withWatermark("timestamp", '5 seconds')\
.groupBy("word") \
.count()

windowedWordCounts = words. \
groupBy(
window("timestamp", "4 seconds", "2 seconds"),
"word").count()

windowQuery = windowedWordCounts \
.writeStream \
.format("console")  \
.outputMode("complete") \
.option("truncate", False) 

windowQuery.start().awaitTermination()

# To get status of queries
spark.streams.active
spark.streams.active[n].status
spark.streams.active[n].lastProgress

waterMarkedWindowCount = words \
.withWatermark("timestamp", "2 minutes") \
.groupBy(
  window("timestamp", "2 minutes", "1 minutes"),
"word").count().toDF("wdw", "word", "counts")

waterMarkedWindowCount.printSchema()

waterMarkedWindowCount.writeStream \
.format("parquet") \
.outputMode("append") \
.option("truncate", False) \
.option("path", "hdfs://localhost:8020/user/cloudera/waterwrdc") \
.option("checkpointLocation", "file:///home/cloudera/watercheck") \
.start() \
.awaitTermination()

batchTbl = spark.read.table("wimptbl")
words.join(batchTbl, ["word"], "left_outer")\
.na.fill(0, ["freq"]) \
.groupBy("word", "freq") \
.count() \
.withColumn("wtdfreq", col("freq") * col("count")) \
.writeStream \
.format("console") \
.outputMode("complete") \
.start() \
.awaitTermination()


