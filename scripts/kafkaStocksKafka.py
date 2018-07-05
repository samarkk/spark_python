# to run this in a terminal at location /home/cloudera
#  ./testkfp.sh findata/cm 2016 JAN 200 1
# delete and create nsecmdaggr
# kafka-topics --zookeeper localhost:2181 --delete --topic nsecmdaggr
# kafka-topics --zookeeper localhost:2181 -list | grep nsecmd
# kafka-topics --zookeeper localhost:2181  --create --topic nsecmdaggr  --replication-factor 1 --partitions 1
# kafka-console-consumer --bootstrap-server localhost:9092 --topic nsecmdaggr  --from-beginning --property print.key=true
# we can have a console consumer checking nsecmd alongside
# and run this in pyspark console

from pyspark.sql.functions import *

spark = SparkSession.builder.appName('StructuredKafkaWordCount') \
.config('spark.warehouse.dir','/apps/hive/warehouse') \
.config('spark.sql.shuffle.partitions', 2) \
.enableHiveSupport().getOrCreate()

spark.sparkContext.setLogLevel('ERROR')

from pyspark.sql.functions import *

stockQuotes = spark.readStream.format("kafka")\
.option("kafka.bootstrap.servers", "localhost:9092") \
.option("subscribe", "nsecmd") \
.option("startingffsets", "earliest") \
.option("failOnDataLoss", "false") \
.load() \
.select("KEY", "VALUE")


splitCol = split('VALUE', ',')

stocksDF = stockQuotes.withColumn('symbol', splitCol.getItem(0)) \
.withColumn('clspr', splitCol.getItem(5)) \
.withColumn('qty', splitCol.getItem(8)) \
.withColumn('vlu', splitCol.getItem(9)) \
.select('symbol', 'qty', 'vlu', 'clspr')

stocksAggregated = stocksDF.groupBy('symbol') \
.agg(sum('qty').alias('totqty'), avg('qty').alias('avgqty'),
sum('vlu').alias('totvlu'), avg('vlu').alias('avgvlu'), 
min('clspr').alias('minpr'), max('clspr').alias('maxpr'))

stag = stocksAggregated

stocksAggregatedForKafka = stag.select(stag.symbol.alias("key"), concat(stag.totqty.cast('string'), lit(','), 
stag.avgqty.cast('string'), lit(','), stag.totvlu.cast('string'), lit(','), 
stag.avgvlu.cast('string'), lit(','), stag.minpr.cast('string'), lit(','), 
stag.maxpr.cast('string')).alias("value"))

stocks_kafka_to_kafka_query = stocksAggregatedForKafka.writeStream \
.format('kafka') \
.outputMode("update") \
.option("kafka.bootstrap.servers", "localhost:9092") \
.option("topic", "nsecmdaggr") \
.option("checkpointLocation", "hdfs://localhost:8020/user/cloudera/stcokskafkackpt")

kafka_query = stocks_kafka_to_kafka_query.start().awaitTermination()


# stocksAggQuery.stop()
# streaming query monitoring, querying options
# query.id()          # get the unique identifier of the running query that persists across restarts from checkpoint data

# query.runId()       # get the unique id of this run of the query, which will be generated at every start/restart

# query.name()        # get the name of the auto-generated or user-specified name

# query.explain()   # print detailed explanations of the query

# query.stop()      # stop the query

# query.awaitTermination()   # block until query is terminated, with stop() or with error

# query.exception()       # the exception if the query has been terminated with error

# query.recentProgress()  # an array of the most recent progress updates for this query

query.lastProgress()    # the most recent progress update of this streaming query


