# to run this in a terminal at location /home/cloudera
#  ./testkfp.sh findata/cm 2016 JAN 200 1
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
.withColumn('qty', splitCol.getItem(8)) \
.withColumn('vlu', splitCol.getItem(9)) \
.select('symbol', 'qty', 'vlu')

stocksAggregated = stocksDF.groupBy('symbol') \
.agg(sum('qty').alias('totqty'), avg('qty').alias('avgqty'),
sum('vlu').alias('totvlu'), avg('vlu').alias('avgvlu')) \
.writeStream.format('console')  \
.outputMode('update') 

stocksAggQuery = stocksAggregated.start()

stocksAggQuery.awaitTermination()


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

# query.lastProgress()    # the most recent progress update of this streaming query


