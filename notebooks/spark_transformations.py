
# coding: utf-8

# In[1]:


import os
import sys
os.environ['SPARK_HOME']='/usr/lib/spark'
os.environ['PYLIB']=os.environ['SPARK_HOME']+'/python/lib'
sys.path.insert(0,os.environ['PYLIB']+'/py4j-0.10.7-src.zip')
sys.path.insert(1,os.environ['PYLIB']+'/pyspark.zip')


# In[2]:


from pyspark.sql import SparkSession
import pandas as pd
import numpy as np


# In[3]:


spark = SparkSession.builder.appName('SparkTransformations') .config('spark.warehouse.dir','/apps/hive/warehouse') .enableHiveSupport().getOrCreate()


# In[4]:


# Having set the driver and driver options we should have spark representing spark session 
# available straight away
spark.version


# In[5]:


sc = spark.sparkContext


# In[6]:


sc.version
sc.setLogLevel('ERROR')

# In[8]:


shak_rdd = sc.textFile('shakespeare.txt')
shak_rdd.map(lambda x: (x, 1)).toDF(['line', 'ucol']).drop('ucol').show(5)


# In[20]:


from pyspark.sql import Row
shak_rdd = sc.textFile('shakespeare.txt')
shak_df = shak_rdd.map(lambda x: Row(x)).toDF(['line'])
shak_df.show()


# In[56]:


# .filter("word != ''")
# .filter(col('word' != ''))
from pyspark.sql.functions import *
shak_df.select(explode(split('line', ' ')).alias('word')) .filter("word != ''") .select(lower(col('word')).alias('word')) .groupBy('word') .agg(count('word').alias('wc')) .orderBy(desc('wc')).show()


# In[63]:


shak_df.createOrReplaceTempView('shktbl')
spark.sql('''
select word, count(*) as wc from(
select explode(split(lower(line), ' ')) as word from shktbl
) f
where word != ''
group by word
order by wc desc
limit 20
''').show()


# In[6]:


spark.sql('show tables').show()


# In[11]:


a_coll = ['a', 'b', 'c']
a_coll_rdd = sc.parallelize(a_coll)
a_coll_rdd.map(lambda x: (x, 1)).collect()


# In[12]:


no_rdd = sc.parallelize(range(100))
print('Filtering to print only odd numbers \n', no_rdd.filter(lambda x: x % 2 != 0).collect())


# In[9]:


ott_rdd = sc.parallelize([1, 2, 3])
print('Flatmapping illustration \n')
ott_rdd.flatMap(lambda x: (x, x * 100, 42)).collect()


# In[10]:


print('RDD groupBy transformation ')
group_by_collect = no_rdd.groupBy(lambda x: x % 2).collect()


# In[11]:


[(x[0], list(x[1])) for x in group_by_collect]


# In[12]:


# A utillity function to print tuples in a list
def printTupleList(tlist):
    return [(x[0], list(x[1])) for x in tlist ]


# In[13]:


# create a rdd to illustrate groupByKey transformation
pow_lt3_rdd = sc.parallelize([(1, 1), (1, 1), (1, 1), (2, 2), (2, 4), (2, 8), (3, 3), (3, 9), (3, 27)])


# In[14]:


print('groupByKey transformation\n')
printTupleList(pow_lt3_rdd.groupByKey().collect())


# In[15]:


print('reduceByKey transformation')
pow_lt3_rdd.reduceByKey(lambda x, y: x + y).collect()


# In[16]:


a_coll_rdd.getNumPartitions()


# In[17]:


a_coll_rdd.glom().collect()


# # MapPartitions

# In[18]:


def show_partitions(idx, itera):
    yield 'index: ' + str(idx) + ' , elements: ' + str(list(itera))


# In[19]:


print('mapPartitionsWithIndex ')
a_coll_rdd.mapPartitionsWithIndex(show_partitions).collect()


# In[20]:


def f(sidx, itr): yield sidx


# In[21]:


a_coll_rdd.mapPartitionsWithIndex(f).collect()


# In[22]:


# we require a function to iterate over the elements of a partition
def map_partitions_function(itera): 
    yield ', '.join(list(itera))


# In[23]:


a_coll_rdd.mapPartitions(map_partitions_function).collect()


# # Pair RDD Operations - Joins

# In[24]:


print('Union of two rdds:\n')
xu = sc.parallelize([1, 2, 3], 2)
yu = sc.parallelize([3, 4], 1)
print("union: " , xu.union(yu).collect())


# In[25]:


xj = sc.parallelize([("a", 1), ("b", 2)])
yj = sc.parallelize([("a", 3), ("a", 4), ("b", 5)])
print(xj.join(yj).collect())


# In[26]:


print("Left Outer Join")
xoj = sc.parallelize([("a", 1), ("b", 2), ("c", 7)])
yoj = sc.parallelize([("a", 3), ("a", 4), ("b", 5), ("d", 4)])
print(xoj.leftOuterJoin(yoj).collect())
print("Right Outer Join")
print(xoj.rightOuterJoin(yoj).collect())


# In[27]:


print("Full Outer Join")
print(xoj.fullOuterJoin(yoj).collect())


# In[28]:


print('Distinct\n')
rpt_list = np.random.randint(0, 5, 10)
print('Randomly generated list with repeat elements ', rpt_list)
rpt_rdd = sc.parallelize(rpt_list)
print(rpt_rdd.distinct().collect())


# In[29]:


print("coalesce to reduce number of partitions")
xCoalesce = sc.parallelize(range(10), 4)
print(xCoalesce.getNumPartitions())
yCoalesce = xCoalesce.coalesce(2)
print(yCoalesce.getNumPartitions())


# In[30]:


print('keyBy transformation')
new_no_rdd = sc.parallelize(range(10))
print(new_no_rdd.keyBy(lambda x: x % 2 == 0).collect())
print(new_no_rdd.keyBy(lambda x: x % 2 == 0).map(lambda x: ('even' if x[0] else 'odd', x[1])).collect())


# # Custom Partitioner Spark RDD

# In[31]:


def part_function(k):
    return 0 if k < 'H' else 1


# In[32]:


x_part = sc.parallelize([('J', "James"), ('F', "Fred"), ('A', "Anna"), ('J', "John")], 3)
x_part.partitionBy(2, part_function).mapPartitionsWithIndex(show_partitions).collect()


# In[33]:


azip = sc.parallelize(range(4))
bzip = sc.parallelize([1, 4, 9, 16])

print("Zipping RDDs")
print(azip.zip(bzip).collect())


# # Sorting RDDs

# In[34]:


rand_rdd = sc.parallelize(np.random.randint(0, 20, 10))
print("(rand_rdd.collect())")


# In[35]:


print('Simple sort ascending')
rand_rdd.sortBy(lambda x: x).collect()


# In[36]:


print('Simple sort descending')
rand_rdd.sortBy(lambda x: -x).collect()


# In[29]:


comp_rdd = sc.parallelize([("arjun", "tendulkar", 5),
    ("sachin", "tendulkar", 102), ("vachin", "tendulkar", 102),
    ("rahul", "dravid", 74), ("vahul", "dravid", 74),
    ("rahul", "shavid", 74), ("vahul", "shavid", 74),
    ("jacques", "kallis", 92), ("ricky", "ponting", 84), ("jacques", "zaalim", 92),
    ("sachin", "vendulkar", 102)])


# In[30]:


comp_rdd.sortBy(lambda x: x[1], False).collect()


# In[31]:


comp_df = comp_rdd.toDF(['fname', 'lname', 'centuries'])
comp_df.show()


# In[40]:


from pyspark.sql.functions import *
comp_df.sort('lname', desc('fname'), desc('centuries')).show()


# # RDD Aggregation
# ### A sequence operation which will run as a combiner of sort on the elements of the partition
# ### A combining operation which will reduce the combined tuples from the partitions to a single tuple

# In[9]:


def seq_op(data, elem):
    return (data[0] + elem, data[1] + 1)

def comb_op(d1, d2):
    return (d1[0] + d2[0], d1[1] + d2[1])

ardd = sc.parallelize(range(1, 13), 6)
print('First take a look at the partitions of the rdd')
print(ardd.mapPartitionsWithIndex(show_partitions).collect())
print(ardd.aggregate((0,0), seq_op, comb_op))

print(ardd.aggregate((0, 0), 
                    lambda data, elem: (data[0] + elem, data[1] + 1),
                    lambda d1, d2: (d1[0] + d2[0], d1[1] + d2[1])))


#  ### Tree aggregations operate exactly in the same way as aggreate  except for one critical difference - there is an intermediate aggregation step  data from some partitions will be sent to executors to aggregate
#  ### so in the above case if there are six partitions  while aggregate will send results of all the six partitions to the driver in tree aggregate, three will go to one executor, three to another and the driver will receive the aggregations from 2 rather than 6
#  ### Where there are many number of partitions tree aggregate performs significantly better than vanilla aggregate

# In[10]:


print(ardd.treeAggregate((0,0), seq_op, comb_op))


# In[11]:


frd = sc.parallelize([(1, 1), (2, 4), (3, 9), (1, 1), (2, 8), (3, 27),
      (1, 1), (2, 16), (3, 81), (4, 256)])


# In[12]:


frd.combineByKey(lambda x: (x, 1),
                lambda acc, vlu: (acc[0] + vlu, acc[1] + 1),  
                lambda v1, v2: (v1[0] + v2[0], v1[1] + v2[1])).collect()


# ### aggregateByKey also will function at the partition level - we need a value to seed the aggregation and we have a combination of a sequence operation and a combo operation playing out

# In[17]:


frd.aggregateByKey(0,
                   lambda x, y: x + y, 
                   lambda a, b: a + b
                  ).collect()

