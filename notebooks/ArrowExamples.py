'''
Apache Arrow is a cross-language development platform for in-memory data. 
It specifies a standardized language-independent columnar memory format for flat and hierarchical data, 
organized for efficient analytic operations on modern hardware

In simple words, It facilitates communication between many components, 
for example, reading a parquet file with Python (pandas) and transforming to a Spark dataframe, 
Falcon Data Visualization or Cassandra without worrying about conversion
'''
# install pyarrow
# conda install -c conda-forge pyarrow
# or
# pip install pyarrow
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import LongType
from typing import Iterator, Tuple
from pyspark.sql import Window

import  numpy as np
import  pandas as pd

spark = SparkSession.builder.appName("ArrowExamples").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel('ERROR')
print(spark.version)

'''
Apache Arrow is an in-memory columnar data format used in Apache Spark to efficiently transfer data between JVM and Python processes

Arrow is available as an optimization when converting a Spark DataFrame to a Pandas DataFrame using the call DataFrame.toPandas() 
and when creating a Spark DataFrame from a Pandas DataFrame with SparkSession.createDataFrame(). 
To use Arrow when executing these calls, users need to first set the Spark configuration 
spark.sql.execution.arrow.pyspark.enabled to true. This is disabled by default

In addition, optimizations enabled by spark.sql.execution.arrow.pyspark.enabled 
could fallback automatically to non-Arrow optimization implementation if an error occurs 
before the actual computation within Spark. 
This can be controlled by spark.sql.execution.arrow.pyspark.fallback.enabled.
'''

# Enable Arrow-based columnar data transfers
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

# Generate a Pandas DataFrame
pdf = pd.DataFrame(np.random.rand(100, 3))

# Create a Spark DataFrame from a Pandas DataFrame using Arrow
df = spark.createDataFrame(pdf)

# Convert the Spark DataFrame back to a Pandas DataFrame using Arrow
result_pdf = df.select("*").toPandas()

# Currently, all Spark SQL data types are supported by Arrow-based conversion
# except MapType, ArrayType of TimestampType, and nested StructType.

# Pandas UDF - Vecotrized UDFs
# caan be defined using Python type hints
# the type hint should use pandas.Series in all cases 
# but there is one variant that pandas.DataFrame should be used for its input or output type hint instead 
# when the input or output column is of StructType
@pandas_udf("col1 string, col2 long")
def func(s1: pd.Series, s2: pd.Series, s3: pd.DataFrame) -> pd.DataFrame:
    s3['col2'] = s1 + s2.str.len()
    return s3

# Create a Spark DataFrame that has three columns including a sturct column.
df = spark.createDataFrame(
    [[1, "a string", ("a nested string",)]],
    "long_col long, string_col string, struct_col struct<col1:string>")

df.printSchema()

df.select(col("long_col"), col("string_col"), col("struct_col"), 
          func("long_col", "string_col", "struct_col").alias("afc")). \
select("long_col", "string_col", "afc.col1", "afc.col2").show(2, False)

df.select(func("long_col", "string_col", "struct_col")).printSchema()

# Series to Series
# Declare the function and create the UDF
def multiply_func(a: pd.Series, b: pd.Series) -> pd.Series:
    return a * b

multiply = pandas_udf(multiply_func, returnType=LongType())

# The function for a pandas_udf should be able to execute with local Pandas data
x = pd.Series([1, 2, 3])
print(multiply_func(x, x))

# Create a Spark DataFrame, 'spark' is an existing SparkSession
df = spark.createDataFrame(pd.DataFrame(x, columns=["x"]))

# Execute function as a Spark vectorized UDF
df.select(multiply(col("x"), col("x")).alias('mcol')).show()

#Iterator of Series to Iterator of Series
# The type hint can be expressed as Iterator[pandas.Series] -> Iterator[pandas.Series]
# Declare the function and create the UDF
@pandas_udf("long")
def plus_one(iterator: Iterator[pd.Series]) -> Iterator[pd.Series]:
    for x in iterator:
        yield x + 1

df.select(plus_one("x").alias('plusone')).show()

#Iterator of Multiple Series to Iterator of Series
# The type hint can be expressed as Iterator[Tuple[pandas.Series, ...]] -> Iterator[pandas.Series]
# Declare the function and create the UDF
@pandas_udf("long")
def multiply_two_cols(
        iterator: Iterator[Tuple[pd.Series, pd.Series]]) -> Iterator[pd.Series]:
    for a, b in iterator:
        yield a * b

df.select(multiply_two_cols("x", "x").alias('twoc_mult')).show()

# Series to Scalar
# The type hint can be expressed as pandas.Series, … -> Any.
df4SerScalar = spark.createDataFrame(
    [(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)],
    ("id", "v"))

# Declare the function and create the UDF
@pandas_udf("double")
def mean_udf(v: pd.Series) -> float:
    return v.mean()

df4SerScalar.select(mean_udf(df4SerScalar['v']).alias('meanv')).show()

w = Window \
    .partitionBy('id') \
    .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
df4SerScalar.withColumn('mean_v', mean_udf(df4SerScalar['v']).over(w)).show()

#Pandas Function APIs
'''
pandas function APIs enable us to directly apply a Python native function 
that takes and outputs pandas instances to a PySpark DataFrame. 
Similar to pandas user-defined functions, function APIs also use Apache Arrow to transfer data 
and pandas to work with the data; however, Python type hints are optional in pandas function APIs
'''
#Grouped Map
...
You transform your grouped data using groupBy().applyInPandas() to implement the “split-apply-combine” pattern. 
Split-apply-combine consists of three steps:

Split the data into groups by using DataFrame.groupBy
Apply a function on each group. The input and output of the function are both pandas.DataFrame
The input data contains all the rows and columns for each group.
Combine the results into a new DataFrame
'''                                    
                                                                                                                                                                                                                                                                                    ''

#To use groupBy().applyInPandas(), the user needs to define the following:

#A Python function that defines the computation for each group.
# A StructType object or a string that defines the schema of the output PySpark DataFrame.
def subtract_mean(pdf):
    # pdf is a pandas.DataFrame
    v = pdf.v
    return pdf.assign(v=v - v.mean())

df4SerScalar.groupby("id").applyInPandas(subtract_mean, schema="id long, v double").show()

#Map
# Map operations with Pandas instances are supported by DataFrame.mapInPandas()
# which maps an iterator of pandas.DataFrames to another iterator of pandas.DataFrames
# that represents the current PySpark DataFrame and returns the result as a PySpark DataFrame

df4Map = spark.createDataFrame([(1, 21), (2, 30)], ("id", "age"))

def filter_func(iterator):
    for pdf in iterator:
        yield pdf[pdf.id == 1]

df4Map.mapInPandas(filter_func, schema=df4Map.schema).show()

# Co-grouped Map
# Co-grouped map operations with Pandas instances are supported by
# DataFrame.groupby().cogroup().applyInPandas() which allows two PySpark DataFrames
# to be cogrouped by a common key and then a Python function applied to each cogroup
df1 = spark.createDataFrame(
    [(20000101, 1, 1.0), (20000101, 2, 2.0), (20000102, 1, 3.0), (20000102, 2, 4.0)],
    ("time", "id", "v1"))

df2 = spark.createDataFrame(
    [(20000101, 1, "x"), (20000101, 2, "y")],
    ("time", "id", "v2"))

def asof_join(l, r):
    return pd.merge_asof(l, r, on="time", by="id")

df1.groupby("id").cogroup(df2.groupby("id")).applyInPandas(
    asof_join, schema="time int, id int, v1 double, v2 string").show()
