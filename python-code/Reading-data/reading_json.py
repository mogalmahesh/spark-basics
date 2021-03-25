# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
import findspark
findspark.init()


# %%
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Reading JSON data").getOrCreate()


# %%
df = spark.read.json("D:\\code\\spark\\spark-basics\\data\\flight-data\\json\\2010-summary.json")


# %%
df.printSchema()


# %%
df.show(2)


# %%
df2 = spark.read    .format("json")    .load("D:\\code\\spark\\spark-basics\\data\\flight-data\\json\\2011-summary.json")
df2.show(2)


# %%
df3 = spark.read    .format("json")    .option("inferschema", "true")    .option("path", "D:\\code\\spark\\spark-basics\\data\\flight-data\\json\\2012-summary.json")    .load()
df3.show(2)


# %%
df = spark.read    .option("multiline","true")    .json("D:\\code\\spark\\spark-basics\\data\\flight-data\\json\\sample.json")
df.show()


# %%
df = spark.read    .json("D:\\code\\spark\\spark-basics\\data\\flight-data\\json")
df.count()


# %%
from pyspark.sql.types import StructField, StructType, StringType,LongType
custom_schema = StructType([
    StructField("ORIGIN_COUNTRY_NAME",StringType(),False),
    StructField("DEST_COUNTRY_NAME",StringType(),False),
    StructField("count",LongType(),False)
])
df = spark.read    .schema(custom_schema)    .json("D:\\code\\spark\\spark-basics\\data\\flight-data\\json\\2010-summary.json")
df.show(2)


# %%
df.printSchema()


# %%
from pyspark.sql.types import StructField, StructType, StringType,LongType
custom_schema = StructType([
    StructField("ORIGIN_COUNTRY_NAME",StringType(),False),
    StructField("DEST_COUNTRY_NAME",StringType(),False),
    StructField("ct",LongType(),False)
])
df = spark.read    .schema(custom_schema)    .json("D:\\code\\spark\\spark-basics\\data\\flight-data\\json\\2010-summary.json")
df.show(2)


# %%



