# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
import findspark
findspark.init()


# %%
import pyspark
from pyspark.sql import SparkSession


# %%
spark = SparkSession.builder.getOrCreate()


# %%
df = spark.read.csv("D:\\code\\spark\\spark-basics\\data\\flight-data\\csv\\2010-summary.csv")


# %%
df.printSchema()


# %%
df.show(2)


# %%
df2 = spark.read.format("csv").load("D:\\code\\spark\\spark-basics\\data\\flight-data\\csv\\2010-summary.csv")


# %%
df3 = spark.read.format("csv").option("path", "D:\\code\\spark\\spark-basics\\data\\flight-data\\csv\\2010-summary.csv").load()


# %%
df = spark.read    .option("header", "true")    .csv("D:\\code\\spark\\spark-basics\\data\\flight-data\\csv\\2010-summary.csv")
df.show(5)


# %%
df = spark.read    .option("header", "true")    .csv("D:\\code\\spark\\spark-basics\\data\\flight-data\\csv")
df.count()


# %%
df = spark.read    .option("header", "true")    .csv("D:\\code\\spark\\spark-basics\\data\\flight-data\\csv\\2010-summary.csv")
df.count()


# %%
df.printSchema()


# %%
df = spark.read    .option("header", "true")    .option("inferSchema", "true")    .csv("D:\\code\\spark\\spark-basics\\data\\flight-data\\csv\\2010-summary.csv")
df.printSchema()


# %%
from pyspark.sql.types import StructField, StructType, StringType,LongType
custom_schema = StructType([
    StructField("destination", StringType(), True),
    StructField("source", StringType(), True),
    StructField("total_flights", LongType(), True),
])
df = spark.read.format("csv")     .schema(custom_schema)     .option("header", True)     .load("D:\\code\\spark\\spark-basics\\data\\flight-data\\csv\\2010-summary.csv")
df.show(2)


# %%
df = spark.read    .option("header", "true")    .option("inferSchema", "true")    .option("sep", "|")     .csv("D:\\code\\spark\\spark-basics\\data\\flight-data\\csv\\2010-summary.csv")


# %%
df = spark.read    .option("header", "true")    .option("inferSchema", "true")    .csv("D:\\code\\spark\\spark-basics\\data\\flight-data\\csv\\sample_data.csv")
df.show()


# %%
df = spark.read    .option("header", "true")    .option("inferSchema", "true")    .option("escapeQuotes", "true")    .csv("D:\\code\\spark\\spark-basics\\data\\flight-data\\csv\\sample_data.csv")
df.show()


# %%
df = spark.read    .option("header", "true")    .option("inferSchema", "true")    .option("sep", "\n")    .csv("D:\\code\\spark\\spark-basics\\data\\flight-data\\csv\\sample_data1.csv")
df.show()


# %%
df2 = df.withColumnRenamed("col1,col2", "data")


# %%
df2.show()


# %%
from pyspark.sql.functions import expr,col,split
df3 = df2.select(split("data", ","))


# %%
df2.select(split("data", "\(")[1]).show()


# %%



