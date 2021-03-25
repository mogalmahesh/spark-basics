# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
import findspark
findspark.init()


# %%
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Parquet and ORC").getOrCreate()


# %%
df = spark.read    .parquet("D:\\code\\spark\\spark-basics\\data\\flight-data\\parquet\\2010-summary.parquet")
df.printSchema()


# %%
df.show(3)


# %%
df2 = spark.read    .format("parquet")    .load("D:\\code\\spark\\spark-basics\\data\\flight-data\\parquet\\2010-summary.parquet")
df2.count()


# %%
df3 = spark.read    .format("parquet")    .option("path","D:\\code\\spark\\spark-basics\\data\\flight-data\\parquet\\2010-summary.parquet")    .load()
df3.count()


# %%
df = spark.read    .option("compression", "gzip")    .parquet("D:\\code\\spark\\spark-basics\\data\\flight-data\\parquet\\2010-summary.parquet")
df.printSchema()


# %%
df.show()


# %%
orc_df = spark.read    .orc("D:\\code\\spark\\spark-basics\\data\\flight-data\\orc\\2010-summary.orc")


# %%
orc_df2 = spark.read    .format("orc")    .option("path","D:\\code\\spark\\spark-basics\\data\\flight-data\\orc\\2010-summary.orc")    .load()
orc_df2.printSchema()


# %%



