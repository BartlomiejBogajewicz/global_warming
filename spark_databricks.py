# Databricks notebook source
dbutils.fs.mount(
  source = "wasbs://data@rdmount.blob.core.windows.net",
  mount_point = "/mnt/data",
  extra_configs = {"fs.azure.account.key.rdmount.blob.core.windows.net":"3NUva/4mVjCmEJYong0xiBGCUDNyH4mKcZMl3uZb8iTT8ApPyZzHBs8A4+03RTgpmU5g06+bA99mfwuBh/b/mQ=="})

# COMMAND ----------

from pyspark.sql.functions import col, mean, stddev, udf,round
from pyspark.sql.types import FloatType

spark_sql = spark.read.option("header" , True).option("inferSchema" , True).csv("/mnt/data/city_temperature.csv")

# COMMAND ----------

@udf(returnType=FloatType())
def farenheit_to_cel(temp : "temp_in_faren") -> "temp_in_cel":
    return (temp -32) * 0.56


# COMMAND ----------

spark_sql = spark_sql.where((col("AvgTemperature") != -99.0 ) & (col("Year") != 2020)).select("Country","Year",farenheit_to_cel(col("AvgTemperature")).alias("AvgTemperature"))
spark_agg = spark_sql.groupBy("Country","Year").agg(round(mean("AvgTemperature"),2).alias("Average_Temperature")\
                                                    ,round(stddev("AvgTemperature"),2).alias("Standard_deviation"))

spark_agg.write.parquet("/mnt/data/processed_data.parquet")

