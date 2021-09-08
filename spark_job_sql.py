import findspark
findspark.init()
import json
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType
from pyspark.sql.functions import col, mean, stddev, udf,round


@udf(returnType=FloatType())
def farenheit_to_cel(temp : "temp_in_faren") -> "temp_in_cel":
    return (temp -32) * 0.56

with open('jdbc_conn.json', 'r') as jdbc_conn:
    properites = json.load(jdbc_conn)

os.environ['PYSPARK_SUBMIT_ARGS'] = properites["path"]
spark_sql = SparkSession.builder.appName("data processing").getOrCreate()

spark_temp = spark_sql.read.csv('city_temperature.csv',header = True)

spark_temp = spark_temp.withColumn("AvgTemperature",spark_temp.AvgTemperature.cast(FloatType()))

spark_temp = spark_temp.where((col("AvgTemperature") != -99.0 ) & (col("Year") != 2020))\
                       .select("Country","Year",farenheit_to_cel(col("AvgTemperature")).alias("AvgTemperature"))

spark_agg = spark_temp.groupBy("Country","Year").agg(round(mean("AvgTemperature"),2).alias("Average Temperature")\
                                                    ,round(stddev("AvgTemperature"),2).alias("Standard deviation"))                                                 ,round(stddev("AvgTemperature"),2).alias("Standard deviation"))

spark_agg.write.mode("overwrite").format("jdbc")\
                    .option("url",properites["url"])\
                    .option("driver",properites["driver"])\
                    .option("user",properites["user"])\
                    .option("password",properites["password"])\
                    .option("dbtable","TEMPERATURE_SCHEMA.RPT_MEASUREMENTS").save()


spark_agg = spark_agg.collect()

with open('temp_data.json','w') as json_file:
    json.dump(spark_agg,json_file)



